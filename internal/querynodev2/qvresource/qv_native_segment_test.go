package qvresource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func (*lifetimeNativeSegment) Reopen(context.Context, *segcore.ReopenRequest) error { panic("mockey") }
func (*lifetimeNativeSegment) Delete(context.Context, *segcore.DeleteRequest) (*segcore.DeleteResult, error) {
	panic("mockey")
}

func TestNativeReopenPublishesOnlyOnSuccess(t *testing.T) {
	collections, initial := pinnedCollectionForTest(t)
	cfg := paramtable.Get()
	key := cfg.QueryNodeCfg.InternalCollectionUseTakeForOutput.Key
	previous := cfg.QueryNodeCfg.InternalCollectionUseTakeForOutput.GetValue()
	require.NoError(t, cfg.Save(key, "true"))
	t.Cleanup(func() { require.NoError(t, cfg.Save(key, previous)) })
	next := acquireRuntime(t, collections, 1)
	defer next.Release()
	native := &lifetimeNativeSegment{}
	patchCollectionLifetime(t, mockey.Mock(segcore.CreateCSegment).Return(native, nil).Build())
	patchCollectionLifetime(t, mockey.Mock((*lifetimeNativeSegment).Release).Return().Build())
	loader := realQVSegmentLoader{}
	info := &querypb.SegmentLoadInfo{CollectionID: 1, SegmentID: 10, NumOfRows: 100}
	loaded, err := loader.NewSegment(context.Background(), initial, info)
	require.NoError(t, err)
	defer loaded.Release(context.Background())
	local := loaded.(*qvLocalSegment)
	info.NumOfRows = 999
	require.EqualValues(t, 100, local.ReadView().LoadInfo.NumOfRows)
	require.True(t, local.ReadView().LoadInfo.UseTakeForOutput)
	require.False(t, info.UseTakeForOutput, "local policies must not mutate the metadata snapshot")
	original := local.collection
	update := &querypb.SegmentLoadInfo{CollectionID: 1, SegmentID: 10, NumOfRows: 200}
	reopenErr := merr.WrapErrServiceInternalMsg("injected reopen failure")
	var captured segcore.ReopenRequest
	patchCollectionLifetime(t, mockey.Mock((*lifetimeNativeSegment).Reopen).To(func(_ *lifetimeNativeSegment, _ context.Context, req *segcore.ReopenRequest) error {
		captured = *req
		captured.LoadInfo = proto.Clone(req.LoadInfo).(*querypb.SegmentLoadInfo)
		return reopenErr
	}).Build())
	refs := initial.runtime.refs
	require.ErrorIs(t, loader.ReopenSegment(context.Background(), local, next, update), reopenErr)
	require.Same(t, original, local.collection)
	require.EqualValues(t, 100, local.ReadView().LoadInfo.NumOfRows)
	require.Equal(t, refs, initial.runtime.refs, "failure must release the tentative collection reference")
	reopenErr = nil
	require.NoError(t, loader.ReopenSegment(context.Background(), local, next, update))
	require.NotSame(t, original, local.collection)
	require.Equal(t, refs, initial.runtime.refs, "success swaps, rather than leaks, the segment reference")
	require.EqualValues(t, next.SchemaVersion(), captured.SchemaVersion)
	require.Same(t, next.Schema(), captured.Schema)
	require.True(t, captured.LoadInfo.UseTakeForOutput)
	require.False(t, update.UseTakeForOutput)
	update.NumOfRows = 999
	require.EqualValues(t, 200, local.ReadView().LoadInfo.NumOfRows)
}

func TestNativeDeleteBaselineAndFailureWatermarks(t *testing.T) {
	paramtable.Init()
	native := &lifetimeNativeSegment{}
	local := &qvLocalSegment{segment: native}
	data := storage.NewDeltaData(2)
	require.NoError(t, data.Append(storage.NewInt64PrimaryKey(1), 100))
	require.NoError(t, data.Append(storage.NewInt64PrimaryKey(2), 80))
	failure := merr.WrapErrServiceInternalMsg("injected delete failure")
	currentError := failure
	baseline := mockey.Mock(segments.LoadSegmentDeletedRecords).To(func(context.Context, segcore.CSegment, *storage.DeltaData) error { return currentError }).Build()
	patchCollectionLifetime(t, baseline)
	live := mockey.Mock((*lifetimeNativeSegment).Delete).To(func(*lifetimeNativeSegment, context.Context, *segcore.DeleteRequest) (*segcore.DeleteResult, error) {
		return nil, currentError
	}).Build()
	patchCollectionLifetime(t, live)
	require.NoError(t, local.LoadDeltaData(context.Background(), storage.NewDeltaData(0)))
	require.ErrorIs(t, local.LoadDeltaData(context.Background(), data), failure)
	require.Zero(t, local.LastDeltaTimestamp())
	require.Zero(t, live.Times(), "baseline replay must never call live Delete")
	currentError = nil
	require.NoError(t, local.LoadDeltaData(context.Background(), data))
	require.EqualValues(t, 100, local.LastDeltaTimestamp(), "unsorted timestamps use their maximum")
	currentError = failure
	require.ErrorIs(t, local.Delete(context.Background(), data.DeletePks(), []uint64{200, 150}), failure)
	require.EqualValues(t, 100, local.LastDeltaTimestamp())
	currentError = nil
	require.NoError(t, local.Delete(context.Background(), data.DeletePks(), []uint64{200, 150}))
	require.EqualValues(t, 200, local.LastDeltaTimestamp())
	require.NoError(t, local.Delete(context.Background(), storage.NewInt64PrimaryKeys(0), nil))
	require.Equal(t, 2, live.Times())
}

// This exercises the real native load, query, persisted-delete and Reopen paths
// without creating any legacy collection/segment manager or LocalSegment.
func TestNativeQueryViewLoadAndQuery(t *testing.T) {
	paramtable.Init()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := paramtable.Get()
	root := t.Name()
	cm, err := storage.NewTestChunkManagerFactory(cfg, root).NewPersistentStorageChunkManager(ctx)
	require.NoError(t, err)
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())
	initcore.InitExecExpressionFunctionFactory()
	require.NoError(t, initcore.InitRemoteChunkManager(cfg))
	require.NoError(t, initcore.InitLocalChunkManager(t.TempDir()))
	require.NoError(t, initcore.InitMmapManager(cfg, 1))
	require.NoError(t, initcore.InitTieredStorage(cfg))
	schema := mock_segcore.GenTestCollectionSchema("native-qv", schemapb.DataType_Int64, false)
	schema.Version = 1
	meta := &fakeQVLoadMetadataProvider{collection: &milvuspb.DescribeCollectionResponse{Schema: schema, DbName: "native-db"}}
	collections := newQueryViewCollectionRuntimeManager(meta)
	guard := acquireRuntime(t, collections, 100)
	defer guard.Release()
	binlogs, statslogs, err := mock_segcore.SaveBinLog(ctx, 100, 10, 1001, 100, schema, cm)
	require.NoError(t, err)
	info := &querypb.SegmentLoadInfo{CollectionID: 100, PartitionID: 10, SegmentID: 1001, NumOfRows: 100, BinlogPaths: binlogs, Statslogs: statslogs, InsertChannel: "by-dev-rootcoord-dml_0_100v0"}
	reservation, err := newQueryViewSegmentResourceEstimator(segments.NewLoadResourceBudget(ctx)).Reserve(ctx, info, guard)
	require.NoError(t, err)
	defer reservation.Release()
	loader := realQVSegmentLoader{cm: cm}
	physical := newQueryViewPhysicalSegmentLoader(loader)
	transform, err := physical.Load(ctx, info, guard)
	require.NoError(t, err)
	defer transform.Release(ctx)
	reservation.Release()
	local := transform.(*queryViewTransformSegment).segment.(*qvLocalSegment)
	require.True(t, local.PkCandidateExist())
	require.EqualValues(t, 10, local.Partition())
	borrowedCollection, err := segments.NewCollectionFromCCollectionForViewQuery(guard.CCollection(), guard.DatabaseName())
	require.NoError(t, err)
	require.Equal(t, "native-db", borrowedCollection.GetDBName())
	borrowed := segments.NewSealedSegmentForViewQuery(local.ReadView().LoadInfo, local.segment, guard.DatabaseName())
	plan, err := mock_segcore.GenSimpleRetrievePlan(borrowedCollection.GetCCollection())
	require.NoError(t, err)
	defer plan.Delete()
	result, err := borrowed.Retrieve(ctx, plan)
	require.NoError(t, err)
	require.Contains(t, result.GetIds().GetIntId().GetData(), int64(1))
	data := storage.NewDeltaData(1)
	require.NoError(t, data.Append(storage.NewInt64PrimaryKey(1), 500))
	require.NoError(t, local.LoadDeltaData(ctx, data))
	require.NoError(t, loader.ReopenSegment(ctx, local, guard, info))
	result, err = borrowed.Retrieve(ctx, plan)
	require.NoError(t, err)
	require.NotContains(t, result.GetIds().GetIntId().GetData(), int64(1))
	require.Contains(t, result.GetIds().GetIntId().GetData(), int64(2))
	borrowed.Release(ctx) // A borrowed adapter must not release native resources.
	require.Positive(t, local.segment.RowNum())
}

func TestNativeLoadPolicyFailureReleasesTentativeGuard(t *testing.T) {
	_, guard := pinnedCollectionForTest(t)
	failure := merr.WrapErrServiceInternalMsg("invalid local load policy")
	patchCollectionLifetime(t, mockey.Mock(segments.PrepareSegmentLoadInfo).Return(failure).Build())
	loader := realQVSegmentLoader{}
	refs := guard.runtime.refs
	info := &querypb.SegmentLoadInfo{CollectionID: 1, SegmentID: 10}
	_, err := loader.NewSegment(context.Background(), guard, info)
	require.ErrorIs(t, err, failure)
	require.Equal(t, refs, guard.runtime.refs)
	local := &qvLocalSegment{collection: guard, info: info}
	require.ErrorIs(t, loader.ReopenSegment(context.Background(), local, guard, info), failure)
	require.Equal(t, refs, guard.runtime.refs)
	require.Same(t, info, local.info)
}

func TestPhysicalLoaderRejectsMissingAndForeignResources(t *testing.T) {
	loader := realQVSegmentLoader{}
	foreign := &fakeQVSegment{}
	info := &querypb.SegmentLoadInfo{}
	ctx := context.Background()
	physical := newQueryViewPhysicalSegmentLoader(loader)
	_, err := physical.Load(ctx, nil, fakeQVCollectionRuntime{})
	require.Error(t, err)
	_, err = physical.Load(ctx, info, nil)
	require.Error(t, err)
	_, err = loader.NewSegment(ctx, fakeQVCollectionRuntime{}, info)
	require.Error(t, err)
	require.Error(t, loader.LoadSegment(ctx, foreign, info))
	require.Error(t, loader.LoadDeltaLogs(ctx, foreign, info))
	require.Error(t, loader.LoadPKCandidate(ctx, foreign, info))
	require.Error(t, loader.ReopenSegment(ctx, foreign, fakeQVCollectionRuntime{}, info))
	require.Error(t, physical.Update(ctx, nil, nil, qnview.SegmentLoadInfoSnapshot{LoadInfo: info}, qnview.SegmentUpdateReopen))
	segment := newQueryViewTransformSegment(foreign, "v1", 0)
	require.Error(t, physical.Update(ctx, segment, nil, qnview.SegmentLoadInfoSnapshot{}, qnview.SegmentUpdateReopen))
}
