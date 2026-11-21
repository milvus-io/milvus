//go:build dynamic && test

package qvresource

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	qnexecutor "github.com/milvus-io/milvus/internal/querynodev2/viewquery"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Only source delivery and View metadata are supplied by the fixture. Each
// registration replays the complete ordered suffix into its own physical base.
type retentionSource struct {
	fakeQVCollectionRuntime
	info *querypb.SegmentLoadInfo
}

func (s *retentionSource) Release() {}
func (s *retentionSource) Close()   {}
func (s *retentionSource) Acquire(context.Context, *qviews.QueryViewAtQueryNode) (qnview.CollectionRuntimeGuard, bool, error) {
	return s, false, nil
}
func (s *retentionSource) GetQueryViewSegmentLoadInfo(context.Context, int64, int64) (qnview.SegmentLoadInfoSnapshot, error) {
	return qnview.SegmentLoadInfoSnapshot{CollectionID: 1, SegmentID: 20, LoadInfo: s.info}, nil
}
func (s *retentionSource) Bind(*qviews.QueryViewAtQueryNode) (qnview.TransformLogGuard, error) {
	return s, nil
}
func (s *retentionSource) Registrar() qnview.TransformSegmentRegistrar { return s }
func (s *retentionSource) Prepare(context.Context) (bool, error)       { return false, nil }
func (s *retentionSource) WaitTransformVisible(ctx context.Context, tick uint64) error {
	if tick > 50 {
		return merr.WrapErrServiceNotReadyMsg("test source only delivered through 50")
	}
	return ctx.Err()
}
func (s *retentionSource) RegisterSegment(ctx context.Context, seg qnview.TransformSegment) (qnview.TransformRegistration, error) {
	for _, tick := range []uint64{20, 30, 50} {
		value := &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: int64(tick) * 10}}
		body := &messagespb.UpdateMessageBody{
			PartitionId: 10,
			PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
			Assignments: []*messagespb.UpdateAssignment{{
				FieldId: 101,
				Literal: &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ScalarValue{ScalarValue: value}},
			}},
		}
		update := &streamingpb.TransformMutationBlock{Mutation: &streamingpb.TransformMutationBlock_Update{Update: body}}
		err := seg.ApplyTransform(ctx, &streamingpb.TransformLogEntry{
			TimeTick: tick,
			Entry: &streamingpb.TransformLogEntry_Mutations{Mutations: &streamingpb.TransformMutationEntry{
				Blocks: []*streamingpb.TransformMutationBlock{update},
			}},
		})
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}
func (s *retentionSource) WaitCatchup(context.Context) error { return nil }
func (s *retentionSource) OnFailure(func(error))             {}
func (s *retentionSource) Unregister()                       {}

type retentionProvider struct {
	viewquery.TaskProvider
	owner *qnview.SegmentResourceManager
	view  *viewpb.QueryViewOfQueryNode
}

func (p retentionProvider) AcquireQuerySegmentTasks(ctx context.Context, shard qviews.ShardID, version qviews.QueryViewVersion, mvcc *viewpb.QueryPlanMVCC, req *internalpb.RetrieveRequest, _ *viewpb.SegmentSelection) (viewquery.QuerySegmentTasks, error) {
	handles, err := p.owner.AcquireSealedSegmentHandles(ctx, qviews.QueryViewKey{ShardID: shard, QueryViewVersion: version}, p.view, mvcc.GetTransformingTimetick())
	if err != nil {
		return nil, err
	}
	return qnview.NewQNQuerySegmentTasks([]qnview.QNQuerySegmentTask{{Handle: handles[0], Request: req, MVCC: mvcc}}), nil
}

func TestSealedViewRetentionAndOriginalManifestReplay(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	paramtable.Init()
	pt := paramtable.Get()
	for key, value := range map[string]string{pt.CommonCfg.StorageType.Key: "local", pt.LocalStorageCfg.Path.Key: root, pt.CommonCfg.BloomFilterEnabled.Key: "false"} {
		require.NoError(t, pt.Save(key, value))
		t.Cleanup(func() { pt.Reset(key) })
	}
	initcore.InitExecExpressionFunctionFactory()
	require.NoError(t, initcore.InitLocalChunkManager(root))
	require.NoError(t, initcore.InitMmapManager(pt, 1))
	require.NoError(t, initcore.InitTieredStorage(pt))
	require.NoError(t, initcore.InitStorageV2FileSystem(pt))
	schema := typeutil.AppendSystemFields(&schemapb.CollectionSchema{
		Name: "retention",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "score", DataType: schemapb.DataType_Int64},
		},
	})
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	cm := storage.NewLocalChunkManager()
	writer, err := storage.NewBinlogRecordWriter(ctx, 1, 10, 20, schema,
		allocator.NewLocalAllocator(100, 1000), 1<<20, 1,
		storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(cfg), storage.WithUploader(cm.MultiWrite))
	require.NoError(t, err)
	record, err := storage.ValueSerializer([]*storage.Value{{Value: map[int64]any{0: int64(8), 1: int64(1), 100: int64(1), 101: int64(2)}}}, schema)
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	record.Release()
	require.NoError(t, writer.Close())
	logs, _, _, manifest, _ := writer.GetLogs()
	_, rev, err := packed.UnmarshalManifestPath(manifest)
	require.NoError(t, err)
	c, err := segments.NewCollection(1, schema, nil, &querypb.LoadMetaInfo{CollectionID: 1, PartitionIDs: []int64{10}})
	require.NoError(t, err)
	defer segments.DeleteCollection(c)
	src := &retentionSource{
		fakeQVCollectionRuntime: fakeQVCollectionRuntime{collection: c, schema: schema, collectionID: 1},
		info: &querypb.SegmentLoadInfo{
			CollectionID: 1, PartitionID: 10, SegmentID: 20, Level: datapb.SegmentLevel_L1,
			NumOfRows: 1, InsertChannel: "by-dev-rootcoord-dml_0_1v0", StorageVersion: storage.StorageV3,
			ManifestPath: manifest, BinlogPaths: storage.SortFieldBinlogs(logs), TransformStartAfterTimetick: 10,
		},
	}
	workers := nodescheduler.New(2)
	defer workers.Close()
	manager := segments.NewManager()
	physical := NewQueryViewPhysicalSegmentLoader(manager, segments.NewLoader(ctx, manager, cm))
	owner := qnview.NewSegmentResourceManager(ctx, workers, physical, nil, src, src, src)
	defer owner.Close()
	view := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{
		PartitionId: 10, SegmentIds: []int64{20}, SegmentManifestVersions: []int64{rev},
	}}}
	bind := func(version int64, floor uint64) qviews.QueryViewKey {
		meta := &viewpb.QueryViewMeta{
			CollectionId: 1, Vchannel: src.info.InsertChannel,
			Version:            (&qviews.QueryViewVersion{QueryVersion: version}).IntoProto(),
			ReadAdmissionFloor: &viewpb.QueryPlanMVCC{TransformingTimetick: floor},
		}
		key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
		ready := make(chan bool, 1)
		owner.Acquire(qnview.AcquireSegments{Key: key, View: view, Meta: meta, OnReady: func(map[int64][]int64) { ready <- true }, OnUnrecoverable: func() { ready <- false }})
		select {
		case ok := <-ready:
			require.True(t, ok)
		case <-time.After(5 * time.Second):
			t.Fatal("preparation timed out")
		}
		return key
	}
	acquire := func(key qviews.QueryViewKey, ts uint64) qnview.SealedSegmentHandle {
		hs, err := owner.AcquireSealedSegmentHandles(ctx, key, view, ts)
		require.NoError(t, err)
		require.Len(t, hs, 1)
		return hs[0]
	}
	read := func(h qnview.SealedSegmentHandle, ts uint64, want int64) {
		encoded, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Limit: 1}}, OutputFieldIds: []int64{101}})
		require.NoError(t, err)
		plan, err := segcore.NewRetrievePlan(c.GetCCollection(), encoded, ts, 100, 0, 0, 0)
		require.NoError(t, err)
		defer plan.Delete()
		result, err := h.Segment().(qnview.ReadableSealedSegment).QuerySegment().RetrieveByOffsets(ctx, &segcore.RetrievePlanWithOffsets{RetrievePlan: plan, Offsets: []int64{0}})
		require.NoError(t, err)
		require.Equal(t, []int64{want}, result.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	}
	old := bind(1, 20)
	newView := bind(2, 40)
	held := acquire(newView, 50)
	defer held.Release()
	count := func() uint64 {
		n, err := segments.SealedReadVersionCountForTest(held.Segment().(qnview.ReadableSealedSegment).QuerySegment())
		require.NoError(t, err)
		return n
	}
	require.EqualValues(t, 3, count())
	latePhaseTwo := acquire(old, 25)
	read(latePhaseTwo, 25, 200)
	// Exercise the actual service scheduler, output conversion, and release
	// defer. The caller can cancel while the Arrow consumer still owns its
	// selection and native read requirement.
	server := viewquery.NewServer(retentionProvider{owner: owner, view: view}, qnexecutor.NewScheduler(qnexecutor.NewDirectSegmentTaskExecutor(1)))
	serialized, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Limit: 1}}, OutputFieldIds: []int64{100, 101}})
	require.NoError(t, err)
	req := &viewpb.QueryOnViewRequest{
		ShardId: old.ShardID.IntoProto(), Version: old.QueryViewVersion.IntoProto(),
		Mvcc: &viewpb.QueryPlanMVCC{TransformingTimetick: 25},
		LegacyReq: &internalpb.RetrieveRequest{
			CollectionID: 1, Limit: 1, OutputFieldsId: []int64{100, 101}, SerializedExprPlan: serialized,
		},
	}
	zeroCopy := pt.CommonCfg.InterfaceZeroCopyEnabled.Key
	defer pt.Reset(zeroCopy)
	require.NoError(t, pt.Save(zeroCopy, "false"))
	pbResult, err := server.QueryOnView(ctx, req)
	require.NoError(t, err)
	score := func(result *viewpb.QueryOnViewResponse) []int64 {
		for _, field := range result.GetLegacyResults().GetFieldsData() {
			if field.GetFieldId() == 101 {
				return field.GetScalars().GetLongData().GetData()
			}
		}
		return nil
	}
	require.Equal(t, []int64{200}, score(pbResult))
	require.NoError(t, pt.Save(zeroCopy, "true"))
	converting, resume := make(chan bool, 1), make(chan struct{})
	var materialize func(*segcorepb.RetrieveResults, *queryutil.ArrowSelection, *schemapb.CollectionSchema) error
	patch := mockey.Mock(queryutil.MaterializeArrowSelection).Origin(&materialize).To(func(header *segcorepb.RetrieveResults, sel *queryutil.ArrowSelection, schema *schemapb.CollectionSchema) error {
		converting <- !sel.Empty()
		<-resume
		return materialize(header, sel, schema)
	}).Build()
	defer patch.UnPatch()
	resumeOnce := sync.OnceFunc(func() { close(resume) })
	defer resumeOnce()
	caller, cancel := context.WithCancel(ctx)
	defer cancel()
	finished := make(chan struct{})
	var arrowResult *viewpb.QueryOnViewResponse
	var arrowErr error
	go func() {
		arrowResult, arrowErr = server.QueryOnView(caller, req)
		close(finished)
	}()
	select {
	case actualArrow := <-converting:
		require.True(t, actualArrow)
	case <-finished:
		t.Fatalf("retrieve finished before materialization: %v", arrowErr)
	case <-time.After(3 * time.Second):
		t.Fatal("missing conversion")
	}
	cancel()
	owner.Release(qnview.ReleaseSegments{Key: old})
	acquire(newView, 50).Release()
	read(latePhaseTwo, 25, 200)
	latePhaseTwo.Release()
	acquire(newView, 50).Release()
	require.EqualValues(t, 3, count(), "conversion pins the executing T after View withdrawal")
	select {
	case <-finished:
		t.Fatal("cancellation returned before Arrow consumer drained")
	default:
	}
	resumeOnce()
	<-finished
	require.NoError(t, arrowErr)
	require.Equal(t, []int64{200}, score(arrowResult))
	acquire(newView, 50).Release()
	require.Eventually(t, func() bool { return count() == 2 }, 3*time.Second, time.Millisecond)
	read(held, 50, 500)
	predecessor := acquire(newView, 40)
	read(predecessor, 40, 300)
	predecessor.Release()
	// An old admitted snapshot requires a different base and a fresh replay.
	restored := bind(3, 20)
	restoredRead := acquire(restored, 25)
	read(restoredRead, 25, 200)
	require.EqualValues(t, 2, count())
	restoredRead.Release()
}
