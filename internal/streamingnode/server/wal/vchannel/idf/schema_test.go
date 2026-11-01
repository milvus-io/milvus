package idf

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func fieldTestSchema(version int32, ids ...int64) *schemapb.CollectionSchema {
	schema := &schemapb.CollectionSchema{Version: version}
	for _, id := range ids {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: id - 1, Name: fmt.Sprintf("text_%d", id), DataType: schemapb.DataType_VarChar}, &schemapb.FieldSchema{FieldID: id, Name: fmt.Sprintf("sparse_%d", id), DataType: schemapb.DataType_SparseFloatVector})
		schema.Functions = append(schema.Functions, &schemapb.FunctionSchema{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{id - 1}, OutputFieldIds: []int64{id}})
	}
	return schema
}

func prepareTestView(t *testing.T, r *Runtime, view walview.VChannelWALView) (*viewBinding, error) {
	t.Helper()
	if view.CaptureQueryView == nil {
		view.CaptureQueryView = func(_ *viewpb.QueryViewMeta, capture func(walview.VChannelWALView)) bool { capture(view); return true }
	}
	if err := r.Prepare(t.Context(), view); err != nil {
		return nil, err
	}
	b := r.BindView(&viewpb.QueryViewMeta{CollectionId: view.CollectionID, SchemaVersion: view.Schema.GetVersion(), LoadInfoVersion: view.LoadInfoVersion, Version: &viewpb.QueryViewVersion{DataVersion: view.SegmentSnapshot.DataVersion.IntoProto()}}).(*viewBinding)
	return b, b.Prepare(t.Context(), nil)
}

func TestAddFieldSharesExistingStatisticsAndReclaimsLastOwner(t *testing.T) {
	r := &Runtime{provider: NewProvider(&mocks.MockMixCoordClient{})}
	defer r.Close()
	version := qviews.DataVersion{StreamingVersion: 10}
	current := walview.VChannelWALView{CollectionID: 1, VChannel: "v1", Schema: fieldTestSchema(1, 102), PartitionIDs: []int64{10}, BaseGrowingTimeTick: 10, SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: version}}
	current.CaptureQueryView = func(meta *viewpb.QueryViewMeta, capture func(walview.VChannelWALView)) bool {
		next := current
		next.Schema = proto.Clone(current.Schema).(*schemapb.CollectionSchema)
		next.Schema.Version = meta.GetSchemaVersion()
		capture(next)
		return true
	}
	reads := map[int64]int{}
	mockey.PatchConvey("reuse actual loaded statistics and single event application", t, func() {
		mockey.Mock((*Provider).getSealedBM25Resources).Return([]*datapb.StreamingNodeBM25Resource{{SegmentId: 30, PartitionId: 10}}, nil).Build()
		mockey.Mock((*Provider).loadSealedContributions).To(func(_ *Provider, _ context.Context, _ map[int64]*datapb.StreamingNodeBM25Resource, fields bm25Stats) (bm25Stats, error) {
			result := make(bm25Stats)
			for field := range fields {
				reads[field]++
				stats := storage.NewBM25Stats()
				stats.Append(map[uint32]float32{7: 2})
				result[field] = stats
			}
			return result, nil
		}).Build()
		first, err := prepareTestView(t, r, current)
		require.NoError(t, err)
		old := first.fields[0]
		bind := func(schema *schemapb.CollectionSchema, load uint64) *viewBinding {
			current.Schema = schema
			b := r.BindView(&viewpb.QueryViewMeta{SchemaVersion: schema.Version, LoadInfoVersion: load, Version: &viewpb.QueryViewVersion{DataVersion: version.IntoProto()}}).(*viewBinding)
			require.NoError(t, b.Prepare(t.Context(), nil))
			return b
		}
		ordinary := fieldTestSchema(2, 102)
		ordinary.Fields = append(ordinary.Fields, &schemapb.FieldSchema{FieldID: 200, DataType: schemapb.DataType_Int64})
		second := bind(ordinary, 20)
		require.Same(t, old, second.fields[0])
		require.Equal(t, 1, reads[int64(102)])
		r.ApplyLiveEvent(t.Context(), walview.VChannelResourceEvent{Message: bm25Insert(t, 20, 4, 11)})
		require.EqualValues(t, 2, old.oracle.currentStats[102].NumRow(), "multiple View owners must not duplicate a WAL insert")
		third := bind(fieldTestSchema(3, 102, 104), 30)
		require.Same(t, old, third.fields[0])
		require.Equal(t, 1, reads[int64(102)])
		require.Equal(t, 1, reads[int64(104)])
		first.Release()
		second.Release()
		require.False(t, old.oracle.closed)
		require.Len(t, r.fields, 2)
		third.Release()
		require.True(t, old.oracle.closed)
		require.Empty(t, r.fields)
		recreated := bind(fieldTestSchema(3, 102), 30)
		require.NotSame(t, old, recreated.fields[0])
		require.Equal(t, 2, reads[int64(102)])
		recreated.Release()
		require.Empty(t, r.fields)
	})
}

func TestFieldSharingRequiresSamePartitionScopeAndSemantics(t *testing.T) {
	r := &Runtime{provider: NewProvider(&mocks.MockMixCoordClient{})}
	defer r.Close()
	current := walview.VChannelWALView{Schema: fieldTestSchema(1, 102), PartitionIDs: []int64{2, 1}, SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: qviews.DataVersion{StreamingVersion: 10}}}
	current.CaptureQueryView = func(_ *viewpb.QueryViewMeta, fn func(walview.VChannelWALView)) bool { fn(current); return true }
	patch := mockey.Mock((*Provider).getSealedBM25Resources).Return(nil, nil).Build()
	defer patch.UnPatch()
	a, err := prepareTestView(t, r, current)
	require.NoError(t, err)
	bind := func() *viewBinding {
		b := r.BindView(&viewpb.QueryViewMeta{SchemaVersion: current.Schema.Version, Version: &viewpb.QueryViewVersion{DataVersion: current.SegmentSnapshot.DataVersion.IntoProto()}}).(*viewBinding)
		require.NoError(t, b.Prepare(t.Context(), nil))
		return b
	}
	current.PartitionIDs = []int64{1, 2}
	b := bind()
	require.Same(t, a.fields[0], b.fields[0])
	current.PartitionIDs = []int64{1}
	c := bind()
	require.NotSame(t, a.fields[0], c.fields[0])
	current.Schema = fieldTestSchema(2, 102)
	current.Schema.Functions[0].Name = "different-semantics"
	d := bind()
	require.NotSame(t, c.fields[0], d.fields[0])
	a.Release()
	b.Release()
	c.Release()
	d.Release()
	require.Empty(t, r.fields)
}

func TestFieldPreparationCancellationUnblocksFullEventQueue(t *testing.T) {
	r := &Runtime{provider: NewProvider(&mocks.MockMixCoordClient{})}
	defer r.Close()
	version := qviews.DataVersion{StreamingVersion: 10}
	view := walview.VChannelWALView{Schema: fieldTestSchema(1, 102), SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: version}}
	view.CaptureQueryView = func(_ *viewpb.QueryViewMeta, capture func(walview.VChannelWALView)) bool { capture(view); return true }
	require.NoError(t, r.Prepare(t.Context(), view))
	binding := r.BindView(&viewpb.QueryViewMeta{SchemaVersion: 1, Version: &viewpb.QueryViewVersion{DataVersion: version.IntoProto()}}).(*viewBinding)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reading := make(chan struct{})
	patch := mockey.Mock((*Provider).getSealedBM25Resources).To(func(_ *Provider, ctx context.Context, _ int64, _ string, _ qviews.DataVersion, _ []int64, _ uint64) ([]*datapb.StreamingNodeBM25Resource, error) {
		close(reading)
		<-ctx.Done()
		return nil, ctx.Err()
	}).Build()
	defer patch.UnPatch()
	done := make(chan error, 1)
	go func() { done <- binding.Prepare(ctx, nil) }()
	<-reading
	event := walview.VChannelResourceEvent{SegmentSealed: &walview.SegmentSealedEvent{SegmentID: 20, SealedAtDataVersion: qviews.DataVersion{StreamingVersion: 11}}}
	for i := 0; i < 1024; i++ {
		r.ApplyLiveEvent(t.Context(), event)
	}
	observed := make(chan struct{})
	go func() { r.ApplyLiveEvent(t.Context(), event); close(observed) }()
	cancel()
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("canceled preparation blocked WAL")
	}
	require.ErrorIs(t, <-done, context.Canceled)
	binding.Release()
	require.Empty(t, r.fields)
}

func TestFieldPreparationUsesSnapshotTailWithoutDuplication(t *testing.T) {
	r := &Runtime{provider: NewProvider(&mocks.MockMixCoordClient{})}
	defer r.Close()
	view := walview.VChannelWALView{CollectionID: 1, Schema: fieldTestSchema(1, 102), BaseGrowingTimeTick: 10, SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: qviews.DataVersion{StreamingVersion: 10}, Segments: []walview.VisibleSegment{{SegmentID: 20, PartitionID: 10, Data: walview.SegmentSnapshotData{InsertMessages: []message.ImmutableMessage{bm25Insert(t, 20, 4)}}}}}}
	patch := mockey.Mock((*Provider).getSealedBM25Resources).To(func(*Provider, context.Context, int64, string, qviews.DataVersion, []int64, uint64) ([]*datapb.StreamingNodeBM25Resource, error) {
		r.ApplyLiveEvent(t.Context(), walview.VChannelResourceEvent{Message: bm25Insert(t, 20, 4)})
		r.ApplyLiveEvent(t.Context(), walview.VChannelResourceEvent{Message: bm25Insert(t, 20, 6, 11)})
		return nil, nil
	}).Build()
	defer patch.UnPatch()
	binding, err := prepareTestView(t, r, view)
	require.NoError(t, err)
	require.EqualValues(t, 2, binding.fields[0].oracle.currentStats[102].NumRow())
	// The dispatcher can also deliver snapshot-covered events after publication.
	r.ApplyLiveEvent(t.Context(), walview.VChannelResourceEvent{Message: bm25Insert(t, 20, 4)})
	require.EqualValues(t, 2, binding.fields[0].oracle.currentStats[102].NumRow())
	binding.Release()
}

// Ensure field scope comes from required fields, not preload hints.
var _ queryresource.QueryViewResource = (*viewBinding)(nil)

func TestNewFieldWaitsForSharedFieldSnapshotPrefix(t *testing.T) {
	r := &Runtime{provider: NewProvider(&mocks.MockMixCoordClient{})}
	defer r.Close()
	current := walview.VChannelWALView{CollectionID: 1, Schema: fieldTestSchema(1, 102), BaseGrowingTimeTick: 10, SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: qviews.DataVersion{StreamingVersion: 10}}}
	current.CaptureQueryView = func(_ *viewpb.QueryViewMeta, capture func(walview.VChannelWALView)) bool {
		capture(current)
		return true
	}
	patch := mockey.Mock((*Provider).getSealedBM25Resources).Return(nil, nil).Build()
	defer patch.UnPatch()
	first, err := prepareTestView(t, r, current)
	require.NoError(t, err)
	defer first.Release()
	original := message.MustAsImmutableInsertMessageV1(bm25Insert(t, 20, 4, 11))
	body := proto.Clone(original.MustBody()).(*msgpb.InsertRequest)
	extra := proto.Clone(body.FieldsData[1]).(*schemapb.FieldData)
	extra.FieldId = 104
	body.FieldsData = append(body.FieldsData, extra)
	event := message.NewInsertMessageBuilderV1().WithVChannel("v1").WithHeader(original.Header()).WithBody(body).MustBuildMutable().WithTimeTick(11).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(11))
	current.Schema = fieldTestSchema(2, 102, 104)
	current.BaseGrowingTimeTick = 11
	current.SegmentSnapshot.Segments = []walview.VisibleSegment{{SegmentID: 20, PartitionID: 10, Schema: current.Schema, Data: walview.SegmentSnapshotData{InsertMessages: []message.ImmutableMessage{event}}}}
	binding := r.BindView(&viewpb.QueryViewMeta{SchemaVersion: 2, Version: &viewpb.QueryViewVersion{DataVersion: current.SegmentSnapshot.DataVersion.IntoProto()}}).(*viewBinding)
	defer binding.Release()
	barriers := 0
	r.view.ResourceEventBarrier = func(context.Context) error {
		barriers++
		require.False(t, binding.resolved)
		require.Zero(t, first.fields[0].oracle.currentStats[102].NumRow())
		require.EqualValues(t, 1, binding.fields[1].oracle.currentStats[104].NumRow())
		// Deliver the delayed event to the old field. The new field already covered it.
		r.ApplyLiveEvent(t.Context(), walview.VChannelResourceEvent{Message: event})
		return nil
	}
	require.NoError(t, binding.Prepare(t.Context(), nil))
	require.Equal(t, 1, barriers)
	require.Same(t, first.fields[0], binding.fields[0])
	require.EqualValues(t, 1, binding.fields[0].oracle.currentStats[102].NumRow())
	require.EqualValues(t, 1, binding.fields[1].oracle.currentStats[104].NumRow())
	_, err = binding.BuildIDFBatch(t.Context(), []queryresource.IDFRequest{{FieldID: 102}, {FieldID: 104}})
	require.NoError(t, err)
}

func TestViewBindingUsesRequiredFieldAndPartitionScope(t *testing.T) {
	client := &mocks.MockMixCoordClient{}
	future := syncutil.NewFuture[types.MixCoordClient]()
	future.Set(client)
	loadInfo := queryresource.NewFutureLoadInfoProvider(future)
	r := &Runtime{provider: NewProvider(client)}
	defer r.Close()
	view := walview.VChannelWALView{CollectionID: 1, Schema: fieldTestSchema(1, 102, 104), SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: qviews.DataVersion{StreamingVersion: 10}}}
	view.CaptureQueryView = func(_ *viewpb.QueryViewMeta, capture func(walview.VChannelWALView)) bool { capture(view); return true }
	require.NoError(t, r.Prepare(t.Context(), view))
	binding := r.BindView(&viewpb.QueryViewMeta{SchemaVersion: 1, LoadInfoVersion: 42, Version: &viewpb.QueryViewVersion{DataVersion: view.SegmentSnapshot.DataVersion.IntoProto()}}).(*viewBinding)
	defer binding.Release()
	fail := true
	rpc := mockey.Mock((*mocks.MockMixCoordClient).GetQueryViewLoadInfo).To(func(_ *mocks.MockMixCoordClient, _ context.Context, req *querypb.GetQueryViewLoadInfoRequest, _ ...grpc.CallOption) (*querypb.GetQueryViewLoadInfoResponse, error) {
		require.EqualValues(t, 42, req.GetVersion())
		if fail {
			return nil, merr.WrapErrServiceNotReadyMsg("load info pending")
		}
		return &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success(), CollectionID: 1, PartitionIDs: []int64{10}, RequiredFields: []*viewpb.LoadFieldConfig{{FieldId: 102}}, LoadFields: []*viewpb.LoadFieldConfig{{FieldId: 104}}}, nil
	}).Build()
	defer rpc.UnPatch()
	resources := mockey.Mock((*Provider).getSealedBM25Resources).To(func(_ *Provider, _ context.Context, _ int64, _ string, _ qviews.DataVersion, partitions []int64, _ uint64) ([]*datapb.StreamingNodeBM25Resource, error) {
		require.Equal(t, []int64{10}, partitions)
		return nil, nil
	}).Build()
	defer resources.UnPatch()
	require.ErrorIs(t, binding.Prepare(t.Context(), loadInfo), merr.ErrServiceNotReady)
	require.Empty(t, r.fields)
	fail = false
	require.NoError(t, binding.Prepare(t.Context(), loadInfo))
	require.Len(t, binding.fields, 1)
	require.EqualValues(t, 102, binding.fields[0].key.fieldID)
	require.Equal(t, "[10]", binding.fields[0].key.scope)
	_, err := binding.BuildIDFBatch(t.Context(), []queryresource.IDFRequest{{FieldID: 104}})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestHybridBatchAlignsSharedFieldVersions(t *testing.T) {
	r := &Runtime{provider: NewProvider(&mocks.MockMixCoordClient{})}
	defer r.Close()
	patch := mockey.Mock((*Provider).getSealedBM25Resources).Return(nil, nil).Build()
	defer patch.UnPatch()
	binding, err := prepareTestView(t, r, walview.VChannelWALView{Schema: fieldTestSchema(1, 102, 104), SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: qviews.DataVersion{StreamingVersion: 10}}})
	require.NoError(t, err)
	defer binding.Release()
	target := qviews.DataVersion{StreamingVersion: 11}
	require.NoError(t, binding.fields[0].oracle.PrepareDataVersion(t.Context(), target))
	// A different View may have advanced one shared field. Never return a hybrid
	// result while its companion is still on the old DataView.
	binding.fields[1].oracle.advanceMu.Lock()
	_, err = binding.BuildIDFBatch(t.Context(), []queryresource.IDFRequest{{FieldID: 102}, {FieldID: 104}})
	binding.fields[1].oracle.advanceMu.Unlock()
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	results, err := binding.BuildIDFBatch(t.Context(), []queryresource.IDFRequest{{FieldID: 102}, {FieldID: 104}})
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, target, binding.fields[1].oracle.currentVersion)
}
