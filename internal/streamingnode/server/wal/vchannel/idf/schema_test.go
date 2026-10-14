package idf

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestSchemaRefreshInitializesDisabledIDFWithoutSnapshotOverlap(t *testing.T) {
	paramtable.Init()
	ctx := t.Context()
	schema := &schemapb.CollectionSchema{Version: 2, Fields: []*schemapb.FieldSchema{
		{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
		{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
	}, Functions: []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101}, OutputFieldIds: []int64{102}}}}
	version := qviews.DataVersion{StreamingVersion: 10}
	view := walview.VChannelWALView{
		CollectionID: 1, VChannel: "v1", Schema: schema, BaseGrowingTimeTick: 10,
		SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: version, Segments: []walview.VisibleSegment{{
			SegmentID: 20, PartitionID: 10, Data: walview.SegmentSnapshotData{InsertMessages: []message.ImmutableMessage{bm25Insert(t, 20, 4)}},
		}}},
	}
	cm := storage.NewLocalChunkManager()
	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{7: 2})
	data, err := stats.Serialize()
	require.NoError(t, err)
	path := t.TempDir() + "/sealed"
	require.NoError(t, cm.Write(ctx, path, data))
	provider := NewProvider(&mocks.MockMixCoordClient{}, WithChunkManager(cm))
	runtime := &Runtime{provider: provider}
	defer runtime.Close()
	base := walview.VChannelWALView{Schema: &schemapb.CollectionSchema{Version: 1}, CaptureQueryView: func(_ *viewpb.QueryViewMeta, capture func(walview.VChannelWALView)) bool {
		capture(view)
		return true
	}}
	require.NoError(t, runtime.Prepare(ctx, base))
	require.True(t, runtime.disabled)
	fail := true
	patch := mockey.Mock((*Provider).getSealedBM25Resources).To(func(*Provider, context.Context, int64, string, qviews.DataVersion, []int64, uint64) ([]*datapb.StreamingNodeBM25Resource, error) {
		// The pre-snapshot insert can still be queued in QueryRuntime. Only the
		// post-snapshot insert belongs in the replacement's live tail.
		runtime.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: bm25Insert(t, 20, 4)})
		runtime.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: bm25Insert(t, 20, 6, 11)})
		if fail {
			return nil, merr.WrapErrServiceNotReadyMsg("retry storage")
		}
		return []*datapb.StreamingNodeBM25Resource{{
			SegmentId: 30, PartitionId: 10, StorageVersion: storage.StorageV2,
			Bm25Binlogs: []*datapb.FieldBinlog{{FieldID: 102, Binlogs: []*datapb.Binlog{{LogPath: path}}}},
		}}, nil
	}).Build()
	defer patch.UnPatch()
	meta := &viewpb.QueryViewMeta{SchemaVersion: 2, Version: &viewpb.QueryViewVersion{DataVersion: version.IntoProto()}}
	require.ErrorIs(t, runtime.PrepareSchema(ctx, meta, nil), merr.ErrServiceNotReady)
	require.EqualValues(t, 1, runtime.SchemaVersion())
	require.Nil(t, runtime.currentOracle())
	fail = false
	require.NoError(t, runtime.PrepareSchema(ctx, meta, nil))
	require.EqualValues(t, 2, runtime.SchemaVersion())
	require.False(t, runtime.disabled)
	require.EqualValues(t, 3, runtime.oracle.currentStats[102].NumRow())
	require.Equal(t, float64(4), runtime.oracle.currentStats[102].GetAvgdl())
	require.NoError(t, runtime.PrepareSchema(ctx, meta, nil))
	require.EqualValues(t, 3, runtime.oracle.currentStats[102].NumRow())
}

func TestSchemaRefreshCancellationUnblocksFullEventQueue(t *testing.T) {
	paramtable.Init()
	lifetime, stop := context.WithCancel(t.Context())
	defer stop()
	version := qviews.DataVersion{StreamingVersion: 10}
	runtime := &Runtime{provider: NewProvider(&mocks.MockMixCoordClient{})}
	defer runtime.Close()
	view := walview.VChannelWALView{Schema: &schemapb.CollectionSchema{Version: 2, Functions: []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{102}}}}, SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: version}}
	require.NoError(t, runtime.Prepare(t.Context(), walview.VChannelWALView{
		Schema: &schemapb.CollectionSchema{Version: 1}, RuntimeContext: lifetime,
		CaptureQueryView: func(_ *viewpb.QueryViewMeta, capture func(walview.VChannelWALView)) bool { capture(view); return true },
	}))
	reading := make(chan struct{})
	patch := mockey.Mock((*Provider).getSealedBM25Resources).To(func(_ *Provider, ctx context.Context, _ int64, _ string, _ qviews.DataVersion, _ []int64, _ uint64) ([]*datapb.StreamingNodeBM25Resource, error) {
		close(reading)
		<-ctx.Done()
		return nil, ctx.Err()
	}).Build()
	defer patch.UnPatch()
	done := make(chan error, 1)
	go func() {
		done <- runtime.PrepareSchema(t.Context(), &viewpb.QueryViewMeta{SchemaVersion: 2, Version: &viewpb.QueryViewVersion{DataVersion: version.IntoProto()}}, nil)
	}()
	<-reading
	event := walview.VChannelResourceEvent{SegmentSealed: &walview.SegmentSealedEvent{SegmentID: 20, SealedAtDataVersion: qviews.DataVersion{StreamingVersion: 11}}}
	for i := 0; i < 1024; i++ {
		runtime.ApplyLiveEvent(t.Context(), event)
	}
	observed := make(chan struct{})
	go func() { runtime.ApplyLiveEvent(t.Context(), event); close(observed) }()
	stop()
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("canceled schema build blocked WAL event application")
	}
	require.ErrorIs(t, <-done, context.Canceled)
	require.EqualValues(t, 1, runtime.SchemaVersion())
	require.Nil(t, runtime.currentOracle())
}

func TestSchemaRefreshReconcilesLaterLoadFieldsAndPreservesCurrentOracle(t *testing.T) {
	paramtable.Init()
	client := &mocks.MockMixCoordClient{}
	future := syncutil.NewFuture[types.MixCoordClient]()
	future.Set(client)
	loadInfo := queryresource.NewFutureLoadInfoProvider(future)
	version := qviews.DataVersion{StreamingVersion: 10}
	schema := &schemapb.CollectionSchema{Version: 2, Functions: []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{102}}}}
	view := walview.VChannelWALView{CollectionID: 1, VChannel: "v1", Schema: schema, LoadInfoVersion: 2, SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: version}}
	runtime := &Runtime{provider: NewProvider(client)}
	defer runtime.Close()
	base := view
	base.LoadInfoVersion = 1
	base.LoadFields = []*messagespb.LoadFieldConfig{{FieldId: 101}}
	base.CaptureQueryView = func(meta *viewpb.QueryViewMeta, capture func(walview.VChannelWALView)) bool {
		view.LoadInfoVersion = meta.GetLoadInfoVersion()
		view.SegmentSnapshot.DataVersion = qviews.FromProtoDataVersion(meta.GetVersion().GetDataVersion())
		capture(view)
		return true
	}
	require.NoError(t, runtime.Prepare(t.Context(), base))
	require.True(t, runtime.disabled)
	var fetchErr error
	loadPatch := mockey.Mock((*mocks.MockMixCoordClient).GetQueryViewLoadInfo).To(func(_ *mocks.MockMixCoordClient, _ context.Context, req *querypb.GetQueryViewLoadInfoRequest, _ ...grpc.CallOption) (*querypb.GetQueryViewLoadInfoResponse, error) {
		return &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success(), CollectionID: 1, Version: req.GetVersion(), LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 102}}}, fetchErr
	}).Build()
	defer loadPatch.UnPatch()
	statsPatch := mockey.Mock((*Provider).getSealedBM25Resources).Return(nil, nil).Build()
	defer statsPatch.UnPatch()
	meta := &viewpb.QueryViewMeta{CollectionId: 1, SchemaVersion: 2, LoadInfoVersion: 2, Version: &viewpb.QueryViewVersion{DataVersion: version.IntoProto()}}
	fetchErr = merr.WrapErrServiceNotReadyMsg("load config unavailable")
	require.ErrorIs(t, runtime.PrepareSchema(t.Context(), meta, loadInfo), merr.ErrServiceNotReady)
	require.True(t, runtime.disabled)
	fetchErr = nil
	require.NoError(t, runtime.PrepareSchema(t.Context(), meta, loadInfo))
	require.False(t, runtime.disabled)
	old := runtime.currentOracle()
	require.Contains(t, old.currentStats, int64(102))
	// Failed refresh preserves an already usable oracle. An older requested
	// DataVersion must not regress the single shared aggregate on retry.
	meta.SchemaVersion = 3
	meta.Version.DataVersion.StreamingVersion = 9
	view.Schema = &schemapb.CollectionSchema{Version: 3, Functions: schema.Functions}
	fetchErr = merr.WrapErrServiceNotReadyMsg("retry")
	require.Error(t, runtime.PrepareSchema(t.Context(), meta, loadInfo))
	require.Same(t, old, runtime.currentOracle())
	require.EqualValues(t, 2, runtime.SchemaVersion())
	fetchErr = nil
	require.NoError(t, runtime.PrepareSchema(t.Context(), meta, loadInfo))
	require.Equal(t, version, runtime.oracle.currentVersion)
	require.EqualValues(t, 3, runtime.SchemaVersion())
	require.False(t, old.closed, "old read schema must retain its IDF aggregate")
	retained := runtime.oracles[queryresource.ReadSchemaKey{SchemaVersion: 2, LoadInfoVersion: 2}]
	require.Same(t, old, retained)
	require.NoError(t, runtime.PrepareSchema(t.Context(), &viewpb.QueryViewMeta{SchemaVersion: 2, LoadInfoVersion: 2}, loadInfo))
	require.Same(t, old, runtime.oracles[queryresource.ReadSchemaKey{SchemaVersion: 2, LoadInfoVersion: 2}])
	runtime.Close()
	require.True(t, old.closed)
	require.ErrorIs(t, runtime.PrepareSchema(t.Context(), meta, loadInfo), context.Canceled)
}

func TestQueryViewReadyWaitsForNewBM25Oracle(t *testing.T) {
	paramtable.Init()
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	dispatcher := queryresource.NewDispatcher(1)
	defer dispatcher.Close()
	provider := NewProvider(&mocks.MockMixCoordClient{})
	manager := queryresource.NewManager(queryresource.Config{
		Scheduler: scheduler, Dispatcher: dispatcher,
		Builders: []queryresource.QueryRuntimeModuleBuilder{provider},
	})
	defer manager.Close()
	version := qviews.DataVersion{StreamingVersion: 10}
	view := walview.VChannelWALView{CollectionID: 1, VChannel: "v1", Schema: &schemapb.CollectionSchema{Version: 1}, SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: version}}
	view.CaptureQueryView = func(_ *viewpb.QueryViewMeta, capture func(walview.VChannelWALView)) bool {
		next := view
		next.Schema = &schemapb.CollectionSchema{Version: 2, Functions: []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{102}}}}
		capture(next)
		return true
	}
	build := func(*viewpb.QueryViewMeta) (walview.VChannelWALView, bool) { return view, true }
	key := qviews.QueryViewKey{ShardID: qviews.ShardID{ReplicaID: 1, VChannel: "v1"}, QueryViewVersion: qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1}}
	ready := make(chan struct{})
	manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: &viewpb.QueryViewMeta{SchemaVersion: 1, Version: key.QueryViewVersion.IntoProto()}, OnReady: func() { close(ready) }}, build)
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("initial view not ready")
	}
	reading, finishRead := make(chan struct{}), make(chan struct{})
	patch := mockey.Mock((*Provider).getSealedBM25Resources).To(func(_ *Provider, ctx context.Context, _ int64, _ string, _ qviews.DataVersion, _ []int64, _ uint64) ([]*datapb.StreamingNodeBM25Resource, error) {
		close(reading)
		select {
		case <-finishRead:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}).Build()
	defer patch.UnPatch()
	key.QueryViewVersion.QueryVersion++
	nextReady := make(chan struct{})
	manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: &viewpb.QueryViewMeta{SchemaVersion: 2, Version: key.QueryViewVersion.IntoProto()}, OnReady: func() { close(nextReady) }}, build)
	<-reading
	select {
	case <-nextReady:
		t.Fatal("view reported Ready before IDF initialization")
	default:
	}
	close(finishRead)
	select {
	case <-nextReady:
	case <-time.After(time.Second):
		t.Fatal("new view not ready after IDF initialization")
	}
}
