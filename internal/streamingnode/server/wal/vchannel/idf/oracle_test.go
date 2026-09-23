package idf

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newTestOracle(t *testing.T) (*oracleRuntime, func(int64, ...float32) *datapb.StreamingNodeBM25Resource) {
	t.Helper()
	paramtable.Init()
	cm := storage.NewLocalChunkManager()
	scheduler := nodescheduler.New(1)
	t.Cleanup(scheduler.Close)
	provider := NewProvider(nil, WithChunkManager(cm), WithNodeScheduler(scheduler))
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar}, {FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector}}, Functions: []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101}, OutputFieldIds: []int64{102}}}}
	r, err := newOracleRuntime(context.Background(), provider, walview.VChannelWALView{Schema: schema, CollectionID: 1, VChannel: "v1", SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: qviews.DataVersion{StreamingVersion: 10}}}, nil)
	require.NoError(t, err)
	t.Cleanup(r.Close)
	return r, func(id int64, values ...float32) *datapb.StreamingNodeBM25Resource {
		stats := storage.NewBM25Stats()
		for _, tf := range values {
			stats.Append(map[uint32]float32{7: tf})
		}
		data, err := stats.Serialize()
		require.NoError(t, err)
		path := t.TempDir() + "/bm25"
		require.NoError(t, cm.Write(context.Background(), path, data))
		return &datapb.StreamingNodeBM25Resource{SegmentId: id, StorageVersion: storage.StorageV2, Bm25Binlogs: []*datapb.FieldBinlog{{FieldID: 102, Binlogs: []*datapb.Binlog{{LogPath: path}}}}}
	}
}

func mockViewResources(t *testing.T, r *oracleRuntime, version *qviews.DataVersion, resources *[]*datapb.StreamingNodeBM25Resource) {
	t.Helper()
	patch := mockey.Mock((*Provider).fetchResources).To(func(_ *Provider, _ context.Context, _ int64, _ string, target qviews.DataVersion, _ []int64, _ uint64) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
		require.True(t, version.GTE(target))
		return &datapb.GetStreamingNodeQueryViewResourcesResponse{DataVersion: target.IntoProto(), Bm25Resources: *resources}, nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
}

func TestOracleSharedAggregateAndSealedEvictionReadsOldObject(t *testing.T) {
	r, resource := newTestOracle(t)
	first := resource(20, 2)
	next := resource(20, 6) // Same ID, new paths must replace the contribution.
	version := qviews.DataVersion{StreamingVersion: 11}
	resources := []*datapb.StreamingNodeBM25Resource{first}
	mockViewResources(t, r, &version, &resources)
	ctx := context.Background()
	require.NoError(t, r.refresh(ctx, version))
	query := &schemapb.SparseFloatArray{Contents: [][]byte{typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{7: 1})}}
	old := qviews.DataVersion{StreamingVersion: 10}
	check := func(avg float64) {
		for _, v := range []qviews.DataVersion{old, version} {
			vectors, actual, err := r.BuildIDF(v, 102, query)
			require.NoError(t, err)
			require.Len(t, vectors, 1)
			require.Equal(t, avg, actual)
		}
	}
	check(2)
	// A failed old-object read prevents publication, proving no sealed cache is used.
	oldPath := first.Bm25Binlogs[0].Binlogs[0].LogPath
	data, err := r.provider.chunkManager.Read(ctx, oldPath)
	require.NoError(t, err)
	require.NoError(t, r.provider.chunkManager.Remove(ctx, oldPath))
	// A newer view can replace the resource without changing its segment ID.
	version.CompactVersion++
	resources = []*datapb.StreamingNodeBM25Resource{next}
	require.Error(t, r.refresh(ctx, version))
	require.Equal(t, int64(11), r.currentVersion.StreamingVersion)
	check(2)
	require.NoError(t, r.provider.chunkManager.Write(ctx, oldPath, data))
	require.NoError(t, r.refresh(ctx, version))
	check(6)
	// Unchanged descriptors never read the objects again.
	require.NoError(t, r.provider.chunkManager.Remove(ctx, next.Bm25Binlogs[0].Binlogs[0].LogPath))
	for i := 0; i < 100; i++ {
		version.StreamingVersion++
		require.NoError(t, r.refresh(ctx, version))
	}
	check(6)
	require.Len(t, r.currentSealed, 1)
}

func TestOracleConcurrentInsertDoesNotInvalidateRefresh(t *testing.T) {
	r, resource := newTestOracle(t)
	version := qviews.DataVersion{StreamingVersion: 11}
	resources := []*datapb.StreamingNodeBM25Resource{resource(20, 2)}
	mockViewResources(t, r, &version, &resources)
	r.barrier = func(ctx context.Context) error {
		r.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: bm25Insert(t, 21, 6)})
		return nil
	}
	require.NoError(t, r.refresh(context.Background(), version))
	require.Equal(t, int64(2), r.currentStats[102].NumRow())
	require.Equal(t, float64(4), r.currentStats[102].GetAvgdl())
	require.Len(t, r.growingStore.segments, 1)
}

func TestOracleWaitsForSealBeforeCompactionHandoff(t *testing.T) {
	r, resource := newTestOracle(t)
	ctx := context.Background()
	r.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: bm25Insert(t, 20, 2)})
	r.growingStore.markFlushed(20)
	version := qviews.DataVersion{StreamingVersion: 11, CompactVersion: 1}
	resources := []*datapb.StreamingNodeBM25Resource{resource(30, 2)}
	mockViewResources(t, r, &version, &resources)
	require.ErrorIs(t, r.refresh(ctx, version), nodescheduler.ErrDelay)
	require.Equal(t, int64(1), r.currentStats[102].NumRow())
	r.growingStore.markSealed(20, qviews.DataVersion{StreamingVersion: 11})
	require.NoError(t, r.BeforeRelease(ctx, version))
	require.Empty(t, r.growingStore.segments)
	require.Equal(t, int64(1), r.currentStats[102].NumRow())
	resources = nil
	version.CompactVersion++
	require.NoError(t, r.refresh(ctx, version))
	results, err := r.BuildIDFBatch([]queryresource.IDFRequest{{FieldID: 102}})
	require.NoError(t, err)
	require.Equal(t, float64(1), results[0].Avgdl)
	require.Zero(t, r.currentStats[102].NumRow())
}

func TestOracleCloseCancelsActiveRead(t *testing.T) {
	r, _ := newTestOracle(t)
	started := make(chan struct{})
	patch := mockey.Mock((*Provider).fetchResources).To(func(_ *Provider, ctx context.Context, _ int64, _ string, _ qviews.DataVersion, _ []int64, _ uint64) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}).Build()
	defer patch.UnPatch()
	r.Advance(qviews.DataVersion{StreamingVersion: 11})
	<-started
	r.Close()
	require.NoError(t, r.BeforeRelease(context.Background(), qviews.DataVersion{StreamingVersion: 12}))
}

// Coordinator progress and seal notifications do not select a resource version.
// Only a requested view advances the one shared aggregate; failures retry that target.
func TestOracleRefreshOnlyFetchesRequestedView(t *testing.T) {
	r, _ := newTestOracle(t)
	ctx := context.Background()
	target := qviews.DataVersion{StreamingVersion: 11}
	var calls atomic.Int32
	patch := mockey.Mock((*Provider).fetchResources).To(func(_ *Provider, _ context.Context, _ int64, _ string, version qviews.DataVersion, _ []int64, _ uint64) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
		require.Equal(t, target, version)
		if calls.Add(1) == 1 {
			return nil, context.DeadlineExceeded
		}
		return &datapb.GetStreamingNodeQueryViewResourcesResponse{DataVersion: version.IntoProto()}, nil
	}).Build()
	defer patch.UnPatch()
	defer r.Close()
	r.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{SegmentSealed: &walview.SegmentSealedEvent{SegmentID: 20, SealedAtDataVersion: qviews.DataVersion{StreamingVersion: 12}}})
	require.False(t, r.advanceScheduled)
	require.Equal(t, int32(0), calls.Load())
	require.NoError(t, r.RequestRefresh(ctx, target))
	require.Eventually(t, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return r.currentVersion.EQ(target) && !r.advanceScheduled
	}, 3*time.Second, time.Millisecond)
	// Old/current views reuse the aggregate without issuing another resource RPC.
	require.NoError(t, r.RequestRefresh(ctx, qviews.DataVersion{StreamingVersion: 10}))
	require.NoError(t, r.RequestRefresh(ctx, target))
	require.False(t, r.advanceScheduled)
	require.Equal(t, int32(2), calls.Load())
}

func TestOracleOnlyLoadsRequestedBM25Fields(t *testing.T) {
	r, resource := newTestOracle(t)
	view := walview.VChannelWALView{Schema: proto.Clone(r.schema).(*schemapb.CollectionSchema), LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 102}}}
	view.Schema.Functions = append(view.Schema.Functions, &schemapb.FunctionSchema{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{103}})
	res := resource(20, 2)
	res.Bm25Binlogs = append(res.Bm25Binlogs, &datapb.FieldBinlog{FieldID: 103, Binlogs: []*datapb.Binlog{{LogPath: "must-not-read"}}})
	oracle, err := newOracleRuntime(context.Background(), r.provider, view, []*datapb.StreamingNodeBM25Resource{res})
	require.NoError(t, err)
	defer oracle.Close()
	require.Len(t, oracle.currentStats, 1)
	_, _, err = oracle.BuildIDF(qviews.DataVersion{}, 103, nil)
	require.Error(t, err)
}

func TestRuntimeRefreshUsesSingleOracleAndCoalescesHints(t *testing.T) {
	r, _ := newTestOracle(t)
	version := qviews.DataVersion{StreamingVersion: 12}
	resources := []*datapb.StreamingNodeBM25Resource{}
	mockViewResources(t, r, &version, &resources)
	module := &Runtime{oracle: r}
	require.NoError(t, module.RequestRefresh(context.Background(), qviews.DataVersion{StreamingVersion: 11}))
	require.NoError(t, module.RequestRefresh(context.Background(), version))
	require.Eventually(t, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return r.currentVersion.EQ(version) && !r.advanceScheduled
	}, time.Second, time.Millisecond)
	require.NoError(t, module.BeforeRelease(context.Background(), version))
	results, err := module.BuildIDFBatch([]queryresource.IDFRequest{{FieldID: 102}, {FieldID: 102}})
	require.NoError(t, err)
	require.Len(t, results, 2)
	module.Advance(version)
	module.Close()
	require.Error(t, r.RequestRefresh(context.Background(), version))
	_, err = r.BuildIDFBatch(nil)
	require.Error(t, err)
	_, err = module.BuildIDFBatch(nil)
	require.Error(t, err)
	require.NoError(t, module.BeforeRelease(context.Background(), version))
	module.Advance(version)
}

func TestOracleRefreshRejectsPartialDeltaAndCancelledBarrier(t *testing.T) {
	r, resource := newTestOracle(t)
	ctx := context.Background()
	version := qviews.DataVersion{StreamingVersion: 11}
	resources := []*datapb.StreamingNodeBM25Resource{resource(20, 2)}
	mockViewResources(t, r, &version, &resources)
	require.NoError(t, r.refresh(ctx, qviews.DataVersion{StreamingVersion: 9}))
	r.ioMu.Lock()
	require.ErrorIs(t, r.refresh(ctx, version), nodescheduler.ErrDelay)
	r.ioMu.Unlock()
	r.barrier = func(context.Context) error { return context.Canceled }
	require.ErrorIs(t, r.refresh(ctx, version), context.Canceled)
	require.Empty(t, r.currentSealed)
	r.barrier = nil
	require.NoError(t, r.refresh(ctx, version))
	// A corrupt removal must not publish a new membership/version.
	r.currentStats[102] = storage.NewBM25Stats()
	version.StreamingVersion++
	resources = nil
	require.Error(t, r.refresh(ctx, version))
	require.Len(t, r.currentSealed, 1)
	require.Equal(t, int64(11), r.currentVersion.StreamingVersion)
	r.Close()
	require.NoError(t, r.refresh(ctx, version))
}

func TestOracleManualFlushWaitsForFinalCommit(t *testing.T) {
	r, resource := newTestOracle(t)
	ctx := context.Background()
	r.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: bm25Insert(t, 20, 2)})
	r.growingStore.registerSegment(21, 10, 200)
	r.growingStore.registerSegment(22, 10, 100)
	flushed := message.NewManualFlushMessageBuilderV2().WithVChannel("v1").WithHeader(&message.ManualFlushMessageHeader{}).WithBody(&message.ManualFlushMessageBody{}).MustBuildMutable().WithTimeTick(200).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(200))
	r.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: flushed})
	require.NotContains(t, r.growingStore.segments, int64(22), "empty segments do not publish a sealed DataVersion")
	require.True(t, r.growingStore.segments[20].flushed)
	require.False(t, r.growingStore.segments[21].flushed)
	version := qviews.DataVersion{StreamingVersion: 11}
	resources := []*datapb.StreamingNodeBM25Resource{resource(30, 2)}
	mockViewResources(t, r, &version, &resources)
	require.ErrorIs(t, r.refresh(ctx, version), nodescheduler.ErrDelay)
	r.growingStore.markSealed(20, version)
	require.NoError(t, r.refresh(ctx, version))
	require.NotContains(t, r.growingStore.segments, int64(20))
	require.Contains(t, r.growingStore.segments, int64(21))
	require.Equal(t, int64(1), r.currentStats[102].NumRow())
}

func TestOraclePreservesResolvedEmptyPartitionScope(t *testing.T) {
	base, resource := newTestOracle(t)
	excluded := resource(20, 2)
	excluded.PartitionId = 10
	for _, partitions := range [][]int64{{}, {10}, {30}} {
		r, err := newOracleRuntime(context.Background(), base.provider, walview.VChannelWALView{Schema: base.schema, CollectionID: 1, VChannel: "v1", PartitionIDs: partitions, SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: qviews.DataVersion{StreamingVersion: 10}}}, []*datapb.StreamingNodeBM25Resource{excluded})
		require.NoError(t, err)
		expected := int64(0)
		if r.includesPartition(10) {
			expected = 1
		}
		require.Equal(t, expected, r.currentStats[102].NumRow())
		r.ApplyLiveEvent(context.Background(), walview.VChannelResourceEvent{Message: bm25Insert(t, 30, 2)})
		// bm25Insert uses partition 10.
		require.Equal(t, expected*2, r.currentStats[102].NumRow())
		version := qviews.DataVersion{StreamingVersion: 11}
		resources := []*datapb.StreamingNodeBM25Resource{excluded}
		patch := mockey.Mock((*Provider).fetchResources).Return(&datapb.GetStreamingNodeQueryViewResourcesResponse{DataVersion: version.IntoProto(), Bm25Resources: resources}, nil).Build()
		require.NoError(t, r.refresh(context.Background(), version))
		patch.UnPatch()
		require.Equal(t, expected*2, r.currentStats[102].NumRow())
		r.Close()
	}
}
