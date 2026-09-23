package idf

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type blockingNodeSchedulerTask struct {
	started chan struct{}
	release chan struct{}
}

func (t *blockingNodeSchedulerTask) Execute(ctx context.Context) error {
	close(t.started)
	select {
	case <-t.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newScheduledOracleRuntime(scheduler nodescheduler.Scheduler, current qviews.DataVersion) *oracleRuntime {
	return &oracleRuntime{
		provider:       &Provider{},
		scheduler:      scheduler,
		collectionID:   1,
		vchannel:       "v1",
		currentVersion: current,
		currentStats:   make(bm25Stats),
		currentSealed:  make(map[int64]sealedContribution),
		currentGrowing: make(map[int64]growingContribution),
		growingStore:   newGrowingStatsStore(nil),
	}
}

func TestOracleRuntimeSchedulesCoalescedAdvance(t *testing.T) {
	current := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	first := qviews.DataVersion{StreamingVersion: 11, CompactVersion: 1}
	latest := qviews.DataVersion{StreamingVersion: 12, CompactVersion: 1}
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()

	blocker := &blockingNodeSchedulerTask{started: make(chan struct{}), release: make(chan struct{})}
	scheduler.Submit(blocker)
	<-blocker.started

	var callsMu sync.Mutex
	calls := make([]qviews.DataVersion, 0, 1)
	mock := mockey.Mock((*Provider).getSealedBM25Resources).To(func(
		_ *Provider,
		_ context.Context,
		_ int64,
		_ string,
		version qviews.DataVersion,
		_ []int64,
		_ uint64,
	) ([]*datapb.StreamingNodeBM25Resource, error) {
		callsMu.Lock()
		calls = append(calls, version)
		callsMu.Unlock()
		return nil, nil
	}).Build()
	defer mock.UnPatch()

	runtime := newScheduledOracleRuntime(scheduler, current)
	runtime.MaybeAdvance(first)
	runtime.MaybeAdvance(latest)

	runtime.mu.RLock()
	require.True(t, runtime.advanceScheduled)
	require.True(t, runtime.pending.EQ(latest))
	runtime.mu.RUnlock()

	close(blocker.release)
	require.Eventually(t, func() bool {
		runtime.mu.RLock()
		defer runtime.mu.RUnlock()
		return runtime.currentVersion.EQ(latest) && !runtime.advanceScheduled
	}, time.Second, 10*time.Millisecond)
	runtime.Close()

	callsMu.Lock()
	require.Equal(t, []qviews.DataVersion{latest}, calls)
	callsMu.Unlock()
}

func TestOracleRuntimeCloseCancelsScheduledAdvance(t *testing.T) {
	current := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	target := qviews.DataVersion{StreamingVersion: 11, CompactVersion: 1}
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()

	started := make(chan struct{})
	canceled := make(chan struct{})
	mock := mockey.Mock((*Provider).getSealedBM25Resources).To(func(
		_ *Provider,
		ctx context.Context,
		_ int64,
		_ string,
		_ qviews.DataVersion,
		_ []int64,
		_ uint64,
	) ([]*datapb.StreamingNodeBM25Resource, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	}).Build()
	defer mock.UnPatch()

	runtime := newScheduledOracleRuntime(scheduler, current)
	runtime.MaybeAdvance(target)
	<-started
	runtime.Close()

	select {
	case <-canceled:
	default:
		t.Fatal("scheduled IDF advance was not canceled")
	}
}

func TestOracleRuntimeRejectsStaleAdvanceDiff(t *testing.T) {
	current := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	target := qviews.DataVersion{StreamingVersion: 11, CompactVersion: 1}
	runtime := &oracleRuntime{
		currentVersion: current,
		currentStats:   make(bm25Stats),
		currentSealed:  make(map[int64]sealedContribution),
		currentGrowing: map[int64]growingContribution{
			20: {
				segmentID:   20,
				partitionID: 10,
				stats:       make(bm25Stats),
			},
		},
		growingStore: newGrowingStatsStore(nil),
		revision:     2,
	}

	committed, retry := runtime.commitDiff(&idfDiff{
		target:      target,
		revision:    1,
		positive:    make(bm25Stats),
		negative:    make(bm25Stats),
		nextSealed:  make(map[int64]sealedContribution),
		nextGrowing: make(map[int64]growingContribution),
	})

	require.False(t, committed)
	require.True(t, retry)
	require.True(t, runtime.currentVersion.EQ(current))
	require.Contains(t, runtime.currentGrowing, int64(20))
}

func TestOracleRuntimePreparesAndServesExactDataVersion(t *testing.T) {
	current := qviews.DataVersion{StreamingVersion: 10}
	target := qviews.DataVersion{StreamingVersion: 11}
	fieldID := int64(102)
	schema := &schemapb.CollectionSchema{Functions: []*schemapb.FunctionSchema{{
		Type:           schemapb.FunctionType_BM25,
		OutputFieldIds: []int64{fieldID},
	}}}
	growingStore := newGrowingStatsStore(schema)
	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{7: 2})
	growingStore.appendStats(20, 10, bm25Stats{fieldID: stats})

	mock := mockey.Mock((*Provider).getSealedBM25Resources).Return(nil, nil).Build()
	defer mock.UnPatch()
	runtime := &oracleRuntime{
		provider:       &Provider{sealedCache: newSegmentCache()},
		collectionID:   1,
		vchannel:       "v1",
		schema:         schema,
		currentVersion: current,
		currentStats:   newBM25StatsFromSchema(schema),
		currentSealed:  make(map[int64]sealedContribution),
		currentGrowing: make(map[int64]growingContribution),
		growingStore:   growingStore,
	}

	require.NoError(t, runtime.PrepareDataVersion(context.Background(), target))
	query := &schemapb.SparseFloatArray{Contents: [][]byte{
		typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{7: 1}),
	}}
	_, avgdl, err := runtime.BuildIDF(target, fieldID, query)
	require.NoError(t, err)
	require.Equal(t, float64(2), avgdl)

	runtime.ReleaseDataVersion(target)
	_, _, err = runtime.BuildIDF(target, fieldID, query)
	require.Error(t, err)
}

func TestOracleCompactionDiffRetriesAndReleasesResources(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := storage.NewLocalChunkManager()
	resources := make([]*datapb.StreamingNodeBM25Resource, 0, 2)
	for i, tf := range []float32{2, 6} {
		stats := storage.NewBM25Stats()
		stats.Append(map[uint32]float32{7: tf})
		data, err := stats.Serialize()
		require.NoError(t, err)
		path := t.TempDir() + "/bm25"
		require.NoError(t, cm.Write(ctx, path, data))
		resources = append(resources, &datapb.StreamingNodeBM25Resource{SegmentId: int64(20 + i), StorageVersion: storage.StorageV2, Bm25Binlogs: []*datapb.FieldBinlog{{FieldID: 102, Binlogs: []*datapb.Binlog{{LogPath: path}}}}})
	}
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	provider := NewProvider(nil, WithChunkManager(cm), WithNodeScheduler(scheduler))
	view := walview.VChannelWALView{Schema: &schemapb.CollectionSchema{Functions: []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{102}}}}, SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: qviews.DataVersion{StreamingVersion: 10}}}
	runtime, err := newOracleRuntime(ctx, provider, view, resources[:1])
	require.NoError(t, err)
	defer runtime.Close()
	patch := mockey.Mock((*Provider).getSealedBM25Resources).To(func(_ *Provider, _ context.Context, _ int64, _ string, v qviews.DataVersion, _ []int64, _ uint64) ([]*datapb.StreamingNodeBM25Resource, error) {
		if v.StreamingVersion == 11 {
			return resources[1:], nil
		}
		return nil, nil
	}).Build()
	defer patch.UnPatch()
	target := qviews.DataVersion{StreamingVersion: 11}
	diff, err := runtime.computeDiff(ctx, target)
	require.NoError(t, err)
	// A live change invalidates a prepared compaction diff before commit.
	runtime.revision++
	committed, retry := runtime.commitDiff(diff)
	require.False(t, committed)
	require.True(t, retry)
	require.Len(t, provider.sealedCache.entries, 1)
	runtime.restorePending(target)
	pending, ok := runtime.popPending()
	require.True(t, ok)
	require.Equal(t, target, pending)
	diff, err = runtime.computeDiff(ctx, target)
	require.NoError(t, err)
	committed, retry = runtime.commitDiff(diff)
	require.True(t, committed)
	require.False(t, retry)
	_, avg, err := runtime.BuildIDF(target, 102, nil)
	require.NoError(t, err)
	require.Equal(t, float64(6), avg)
	require.Len(t, provider.sealedCache.entries, 1)
	target.StreamingVersion++
	diff, err = runtime.computeDiff(ctx, target)
	require.NoError(t, err)
	committed, retry = runtime.commitDiff(diff)
	require.True(t, committed)
	require.False(t, retry)
	_, avg, err = runtime.BuildIDF(target, 102, nil)
	require.NoError(t, err)
	require.Zero(t, avg)
	require.Empty(t, provider.sealedCache.entries)
	runtime.Close()
	require.ErrorIs(t, runtime.PrepareDataVersion(ctx, target), context.Canceled)
}
