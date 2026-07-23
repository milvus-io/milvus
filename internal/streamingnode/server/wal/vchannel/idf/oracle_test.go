package idf

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
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
