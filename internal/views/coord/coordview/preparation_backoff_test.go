package coordview

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestPreparationBackoffCountsDistinctFailuresAndResetsOnSuccess(t *testing.T) {
	var b preparationBackoff
	now := time.Now()
	for i := int64(1); i <= 10; i++ {
		b.onFailure(testVersion(1, 1, i), now)
		want := min(maxPreparationBackoff, initialPreparationBackoff<<(i-1))
		require.Equal(t, want, b.delay)
		require.Equal(t, now.Add(want), b.notBefore)
		b.onFailure(testVersion(1, 1, i), now.Add(time.Hour))
		require.Equal(t, now.Add(want), b.notBefore, "duplicate reports cannot extend backoff")
	}
	b.onUp(testVersion(1, 1, 9))
	require.True(t, b.failed, "old serving schema cannot reset newer failures")
	b.onUp(testVersion(1, 1, 11))
	require.Equal(t, preparationBackoff{}, b)
}

func TestManagerPreparationFailureBackoffAndUpReset(t *testing.T) {
	catalog, syncer := newMockCatalog(), newMockSyncer()
	m := newTestManager(t, catalog, syncer)
	ctx := context.Background()
	require.NoError(t, m.AddPreparing(ctx, testBuilder(1, 1, 1)))
	v1 := testVersion(1, 1, 1)
	simulateNodeResponse(t, syncer, testQN1, v1, qviews.QueryViewStateUnrecoverable)
	m.mu.Lock()
	require.Equal(t, initialPreparationBackoff, m.prepareBackoff.delay)
	m.mu.Unlock()
	require.NoError(t, m.AddPreparing(ctx, testBuilder(1, 2, 1)))
	m.mu.Lock()
	require.Equal(t, initialPreparationBackoff, m.prepareBackoff.delay, "admission and DataVersion advance do not reset failures")
	m.mu.Unlock()
	v2 := testVersion(1, 2, 1)
	simulateNodeResponse(t, syncer, testQN1, v2, qviews.QueryViewStateReady)
	simulateNodeResponse(t, syncer, testSN, v2, qviews.QueryViewStateReady)
	simulateNodeResponse(t, syncer, testSN, v2, qviews.QueryViewStateUp)
	m.mu.Lock()
	require.Equal(t, preparationBackoff{}, m.prepareBackoff)
	m.mu.Unlock()
}

func TestManagerDelayedAdmissionReleaseAndPreemption(t *testing.T) {
	catalog, syncer := newMockCatalog(), newMockSyncer()
	m := newTestManager(t, catalog, syncer)
	m.mu.Lock()
	m.prepareBackoff.onFailure(testVersion(1, 0, 1), time.Now())
	m.prepareBackoff.notBefore = time.Now().Add(time.Hour)
	m.mu.Unlock()
	// Call the real asynchronous API rather than the test wrapper's Flush.
	require.NoError(t, m.ShardViewManager.AddPreparing(context.Background(), testBuilder(1, 1, 1)))
	require.NotNil(t, m.Stats().PreparingVersion)
	require.Empty(t, catalog.savedStates())
	require.NoError(t, m.ShardViewManager.AddPreparing(context.Background(), testBuilder(1, 2, 1)))
	m.mu.Lock()
	require.Equal(t, initialPreparationBackoff, m.prepareBackoff.delay, "preemption is not a load failure")
	m.mu.Unlock()
	require.NoError(t, m.ShardViewManager.RequestRelease(context.Background()))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, m.scheduler.Flush(ctx), "release cancels the publication deadline")
	for _, state := range catalog.savedStates() {
		require.NotEqual(t, viewpb.QueryViewState_QueryViewStatePreparing, state)
	}
}

func TestManagerNodeLossClearsDeadlineWithoutCountingFailure(t *testing.T) {
	catalog, syncer := newMockCatalog(), newMockSyncer()
	m := newTestManager(t, catalog, syncer)
	require.NoError(t, m.AddPreparing(context.Background(), testBuilder(1, 1, 1)))
	m.mu.Lock()
	m.prepareBackoff.onFailure(testVersion(1, 0, 1), time.Now())
	m.prepareBackoff.notBefore = time.Now().Add(time.Hour)
	m.mu.Unlock()
	lost := syncer.findOnQueryNodeLost(testQN1, testVersion(1, 1, 1))
	require.NotNil(t, lost)
	lost(testQN1)
	m.mu.Lock()
	require.True(t, m.prepareBackoff.notBefore.IsZero())
	require.Equal(t, initialPreparationBackoff, m.prepareBackoff.delay)
	m.mu.Unlock()
}

func TestDirtyViewDeadlineDoesNotBlockOtherShardsAndCanBeSuperseded(t *testing.T) {
	catalog := newMockCatalog()
	s := newTestDirtyViewFlushScheduler(t, catalog, newMockSyncer(), 128)
	delayed := dirtyPersistEvent(testShardID, 1)
	delayed.notBefore = time.Now().Add(time.Hour)
	s.Submit(delayed)
	otherID := testShardID
	otherID.ReplicaID++
	done := make(chan struct{})
	other := dirtyPersistEvent(otherID, 1)
	other.afterPersist = []func(){func() { close(done) }}
	s.Submit(other)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("unrelated shard blocked")
	}
	require.Len(t, catalog.savedStates(), 1)
	release := dirtyPersistEvent(testShardID, 1)
	release.persists[0].Meta.State = viewpb.QueryViewState_QueryViewStateDropped
	s.Submit(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, s.Flush(ctx))
	require.Equal(t, []viewpb.QueryViewState{viewpb.QueryViewState_QueryViewStatePreparing, viewpb.QueryViewState_QueryViewStateDropped}, catalog.savedStates())
}

func TestDirtyViewDeadlineEventuallyPublishesWithoutNewTrigger(t *testing.T) {
	s := newTestDirtyViewFlushScheduler(t, newMockCatalog(), newMockSyncer(), 128)
	event := dirtyPersistEvent(testShardID, 1)
	event.notBefore = time.Now().Add(100 * time.Millisecond)
	persisted := make(chan time.Time, 1)
	event.afterPersist = []func(){func() { persisted <- time.Now() }}
	s.Submit(event)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, s.Flush(ctx))
	require.False(t, (<-persisted).Before(event.notBefore))
}

func TestManagerReplacementNodeSetBypassesPendingDeadline(t *testing.T) {
	catalog, syncer := newMockCatalog(), newMockSyncer()
	m := newTestManager(t, catalog, syncer)
	m.mu.Lock()
	m.prepareBackoff.onFailure(testVersion(1, 0, 1), time.Now())
	m.prepareBackoff.notBefore = time.Now().Add(time.Hour)
	m.mu.Unlock()
	require.NoError(t, m.ShardViewManager.AddPreparing(t.Context(), testBuilder(1, 1, 1)))
	// No QN callback exists for this delayed view. Balancer can still replace
	// its lost worker through the ordinary AddPreparing API.
	replacement := testBuilder(1, 1, 1)
	replacement.SetAssignments(map[int64]map[int64][]int64{2: {10: {1001}}})
	require.NoError(t, m.ShardViewManager.AddPreparing(t.Context(), replacement))
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, m.scheduler.Flush(ctx))
	m.mu.Lock()
	require.True(t, m.prepareBackoff.notBefore.IsZero())
	require.Equal(t, initialPreparationBackoff, m.prepareBackoff.delay)
	m.mu.Unlock()
}
