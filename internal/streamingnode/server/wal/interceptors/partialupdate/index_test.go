package partialupdate

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func snapshotPKVersionIndex(idx *pkVersionIndex) (size int, withinCapacity bool) {
	idx.channels.Range(func(_, value any) bool {
		channel := value.(*vchannelPKVersionIndex)
		channel.mu.Lock()
		size += len(channel.int64Versions) + len(channel.stringVersions)
		channel.mu.Unlock()
		return true
	})
	return size, idx.budget.used.Load() <= idx.budget.limit
}

func versionIndexBudgetForEntries(entries int64) int64 {
	return entries * estimatedVersionEntryFixedBytes
}

func TestVersionByteBudget(t *testing.T) {
	budget := newVersionByteBudget(10)
	require.True(t, budget.tryReserve(0))
	budget.release(0)
	require.True(t, budget.tryReserve(10))
	require.False(t, budget.tryReserve(1))
	budget.release(10)
	require.Zero(t, budget.used.Load())
	require.Panics(t, func() {
		budget.release(1)
	})
}

func TestVersionByteBudgetRecordsMissedWriteOnce(t *testing.T) {
	missed := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_partial_update_missed_writes_total",
	})
	budget := newVersionByteBudgetWithMetrics(
		estimatedVersionEntryFixedBytes,
		nil,
		missed,
	)
	idx := newPKVersionIndexWithBudget(30*time.Second, budget)

	idx.UpdateAllTyped("v1", primaryKeys{
		kind:        primaryKeyKindInt64,
		int64Values: []int64{1, 2, 3},
	}, 100)

	require.Equal(t, float64(1), testutil.ToFloat64(missed))
}

func TestVersionByteBudgetConcurrentReservations(t *testing.T) {
	const entries = 10
	budget := newVersionByteBudget(versionIndexBudgetForEntries(entries))
	var admitted atomic.Int64
	var waitGroup sync.WaitGroup
	for range 100 {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			if budget.tryReserve(estimatedVersionEntryFixedBytes) {
				admitted.Add(1)
			}
		}()
	}
	waitGroup.Wait()

	require.Equal(t, int64(entries), admitted.Load())
	require.Equal(t, budget.limit, budget.used.Load())
}

func TestPKVersionIndexVerifyConflict(t *testing.T) {
	idx := newPKVersionIndex(10*time.Second, versionIndexBudgetForEntries(100))
	idx.UpdateAll("v1", []any{int64(10)}, 100)

	err := idx.Verify("v1", []any{int64(11), int64(10)}, 99, 101)
	requirePartialUpdateRetryable(t, err)
	require.True(t, status.AsStreamingError(err).IsPartialUpdateRetryableCAS())
	require.NoError(t, idx.Verify("v1", []any{int64(10)}, 100, 101))
}

func TestPKVersionIndexNormalizesNegativeBudget(t *testing.T) {
	idx := newPKVersionIndex(time.Second, -1)
	require.Zero(t, idx.budget.limit)
}

func TestPKVersionIndexRejectsEvictedReadWindow(t *testing.T) {
	idx := newPKVersionIndex(30*time.Second, versionIndexBudgetForEntries(100))
	idx.channel("").retainedSinceTS = 50
	requirePartialUpdateRetryable(t, idx.Verify("", nil, 49, 80))
}

func TestPKVersionIndexRejectsCommitBeforeRead(t *testing.T) {
	idx := newPKVersionIndex(30*time.Second, versionIndexBudgetForEntries(100))
	requireUnrecoverable(t, idx.Verify("", nil, 100, 99))
}

func TestPKVersionIndexRejectsReadWindowBeyondTTL(t *testing.T) {
	idx := newPKVersionIndex(30*time.Second, versionIndexBudgetForEntries(100))
	readTS := tsoutil.ComposeTSByTime(time.Now())
	commitTS := tsoutil.AddPhysicalDurationOnTs(readTS, 31*time.Second)
	requirePartialUpdateRetryable(t, idx.Verify("", nil, readTS, commitTS))
}

func TestPKVersionIndexDirectlyRejectsReadWindowBeyondTTL(t *testing.T) {
	idx := newPKVersionIndex(30*time.Second, versionIndexBudgetForEntries(100))
	readTS := tsoutil.ComposeTSByTime(time.Now())
	commitTS := tsoutil.AddPhysicalDurationOnTs(readTS, 31*time.Second)

	requirePartialUpdateRetryable(t, idx.channel("").verifyReadWindowLocked(readTS, commitTS))
}

func TestPKVersionIndexBudgetFailsClosed(t *testing.T) {
	idx := newPKVersionIndex(30*time.Second, versionIndexBudgetForEntries(1))
	idx.UpdateAll("v1", []any{int64(1)}, 10)
	idx.UpdateAll("v1", []any{int64(2)}, 11)

	size, withinCapacity := snapshotPKVersionIndex(idx)
	require.True(t, withinCapacity)
	require.Equal(t, 1, size)
	channel := idx.channel("v1")
	channel.mu.Lock()
	require.Len(t, channel.expirations, 1)
	channel.mu.Unlock()
	requirePartialUpdateRetryable(t, idx.Verify("v1", nil, 11, 12))
}

func TestPKVersionIndexUpdatesExistingPKAtCapacity(t *testing.T) {
	idx := newPKVersionIndex(30*time.Second, versionIndexBudgetForEntries(1))
	now := time.Now()
	firstCommit := tsoutil.ComposeTSByTime(now)
	updatedCommit := tsoutil.ComposeTSByTime(now.Add(time.Second))
	verifyCommit := tsoutil.ComposeTSByTime(now.Add(2 * time.Second))

	idx.UpdateAll("v1", []any{int64(1)}, firstCommit)
	idx.UpdateAll("v1", []any{int64(1)}, updatedCommit)

	require.Equal(t, estimatedVersionEntryFixedBytes, idx.budget.used.Load())
	require.NoError(t, idx.Verify("v1", nil, updatedCommit, verifyCommit))
	requirePartialUpdateRetryable(t, idx.Verify("v1", []any{int64(1)}, firstCommit, verifyCommit))
}

func TestPKVersionIndexIgnoresOlderWriteForExistingPK(t *testing.T) {
	idx := newPKVersionIndex(30*time.Second, versionIndexBudgetForEntries(1))
	idx.UpdateAll("v1", []any{int64(1)}, 100)
	idx.UpdateAll("v1", []any{int64(1)}, 90)

	requirePartialUpdateRetryable(t, idx.Verify("v1", []any{int64(1)}, 99, 101))
}

func TestPKVersionIndexIgnoresWriteOlderThanRetention(t *testing.T) {
	idx := newPKVersionIndex(30*time.Second, versionIndexBudgetForEntries(1))
	idx.channel("v1").retainedSinceTS = 100
	idx.UpdateAll("v1", []any{int64(1)}, 90)

	size, _ := snapshotPKVersionIndex(idx)
	require.Zero(t, size)
}

func TestPKVersionIndexPanicsOnInvalidPublishedPK(t *testing.T) {
	idx := newPKVersionIndex(30*time.Second, versionIndexBudgetForEntries(1))
	require.Panics(t, func() {
		idx.UpdateAll("v1", []any{struct{}{}}, 100)
	})
}

func TestPKVersionIndexLatestMissExtendsIncompleteWindow(t *testing.T) {
	idx := newPKVersionIndex(30*time.Second, versionIndexBudgetForEntries(1))
	now := time.Now()
	firstCommit := tsoutil.ComposeTSByTime(now)
	firstMiss := tsoutil.ComposeTSByTime(now.Add(time.Second))
	latestMiss := tsoutil.ComposeTSByTime(now.Add(20 * time.Second))
	readAfterFirstMiss := tsoutil.ComposeTSByTime(now.Add(2 * time.Second))
	commitAfterFirstMissExpires := tsoutil.ComposeTSByTime(now.Add(32 * time.Second))
	readAfterLatestMiss := tsoutil.ComposeTSByTime(now.Add(21 * time.Second))
	commitAfterLatestMissExpires := tsoutil.ComposeTSByTime(now.Add(51 * time.Second))

	idx.UpdateAll("v1", []any{int64(1)}, firstCommit)
	idx.UpdateAll("v1", []any{int64(2)}, firstMiss)
	idx.UpdateAll("v1", []any{int64(3)}, latestMiss)

	requirePartialUpdateRetryable(t, idx.Verify("v1", nil, readAfterFirstMiss, commitAfterFirstMissExpires))
	require.NoError(t, idx.Verify("v1", nil, readAfterLatestMiss, commitAfterLatestMissExpires))
	size, withinCapacity := snapshotPKVersionIndex(idx)
	require.True(t, withinCapacity)
	require.Zero(t, size)
}

func TestPKVersionIndexSweepsExpiredVersionsBeforeCapacityCheck(t *testing.T) {
	idx := newPKVersionIndex(30*time.Second, versionIndexBudgetForEntries(1))
	now := time.Now()
	firstCommit := tsoutil.ComposeTSByTime(now)
	secondCommit := tsoutil.ComposeTSByTime(now.Add(31 * time.Second))
	readAfterFirstExpired := tsoutil.ComposeTSByTime(now.Add(30 * time.Second))
	commitAfterSecond := tsoutil.ComposeTSByTime(now.Add(32 * time.Second))

	idx.UpdateAll("v1", []any{int64(1)}, firstCommit)
	idx.UpdateAll("v1", []any{int64(2)}, secondCommit)

	size, withinCapacity := snapshotPKVersionIndex(idx)
	require.True(t, withinCapacity)
	require.Equal(t, 1, size)
	require.NoError(t, idx.Verify("v1", []any{int64(1)}, readAfterFirstExpired, commitAfterSecond))
	requirePartialUpdateRetryable(t, idx.Verify("v1", []any{int64(2)}, readAfterFirstExpired, commitAfterSecond))
}

func TestPKVersionIndexKeepsUpdatedVersionPastOriginalExpiry(t *testing.T) {
	idx := newPKVersionIndex(30*time.Second, versionIndexBudgetForEntries(100))
	now := time.Now()
	firstCommit := tsoutil.ComposeTSByTime(now)
	updatedCommit := tsoutil.ComposeTSByTime(now.Add(20 * time.Second))
	commitAfterOriginalExpiry := tsoutil.ComposeTSByTime(now.Add(31 * time.Second))

	idx.UpdateAll("v1", []any{int64(1)}, firstCommit)
	idx.UpdateAll("v1", []any{int64(1)}, updatedCommit)
	requirePartialUpdateRetryable(t, idx.Verify("v1", []any{int64(1)}, firstCommit, commitAfterOriginalExpiry))
}

func TestPKVersionIndexMaintainsOneExpirationPerPK(t *testing.T) {
	idx := newPKVersionIndex(30*time.Second, versionIndexBudgetForEntries(100))
	base := time.Now()
	for offset := range 10 {
		idx.UpdateAll("v1", []any{int64(1)}, tsoutil.ComposeTSByTime(base.Add(time.Duration(offset)*time.Second)))
	}

	channel := idx.channel("v1")
	channel.mu.Lock()
	defer channel.mu.Unlock()
	require.Len(t, channel.int64Versions, 1)
	require.Empty(t, channel.stringVersions)
	require.Len(t, channel.expirations, 1)
}

func TestPKVersionIndexRecoversAfterBudgetEntriesExpire(t *testing.T) {
	idx := newPKVersionIndex(30*time.Second, versionIndexBudgetForEntries(1))
	now := time.Now()
	firstCommit := tsoutil.ComposeTSByTime(now)
	secondCommit := tsoutil.ComposeTSByTime(now.Add(time.Second))
	thirdCommit := tsoutil.ComposeTSByTime(now.Add(32 * time.Second))

	idx.UpdateAll("v1", []any{int64(1)}, firstCommit)
	idx.UpdateAll("v1", []any{int64(2)}, secondCommit)
	size, withinCapacity := snapshotPKVersionIndex(idx)
	require.True(t, withinCapacity)
	require.Equal(t, 1, size)

	idx.UpdateAll("v1", []any{int64(3)}, thirdCommit)
	size, withinCapacity = snapshotPKVersionIndex(idx)
	require.True(t, withinCapacity)
	require.Equal(t, 1, size)
}

func TestPKVersionIndexVerifyRecoversAfterBudgetEntriesExpire(t *testing.T) {
	idx := newPKVersionIndex(30*time.Second, versionIndexBudgetForEntries(1))
	now := time.Now()
	firstCommit := tsoutil.ComposeTSByTime(now)
	secondCommit := tsoutil.ComposeTSByTime(now.Add(time.Second))
	readAfterExpiry := tsoutil.ComposeTSByTime(now.Add(31 * time.Second))
	commitAfterExpiry := tsoutil.ComposeTSByTime(now.Add(32 * time.Second))

	idx.UpdateAll("v1", []any{int64(1)}, firstCommit)
	idx.UpdateAll("v1", []any{int64(2)}, secondCommit)
	size, withinCapacity := snapshotPKVersionIndex(idx)
	require.True(t, withinCapacity)
	require.Equal(t, 1, size)

	require.NoError(t, idx.Verify("v1", nil, readAfterExpiry, commitAfterExpiry))
	size, withinCapacity = snapshotPKVersionIndex(idx)
	require.True(t, withinCapacity)
	require.Zero(t, size)
	require.Zero(t, idx.budget.used.Load())
}

func TestPKVersionIndexStringPKConflict(t *testing.T) {
	idx := newPKVersionIndex(10*time.Second, versionIndexBudgetForEntries(100))
	idx.UpdateAll("v1", []any{"pk-1"}, 100)

	requirePartialUpdateRetryable(t, idx.Verify("v1", []any{"pk-1"}, 99, 101))
	require.NoError(t, idx.Verify("v1", []any{"pk-1"}, 100, 101))
}

func TestPKVersionIndexRejectsMalformedPK(t *testing.T) {
	idx := newPKVersionIndex(10*time.Second, versionIndexBudgetForEntries(100))

	err := idx.Verify("v1", []any{nil}, 1, 2)
	requireUnrecoverable(t, err)
	require.Contains(t, err.Error(), "int64 or string")
	require.False(t, status.AsStreamingError(err).IsPartialUpdateRetryableCAS())

	err = idx.Verify("v1", []any{struct{}{}}, 1, 2)
	requireUnrecoverable(t, err)
}

func TestPKVersionIndexAccountsStringBytes(t *testing.T) {
	budget := estimatedVersionEntryFixedBytes + 3
	idx := newPKVersionIndex(30*time.Second, budget)
	idx.UpdateAll("v1", []any{"abc"}, 100)
	idx.UpdateAll("v1", []any{strings.Repeat("x", 4)}, 101)

	require.Equal(t, budget, idx.budget.used.Load())
	requirePartialUpdateRetryable(t, idx.Verify("v1", nil, 101, 102))
}

func TestPKVersionIndexBudgetMissIsVChannelScoped(t *testing.T) {
	idx := newPKVersionIndex(30*time.Second, versionIndexBudgetForEntries(1))
	idx.UpdateAll("v1", []any{int64(1)}, 100)
	idx.UpdateAll("v2", []any{int64(2)}, 101)

	require.NoError(t, idx.Verify("v1", nil, 100, 102))
	requirePartialUpdateRetryable(t, idx.Verify("v2", nil, 101, 102))
}

func TestPKVersionIndexKeepsVChannelsSeparate(t *testing.T) {
	idx := newPKVersionIndex(30*time.Second, versionIndexBudgetForEntries(2))
	idx.UpdateAll("v1", []any{int64(1)}, 100)

	requirePartialUpdateRetryable(t, idx.Verify("v1", []any{int64(1)}, 99, 101))
	require.NoError(t, idx.Verify("v2", []any{int64(1)}, 99, 101))
}

func TestPKVersionIndexVChannelsUseIndependentLocks(t *testing.T) {
	idx := newPKVersionIndex(30*time.Second, versionIndexBudgetForEntries(2))
	v1 := idx.channel("v1")
	v1.mu.Lock()

	done := make(chan struct{})
	go func() {
		idx.UpdateAll("v2", []any{int64(2)}, 100)
		close(done)
	}()

	select {
	case <-done:
		v1.mu.Unlock()
	case <-time.After(time.Second):
		v1.mu.Unlock()
		t.Fatal("v2 update blocked on v1 index lock")
	}
}

func TestPKVersionIndexRemoveVChannelReleasesBudget(t *testing.T) {
	idx := newPKVersionIndex(30*time.Second, versionIndexBudgetForEntries(3))
	idx.UpdateAll("v1", []any{int64(1), int64(2)}, 100)
	idx.UpdateAll("v2", []any{int64(3)}, 100)
	require.Equal(t, versionIndexBudgetForEntries(3), idx.budget.used.Load())

	idx.Remove("v1")

	_, loaded := idx.channels.Load("v1")
	require.False(t, loaded)
	_, loaded = idx.channels.Load("v2")
	require.True(t, loaded)
	require.Equal(t, versionIndexBudgetForEntries(1), idx.budget.used.Load())
	requirePartialUpdateRetryable(t, idx.Verify("v2", []any{int64(3)}, 99, 101))

	idx.Remove("v1")
	require.Equal(t, versionIndexBudgetForEntries(1), idx.budget.used.Load())
}

func TestPKVersionIndexRemoveVChannelConcurrentWithAdvance(t *testing.T) {
	idx := newPKVersionIndex(30*time.Second, versionIndexBudgetForEntries(1))
	now := time.Now()

	for offset := range 100 {
		commitTS := tsoutil.ComposeTSByTime(now.Add(time.Duration(offset) * time.Minute))
		idx.UpdateAll("v1", []any{int64(offset)}, commitTS)

		var waitGroup sync.WaitGroup
		waitGroup.Add(2)
		go func() {
			defer waitGroup.Done()
			idx.Advance(tsoutil.AddPhysicalDurationOnTs(commitTS, 31*time.Second))
		}()
		go func() {
			defer waitGroup.Done()
			idx.Remove("v1")
		}()
		waitGroup.Wait()

		require.Zero(t, idx.budget.used.Load())
	}

	_, loaded := idx.channels.Load("v1")
	require.False(t, loaded)
}

func TestCollectionFenceIndexRejectsFenceAfterRead(t *testing.T) {
	fences := newCollectionFenceIndex()
	fences.Update("v1", 1, 100)
	requirePartialUpdateRetryable(t, fences.Verify("v1", 1, 99))
	require.NoError(t, fences.Verify("v1", 1, 100))
}

func TestCollectionFenceIndexKeepsCollectionsSeparate(t *testing.T) {
	fences := newCollectionFenceIndex()
	fences.Update("v1", 1, 100)
	fences.Update("v1", 2, 200)

	requirePartialUpdateRetryable(t, fences.Verify("v1", 1, 99))
	require.NoError(t, fences.Verify("v1", 1, 100))
	requirePartialUpdateRetryable(t, fences.Verify("v1", 2, 199))
	require.NoError(t, fences.Verify("v1", 2, 200))
}

func TestCollectionFenceIndexKeepsEntriesUntilExplicitRemoval(t *testing.T) {
	fences := newCollectionFenceIndex()
	now := time.Now()
	oldFence := tsoutil.ComposeTSByTime(now)
	newFence := tsoutil.ComposeTSByTime(now.Add(31 * time.Second))
	fences.Update("v1", 1, oldFence)
	fences.Update("v2", 2, newFence)

	requirePartialUpdateRetryable(t, fences.Verify("v1", 1, oldFence-1))
	requirePartialUpdateRetryable(t, fences.Verify("v2", 2, newFence-1))
}

func TestCollectionFenceIndexRemovesOnlyTargetCollection(t *testing.T) {
	fences := newCollectionFenceIndex()
	fences.Update("v1", 1, 100)
	fences.Update("v1", 2, 100)
	fences.Update("v2", 1, 100)

	fences.Remove("v1", 1)

	require.NoError(t, fences.Verify("v1", 1, 99))
	requirePartialUpdateRetryable(t, fences.Verify("v1", 2, 99))
	requirePartialUpdateRetryable(t, fences.Verify("v2", 1, 99))
}

func TestCollectionFenceIndexRemoveIgnoresEmptyCollection(t *testing.T) {
	fences := newCollectionFenceIndex()
	fences.Update("v1", 1, 100)

	fences.Remove("v1", 0)

	requirePartialUpdateRetryable(t, fences.Verify("v1", 1, 99))
}

func TestCollectionFenceIndexRejectsMalformedCollectionID(t *testing.T) {
	fences := newCollectionFenceIndex()

	err := fences.Verify("v1", 0, 100)
	requireUnrecoverable(t, err)
	require.Contains(t, err.Error(), "collection fence id is empty")
	require.False(t, status.AsStreamingError(err).IsPartialUpdateRetryableCAS())

	fences.Update("v1", 0, 100)
	require.NoError(t, fences.Verify("v1", 1, 100))
}

func TestVChannelFenceIndexRejectsEarlierRead(t *testing.T) {
	fences := newVChannelFenceIndex()
	fences.Update("v1", 100)

	requirePartialUpdateRetryable(t, fences.Verify("v1", 99))
	require.NoError(t, fences.Verify("v1", 100))
	require.NoError(t, fences.Verify("v2", 99))
}

func TestVChannelFenceIndexAdvancesMonotonically(t *testing.T) {
	fences := newVChannelFenceIndex()
	fences.Update("v1", 100)
	fences.Update("v1", 90)
	fences.Update("v1", 110)

	requirePartialUpdateRetryable(t, fences.Verify("v1", 109))
	require.NoError(t, fences.Verify("v1", 110))
}

func TestVChannelFenceIndexRemove(t *testing.T) {
	fences := newVChannelFenceIndex()
	fences.Update("v1", 100)
	fences.Update("v2", 200)

	fences.Remove("v1")

	require.NoError(t, fences.Verify("v1", 99))
	requirePartialUpdateRetryable(t, fences.Verify("v2", 199))
	fences.Remove("v1")
}
