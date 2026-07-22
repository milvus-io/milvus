package idempotency

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestDIDWindowBeginCompleteAndDuplicate(t *testing.T) {
	startedAt := time.Unix(100, 0)
	window := NewWindow(WindowConfig{Now: func() time.Time { return startedAt }})

	begin := window.Begin("key-1", nil)
	require.Equal(t, BeginDecisionOwner, begin.Decision)
	require.NotNil(t, begin.Pending)
	assert.Equal(t, startedAt, begin.Pending.StartedAt)
	assert.Equal(t, 1, window.InflightLen())

	completed, evicted := window.Complete(begin.Pending, CommitResult{CommitTimeTick: 100}, nil)
	require.True(t, completed)
	require.Zero(t, evicted)
	assert.Equal(t, 0, window.InflightLen())
	assert.Equal(t, 1, window.Len())

	duplicate := window.Begin("key-1", nil)
	require.Equal(t, BeginDecisionDuplicate, duplicate.Decision)
	require.NotNil(t, duplicate.Entry)
	assert.Equal(t, "key-1", duplicate.Entry.GetKey())
	assert.Equal(t, uint64(100), duplicate.Entry.GetCommitTimetick())
}

// The TTL eviction bound derives from the committing entry's timetick, not the
// local wall clock, so the live window and the clock-free recovery-side window
// retain the same key set under NTP skew.
func TestWindowTTLEvictionUsesCommitTimetick(t *testing.T) {
	window := NewWindow(WindowConfig{WindowTTL: 5 * time.Second})
	oldTT := tsoutil.ComposeTS(1_000, 0)
	newTT := tsoutil.ComposeTS(100_000, 0)

	completeKey(t, window, "old", oldTT)
	require.Contains(t, window.entries, IdempotencyKey("old"))

	// An entry older than TTL relative to the NEW commit's timetick is evicted,
	// with no wall clock involved.
	completeKey(t, window, "new", newTT)
	require.NotContains(t, window.entries, IdempotencyKey("old"))
	require.Contains(t, window.entries, IdempotencyKey("new"))

	// A commit timetick younger than the TTL must not underflow into an
	// evict-everything bound.
	tiny := NewWindow(WindowConfig{WindowTTL: 10 * time.Minute})
	completeKey(t, tiny, "a", tsoutil.ComposeTS(1_000, 0))
	completeKey(t, tiny, "b", tsoutil.ComposeTS(2_000, 0))
	require.Len(t, tiny.entries, 2)
}

// One client idempotency key fans out to a window per vchannel, so "is this key
// still deduplicated" must not depend on how loaded each individual shard is.
// Under shard skew the busy window sits above the minEntries floor and drops the
// key at the TTL bound while the quiet window stays below the floor and keeps it:
// a retry would then be appended again on the busy shard (duplicate rows) and
// deduplicated on the quiet one, and the proxy merges that mix into one
// successful response. Duplicate visibility must therefore end at the TTL on
// every shard alike.
func TestWindowDuplicateVisibilityIsUniformAcrossSkewedShards(t *testing.T) {
	const ttl = 10 * time.Minute
	// Both shards run the SAME config; only their load differs.
	newShard := func() *Window { return NewWindow(WindowConfig{WindowTTL: ttl, MinEntries: 2}) }
	busy, quiet := newShard(), newShard()

	commitTT := tsoutil.ComposeTS(1_000_000, 0)
	completeKey(t, busy, "shared-key", commitTT)
	completeKey(t, quiet, "shared-key", commitTT)
	// The busy shard carries unrelated traffic that lifts it above the floor.
	completeKey(t, busy, "other-1", commitTT+1)
	completeKey(t, busy, "other-2", commitTT+2)
	completeKey(t, busy, "other-3", commitTT+3)

	// Within the TTL both shards deduplicate the retry.
	require.Equal(t, BeginDecisionDuplicate, busy.Begin("shared-key", nil).Decision)
	require.Equal(t, BeginDecisionDuplicate, quiet.Begin("shared-key", nil).Decision)

	// Past the TTL, the periodic timetick sweep hits every window of the pchannel
	// with the same bound.
	expiredBound := evictBeforeCommitTT(commitTT+tsoutil.ComposeTS(ttl.Milliseconds()+1_000, 0), ttl)
	require.NotZero(t, expiredBound)
	busy.Evict(expiredBound, "")
	quiet.Evict(expiredBound, "")

	// The floor still holds the quiet shard's entry in memory...
	require.Contains(t, quiet.entries, IdempotencyKey("shared-key"))
	require.NotContains(t, busy.entries, IdempotencyKey("shared-key"))
	// ...but it no longer answers the retry, so both shards agree it is a new write.
	require.Equal(t, BeginDecisionOwner, busy.Begin("shared-key", nil).Decision)
	begin := quiet.Begin("shared-key", nil)
	require.Equal(t, BeginDecisionOwner, begin.Decision)

	// The dropped entry leaves no stale commitOrder slot behind: the re-append
	// owns exactly one entry, at its new commit timetick.
	retryTT := commitTT + tsoutil.ComposeTS(ttl.Milliseconds()+2_000, 0)
	completed, _ := quiet.Complete(begin.Pending, CommitResult{CommitTimeTick: retryTT}, nil)
	require.True(t, completed)
	require.Equal(t, 1, quiet.Len())
	require.Len(t, quiet.commitOrder, 1)
	duplicate := quiet.Begin("shared-key", nil)
	require.Equal(t, BeginDecisionDuplicate, duplicate.Decision)
	require.Equal(t, retryTT, duplicate.Entry.GetCommitTimetick())
}

// Complete order is append-completion order, not commit-timetick order: the
// idempotency interceptor is outermost while the timetick is assigned by the
// inner timetick interceptor, so concurrent appends on one vchannel can
// complete out of order. commitOrder must stay sorted by commit timetick —
// eviction and the evicted watermark read its head as "oldest", and the
// recovery-side window sorts its entries the same way.
func TestWindowCommitOrderSortedByCommitTimetick(t *testing.T) {
	window := NewWindow(WindowConfig{})
	completeKey(t, window, "a", 100)
	completeKey(t, window, "b", 90)
	completeKey(t, window, "c", 95)

	require.Equal(t, []IdempotencyKey{"b", "c", "a"}, window.commitOrder)
	// The watermark points at the oldest retained commit timetick, regardless of
	// completion order.
	require.Equal(t, uint64(90), window.EvictedWatermarkTT())

	// Count-cap eviction drops the oldest entry by commit timetick ("b"), not
	// the first-completed one ("a").
	capped := NewWindow(WindowConfig{MaxEntries: 2})
	completeKey(t, capped, "a", 100)
	completeKey(t, capped, "b", 90)
	completeKey(t, capped, "c", 95)
	require.NotContains(t, capped.entries, IdempotencyKey("b"))
	require.Contains(t, capped.entries, IdempotencyKey("a"))
	require.Contains(t, capped.entries, IdempotencyKey("c"))
}

func TestDIDWindowSameKeyAlwaysDuplicate(t *testing.T) {
	window := NewWindow(WindowConfig{})

	begin := window.Begin("key-1", nil)
	require.Equal(t, BeginDecisionOwner, begin.Decision)

	waiter := window.Begin("key-1", nil)
	require.Equal(t, BeginDecisionWait, waiter.Decision)
	require.Same(t, begin.Pending, waiter.Pending)

	completed, evicted := window.Complete(begin.Pending, CommitResult{CommitTimeTick: 100}, nil)
	require.True(t, completed)
	require.Zero(t, evicted)

	duplicate := window.Begin("key-1", nil)
	require.Equal(t, BeginDecisionDuplicate, duplicate.Decision)
}

func TestDIDWindowWaitsForInflightResult(t *testing.T) {
	window := NewWindow(WindowConfig{})

	owner := window.Begin("key-1", nil)
	require.Equal(t, BeginDecisionOwner, owner.Decision)

	waiter := window.Begin("key-1", nil)
	require.Equal(t, BeginDecisionWait, waiter.Decision)
	require.Same(t, owner.Pending, waiter.Pending)

	completed, evicted := window.Complete(owner.Pending, CommitResult{CommitTimeTick: 100}, nil)
	require.True(t, completed)
	require.Zero(t, evicted)
	result := waiter.Pending.Wait(context.Background(), nil)
	require.NoError(t, result.Err)
	require.NotNil(t, result.Entry)
	assert.Equal(t, uint64(100), result.Entry.GetCommitTimetick())
}

func TestDIDWindowMultipleWaitersAllReceiveResult(t *testing.T) {
	window := NewWindow(WindowConfig{})

	owner := window.Begin("key-1", nil)
	require.Equal(t, BeginDecisionOwner, owner.Decision)

	const waiterCount = 8
	waiters := make([]BeginResult, waiterCount)
	for i := range waiters {
		waiters[i] = window.Begin("key-1", nil)
		require.Equal(t, BeginDecisionWait, waiters[i].Decision)
		require.Same(t, owner.Pending, waiters[i].Pending)
	}

	results := make([]PendingResult, waiterCount)
	var wg sync.WaitGroup
	for i := range waiters {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = waiters[i].Pending.Wait(context.Background(), nil)
		}(i)
	}

	completed, _ := window.Complete(owner.Pending, CommitResult{CommitTimeTick: 100}, nil)
	require.True(t, completed)

	wg.Wait()
	for i := range results {
		require.NoErrorf(t, results[i].Err, "waiter %d", i)
		require.NotNilf(t, results[i].Entry, "waiter %d", i)
		assert.Equal(t, uint64(100), results[i].Entry.GetCommitTimetick())
	}
}

func TestDIDWindowMultipleWaitersAllReceiveFailure(t *testing.T) {
	window := NewWindow(WindowConfig{})
	appendErr := errors.New("append failed")

	owner := window.Begin("key-1", nil)
	require.Equal(t, BeginDecisionOwner, owner.Decision)

	const waiterCount = 8
	waiters := make([]BeginResult, waiterCount)
	for i := range waiters {
		waiters[i] = window.Begin("key-1", nil)
		require.Equal(t, BeginDecisionWait, waiters[i].Decision)
	}

	results := make([]PendingResult, waiterCount)
	var wg sync.WaitGroup
	for i := range waiters {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = waiters[i].Pending.Wait(context.Background(), nil)
		}(i)
	}

	require.True(t, window.Fail(owner.Pending, appendErr, nil))

	wg.Wait()
	for i := range results {
		assert.ErrorIsf(t, results[i].Err, appendErr, "waiter %d", i)
	}
}

func TestDIDWindowFailureRemovesInflight(t *testing.T) {
	window := NewWindow(WindowConfig{})
	appendErr := errors.New("append failed")

	owner := window.Begin("key-1", nil)
	require.Equal(t, BeginDecisionOwner, owner.Decision)
	require.True(t, window.Fail(owner.Pending, appendErr, nil))

	result := owner.Pending.Wait(context.Background(), nil)
	assert.ErrorIs(t, result.Err, appendErr)
	assert.Equal(t, 0, window.InflightLen())
	assert.Equal(t, 0, window.Len())

	retry := window.Begin("key-1", nil)
	assert.Equal(t, BeginDecisionOwner, retry.Decision)
}

func TestDIDWindowEvictRespectsMinEntries(t *testing.T) {
	window := NewWindow(WindowConfig{MinEntries: 2})

	completeKey(t, window, "key-1", 10)
	completeKey(t, window, "key-2", 20)
	completeKey(t, window, "key-3", 30)

	window.Evict(100, "")
	assert.Equal(t, 2, window.Len())
	assert.Equal(t, uint64(20), window.EvictedWatermarkTT())

	assert.Equal(t, BeginDecisionOwner, window.Begin("key-1", nil).Decision)
	assert.Equal(t, BeginDecisionDuplicate, window.Begin("key-2", nil).Decision)
	assert.Equal(t, BeginDecisionDuplicate, window.Begin("key-3", nil).Decision)
}

func TestDIDWindowEvictAppliesMaxEntries(t *testing.T) {
	window := NewWindow(WindowConfig{MinEntries: 1, MaxEntries: 2})

	completeKey(t, window, "key-1", 10)
	completeKey(t, window, "key-2", 20)
	completeKey(t, window, "key-3", 30)

	window.Evict(0, "")
	assert.Equal(t, 2, window.Len())
	assert.Equal(t, uint64(20), window.EvictedWatermarkTT())
	assert.Equal(t, BeginDecisionOwner, window.Begin("key-1", nil).Decision)
	assert.Equal(t, BeginDecisionDuplicate, window.Begin("key-2", nil).Decision)
	assert.Equal(t, BeginDecisionDuplicate, window.Begin("key-3", nil).Decision)
}

func TestDIDWindowCompleteReportsEvictionCount(t *testing.T) {
	window := NewWindow(WindowConfig{MinEntries: 0, MaxEntries: 1})

	begin := window.Begin("key-1", nil)
	require.Equal(t, BeginDecisionOwner, begin.Decision)
	completed, evicted := window.Complete(begin.Pending, CommitResult{CommitTimeTick: 10}, nil)
	require.True(t, completed)
	require.Zero(t, evicted)

	begin = window.Begin("key-2", nil)
	require.Equal(t, BeginDecisionOwner, begin.Decision)
	completed, evicted = window.Complete(begin.Pending, CommitResult{CommitTimeTick: 20}, nil)
	require.True(t, completed)
	require.Equal(t, 1, evicted)
	require.Equal(t, 1, window.Len())
	require.Equal(t, BeginDecisionOwner, window.Begin("key-1", nil).Decision)
	require.Equal(t, BeginDecisionDuplicate, window.Begin("key-2", nil).Decision)
}

func TestDIDWindowWatermarkFallsBackToSnapshotCheckpoint(t *testing.T) {
	window := NewWindow(WindowConfig{})
	window.SetSnapshotCheckpointTT(100)
	assert.Equal(t, uint64(100), window.SnapshotCheckpointTT())
	assert.Equal(t, uint64(100), window.EvictedWatermarkTT())
}

func TestDIDWindowRestoreFromSnapshot(t *testing.T) {
	window := NewWindowFromSnapshot(WindowConfig{}, &streamingpb.WindowSnapshot{
		SnapshotCheckpointTimetick: 100,
		EvictedWatermarkTimetick:   90,
		Entries: []*streamingpb.WindowEntry{
			{
				Key:                    "key-1",
				CommitTimetick:         90,
				MessageId:              &commonpb.MessageID{WALName: commonpb.WALName_Test, Id: "10"},
				LastConfirmedMessageId: &commonpb.MessageID{WALName: commonpb.WALName_Test, Id: "9"},
				IdempotentResult: &messagespb.IdempotentInsertResult{
					RowOffsets: []uint32{1, 0},
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{11, 10}}},
					},
				},
			},
		},
	})

	duplicate := window.Begin("key-1", nil)
	require.Equal(t, BeginDecisionDuplicate, duplicate.Decision)
	require.Equal(t, []uint32{1, 0}, duplicate.Entry.GetIdempotentResult().GetRowOffsets())
	require.Equal(t, []int64{11, 10}, duplicate.Entry.GetIdempotentResult().GetIds().GetIntId().GetData())
	require.Equal(t, uint64(90), window.EvictedWatermarkTT())
	require.Equal(t, uint64(100), window.SnapshotCheckpointTT())
}

func completeKey(t *testing.T, window *Window, key IdempotencyKey, commitTT uint64) {
	t.Helper()
	begin := window.Begin(key, nil)
	require.Equal(t, BeginDecisionOwner, begin.Decision)
	completed, _ := window.Complete(begin.Pending, CommitResult{CommitTimeTick: commitTT}, nil)
	require.True(t, completed)
}
