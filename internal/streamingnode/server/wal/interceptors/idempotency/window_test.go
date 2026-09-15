package idempotency

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/idempotencyview"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

func TestIdempotencyWindowBeginCompleteAndDuplicate(t *testing.T) {
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
	assert.Equal(t, "key-1", duplicate.Entry.IdempotencyKey)
	assert.Equal(t, uint64(100), duplicate.Entry.SourceTimeTick)
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

	// Byte-cap eviction drops the oldest entry by commit timetick ("b"), not
	// the first-completed one ("a").
	probe := NewWindow(WindowConfig{})
	completeKey(t, probe, "a", 100)
	entrySize := probe.bytes
	require.Positive(t, entrySize)
	capped := NewWindow(WindowConfig{MaxBytes: entrySize * 2})
	completeKey(t, capped, "a", 100)
	completeKey(t, capped, "b", 90)
	completeKey(t, capped, "c", 95)
	require.NotContains(t, capped.entries, IdempotencyKey("b"))
	require.Contains(t, capped.entries, IdempotencyKey("a"))
	require.Contains(t, capped.entries, IdempotencyKey("c"))
	require.Equal(t, BeginDecisionOwner, capped.Begin("b", nil).Decision)
}

// The byte cap is the window's only retention bound. Age is deliberately not
// one: an entry the store handed over is servable however old it is, because an
// upstream resuming after an outage is exactly who must be deduplicated. What
// bounds the window instead is memory, since each entry carries the per-row
// primary keys of its insert.
func TestWindowByteCapEvictsOldestFirst(t *testing.T) {
	// Measure the size of one entry so the cap below admits exactly one. Keys and
	// commit timeticks are chosen so both entries account the same.
	probe := NewWindow(WindowConfig{})
	completeKey(t, probe, "a", 100)
	entrySize := probe.bytes
	require.Positive(t, entrySize)

	window := NewWindow(WindowConfig{MaxBytes: entrySize})
	completeKey(t, window, "a", 100)
	require.Contains(t, window.entries, IdempotencyKey("a"))
	// The second entry pushes the window over the byte cap; the oldest goes.
	completeKey(t, window, "b", 101)
	require.NotContains(t, window.entries, IdempotencyKey("a"))
	require.Contains(t, window.entries, IdempotencyKey("b"))
	require.Len(t, window.entries, 1)
	require.Equal(t, entrySize, window.bytes)
	require.Equal(t, BeginDecisionOwner, window.Begin("a", nil).Decision)
}

func TestIdempotencyWindowSameKeyAlwaysDuplicate(t *testing.T) {
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

func TestIdempotencyWindowWaitsForInflightResult(t *testing.T) {
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
	assert.Equal(t, uint64(100), result.Entry.SourceTimeTick)
}

func TestIdempotencyWindowMultipleWaitersAllReceiveResult(t *testing.T) {
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
		assert.Equal(t, uint64(100), results[i].Entry.SourceTimeTick)
	}
}

func TestIdempotencyWindowMultipleWaitersAllReceiveFailure(t *testing.T) {
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

func TestIdempotencyWindowFailureRemovesInflight(t *testing.T) {
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

func TestIdempotencyWindowEvictAppliesMaxBytes(t *testing.T) {
	probe := NewWindow(WindowConfig{})
	completeKey(t, probe, "key-1", 10)
	entrySize := probe.bytes
	require.Positive(t, entrySize)

	window := NewWindow(WindowConfig{MaxBytes: entrySize * 2})

	completeKey(t, window, "key-1", 10)
	completeKey(t, window, "key-2", 20)
	completeKey(t, window, "key-3", 30)

	assert.Equal(t, 2, window.Len())
	assert.Equal(t, BeginDecisionOwner, window.Begin("key-1", nil).Decision)
	assert.Equal(t, BeginDecisionDuplicate, window.Begin("key-2", nil).Decision)
	assert.Equal(t, BeginDecisionDuplicate, window.Begin("key-3", nil).Decision)
}

func TestIdempotencyWindowCompleteReportsEvictionCount(t *testing.T) {
	probe := NewWindow(WindowConfig{})
	completeKey(t, probe, "key-1", 10)
	entrySize := probe.bytes
	require.Positive(t, entrySize)

	window := NewWindow(WindowConfig{MaxBytes: entrySize})

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

// A key can appear twice in a snapshot: the store retains by maxRetainedBytes
// per pchannel while the window caps itself by maxBytesPerWindow per vchannel,
// so a key evicted here and later reused is written again and both records can
// survive in the retained chunk set.
//
// Loading both would double-count the bytes and put the key in commitOrder
// twice; the first eviction would then delete the LIVE record while the second
// pop refunded nothing, leaving the byte count permanently inflated and the key
// silently no longer deduplicating.
func TestIdempotencyWindowRestoreDeduplicatesRepeatedKey(t *testing.T) {
	older := &idempotencyview.Record{
		SourceMessageID:        &commonpb.MessageID{WALName: commonpb.WALName_Test, Id: "10"},
		SourceTimeTick:         90,
		LastConfirmedMessageID: &commonpb.MessageID{WALName: commonpb.WALName_Test, Id: "9"},
		IdempotencyKey:         "key-1",
		InsertResult: &messagespb.IdempotentInsertResult{
			RowOffsets: []uint32{0},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}},
			},
		},
	}
	newer := &idempotencyview.Record{
		SourceMessageID:        &commonpb.MessageID{WALName: commonpb.WALName_Test, Id: "20"},
		SourceTimeTick:         190,
		LastConfirmedMessageID: &commonpb.MessageID{WALName: commonpb.WALName_Test, Id: "19"},
		IdempotencyKey:         "key-1",
		InsertResult: &messagespb.IdempotentInsertResult{
			RowOffsets: []uint32{0},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{20}}},
			},
		},
	}
	window := NewWindowFromSnapshot(WindowConfig{}, &idempotencyview.Snapshot{
		Records: []*idempotencyview.Record{older, newer},
	})

	window.mu.Lock()
	defer window.mu.Unlock()
	require.Len(t, window.commitOrder, 1, "a repeated key must occupy one commit-order slot")
	assert.Equal(t, newer.Size(), window.bytes, "the repeated key's bytes must be counted once")
	entry, ok := window.entries["key-1"]
	require.True(t, ok)
	assert.Equal(t, int64(20), entry.InsertResult.GetIds().GetIntId().GetData()[0],
		"the newest record must win")
}

// The store's retention budget is per pchannel while the window's is per
// vchannel, so a restored set can legitimately arrive over this window's cap.
// It must be trimmed at load rather than carried until the next write, which on
// an idle vchannel may never come.
func TestIdempotencyWindowRestoreEnforcesByteCap(t *testing.T) {
	records := make([]*idempotencyview.Record, 0, 8)
	for i := 0; i < 8; i++ {
		records = append(records, &idempotencyview.Record{
			SourceMessageID:        &commonpb.MessageID{WALName: commonpb.WALName_Test, Id: fmt.Sprintf("%d", i)},
			SourceTimeTick:         uint64(100 + i),
			LastConfirmedMessageID: &commonpb.MessageID{WALName: commonpb.WALName_Test, Id: fmt.Sprintf("%d", i)},
			IdempotencyKey:         fmt.Sprintf("key-%d", i),
			InsertResult: &messagespb.IdempotentInsertResult{
				RowOffsets: []uint32{0},
				Ids: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{int64(i)}}},
				},
			},
		})
	}
	// A cap that fits roughly half of them.
	cap := records[0].Size() * 4
	window := NewWindowFromSnapshot(WindowConfig{MaxBytes: cap}, &idempotencyview.Snapshot{Records: records})

	window.mu.Lock()
	defer window.mu.Unlock()
	assert.LessOrEqual(t, window.bytes, cap, "the restored window must respect its own byte cap")
	assert.Less(t, len(window.entries), len(records), "the oldest records must have been evicted")
	// Eviction is oldest-first, so the newest key must survive.
	_, ok := window.entries[IdempotencyKey("key-7")]
	assert.True(t, ok, "the newest record must survive the load-time trim")
}

func TestIdempotencyWindowRestoreFromSnapshot(t *testing.T) {
	// Everything the store hands over is immediately servable, however old it is.
	window := NewWindowFromSnapshot(WindowConfig{}, &idempotencyview.Snapshot{
		Records: []*idempotencyview.Record{
			{
				SourceMessageID:        &commonpb.MessageID{WALName: commonpb.WALName_Test, Id: "10"},
				SourceTimeTick:         90,
				LastConfirmedMessageID: &commonpb.MessageID{WALName: commonpb.WALName_Test, Id: "9"},
				IdempotencyKey:         "key-1",
				InsertResult: &messagespb.IdempotentInsertResult{
					RowOffsets: []uint32{1, 0},
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{11, 10}}},
					},
				},
			},
			// A keyless record materializes nothing: it is not a dedup entry.
			{SourceMessageID: &commonpb.MessageID{WALName: commonpb.WALName_Test, Id: "11"}, SourceTimeTick: 91},
		},
	})

	require.Equal(t, 1, window.Len())
	duplicate := window.Begin("key-1", nil)
	require.Equal(t, BeginDecisionDuplicate, duplicate.Decision)
	// The two halves of the duplicate response were stored in different chunk
	// sections and must arrive rejoined.
	require.Equal(t, []uint32{1, 0}, duplicate.Entry.InsertResult.GetRowOffsets())
	require.Equal(t, []int64{11, 10}, duplicate.Entry.InsertResult.GetIds().GetIntId().GetData())
	require.Equal(t, uint64(90), duplicate.Entry.SourceTimeTick)
	require.Equal(t, "9", duplicate.Entry.LastConfirmedMessageID.GetId())
}

func completeKey(t *testing.T, window *Window, key IdempotencyKey, commitTT uint64) {
	t.Helper()
	begin := window.Begin(key, nil)
	require.Equal(t, BeginDecisionOwner, begin.Decision)
	completed, _ := window.Complete(begin.Pending, CommitResult{CommitTimeTick: commitTT}, nil)
	require.True(t, completed)
}
