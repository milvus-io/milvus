package idempotency

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
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

	window.Evict(100, nil)
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

	window.Evict(0, nil)
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
