package wab

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestPendingQueue(t *testing.T) {
	q := newPendingQueue(1, time.Hour, createTimeTickMessage(99, true))
	messages := []message.ImmutableMessage{createInsertMessage(100), createInsertMessage(101), createTimeTickMessage(110, true), createInsertMessage(120), createTimeTickMessage(130, true)}
	q.Push(messages)
	require.Equal(t, 5, q.CurrentOffset())
	snapshot, err := q.CreateSnapshotFromOffset(1)
	require.NoError(t, err)
	require.Len(t, snapshot, 5)
	for _, tick := range []uint64{100, 105, 110} {
		got, err := q.CreateSnapshotFromExclusiveTimeTick(tick)
		require.NoError(t, err)
		require.Greater(t, got[0].Message.TimeTick(), tick)
	}
	_, err = q.CreateSnapshotFromExclusiveTimeTick(98)
	require.ErrorIs(t, err, ErrEvicted)
	_, err = q.CreateSnapshotFromExclusiveTimeTick(130)
	require.ErrorIs(t, err, io.EOF)
	_, err = q.CreateSnapshotFromOffset(6)
	require.ErrorIs(t, err, io.EOF)
	require.Panics(t, func() { _, _ = q.CreateSnapshotFromOffset(7) })

	// Over capacity alone cannot evict the unconsumed prefix.
	q.Evict()
	require.Len(t, q.buf, 5) // Only the consumed seed may go.
	q.ReleaseThrough(110)
	require.Len(t, q.buf, 2)
	require.EqualValues(t, 110, q.readableAfter)
	_, err = q.CreateSnapshotFromOffset(3)
	require.ErrorIs(t, err, ErrEvicted)
	got, err := q.CreateSnapshotFromOffset(4)
	require.NoError(t, err)
	require.Len(t, got, 2)
	got, err = q.CreateSnapshotFromExclusiveTimeTick(110)
	require.NoError(t, err)
	require.EqualValues(t, 120, got[0].Message.TimeTick())
	// Existing reader snapshots still own the evicted messages.
	require.Equal(t, messages[0], snapshot[0].Message)
	require.Equal(t, messages[2], snapshot[2].Message)
	q.ReleaseThrough(100)
	require.EqualValues(t, 110, q.consumedThrough)
	q.ReleaseThrough(150)
	require.Empty(t, q.buf)
	_, err = q.CreateSnapshotFromExclusiveTimeTick(130)
	require.ErrorIs(t, err, io.EOF)
	require.Panics(t, func() { q.Push([]message.ImmutableMessage{createInsertMessage(129)}) })
	require.Panics(t, func() { q.Push([]message.ImmutableMessage{createMVCCBarrier(129, "a")}) })
}

func TestCacheEvictionRequiresPolicyAndConsumption(t *testing.T) {
	for _, policy := range []string{"capacity", "ttl"} {
		t.Run(policy, func(t *testing.T) {
			q := newPendingQueue(1<<20, time.Hour, createTimeTickMessage(1, true))
			q.Push([]message.ImmutableMessage{createInsertMessage(10), createTimeTickMessage(20, true)})
			q.ReleaseThrough(20)
			require.Len(t, q.buf, 3, "consumption alone must not discard cache")
			if policy == "capacity" {
				q.capacity = 1
				q.Evict()
			} else {
				q.evict(time.Now().Add(2 * time.Hour))
			}
			require.Empty(t, q.buf, "the durable anchor is retained outside the cache")

			q.Push([]message.ImmutableMessage{createInsertMessage(30), createTimeTickMessage(40, true)})
			q.evict(time.Now().Add(2 * time.Hour))
			require.Len(t, q.buf, 2, "capacity/TTL cannot evict the unconsumed data")
			q.ReleaseThrough(40)
			if policy == "ttl" {
				q.evict(time.Now().Add(2 * time.Hour))
			}
			require.Empty(t, q.buf)
		})
	}
}
