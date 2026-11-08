package wab

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func createMVCCBarrier(tick uint64, channels ...string) message.ImmutableMessage {
	return message.NewMVCCBarrierMessageBuilderV2().WithAllVChannel().WithNotPersisted().
		WithHeader(&message.MVCCBarrierMessageHeader{Vchannels: channels}).
		WithBody(&message.MVCCBarrierMessageBody{}).MustBuildMutable().
		WithTimeTick(tick).WithLastConfirmedUseMessageID().
		IntoImmutableMessage(walimplstest.NewTestMessageID(1))
}

func TestWriteAheadBufferDoesNotCoalesceMVCCBarriers(t *testing.T) {
	w := NewWriteAheadBuffer("barrier-order", mlog.With(), 1024*1024, time.Minute, createTimeTickMessage(1, true))
	defer w.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	tailing, err := w.ReadFromExclusiveTimeTick(ctx, 1)
	require.NoError(t, err)
	// The ordinary volatile tick uses a synthetic offset. The following queued
	// barrier must get a new offset so the existing reader does not skip it.
	w.Append(nil, createTimeTickMessage(5, false))
	msg, err := tailing.Next(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 5, msg.TimeTick())

	entry := createInsertMessage(10)
	first := createMVCCBarrier(20, "a")
	second := createMVCCBarrier(30, "b")
	third := createMVCCBarrier(40, "a")
	w.Append([]message.ImmutableMessage{entry}, first)
	w.Append(nil, createTimeTickMessage(25, false))
	w.Append(nil, second)
	w.Append(nil, third)
	latest := createTimeTickMessage(50, false)
	w.Append(nil, latest)
	late, err := w.ReadFromExclusiveTimeTick(ctx, 5)
	require.NoError(t, err)
	for _, reader := range []*WriteAheadBufferReader{tailing, late} {
		for _, want := range []message.ImmutableMessage{entry, first, second, third, latest} {
			got, err := reader.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, want, got)
		}
	}
}

func TestMVCCBarriersRetainedUntilConsumed(t *testing.T) {
	w := NewWriteAheadBuffer("barrier-catchup", mlog.With(), 1, 0, createTimeTickMessage(10, true))
	defer w.Close()
	first, second := createMVCCBarrier(20, "a"), createMVCCBarrier(30, "b")
	w.Append(nil, first)
	w.Append(nil, second)
	w.Append(nil, createTimeTickMessage(40, false))
	require.Greater(t, w.pendingMessages.Size(), 1)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	reader, err := w.ReadFromExclusiveTimeTick(ctx, 10)
	require.NoError(t, err)
	for _, want := range []message.ImmutableMessage{first, second} {
		got, err := reader.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	msg, err := reader.Next(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 40, msg.TimeTick())
	w.ReleaseThrough(20)
	require.Len(t, w.pendingMessages.buf, 1)
	w.ReleaseThrough(10) // stale consumption cannot change the protected boundary.
	require.Len(t, w.pendingMessages.buf, 1)
	w.ReleaseThrough(40)
	require.Empty(t, w.pendingMessages.buf, "consumed barriers can expire without another durable confirmation")
	require.EqualValues(t, 10, w.lastPersistedTimeTickMessage.TimeTick())
	w.Append(nil, createMVCCBarrier(50, "a"))
	msg, err = reader.Next(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 50, msg.TimeTick())
	// Publishing another durable confirmation does not evict unconsumed barriers.
	w.Append(nil, createTimeTickMessage(60, true))
	require.Len(t, w.pendingMessages.buf, 2)
	require.EqualValues(t, 50, w.pendingMessages.buf[0].Message.TimeTick())
	w.ReleaseThrough(60)
	require.Empty(t, w.pendingMessages.buf)
	require.EqualValues(t, 60, w.lastPersistedTimeTickMessage.TimeTick())
}

func TestCatchupHandoffSurvivesConsumedVolatileConfirmations(t *testing.T) {
	w := NewWriteAheadBuffer("idle-handoff", mlog.With(), 1, 0, createTimeTickMessage(100, true))
	defer w.Close()
	w.Append(nil, createMVCCBarrier(110, "a"))
	w.Append(nil, createTimeTickMessage(120, false))
	w.ReleaseThrough(120)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	reader, err := w.ReadFromExclusiveTimeTick(ctx, 100)
	require.NoError(t, err, "the latest durable confirmation must still join WAB")
	msg, err := reader.Next(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 120, msg.TimeTick())
	w.Append(nil, createTimeTickMessage(130, false))
	w.ReleaseThrough(130)
	msg, err = reader.Next(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 130, msg.TimeTick())
	w.Append(nil, createTimeTickMessage(140, true))
	w.ReleaseThrough(140)
	_, err = w.ReadFromExclusiveTimeTick(ctx, 100)
	require.ErrorIs(t, err, ErrEvicted, "do not skip a released prefix before a newer durable anchor")
	_, err = w.ReadFromExclusiveTimeTick(ctx, 140)
	require.NoError(t, err)
}

func TestCacheRetainsConfirmationWithoutItsConsumedDataAtSameTick(t *testing.T) {
	w := NewWriteAheadBuffer("same-tick-handoff", mlog.With(), 1, 0, createTimeTickMessage(1, true))
	defer w.Close()
	confirmation := createTimeTickMessage(10, true)
	w.Append([]message.ImmutableMessage{createInsertMessage(10)}, confirmation)
	w.ReleaseThrough(10)
	require.Empty(t, w.pendingMessages.buf)
	require.Equal(t, confirmation, w.lastPersistedTimeTickMessage)
	_, err := w.ReadFromExclusiveTimeTick(t.Context(), 10)
	require.NoError(t, err)
}

func TestConsumedCacheStillServesLaggingTailingReader(t *testing.T) {
	w := NewWriteAheadBuffer("lagging-reader", mlog.With(), 1<<20, time.Hour, createTimeTickMessage(10, true))
	defer w.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	reader, err := w.ReadFromExclusiveTimeTick(ctx, 10)
	require.NoError(t, err)
	// RecoveryStorage finishes this entire batch before the ordinary reader
	// asks for its next snapshot. Cache policy still permits retaining it.
	w.Append([]message.ImmutableMessage{createInsertMessage(20)}, createTimeTickMessage(30, true))
	w.ReleaseThrough(30)
	for _, tick := range []uint64{20, 30} {
		msg, err := reader.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tick, msg.TimeTick())
	}
}

func TestConsumedVolatileBarriersDoNotPinCache(t *testing.T) {
	for _, policy := range []string{"capacity", "ttl"} {
		t.Run(policy, func(t *testing.T) {
			capacity, keepalive := 1, time.Hour
			if policy == "ttl" {
				capacity, keepalive = 1<<20, 0
			}
			anchor := createTimeTickMessage(1, true)
			w := NewWriteAheadBuffer("barrier-churn", mlog.With(), capacity, keepalive, anchor)
			defer w.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			recovery, err := w.ReadFromExclusiveTimeTick(ctx, 1)
			require.NoError(t, err)
			slow, err := w.ReadFromExclusiveTimeTick(ctx, 1)
			require.NoError(t, err)
			for tick := uint64(2); tick <= 10001; tick++ {
				barrier := createMVCCBarrier(tick, fmt.Sprintf("channel-%d", tick))
				w.Append(nil, barrier)
				got, err := recovery.Next(ctx)
				require.NoError(t, err)
				require.Equal(t, barrier, got, "every unconsumed barrier reaches RecoveryStorage")
				w.ReleaseThrough(tick)
				require.Empty(t, w.pendingMessages.buf)
				require.Zero(t, w.pendingMessages.Size())
				require.Equal(t, anchor, w.lastPersistedTimeTickMessage)
			}
			// A lagging reader past the durable anchor can skip only consumed,
			// expired volatile confirmations and resume at the remaining queue.
			remaining := createMVCCBarrier(10002, "not-consumed")
			w.Append(nil, remaining)
			for _, reader := range []*WriteAheadBufferReader{slow, recovery} {
				got, err := reader.Next(ctx)
				require.NoError(t, err)
				require.Equal(t, remaining, got)
			}
			fresh, err := w.ReadFromExclusiveTimeTick(ctx, 1)
			require.NoError(t, err)
			got, err := fresh.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, remaining, got)
		})
	}
}

func TestEvictedDurableDataStillRequiresCatchup(t *testing.T) {
	w := NewWriteAheadBuffer("durable-gap", mlog.With(), 1, 0, createTimeTickMessage(1, true))
	defer w.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	reader, err := w.ReadFromExclusiveTimeTick(ctx, 1)
	require.NoError(t, err)
	w.Append([]message.ImmutableMessage{createInsertMessage(10)}, createTimeTickMessage(20, true))
	w.ReleaseThrough(20)
	w.Append(nil, createMVCCBarrier(30, "a"))
	w.ReleaseThrough(30)
	_, err = reader.Next(ctx)
	require.ErrorIs(t, err, ErrEvicted, "offset handoff must not skip persisted data")
	_, err = w.ReadFromExclusiveTimeTick(ctx, 1)
	require.ErrorIs(t, err, ErrEvicted, "time tick handoff must not skip persisted data")
	reader, err = w.ReadFromExclusiveTimeTick(ctx, 20)
	require.NoError(t, err)
	got, err := reader.Next(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 30, got.TimeTick())
	// The same reader can survive another expired volatile prefix while idle.
	w.Append(nil, createMVCCBarrier(40, "b"))
	w.ReleaseThrough(40)
	w.Append(nil, createTimeTickMessage(50, false))
	got, err = reader.Next(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 50, got.TimeTick())
}
