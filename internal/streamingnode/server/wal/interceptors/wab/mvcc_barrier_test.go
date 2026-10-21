package wab

import (
	"context"
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
	require.Empty(t, w.pendingMessages.buf)
	w.Append(nil, createMVCCBarrier(50, "a"))
	msg, err = reader.Next(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 50, msg.TimeTick())
}
