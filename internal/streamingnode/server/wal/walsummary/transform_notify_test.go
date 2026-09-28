package walsummary

import (
	"context"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func requireNotification(t *testing.T, ch <-chan struct{}, closed bool) {
	t.Helper()
	require.NotNil(t, ch)
	select {
	case <-ch:
		require.True(t, closed, "unrelated message woke the reader")
	default:
		require.False(t, closed, "reader missed a notification")
	}
}

func readNotification(t *testing.T, m *Manager, vc string) TransformBatch {
	t.Helper()
	batch, err := m.ReadTransform(context.Background(), vc, 0, math.MaxUint64, ReadLimits{})
	require.NoError(t, err)
	return batch
}

func notificationMessage(typ message.MessageType, vc string, tt uint64) message.ImmutableMessage {
	return message.NewMutableMessageBeforeAppend(nil, map[string]string{"_t": strconv.Itoa(int(typ)), "_vc": vc}).
		WithTimeTick(tt).WithLastConfirmed(walimplstest.NewTestMessageID(int64(tt))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tt + 1)))
}

func TestTransformNotificationsAreVChannelScoped(t *testing.T) {
	m := NewManager(ManagerConfig{})
	releaseA, releaseB := m.WatchTransform("a"), m.WatchTransform("b")
	defer releaseA()
	defer releaseB()
	a, b := readNotification(t, m, "a"), readNotification(t, m, "b")
	m.ObserveMessage(context.Background(), newTestDeleteMessage(t, "b", 10, 1, 1))
	requireNotification(t, a.TransformChanged, false)
	requireNotification(t, b.TransformChanged, true)
	requireNotification(t, a.Changed, true) // Bounded readers still need global coverage.
	require.Equal(t, uint64(10), readNotification(t, m, "a").CoveredThrough)

	// Plain inserts and TimeTick confirmations do not advance query Transform MVCC.
	b = readNotification(t, m, "b")
	m.ObserveMessage(context.Background(), notificationMessage(message.MessageTypeInsert, "a", 20))
	m.ObserveMessage(context.Background(), notificationMessage(message.MessageTypeTimeTick, "", 30))
	requireNotification(t, a.TransformChanged, false)
	requireNotification(t, b.TransformChanged, false)

	// An assembled insert-only commit advances Transform MVCC without a Delete entry.
	txn := newTestIdempotentTxnMessage(t, "a", 40, "", [][]int64{{1}})
	m.ObserveMessage(context.Background(), txn)
	requireNotification(t, a.TransformChanged, true)
	requireNotification(t, b.TransformChanged, false)
	batch := readNotification(t, m, "a")
	require.Empty(t, batch.Entries)
	require.Equal(t, txn.TimeTick(), batch.CoveredThrough)
}

func TestTransformBarrierNotifications(t *testing.T) {
	m := NewManager(ManagerConfig{})
	defer m.WatchTransform("a")()
	defer m.WatchTransform("b")()
	for i, typ := range []message.MessageType{
		message.MessageTypeCommitImport, message.MessageTypeFlush, message.MessageTypeManualFlush,
		message.MessageTypeDropPartition, message.MessageTypeDropCollection, message.MessageTypeTruncateCollection,
		message.MessageTypeFlushAll, message.MessageTypeAlterWAL,
	} {
		a, b := readNotification(t, m, "a"), readNotification(t, m, "b")
		m.ObserveMessage(context.Background(), notificationMessage(typ, "a", uint64(i+1)))
		requireNotification(t, a.TransformChanged, true)
		requireNotification(t, b.TransformChanged, false)
	}
	for i, typ := range []message.MessageType{message.MessageTypeFlushAll, message.MessageTypeAlterWAL, message.MessageTypeRecoveryBarrier} {
		a, b := readNotification(t, m, "a"), readNotification(t, m, "b")
		m.ObserveMessage(context.Background(), notificationMessage(typ, "", uint64(i+20)))
		requireNotification(t, a.TransformChanged, true)
		requireNotification(t, b.TransformChanged, true)
	}
}

func TestTransformWatchLifetimeAndTerminalFailure(t *testing.T) {
	m := NewManager(ManagerConfig{})
	first, second := m.WatchTransform("a"), m.WatchTransform("a")
	a, b := readNotification(t, m, "a"), readNotification(t, m, "a")
	require.Equal(t, a.TransformChanged, b.TransformChanged)
	first()
	first() // Release is idempotent and must not remove another subscription's ref.
	require.Equal(t, 1, m.transformNotifiers["a"].refs)
	m.mu.Lock()
	m.setTerminalErrorLocked(ErrStoreCorrupted)
	m.mu.Unlock()
	requireNotification(t, b.TransformChanged, true)
	_, err := m.ReadTransform(context.Background(), "a", 0, 100, ReadLimits{})
	require.ErrorIs(t, err, ErrStoreCorrupted)
	second()
	require.Empty(t, m.transformNotifiers)
}

func TestTransformGCNotifiesAffectedVChannel(t *testing.T) {
	m, _ := newTransformTestManagerWithStore(t)
	var released bool
	flushTransform(t, m, "a", 10, &released)
	defer m.WatchTransform("a")()
	defer m.WatchTransform("b")()
	a, b := readNotification(t, m, "a"), readNotification(t, m, "b")
	m.AdvanceGCTimeTick("a", 10)
	m.cfg.RetentionMaxBytes = 1
	require.NoError(t, m.GCOnce(context.Background()))
	requireNotification(t, a.TransformChanged, true)
	requireNotification(t, b.TransformChanged, false)
	require.Equal(t, uint64(10), readNotification(t, m, "a").FastForwardTimeTick)
}

func nextTransformEvent(t *testing.T, h *recordingTransformHandler) wal.TransformLogStreamEvent {
	t.Helper()
	select {
	case event := <-h.events:
		return event
	case <-time.After(time.Second):
		t.Fatal("subscription did not deliver an event")
		return wal.TransformLogStreamEvent{}
	}
}

func TestUnboundedStreamUsesScopedNotifications(t *testing.T) {
	m := NewManager(ManagerConfig{})
	stream := NewStream(m)
	defer stream.Close()
	h := newRecordingTransformHandler()
	h.events = make(chan wal.TransformLogStreamEvent, 64)
	sub, err := stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{VChannel: "a", Handler: h})
	require.NoError(t, err)
	defer sub.Close()
	require.Equal(t, uint64(0), nextTransformEvent(t, h).SyncUp.TimeTick)
	for tt := uint64(1); tt <= 20; tt++ {
		m.ObserveMessage(context.Background(), newTestDeleteMessage(t, "b", tt, 1, int64(tt)))
	}
	m.ObserveMessage(context.Background(), newTestDeleteMessage(t, "a", 21, 1, 1))
	// Any unwanted B-driven SyncUp would arrive ahead of this Delete.
	require.Equal(t, uint64(21), nextTransformEvent(t, h).Entry.GetTimeTick())
	require.Equal(t, uint64(21), nextTransformEvent(t, h).SyncUp.TimeTick)
	m.ObserveMessage(context.Background(), notificationMessage(message.MessageTypeFlushAll, "", 30))
	require.Equal(t, uint64(30), nextTransformEvent(t, h).SyncUp.TimeTick)
	require.NoError(t, sub.Close())
	m.mu.Lock()
	remaining := len(m.transformNotifiers)
	m.mu.Unlock()
	require.Zero(t, remaining)
}

func TestUnboundedStreamReleasesWatchOnShutdown(t *testing.T) {
	for _, cause := range []string{"context", "stream", "terminal"} {
		t.Run(cause, func(t *testing.T) {
			m := NewManager(ManagerConfig{})
			stream := NewStream(m)
			defer stream.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h := newRecordingTransformHandler()
			_, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "a", Handler: h})
			require.NoError(t, err)
			require.NotNil(t, nextTransformEvent(t, h).SyncUp)
			want := context.Canceled
			switch cause {
			case "context":
				cancel()
			case "stream":
				require.NoError(t, stream.Close())
			case "terminal":
				want = ErrStoreCorrupted
				m.mu.Lock()
				m.setTerminalErrorLocked(want)
				m.mu.Unlock()
			}
			require.ErrorIs(t, nextTransformEvent(t, h).Err, want)
			select {
			case <-h.done:
			case <-time.After(time.Second):
				t.Fatal("subscription did not close")
			}
			m.mu.Lock()
			remaining := len(m.transformNotifiers)
			m.mu.Unlock()
			require.Zero(t, remaining)
		})
	}
}
