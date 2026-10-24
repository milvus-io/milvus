package walsummary

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestDispatchStreamHandoff(t *testing.T) {
	m := NewManager(ManagerConfig{})
	m.ObserveMessage(t.Context(), newTestDeleteMessage(t, "v", 10, 1, 10))
	s := NewDispatchStream(m)
	defer s.Close()
	h := newRecordingTransformHandler()
	entered, resume := make(chan struct{}), make(chan struct{})
	var original func(*Manager, context.Context, string, uint64, uint64, ReadLimits) (TransformBatch, error)
	var reads atomic.Int32
	patch := mockey.Mock((*Manager).ReadTransform).Origin(&original).To(func(m *Manager, ctx context.Context, vc string, after, through uint64, limits ReadLimits) (TransformBatch, error) {
		reads.Add(1)
		close(entered)
		select {
		case <-resume:
		case <-ctx.Done():
			return TransformBatch{}, ctx.Err()
		}
		return original(m, ctx, vc, after, through, limits)
	}).Build()
	defer patch.UnPatch()
	sub, err := s.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "v", Handler: h})
	require.NoError(t, err)
	defer sub.Close()
	<-entered
	m.ObserveMessage(t.Context(), newTestDeleteMessage(t, "v", 20, 1, 20))
	m.ObserveQueryBarrier("v", 30)
	close(resume)
	require.Equal(t, uint64(10), nextTransformEvent(t, h).Entry.GetTimeTick())
	require.Equal(t, uint64(20), nextTransformEvent(t, h).Entry.GetTimeTick())
	require.Equal(t, uint64(30), nextTransformEvent(t, h).SyncUp.TimeTick)
	m.ObserveMessage(t.Context(), newTestDeleteMessage(t, "v", 40, 1, 40))
	require.Equal(t, uint64(40), nextTransformEvent(t, h).Entry.GetTimeTick())
	m.ObserveQueryBarrier("v", 50)
	require.Equal(t, uint64(50), nextTransformEvent(t, h).SyncUp.TimeTick)
	require.EqualValues(t, 1, reads.Load(), "live dispatch must not reread Summary")
}

func TestDispatchStreamScopeAndResume(t *testing.T) {
	m := NewManager(ManagerConfig{})
	s := NewDispatchStream(m)
	defer s.Close()
	h := newRecordingTransformHandler()
	sub, err := s.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "v", StartAfterTimeTick: 100, Handler: h})
	require.NoError(t, err)
	defer sub.Close()
	waitDispatchLive(t, sub)
	require.Empty(t, h.events, "resume cursor must not manufacture coverage")
	m.ObserveMessage(t.Context(), notificationMessage(message.MessageTypeTimeTick, "", 110))
	m.ObserveMessage(t.Context(), newTestIdempotentTxnMessage(t, "v", 120, "", [][]int64{{1}}))
	m.ObserveMessage(t.Context(), newTestDeleteMessage(t, "other", 130, 1, 13))
	require.Empty(t, h.events, "unrelated traffic and pure Inserts must stay silent")
	m.ObserveQueryBarrier("v", 140)
	require.Equal(t, uint64(140), nextTransformEvent(t, h).SyncUp.TimeTick)
	m.ObserveMessage(t.Context(), notificationMessage(message.MessageTypeFlushAll, "", 150))
	require.Equal(t, uint64(150), nextTransformEvent(t, h).SyncUp.TimeTick)
}

func waitDispatchLive(t *testing.T, sub wal.TransformLogSubscription) {
	t.Helper()
	d := sub.(*dispatchSubscription)
	require.Eventually(t, func() bool { d.stream.mu.Lock(); defer d.stream.mu.Unlock(); return d.live }, time.Second, time.Millisecond)
}

type countingTransformHandler struct {
	count atomic.Int64
	once  sync.Once
	done  chan struct{}
}

func (h *countingTransformHandler) Handle(wal.TransformLogStreamEvent) { h.count.Add(1) }
func (h *countingTransformHandler) Close()                             { h.once.Do(func() { close(h.done) }) }

func TestDispatchStreamIdleSubscriptionsHaveNoWorkers(t *testing.T) {
	m := NewManager(ManagerConfig{})
	s := NewDispatchStream(m)
	defer s.Close()
	before := runtime.NumGoroutine()
	const n = 10000
	var subs []wal.TransformLogSubscription
	h := &countingTransformHandler{done: make(chan struct{})}
	for i := 0; i < n; i++ {
		sub, err := s.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: fmt.Sprint("v", i), Handler: h})
		require.NoError(t, err)
		subs = append(subs, sub)
	}
	require.Eventually(t, func() bool { return h.count.Load() == n }, 10*time.Second, time.Millisecond)
	require.Less(t, runtime.NumGoroutine()-before, 20, "idle subscriptions must not own waiting goroutines")
	for _, sub := range subs {
		require.NoError(t, sub.Close())
	}
	m.mu.Lock()
	require.Empty(t, m.transformSubscribers)
	m.mu.Unlock()
}

func TestDispatchStreamFailureAndCancellation(t *testing.T) {
	m := NewManager(ManagerConfig{})
	s := NewDispatchStream(m)
	defer s.Close()
	h := newRecordingTransformHandler()
	ctx, cancel := context.WithCancel(t.Context())
	_, err := s.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "v", Handler: h})
	require.NoError(t, err)
	nextTransformEvent(t, h)
	cancel()
	select {
	case <-h.done:
	case <-time.After(time.Second):
		t.Fatal("canceled subscription not released")
	}
	select {
	case <-s.Done():
		t.Fatal("subscription cancellation failed siblings")
	default:
	}
	m.mu.Lock()
	m.setTerminalErrorLocked(ErrStoreCorrupted)
	m.mu.Unlock()
	// No subscribers remain, so create a new one to observe the sticky error.
	_, err = s.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "v", Handler: newRecordingTransformHandler()})
	require.ErrorIs(t, err, ErrStoreCorrupted)
}

func TestDispatchStreamTerminalFailureWakesLiveSubscriber(t *testing.T) {
	m := NewManager(ManagerConfig{})
	s := NewDispatchStream(m)
	defer s.Close()
	h := newRecordingTransformHandler()
	sub, err := s.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "v", Handler: h})
	require.NoError(t, err)
	defer sub.Close()
	nextTransformEvent(t, h)
	m.mu.Lock()
	m.setTerminalErrorLocked(ErrStoreCorrupted)
	m.mu.Unlock()
	select {
	case <-s.Done():
		require.ErrorIs(t, s.Error(), ErrStoreCorrupted)
	case <-time.After(time.Second):
		t.Fatal("live subscriber missed terminal failure")
	}
}

func TestDispatchStreamSlowConsumerDoesNotBlockObservation(t *testing.T) {
	m := NewManager(ManagerConfig{})
	s := NewDispatchStream(m)
	defer s.Close()
	h := newRecordingTransformHandler()
	blocked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	patch := mockey.Mock((*recordingTransformHandler).Handle).To(func(*recordingTransformHandler, wal.TransformLogStreamEvent) {
		once.Do(func() { close(blocked) })
		<-release
	}).Build()
	defer patch.UnPatch()
	sub, err := s.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "v", Handler: h})
	require.NoError(t, err)
	defer sub.Close()
	defer close(release)
	<-blocked
	completed := make(chan struct{})
	go func() {
		for i := 1; i <= transformStreamQueueEvents+1; i++ {
			m.ObserveQueryBarrier("v", uint64(i))
		}
		close(completed)
	}()
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("slow subscriber blocked PChannel observation")
	}
	select {
	case <-s.Done():
		require.ErrorContains(t, s.Error(), "backlog exceeded")
	case <-time.After(time.Second):
		t.Fatal("unbounded delivery backlog")
	}
}

func TestDispatchStreamConcurrentSubscribeCancel(t *testing.T) {
	m := NewManager(ManagerConfig{})
	s := NewDispatchStream(m)
	defer s.Close()
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(t.Context())
		h := newRecordingTransformHandler()
		go cancel()
		sub, err := s.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "v", Handler: h})
		if err != nil {
			require.ErrorIs(t, err, context.Canceled)
		} else {
			require.NoError(t, sub.Close())
		}
	}
}

func TestDispatchStreamPagedReplayBackpressureAndTransaction(t *testing.T) {
	m := NewManager(ManagerConfig{})
	const n = 5000
	for i := 1; i <= n; i++ {
		m.ObserveMessage(t.Context(), newTestDeleteMessage(t, "v", uint64(i), 1, int64(i)))
	}
	s := NewDispatchStream(m)
	defer s.Close()
	h := &recordingTransformHandler{events: make(chan wal.TransformLogStreamEvent, n+4), done: make(chan struct{})}
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	var original func(*recordingTransformHandler, wal.TransformLogStreamEvent)
	patch := mockey.Mock((*recordingTransformHandler).Handle).Origin(&original).To(func(h *recordingTransformHandler, e wal.TransformLogStreamEvent) {
		once.Do(func() { close(entered); <-release })
		original(h, e)
	}).Build()
	defer patch.UnPatch()
	sub, err := s.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{SubscriptionID: 42, VChannel: "v", Handler: h})
	require.NoError(t, err)
	defer sub.Close()
	defer releaseOnce.Do(func() { close(release) })
	require.EqualValues(t, 42, sub.ID())
	require.Equal(t, "v", sub.VChannel())
	<-entered
	require.Eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.outgoing) == transformStreamQueueEvents }, 3*time.Second, time.Millisecond)
	select {
	case <-s.Done():
		t.Fatal("history must yield to delivery capacity instead of failing")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	for i := 1; i <= n; i++ {
		require.Equal(t, uint64(i), nextTransformEvent(t, h).Entry.GetTimeTick())
	}
	require.Equal(t, uint64(n), nextTransformEvent(t, h).SyncUp.TimeTick)
	originalTxn := message.AsImmutableTxnMessage(newTestIdempotentTxnMessage(t, "v", 6000, "", [][]int64{{1}}))
	builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(originalTxn.Begin()))
	builder.Add(newTestIdempotentInsertMessage(t, "v", 6001, "", []int64{1}, []uint32{0}))
	builder.Add(newTestDeleteMessage(t, "v", 6002, 1, 99))
	builder.Add(newTestDeleteMessage(t, "v", 6003, 2, 100))
	txn, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(originalTxn.Commit()))
	require.NoError(t, err)
	m.ObserveMessage(t.Context(), txn)
	entry := nextTransformEvent(t, h).Entry
	require.Equal(t, txn.TimeTick(), entry.GetTimeTick())
	require.Len(t, entry.GetDelete().GetBlocks(), 2, "transaction must remain one complete entry")
}

func TestDispatchStreamCatchupOverflowAndFailure(t *testing.T) {
	for _, overflow := range []bool{false, true} {
		t.Run(fmt.Sprint(overflow), func(t *testing.T) {
			m := NewManager(ManagerConfig{})
			m.ObserveQueryBarrier("v", 10)
			s := NewDispatchStream(m)
			defer s.Close()
			h := newRecordingTransformHandler()
			entered := make(chan struct{})
			patch := mockey.Mock((*Manager).ReadTransform).To(func(_ *Manager, ctx context.Context, _ string, _, _ uint64, _ ReadLimits) (TransformBatch, error) {
				close(entered)
				if overflow {
					<-ctx.Done()
					return TransformBatch{}, ctx.Err()
				}
				return TransformBatch{}, ErrStoreCorrupted
			}).Build()
			defer patch.UnPatch()
			sub, err := s.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "v", Handler: h})
			require.NoError(t, err)
			defer sub.Close()
			<-entered
			if overflow {
				for i := 11; i <= transformStreamQueueEvents+11; i++ {
					m.ObserveMessage(t.Context(), newTestDeleteMessage(t, "v", uint64(i), 1, int64(i)))
				}
			}
			select {
			case <-s.Done():
			case <-time.After(time.Second):
				t.Fatal("failed catchup did not end stream")
			}
			if overflow {
				require.ErrorContains(t, s.Error(), "catchup backlog exceeded")
			} else {
				require.ErrorIs(t, s.Error(), ErrStoreCorrupted)
			}
			require.Empty(t, h.events, "failed replay must not acknowledge a suffix")
		})
	}
}

func TestDispatchStreamBoundedAndClosed(t *testing.T) {
	m := NewManager(ManagerConfig{})
	m.ObserveQueryBarrier("v", 20)
	s := NewDispatchStream(m)
	defer s.Close()
	_, err := s.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{})
	require.ErrorIs(t, err, wal.ErrTransformLogInvalidReadOption)
	h := newRecordingTransformHandler()
	sub, err := s.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "v", EndTimeTick: 20, Handler: h})
	require.NoError(t, err)
	require.Equal(t, uint64(20), nextTransformEvent(t, h).SyncUp.TimeTick)
	require.NoError(t, sub.Close())
	require.NoError(t, s.Close())
	_, err = s.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "v", Handler: h})
	require.ErrorIs(t, err, context.Canceled)
}

func TestDispatchStreamConcurrentCaptureHasNoGaps(t *testing.T) {
	m := NewManager(ManagerConfig{})
	s := NewDispatchStream(m)
	defer s.Close()
	const n = 2000
	start := make(chan struct{})
	written := make(chan struct{})
	go func() {
		defer close(written)
		<-start
		for i := 1; i <= n; i++ {
			m.ObserveMessage(t.Context(), newTestDeleteMessage(t, "v", uint64(i), 1, int64(i)))
		}
		m.ObserveQueryBarrier("v", n+1)
	}()
	close(start)
	h := &recordingTransformHandler{events: make(chan wal.TransformLogStreamEvent, n+10), done: make(chan struct{})}
	sub, err := s.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "v", Handler: h})
	require.NoError(t, err)
	defer sub.Close()
	seen := uint64(0)
	for {
		e := nextTransformEvent(t, h)
		if e.Entry != nil {
			seen++
			require.Equal(t, seen, e.Entry.GetTimeTick(), "no duplicate or gap across capture/replay/live")
		} else {
			require.LessOrEqual(t, e.SyncUp.TimeTick, seen+1, "progress cannot bypass a Delete")
			if e.SyncUp.TimeTick == n+1 {
				break
			}
		}
	}
	<-written
	require.EqualValues(t, n, seen)
}

func TestDispatchStreamFailureCancelsBoundedSibling(t *testing.T) {
	m := NewManager(ManagerConfig{})
	s := NewDispatchStream(m)
	defer s.Close()
	h := newRecordingTransformHandler()
	sub, err := s.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "v", EndTimeTick: 100, Handler: h})
	require.NoError(t, err)
	defer sub.Close()
	// A live attempt failure must stop even a bounded sibling blocked on data.
	s.cancel(ErrStoreCorrupted)
	select {
	case <-h.done:
	case <-time.After(time.Second):
		t.Fatal("bounded sibling remained alive after physical stream failure")
	}
	require.Empty(t, h.events)
}

func TestDispatchStreamAheadCursorGetsInitialSyncUpOnDelete(t *testing.T) {
	m := NewManager(ManagerConfig{})
	s := NewDispatchStream(m)
	defer s.Close()
	h := newRecordingTransformHandler()
	sub, err := s.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "v", StartAfterTimeTick: 100, Handler: h})
	require.NoError(t, err)
	defer sub.Close()
	waitDispatchLive(t, sub)
	m.ObserveMessage(t.Context(), newTestDeleteMessage(t, "v", 90, 1, 9))
	require.Empty(t, h.events)
	m.ObserveMessage(t.Context(), newTestDeleteMessage(t, "v", 110, 1, 11))
	require.EqualValues(t, 110, nextTransformEvent(t, h).Entry.GetTimeTick())
	require.EqualValues(t, 110, nextTransformEvent(t, h).SyncUp.TimeTick)
	m.ObserveMessage(t.Context(), newTestDeleteMessage(t, "v", 120, 1, 12))
	require.EqualValues(t, 120, nextTransformEvent(t, h).Entry.GetTimeTick())
	require.Empty(t, h.events, "ordinary live Deletes need no redundant SyncUp")
}

func TestDispatchStreamSharesImmutableLivePayload(t *testing.T) {
	m := NewManager(ManagerConfig{})
	s := NewDispatchStream(m)
	defer s.Close()
	handlers := []*recordingTransformHandler{newRecordingTransformHandler(), newRecordingTransformHandler()}
	for _, h := range handlers {
		sub, err := s.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "v", Handler: h})
		require.NoError(t, err)
		defer sub.Close()
		require.Zero(t, nextTransformEvent(t, h).SyncUp.TimeTick)
	}
	m.ObserveMessage(t.Context(), newTestDeleteMessage(t, "v", 10, 1, 123))
	first, second := nextTransformEvent(t, handlers[0]), nextTransformEvent(t, handlers[1])
	require.Same(t, first.Entry, second.Entry)
	m.mu.Lock()
	require.Same(t, m.pending[0].entry, first.Entry, "one built entry shared with Summary and all live subscribers")
	m.mu.Unlock()
}

func TestDispatchStreamKeepsExistingFastForwardPolicy(t *testing.T) {
	m, _ := newTestManagerWithStore(t)
	m.ObserveMessage(t.Context(), newTestDeleteMessage(t, "v", 10, 1, 10))
	require.NoError(t, persistSummary(t.Context(), m))
	m.AdvanceGCTimeTick("v", 10)
	m.cfg.RetentionMaxBytes = 1
	require.NoError(t, gcSummary(t.Context(), m))
	m.ObserveMessage(t.Context(), newTestDeleteMessage(t, "v", 20, 1, 20))
	s := NewDispatchStream(m)
	defer s.Close()
	h := newRecordingTransformHandler()
	sub, err := s.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "v", Handler: h})
	require.NoError(t, err)
	defer sub.Close()
	require.EqualValues(t, 20, nextTransformEvent(t, h).Entry.GetTimeTick())
	require.EqualValues(t, 20, nextTransformEvent(t, h).SyncUp.TimeTick)
	m.ObserveMessage(t.Context(), newTestDeleteMessage(t, "v", 30, 1, 30))
	require.EqualValues(t, 30, nextTransformEvent(t, h).Entry.GetTimeTick())
}
