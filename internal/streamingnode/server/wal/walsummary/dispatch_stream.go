package walsummary

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
)

const (
	transformStreamQueueBytes  = 8 << 20
	transformStreamQueueEvents = 4096
)

// DispatchStream follows the PChannel's ordered observation after a bounded
// Summary replay. Only the physical stream owns workers; idle subscriptions own
// cursors and references, never a goroutine. All payloads are immutable copies,
// shared with Summary, and carry no recovery acknowledgement handles.
type DispatchStream struct {
	manager       *Manager
	historical    *Stream
	ctx           context.Context
	cancel        context.CancelCauseFunc
	mu            sync.Mutex
	subs          map[*dispatchSubscription]struct{}
	catchups      []*dispatchSubscription
	outgoing      []transformDelivery
	outgoingBytes int
	pendingBytes  int
	pendingEvents int
	changed       chan struct{}
	wg            sync.WaitGroup
}

type transformDelivery struct {
	sub   *dispatchSubscription
	event wal.TransformLogStreamEvent
	size  int
}

type dispatchSubscription struct {
	stream        *DispatchStream
	opt           wal.TransformLogSubscriptionOption
	ctx           context.Context
	cancel        context.CancelFunc
	stop          func() bool
	closeOnce     sync.Once
	handleMu      sync.Mutex
	replayThrough uint64 // immutable capture, distinct from the live frontier
	replayAfter   uint64 // owned by the catchup worker
	// Protected by stream.mu. Manager.mu serializes registration with observation.
	live        bool
	needsSyncUp bool // replay started ahead of the new owner's coverage
	through     uint64
	pending     []transformDelivery
}

func NewDispatchStream(manager *Manager) *DispatchStream {
	ctx, cancel := context.WithCancelCause(context.Background())
	s := &DispatchStream{
		manager: manager, historical: NewStream(manager), ctx: ctx, cancel: cancel,
		subs: make(map[*dispatchSubscription]struct{}), changed: make(chan struct{}),
	}
	context.AfterFunc(s.historical.ctx, func() { s.cancel(s.historical.Error()) })
	// Bounded and continuous subscriptions share a physical stream's lifetime.
	// A live overflow/failure must also cancel a bounded read waiting for coverage.
	context.AfterFunc(s.ctx, func() { s.historical.cancel(s.Error()) })
	s.wg.Add(2)
	go s.catchupLoop()
	go s.deliverLoop()
	return s
}

func (s *DispatchStream) Done() <-chan struct{} { return s.ctx.Done() }
func (s *DispatchStream) Error() error          { return context.Cause(s.ctx) }

func (s *DispatchStream) Subscribe(ctx context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
	if err := context.Cause(s.ctx); err != nil {
		return nil, err
	}
	if opt.EndTimeTick != 0 {
		return s.historical.Subscribe(ctx, opt)
	}
	if opt.VChannel == "" || opt.Handler == nil {
		return nil, wal.ErrTransformLogInvalidReadOption
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	subctx, cancel := context.WithCancel(s.ctx)
	sub := &dispatchSubscription{stream: s, opt: opt, ctx: subctx, cancel: cancel}
	// Capture the complete readable prefix and install its live suffix under the
	// exact lock used to publish Summary records. No I/O or callback runs here.
	s.manager.mu.Lock()
	s.mu.Lock()
	err := s.manager.terminalErr
	if err == nil {
		err = context.Cause(s.ctx)
	}
	if err != nil {
		s.mu.Unlock()
		s.manager.mu.Unlock()
		cancel()
		return nil, err
	}
	sub.through = max(s.manager.readableThrough, s.manager.queryBarriers[opt.VChannel])
	sub.replayThrough = sub.through
	sub.replayAfter = opt.StartAfterTimeTick
	if s.manager.transformSubscribers == nil {
		s.manager.transformSubscribers = make(map[string]map[*dispatchSubscription]struct{})
	}
	if s.manager.transformSubscribers[opt.VChannel] == nil {
		s.manager.transformSubscribers[opt.VChannel] = make(map[*dispatchSubscription]struct{})
	}
	s.manager.transformSubscribers[opt.VChannel][sub] = struct{}{}
	s.subs[sub] = struct{}{}
	s.catchups = append(s.catchups, sub)
	// Install cancellation before exposing the subscription to either worker.
	sub.stop = context.AfterFunc(ctx, func() { _ = sub.Close() })
	s.signalLocked()
	s.mu.Unlock()
	s.manager.mu.Unlock()
	return sub, nil
}

func (s *DispatchStream) signalLocked() { close(s.changed); s.changed = make(chan struct{}) }

func (s *DispatchStream) Close() error {
	s.cancel(context.Canceled)
	s.mu.Lock()
	subs := make([]*dispatchSubscription, 0, len(s.subs))
	for sub := range s.subs {
		subs = append(subs, sub)
	}
	s.mu.Unlock()
	for _, sub := range subs {
		_ = sub.Close()
	}
	_ = s.historical.Close()
	s.wg.Wait()
	s.mu.Lock()
	s.catchups = nil
	s.outgoing = nil
	s.outgoingBytes = 0
	s.mu.Unlock()
	return nil
}

func (s *dispatchSubscription) ID() int64        { return s.opt.SubscriptionID }
func (s *dispatchSubscription) VChannel() string { return s.opt.VChannel }
func (s *dispatchSubscription) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		stream := s.stream
		stream.manager.mu.Lock()
		s.stop()
		subscribers := stream.manager.transformSubscribers[s.opt.VChannel]
		delete(subscribers, s)
		if len(subscribers) == 0 {
			delete(stream.manager.transformSubscribers, s.opt.VChannel)
		}
		stream.mu.Lock()
		delete(stream.subs, s)
		catchups := stream.catchups[:0]
		for _, pending := range stream.catchups {
			if pending != s {
				catchups = append(catchups, pending)
			}
		}
		clear(stream.catchups[len(catchups):])
		stream.catchups = catchups
		kept := stream.outgoing[:0]
		for _, delivery := range stream.outgoing {
			if delivery.sub == s {
				stream.outgoingBytes -= delivery.size
			} else {
				kept = append(kept, delivery)
			}
		}
		clear(stream.outgoing[len(kept):])
		stream.outgoing = kept
		for _, delivery := range s.pending {
			stream.pendingBytes -= delivery.size
			stream.pendingEvents--
		}
		s.pending = nil
		stream.signalLocked()
		stream.mu.Unlock()
		stream.manager.mu.Unlock()
		// Callers must abort a blocked Handler before joining Close, as with the
		// historical stream. The serverSubscription already implements this gate.
		s.handleMu.Lock()
		s.opt.Handler.Close()
		s.handleMu.Unlock()
	})
	return nil
}

func (s *DispatchStream) queueHasSpaceLocked(size int) bool {
	// Admit one indivisible oversized transaction into an otherwise empty queue.
	return len(s.outgoing) == 0 || (len(s.outgoing) < transformStreamQueueEvents && s.outgoingBytes+size <= transformStreamQueueBytes)
}

func (s *DispatchStream) enqueueLocked(delivery transformDelivery) {
	s.outgoing = append(s.outgoing, delivery)
	s.outgoingBytes += delivery.size
	s.signalLocked()
}

func (s *DispatchStream) enqueue(ctx context.Context, delivery transformDelivery) error {
	for {
		s.mu.Lock()
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			return err
		}
		if s.queueHasSpaceLocked(delivery.size) {
			s.enqueueLocked(delivery)
			s.mu.Unlock()
			return nil
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (s *DispatchStream) deliverLoop() {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		if s.ctx.Err() != nil {
			s.outgoing = nil
			s.outgoingBytes = 0
			s.mu.Unlock()
			return
		}
		if len(s.outgoing) == 0 {
			changed := s.changed
			s.mu.Unlock()
			select {
			case <-s.ctx.Done():
				return
			case <-changed:
			}
			continue
		}
		delivery := s.outgoing[0]
		s.outgoing[0] = transformDelivery{}
		s.outgoing = s.outgoing[1:]
		s.outgoingBytes -= delivery.size
		s.signalLocked()
		s.mu.Unlock()
		sub := delivery.sub
		sub.handleMu.Lock()
		if sub.ctx.Err() == nil {
			sub.opt.Handler.Handle(delivery.event)
		}
		sub.handleMu.Unlock()
	}
}

func (s *DispatchStream) catchupLoop() {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		if s.ctx.Err() != nil {
			s.catchups = nil
			s.mu.Unlock()
			return
		}
		if len(s.catchups) == 0 {
			changed := s.changed
			s.mu.Unlock()
			select {
			case <-s.ctx.Done():
				return
			case <-changed:
			}
			continue
		}
		sub := s.catchups[0]
		s.catchups[0] = nil
		s.catchups = s.catchups[1:]
		s.mu.Unlock()
		done, err := sub.catchupPage()
		if sub.ctx.Err() != nil {
			continue
		}
		if err != nil {
			s.cancel(err)
			continue
		}
		if !done {
			s.mu.Lock()
			if sub.ctx.Err() == nil {
				s.catchups = append(s.catchups, sub)
				s.signalLocked()
			}
			s.mu.Unlock()
		}
	}
}

func (s *dispatchSubscription) delivery(entry *streamingpb.TransformLogEntry, tick uint64) transformDelivery {
	event := wal.TransformLogStreamEvent{SubscriptionID: s.opt.SubscriptionID, VChannel: s.opt.VChannel, Entry: entry}
	size := 32
	if entry == nil {
		event.SyncUp = &wal.TransformLogSyncUp{TimeTick: tick}
	} else {
		size += proto.Size(entry)
	}
	return transformDelivery{sub: s, event: event, size: size}
}

func (s *dispatchSubscription) catchupPage() (bool, error) {
	after, target := s.replayAfter, s.replayThrough
	if after < target {
		batch, err := s.stream.manager.ReadTransform(s.ctx, s.opt.VChannel, after, target, ReadLimits{MaxRows: 4096, MaxBytes: 4 << 20})
		if err != nil {
			return false, err
		}
		for _, entry := range batch.Entries {
			if err := s.stream.enqueue(s.ctx, s.delivery(entry, 0)); err != nil {
				return false, err
			}
		}
		s.replayAfter = max(batch.CoveredThrough, min(batch.FastForwardTimeTick, target))
		if s.replayAfter < target {
			return false, nil
		}
	}
	// The pending suffix and switch to Live share the producer lock. SyncUp is
	// queued after the entire captured suffix, never ahead of an earlier Delete.
	stream := s.stream
	for delivered := 0; delivered < 64; delivered++ {
		stream.mu.Lock()
		if err := s.ctx.Err(); err != nil {
			stream.mu.Unlock()
			return false, err
		}
		if len(s.pending) > 0 {
			delivery := s.pending[0]
			if stream.queueHasSpaceLocked(delivery.size) {
				s.pending[0] = transformDelivery{}
				s.pending = s.pending[1:]
				stream.pendingBytes -= delivery.size
				stream.pendingEvents--
				stream.enqueueLocked(delivery)
				stream.mu.Unlock()
				continue
			}
		} else if s.through < s.opt.StartAfterTimeTick {
			// A resume cursor is not proof that the new owner has replayed this prefix.
			s.live = true
			s.needsSyncUp = true
			stream.mu.Unlock()
			return true, nil
		} else {
			delivery := s.delivery(nil, s.through)
			if stream.queueHasSpaceLocked(delivery.size) {
				stream.enqueueLocked(delivery)
				s.live = true
				stream.mu.Unlock()
				return true, nil
			}
		}
		changed := stream.changed
		stream.mu.Unlock()
		select {
		case <-s.ctx.Done():
			return false, s.ctx.Err()
		case <-changed:
		}
	}
	return false, nil
}

// Called only by ordered PChannel observation while Manager.mu is held. This
// path does not wait for storage, callbacks, gRPC, or a slow subscription.
func (m *Manager) dispatchTransformLocked(vchannel string, entry *streamingpb.TransformLogEntry, tick uint64) {
	size := 32 + proto.Size(entry)
	for sub := range m.transformSubscribers[vchannel] {
		stream := sub.stream
		stream.mu.Lock()
		if sub.ctx.Err() != nil || tick <= sub.through {
			stream.mu.Unlock()
			continue
		}
		sub.through = tick
		if tick < sub.opt.StartAfterTimeTick {
			stream.mu.Unlock()
			continue
		}
		delivery := sub.delivery(nil, tick)
		if entry != nil && tick > sub.opt.StartAfterTimeTick {
			delivery.event.Entry = entry
			delivery.event.SyncUp = nil
			delivery.size = size
		}
		if sub.live {
			// A whole ordered Delete already proves coverage through its tick.
			// Only payload-free progress needs a separate SyncUp.
			if !stream.queueHasSpaceLocked(delivery.size) {
				stream.cancel(status.NewOnShutdownError("transform stream delivery backlog exceeded"))
			} else {
				stream.enqueueLocked(delivery)
				if sub.needsSyncUp {
					// A new owner may initially lag the resume cursor. Even if
					// coverage first arrives as a Delete, QN still needs its
					// initial catchup signal, ordered after that complete entry.
					if delivery.event.Entry != nil {
						syncUp := sub.delivery(nil, tick)
						if !stream.queueHasSpaceLocked(syncUp.size) {
							stream.cancel(status.NewOnShutdownError("transform stream delivery backlog exceeded"))
						} else {
							stream.enqueueLocked(syncUp)
						}
					}
					sub.needsSyncUp = false
				}
			}
		} else if delivery.event.Entry != nil {
			if stream.pendingEvents > 0 && (stream.pendingEvents >= transformStreamQueueEvents || stream.pendingBytes+delivery.size > transformStreamQueueBytes) {
				stream.cancel(status.NewOnShutdownError("transform stream catchup backlog exceeded"))
			} else {
				sub.pending = append(sub.pending, delivery)
				stream.pendingBytes += delivery.size
				stream.pendingEvents++
			}
		}
		stream.mu.Unlock()
	}
}

func (m *Manager) dispatchTransformMessageLocked(msg message.ImmutableMessage, entry *streamingpb.TransformLogEntry) {
	if msg.IsPChannelLevel() || msg.VChannel() == "" {
		if messageutil.IsPChannelTransformBarrier(msg.MessageType()) || msg.MessageType() == message.MessageTypeRecoveryBarrier {
			for vchannel := range m.transformSubscribers {
				m.dispatchTransformLocked(vchannel, nil, msg.TimeTick())
			}
		}
		return
	}
	if msg.MessageType() == message.MessageTypeTxn {
		if messageutil.ClassifyTransformLogMessage(msg) == messageutil.TransformLogKindNone {
			return
		}
		// Classify the assembled transaction, but use the original outer timestamp.
		if messageutil.AdvancesQueryTransformMVCC(message.AsImmutableTxnMessage(msg).Commit()) {
			m.dispatchTransformLocked(msg.VChannel(), entry, msg.TimeTick())
		}
	} else if messageutil.AdvancesQueryTransformMVCC(msg) {
		m.dispatchTransformLocked(msg.VChannel(), entry, msg.TimeTick())
	}
}
