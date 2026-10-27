package replicadiscovery

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// StreamFactory resolves and validates a local or remote physical attempt.
type StreamFactory func(context.Context) (Stream, error)

const stableWatchInterval = 10 * time.Second

// resumableStream owns one resumable PChannel stream, shared by its Client.
// Logical subscriptions survive physical failures; availability does not.
type resumableStream struct {
	ctx           context.Context
	cancel        context.CancelFunc
	pchannel      string
	factory       StreamFactory
	mu            sync.Mutex
	subscriptions map[string]*replicaSubscription
	attemptCancel context.CancelFunc
	wake          chan struct{}
	done          chan struct{}
	dispatchDone  chan struct{}
	dispatchWake  chan struct{}
	pending       map[*Subscription]struct{}
}

type replicaSubscription struct {
	replicas []Replica
	valid    bool
	handlers map[*Subscription]struct{}
}

// Subscription owns one handler registration, not a goroutine.
type Subscription struct {
	watcher  *resumableStream
	vchannel string
	state    *replicaSubscription
	handler  Handler
	closed   bool
	once     sync.Once
}

func newResumableStream(ctx context.Context, pchannel string, factory StreamFactory) *resumableStream {
	ctx, cancel := context.WithCancel(ctx)
	w := &resumableStream{
		ctx: ctx, cancel: cancel, pchannel: pchannel, factory: factory,
		subscriptions: make(map[string]*replicaSubscription),
		wake:          make(chan struct{}, 1), done: make(chan struct{}),
		dispatchDone: make(chan struct{}), dispatchWake: make(chan struct{}, 1),
		pending: make(map[*Subscription]struct{}),
	}
	go w.dispatch()
	go w.run()
	return w
}

func (w *resumableStream) subscribe(vchannel string, handler Handler) (*Subscription, error) {
	if vchannel == "" || funcutil.ToPhysicalChannel(vchannel) != w.pchannel {
		return nil, status.NewInvalidRequestSeq("replica subscription VChannel does not belong to this PChannel")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.ctx.Err(); err != nil {
		return nil, err
	}
	state := w.subscriptions[vchannel]
	if state == nil {
		state = &replicaSubscription{handlers: make(map[*Subscription]struct{})}
		w.subscriptions[vchannel] = state
		w.signal()
	}
	sub := &Subscription{watcher: w, vchannel: vchannel, state: state, handler: handler}
	state.handlers[sub] = struct{}{}
	w.enqueueLocked(sub)
	return sub, nil
}

// Snapshot reads the cache synchronously. Notifications are delivered by Handler.
func (s *Subscription) Snapshot() Snapshot {
	s.watcher.mu.Lock()
	defer s.watcher.mu.Unlock()
	return s.snapshotLocked()
}

func (s *Subscription) snapshotLocked() Snapshot {
	return Snapshot{Replicas: slices.Clone(s.state.replicas), Valid: !s.closed && s.watcher.ctx.Err() == nil && s.state.valid}
}

func (s *Subscription) Close() error {
	s.once.Do(func() {
		w := s.watcher
		w.mu.Lock()
		defer w.mu.Unlock()
		s.closed = true
		s.handler = nil
		delete(w.pending, s)
		delete(s.state.handlers, s)
		if len(s.state.handlers) == 0 {
			delete(w.subscriptions, s.vchannel)
			if len(w.subscriptions) == 0 && w.attemptCancel != nil {
				w.attemptCancel()
			}
			w.signal()
		}
	})
	return nil
}

func (w *resumableStream) Close() {
	w.cancel()
	<-w.done
	<-w.dispatchDone
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, state := range w.subscriptions {
		state.replicas = nil
		state.valid = false
		for sub := range state.handlers {
			sub.handler = nil
		}
		state.handlers = nil
	}
	w.subscriptions = nil
	w.pending = nil
}
func (w *resumableStream) Done() <-chan struct{} { return w.done }

func (w *resumableStream) enqueueLocked(sub *Subscription) {
	w.pending[sub] = struct{}{}
	select {
	case w.dispatchWake <- struct{}{}:
	default:
	}
}

func (w *resumableStream) notifyLocked(state *replicaSubscription) {
	for sub := range state.handlers {
		w.enqueueLocked(sub)
	}
}

// One dispatcher per PChannel; callbacks do not hold state/transport locks.
func (w *resumableStream) dispatch() {
	defer close(w.dispatchDone)
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.dispatchWake:
		}
		for w.ctx.Err() == nil {
			w.mu.Lock()
			var next *Subscription
			var snapshot Snapshot
			var handler Handler
			for sub := range w.pending {
				delete(w.pending, sub)
				next = sub
				handler = sub.handler
				snapshot = sub.snapshotLocked()
				break
			}
			w.mu.Unlock()
			if next == nil {
				break
			}
			handler(snapshot)
		}
	}
}

func (w *resumableStream) signal() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *resumableStream) invalidateLocked() {
	for _, state := range w.subscriptions {
		if state.valid {
			state.valid = false
			state.replicas = nil
			w.notifyLocked(state)
		}
	}
}

func (w *resumableStream) run() {
	defer func() { w.mu.Lock(); w.invalidateLocked(); w.mu.Unlock(); close(w.done) }()
	retry := backoff.NewExponentialBackOff()
	retry.InitialInterval = 100 * time.Millisecond
	retry.MaxInterval = 10 * time.Second
	retry.MaxElapsedTime = 0
	retry.Reset()
	for w.ctx.Err() == nil {
		w.mu.Lock()
		if len(w.subscriptions) == 0 {
			w.mu.Unlock()
			select {
			case <-w.ctx.Done():
				return
			case <-w.wake:
			}
			continue
		}
		ctx, cancel := context.WithCancel(w.ctx)
		w.attemptCancel = cancel
		w.mu.Unlock()
		stream, err := w.factory(ctx)
		w.mu.Lock()
		w.attemptCancel = nil
		w.mu.Unlock()
		// Only last-subscriber removal can cancel acquisition while the owner is live.
		if ctx.Err() != nil && w.ctx.Err() == nil {
			cancel()
			if stream != nil {
				_ = stream.Close()
			}
			continue
		}
		idle, stable := false, false
		if err == nil {
			idle, stable, err = w.runAttempt(ctx, stream)
		}
		cancel()
		if stream != nil {
			_ = stream.Close()
		}
		w.mu.Lock()
		w.invalidateLocked()
		w.mu.Unlock()
		if w.ctx.Err() != nil {
			return
		}
		if idle {
			retry.Reset()
			continue
		}
		mlog.Warn(w.ctx, "replica watch unavailable; rediscovering owner", mlog.FieldPChannel(w.pchannel), mlog.Err(err))
		if stable {
			retry.Reset()
		}
		timer := time.NewTimer(retry.NextBackOff())
		select {
		case <-w.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

const idleStreamTimeout = time.Minute

// Physical subscriptions own delivery. This loop only reconciles logical demand.
func (w *resumableStream) runAttempt(ctx context.Context, stream Stream) (bool, bool, error) {
	active := make(map[*replicaSubscription]wal.ReplicaWatchSubscription)
	reconcile := func() error {
		w.mu.Lock()
		desired := make(map[*replicaSubscription]string, len(w.subscriptions))
		for vc, state := range w.subscriptions {
			desired[state] = vc
		}
		w.mu.Unlock()
		for state, sub := range active {
			if _, ok := desired[state]; !ok {
				if err := sub.Close(); err != nil {
					return err
				}
				delete(active, state)
			}
		}
		for state, vc := range desired {
			if active[state] != nil {
				continue
			}
			sub, err := stream.Subscribe(ctx, vc, func(snapshot Snapshot) {
				w.mu.Lock()
				defer w.mu.Unlock()
				if w.subscriptions[vc] != state || ctx.Err() != nil {
					return
				}
				if state.valid != snapshot.Valid || !slices.Equal(state.replicas, snapshot.Replicas) {
					state.valid = snapshot.Valid
					state.replicas = slices.Clone(snapshot.Replicas)
					w.notifyLocked(state)
				}
			})
			if err != nil {
				return err
			}
			active[state] = sub
		}
		return nil
	}
	if err := reconcile(); err != nil {
		return false, false, err
	}
	availableSince := time.Now()
	timer := time.NewTimer(idleStreamTimeout)
	timer.Stop()
	defer timer.Stop()
	var idleC <-chan time.Time
	for {
		w.mu.Lock()
		idle := len(w.subscriptions) == 0
		w.mu.Unlock()
		if idle && idleC == nil {
			timer.Reset(idleStreamTimeout)
			idleC = timer.C
		} else if !idle && idleC != nil {
			timer.Stop()
			idleC = nil
		}
		select {
		case <-ctx.Done():
			return false, false, ctx.Err()
		case <-stream.Done():
			return false, time.Since(availableSince) >= stableWatchInterval, stream.Error()
		case <-idleC:
			idleC = nil
			w.mu.Lock()
			idle = len(w.subscriptions) == 0
			w.mu.Unlock()
			if idle {
				return true, false, nil
			}
		case <-w.wake:
			if err := reconcile(); err != nil {
				return false, time.Since(availableSince) >= stableWatchInterval, err
			}
		}
	}
}
