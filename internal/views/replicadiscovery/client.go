package replicadiscovery

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// StreamFactory resolves and validates a local or remote physical attempt.
type StreamFactory func(context.Context) (Stream, error)

const stableWatchInterval = 10 * time.Second

// watcher owns one resumable PChannel stream, shared by its Manager.
// Logical subscriptions survive physical failures; availability does not.
type watcher struct {
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
	watcher  *watcher
	vchannel string
	state    *replicaSubscription
	handler  Handler
	closed   bool
	once     sync.Once
}

func newWatcher(ctx context.Context, pchannel string, factory StreamFactory) *watcher {
	ctx, cancel := context.WithCancel(ctx)
	w := &watcher{
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

func (w *watcher) subscribe(vchannel string, handler Handler) (*Subscription, error) {
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

func (s *Subscription) Close() {
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
}

func (w *watcher) Close() {
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
func (w *watcher) Done() <-chan struct{} { return w.done }

func (w *watcher) enqueueLocked(sub *Subscription) {
	w.pending[sub] = struct{}{}
	select {
	case w.dispatchWake <- struct{}{}:
	default:
	}
}

func (w *watcher) notifyLocked(state *replicaSubscription) {
	for sub := range state.handlers {
		w.enqueueLocked(sub)
	}
}

// One dispatcher per PChannel; callbacks do not hold state/transport locks.
func (w *watcher) dispatch() {
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

func (w *watcher) signal() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *watcher) invalidateLocked() {
	for _, state := range w.subscriptions {
		if state.valid {
			state.valid = false
			state.replicas = nil
			w.notifyLocked(state)
		}
	}
}

func (w *watcher) run() {
	defer func() {
		w.mu.Lock()
		w.invalidateLocked()
		w.mu.Unlock()
		close(w.done)
	}()
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
		stable := false
		if err == nil {
			started := time.Now()
			err = w.runAttempt(ctx, cancel, stream)
			stable = time.Since(started) >= stableWatchInterval
		}
		cancel()
		w.mu.Lock()
		w.attemptCancel = nil
		idle := len(w.subscriptions) == 0
		w.invalidateLocked()
		w.mu.Unlock()
		if w.ctx.Err() != nil {
			return
		}
		if idle {
			continue
		}
		mlog.Warn(w.ctx, "replica watch disconnected; rediscovering owner", mlog.FieldPChannel(w.pchannel), mlog.Err(err))
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

type watchedReplica struct {
	state   *replicaSubscription
	closing bool
}

type watchResponse struct {
	response *viewpb.WatchVChannelReplicasResponse
	err      error
}

func (w *watcher) runAttempt(ctx context.Context, cancel context.CancelFunc, stream Stream) error {
	// The attempt is canceled before joining Recv. No old receiver may publish
	// into a replacement connection's cache, or block logical Close.
	responses := make(chan watchResponse)
	received := make(chan struct{})
	defer func() { cancel(); stream.Close(); <-received }()
	go func() {
		defer close(received)
		for {
			resp, err := stream.Recv()
			select {
			case responses <- watchResponse{resp, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	active := make(map[string]*watchedReplica)
	reconcile := true
	for {
		if reconcile {
			if err := w.reconcile(stream, active); err != nil {
				return err
			}
			reconcile = false
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.wake:
			reconcile = true
		case result := <-responses:
			if result.err != nil {
				return result.err
			}
			switch response := result.response.GetResponse().(type) {
			case *viewpb.WatchVChannelReplicasResponse_Snapshot:
				snapshot := response.Snapshot
				current := active[snapshot.GetVchannel()]
				if current == nil || current.closing {
					continue
				}
				w.mu.Lock()
				if w.subscriptions[snapshot.GetVchannel()] == current.state {
					state := current.state
					replicas := replicasFromProto(snapshot.GetReplicas())
					if !state.valid || !slices.Equal(state.replicas, replicas) {
						state.replicas = replicas
						state.valid = true
						w.notifyLocked(state)
					}
				}
				w.mu.Unlock()
			case *viewpb.WatchVChannelReplicasResponse_UnsubscribedVchannel:
				current := active[response.UnsubscribedVchannel]
				if current == nil || !current.closing {
					return status.NewInvalidRequestSeq("unexpected replica unsubscribe acknowledgement")
				}
				delete(active, response.UnsubscribedVchannel)
				reconcile = true
			default:
				return status.NewInvalidRequestSeq("unexpected replica watch response")
			}
		}
	}
}

func (w *watcher) reconcile(stream Stream, active map[string]*watchedReplica) error {
	// Snapshot desired state under mu; never hold it during network I/O.
	w.mu.Lock()
	desired := make(map[string]*replicaSubscription, len(w.subscriptions))
	for vc, state := range w.subscriptions {
		desired[vc] = state
	}
	w.mu.Unlock()
	for vc, current := range active {
		if desired[vc] != current.state && !current.closing {
			current.closing = true
			if err := stream.Send(&viewpb.WatchVChannelReplicasRequest{Vchannel: vc, Unsubscribe: true}); err != nil {
				return err
			}
		}
	}
	for vc, state := range desired {
		if active[vc] == nil {
			active[vc] = &watchedReplica{state: state}
			if err := stream.Send(&viewpb.WatchVChannelReplicasRequest{Vchannel: vc}); err != nil {
				return err
			}
		}
	}
	return nil
}
