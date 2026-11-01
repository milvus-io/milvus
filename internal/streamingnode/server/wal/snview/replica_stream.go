package snview

import (
	"context"
	"slices"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// AcquireStream subscribes directly to the directory. No RPC messages or ACKs
// exist on this path. One delivery loop coalesces updates for all VChannels.
func (d *replicaDirectory) AcquireStream(ctx context.Context, pchannel string, unavailable <-chan struct{}) wal.ReplicaWatchStream {
	ctx, cancel := context.WithCancel(ctx)
	s := &directoryStream{ctx: ctx, cancel: cancel, pchannel: pchannel, watch: d.Watch(), unavailable: unavailable, done: make(chan struct{}), subs: make(map[string]map[*directorySubscription]struct{})}
	go s.run()
	return s
}

type directoryStream struct {
	ctx         context.Context
	cancel      context.CancelFunc
	pchannel    string
	watch       *replicaWatch
	unavailable <-chan struct{}
	done        chan struct{}
	mu          sync.Mutex
	err         error
	subs        map[string]map[*directorySubscription]struct{}
}

type directorySubscription struct {
	stream   *directoryStream
	vchannel string
	mu       sync.Mutex
	handler  wal.ReplicaWatchHandler
	snapshot qviews.ReplicaSnapshot
	closed   bool
}

func (s *directoryStream) Subscribe(ctx context.Context, vc string, handler wal.ReplicaWatchHandler) (wal.ReplicaWatchSubscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if handler == nil || vc == "" || funcutil.ToPhysicalChannel(vc) != s.pchannel {
		return nil, status.NewInvalidArgument("replica subscription requires a handler and a VChannel in this PChannel")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return nil, status.NewOnShutdownError("replica directory stream closed")
	}
	sub := &directorySubscription{stream: s, vchannel: vc, handler: handler}
	if s.subs[vc] == nil {
		s.subs[vc] = make(map[*directorySubscription]struct{})
	}
	s.subs[vc][sub] = struct{}{}
	// Every new handler needs a full initial snapshot, even for an existing VC.
	s.watch.Subscribe(vc)
	s.watch.directory.mu.Lock()
	s.watch.enqueueLocked(vc)
	s.watch.directory.mu.Unlock()
	return sub, nil
}
func (s *directoryStream) Done() <-chan struct{} { return s.done }
func (s *directoryStream) Error() error          { s.mu.Lock(); defer s.mu.Unlock(); return s.err }
func (s *directoryStream) Close() error          { s.cancel(); <-s.done; return s.Error() }
func (s *directoryStream) run() {
	defer close(s.done)
	defer s.watch.Close()
	defer func() {
		s.cancel()
		s.mu.Lock()
		subs := s.subs
		s.subs = nil
		s.mu.Unlock()
		for _, handlers := range subs {
			for sub := range handlers {
				sub.mu.Lock()
				sub.closed = true
				sub.handler = nil
				sub.mu.Unlock()
			}
		}
	}()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.unavailable:
			s.fail()
			return
		case <-s.watch.Done():
			s.fail()
			return
		case <-s.watch.Changed():
			// Capture subscriptions and snapshot together with respect to Subscribe/Close.
			s.mu.Lock()
			snapshot := s.watch.Pop()
			var subs []*directorySubscription
			if snapshot != nil {
				for sub := range s.subs[snapshot.GetVchannel()] {
					subs = append(subs, sub)
				}
			}
			s.mu.Unlock()
			if snapshot != nil {
				for _, sub := range subs {
					sub.deliver(snapshot)
				}
			}
		}
	}
}

func (s *directoryStream) fail() {
	s.mu.Lock()
	s.err = status.NewOnShutdownError("replica WAL owner unavailable")
	s.mu.Unlock()
}

func (s *directorySubscription) deliver(update *replicaDirectorySnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	// Defer only the first snapshot. Existing subscribers must see retired
	// Up replicas disappear even while replacement views are preparing.
	if !s.snapshot.Valid && len(update.GetReplicas()) == 0 && update.preparing {
		return
	}
	snapshot := qviews.ReplicaSnapshot{Valid: true, Replicas: qviews.ReplicasFromProto(update.GetReplicas())}
	s.snapshot = qviews.ReplicaSnapshot{Valid: true, Replicas: slices.Clone(snapshot.Replicas)}
	s.handler(snapshot)
}

func (s *directorySubscription) Snapshot() qviews.ReplicaSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return qviews.ReplicaSnapshot{Valid: !s.closed && s.stream.ctx.Err() == nil && s.snapshot.Valid, Replicas: slices.Clone(s.snapshot.Replicas)}
}

func (s *directorySubscription) Close() error {
	s.mu.Lock()
	s.closed = true
	s.handler = nil
	s.mu.Unlock()
	s.stream.mu.Lock()
	defer s.stream.mu.Unlock()
	delete(s.stream.subs[s.vchannel], s)
	if len(s.stream.subs[s.vchannel]) == 0 {
		delete(s.stream.subs, s.vchannel)
		s.stream.watch.Unsubscribe(s.vchannel)
	}
	return nil
}
