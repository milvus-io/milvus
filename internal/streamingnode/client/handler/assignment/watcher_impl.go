package assignment

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func NewWatcher(r resolver.Resolver) *watcherImpl {
	ctx, cancel := context.WithCancel(context.Background())
	w := &watcherImpl{
		ctx:         ctx,
		cancel:      cancel,
		r:           r,
		cond:        *syncutil.NewContextCond(&sync.Mutex{}),
		assignments: make(map[string]types.PChannelInfoAssigned),
		changed:     make(chan struct{}),
	}
	go w.execute()
	return w
}

// watcherImpl is the implementation of the assignment watcher.
type watcherImpl struct {
	ctx         context.Context
	cancel      context.CancelFunc
	r           resolver.Resolver
	cond        syncutil.ContextCond
	assignments map[string]types.PChannelInfoAssigned
	readers     map[string][]types.PChannelInfoAssigned
	next        uint64
	changed     chan struct{}
}

// execute starts the watcher.
func (w *watcherImpl) execute() {
	mlog.Info(w.ctx, "assignment watcher start")
	var err error
	defer func() {
		w.cond.LockAndBroadcast()
		close(w.changed)
		w.changed = nil
		w.cond.L.Unlock()
		mlog.Info(w.ctx, "assignment watcher close", mlog.Err(err))
	}()

	// error can be ignored here, error is always cancel by watcher's close as expected.
	// otherwise, the resolver's close is unexpected.
	err = w.r.Watch(w.ctx, func(state discoverer.VersionedState) error {
		w.updateAssignment(state)
		return nil
	})
}

// updateAssignment updates the assignment.
func (w *watcherImpl) updateAssignment(state discoverer.VersionedState) {
	newAssignments := make(map[string]types.PChannelInfoAssigned)
	readers := make(map[string][]types.PChannelInfoAssigned)
	for _, assignments := range state.ChannelAssignmentInfo() {
		for name, ch := range assignments.ReadOnlyChannels {
			readers[name] = append(readers[name], types.PChannelInfoAssigned{Channel: ch, Node: assignments.NodeInfo})
		}
		for _, pChannelInfo := range assignments.Channels {
			newAssignments[pChannelInfo.Name] = types.PChannelInfoAssigned{
				Channel: pChannelInfo,
				Node:    assignments.NodeInfo,
			}
		}
	}
	w.cond.LockAndBroadcast()
	w.assignments = newAssignments
	w.readers = readers
	close(w.changed)
	w.changed = make(chan struct{})
	w.cond.L.Unlock()
}

// Get gets the current pchannel assignment.
func (w *watcherImpl) Get(ctx context.Context, channel string) *types.PChannelInfoAssigned {
	w.cond.L.Lock()
	defer w.cond.L.Unlock()

	if info, ok := w.assignments[channel]; ok {
		return &info
	}
	return nil
}

// Readable returns each read-only candidate once in rotating order, then the writer.
func (w *watcherImpl) Readable(channel string) ([]types.PChannelInfoAssigned, <-chan struct{}) {
	w.cond.L.Lock()
	defer w.cond.L.Unlock()
	if w.changed == nil {
		return nil, nil
	}
	readers := w.readers[channel]
	routes := make([]types.PChannelInfoAssigned, 0, len(readers)+1)
	if len(readers) > 0 {
		start := int(w.next % uint64(len(readers)))
		w.next++
		routes = append(routes, readers[start:]...)
		routes = append(routes, readers[:start]...)
	}
	if writer, ok := w.assignments[channel]; ok {
		routes = append(routes, writer)
	}
	return routes, w.changed
}

// Watch watches the channel assignment.
func (w *watcherImpl) Watch(ctx context.Context, channel string, previous *types.PChannelInfoAssigned) error {
	w.cond.L.Lock()

	term := types.InitialTerm
	if previous != nil {
		term = previous.Channel.Term
	}

	for {
		if w.changed == nil {
			w.cond.L.Unlock()
			return context.Canceled
		}
		if info, ok := w.assignments[channel]; ok {
			if info.Channel.Term > term {
				break
			}
		}
		if err := w.cond.Wait(ctx); err != nil {
			return err
		}
	}
	w.cond.L.Unlock()
	return nil
}

// Close closes the watcher.
func (w *watcherImpl) Close() {
	w.cancel()
}
