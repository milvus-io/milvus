package assignment

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func NewWatcher(r resolver.Resolver) Watcher {
	ctx, cancel := context.WithCancel(context.Background())
	w := &watcherImpl{
		ctx:         ctx,
		cancel:      cancel,
		r:           r,
		cond:        *syncutil.NewContextCond(&sync.Mutex{}),
		assignments: make(map[string]types.PChannelInfoAssigned),
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
	assignments map[string]types.PChannelInfoAssigned // map pchannel to node.
}

// execute starts the watcher.
func (w *watcherImpl) execute() {
	log.Info("assignment watcher start")
	var err error
	defer func() {
		// error can be ignored here, so use info level log here.
		log.Info("assignment watcher close", zap.Error(err))
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
	for _, assignments := range state.ChannelAssignmentInfo() {
		for _, pChannelInfo := range assignments.Channels {
			newAssignments[pChannelInfo.Name] = types.PChannelInfoAssigned{
				Channel: pChannelInfo,
				Node:    assignments.NodeInfo,
			}
		}
	}
	w.cond.LockAndBroadcast()
	w.assignments = newAssignments
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

// Watch watches the channel assignment.
func (w *watcherImpl) Watch(ctx context.Context, channel string, previous *types.PChannelInfoAssigned) error {
	w.cond.L.Lock()

	term := types.InitialTerm
	if previous != nil {
		term = previous.Channel.Term
	}

	for {
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
