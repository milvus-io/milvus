package assignment

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/discoverer"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/pingcap/log"
)

func NewWatcher(r resolver.Resolver) Watcher {
	ctx, cancel := context.WithCancel(context.Background())
	w := &watcherImpl{
		ctx:         ctx,
		cancel:      cancel,
		r:           r,
		cond:        *syncutil.NewContextCond(&sync.Mutex{}),
		assignments: make(map[string]*logpb.PChannelInfo),
	}
	go w.execute()
	return w
}

type watcherImpl struct {
	ctx    context.Context
	cancel context.CancelFunc
	r      resolver.Resolver

	cond        syncutil.ContextCond
	assignments map[string]*logpb.PChannelInfo // map pchannel to node, TODO: should be modified map vchannel to node in future.
}

func (w *watcherImpl) execute() {
	log.Info("assignment watcher start")
	defer log.Info("assignment watcher close")

	// error can be ignored here, error is always context canceled.
	_ = w.r.Watch(w.ctx, func(state discoverer.VersionedState) error {
		w.updateAssignment(state)
		return nil
	})
}

func (w *watcherImpl) updateAssignment(state discoverer.VersionedState) {
	newAssignments := make(map[string]*logpb.PChannelInfo)
	for _, assignments := range state.ChannelAssignmentInfo() {
		for _, pChannelInfo := range assignments.Channels {
			newAssignments[pChannelInfo.Name] = pChannelInfo
		}
	}
	w.cond.LockAndBroadcast()
	w.assignments = newAssignments
	w.cond.L.Unlock()
}

func (w *watcherImpl) Get(ctx context.Context, channel string) *Assignment {
	w.cond.L.Lock()
	defer w.cond.L.Unlock()

	if info, ok := w.assignments[channel]; ok {
		return &Assignment{
			ChannelName: channel,
			Term:        info.Term,
			ServerID:    info.ServerID,
		}
	}
	return nil
}

func (w *watcherImpl) Watch(ctx context.Context, channel string, oldAssign *Assignment) error {
	w.cond.L.Lock()

	term := minimalTerm
	if oldAssign != nil {
		term = oldAssign.Term
	}

	for {
		if info, ok := w.assignments[channel]; ok {
			if info.Term > term {
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

func (w *watcherImpl) Close() {
	w.cancel()
}
