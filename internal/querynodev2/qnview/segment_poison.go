package qnview

import (
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// This adapter binds transform failure to the same concrete state as catch-up.
// A late failure must never affect a replacement with the same segment ID.
type observedTransformSegment struct {
	TransformSegment
	manager *QueryViewSegmentReadinessManager
	state   *transformSegmentState
}

func (s *observedTransformSegment) UnwrapTransformSegment() TransformSegment {
	return s.TransformSegment
}

func (s *observedTransformSegment) OnTransformFailed(timetick uint64, err error) {
	s.manager.poisonSegment(s.ID(), s.state, timetick, err)
}

func (m *QueryViewSegmentReadinessManager) poisonSegment(id int64, expected *transformSegmentState, timetick uint64, err error) {
	m.mu.Lock()
	state := m.segments[id]
	if state != expected || state.poison != nil {
		m.mu.Unlock()
		return
	}
	poison := &viewpb.PoisonedSegment{SegmentId: id, Generation: state.generation, FailedTimetick: timetick}
	state.poison, state.poisonErr = poison, err
	callbacks := make([]func(*viewpb.PoisonedSegment), 0, len(state.refs))
	waiters := make([]transformSegmentWaiter, 0, len(state.waiters))
	for _, waiter := range state.waiters {
		waiters = append(waiters, waiter)
	}
	for key := range state.refs {
		if ref := m.views[key]; ref != nil && ref.onPoisoned != nil {
			callbacks = append(callbacks, ref.onPoisoned)
		}
	}
	m.mu.Unlock()
	// Shard callbacks acquire the shard mutex, which may be held by Release
	// waiting for this ApplyTransform. Local publication must be synchronous;
	// reports must run independently of transform consumption and unregistration.
	go func() {
		for _, callback := range callbacks {
			callback(poison)
		}
		for _, waiter := range waiters {
			m.notifyUnrecoverable(waiter.key, waiter.onUnrecoverable)
		}
	}()
}

// CheckTransformReadable validates the pinned instance after shared visibility
// has been reached. Historical MVCCs remain usable while the view is retained.
func (h *sealedSegmentHandle) CheckTransformReadable(timetick uint64) error {
	h.manager.mu.Lock()
	defer h.manager.mu.Unlock()
	if poison := h.state.poison; poison != nil && timetick >= poison.FailedTimetick {
		return viewerror.NewViewInvalidated("segment %d generation %d is poisoned from timetick %d", h.ID(), poison.Generation, poison.FailedTimetick)
	}
	return nil
}

func checkTransformReadable(handles []SealedSegmentHandle, timetick uint64) error {
	for _, handle := range handles {
		if checker, ok := handle.(interface{ CheckTransformReadable(uint64) error }); ok {
			if err := checker.CheckTransformReadable(timetick); err != nil {
				for _, acquired := range handles {
					acquired.Release()
				}
				return err
			}
		}
	}
	return nil
}
