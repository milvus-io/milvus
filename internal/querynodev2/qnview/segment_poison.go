package qnview

import "github.com/milvus-io/milvus/internal/views/viewerror"

// segmentPoison belongs only to one local segment instance.
type segmentPoison struct {
	failedTimeTick uint64
}

// This adapter binds transform failure to the same concrete state as catch-up.
// A late failure must never affect a replacement with the same segment ID.
type observedTransformSegment struct {
	TransformSegment
	manager *QueryViewSegmentManager
	state   *segmentState
}

func (s *observedTransformSegment) UnwrapTransformSegment() TransformSegment {
	return s.TransformSegment
}

func (s *observedTransformSegment) OnTransformFailed(timetick uint64, _ error) {
	s.manager.poisonSegment(s.ID(), s.state, timetick)
}

func (m *QueryViewSegmentManager) poisonSegment(id int64, expected *segmentState, timetick uint64) {
	m.mu.Lock()
	state := m.segments[id]
	if state != expected || state.poison != nil {
		m.mu.Unlock()
		return
	}
	state.poison = &segmentPoison{failedTimeTick: timetick}
	callbacks := make([]func(), 0, len(state.refs))
	for key := range state.refs {
		if ref := m.views[key]; ref != nil {
			callbacks = append(callbacks, ref.onUnrecoverable)
		}
	}
	m.mu.Unlock()
	// Shard callbacks acquire the shard mutex, which may be held by Release
	// waiting for this ApplyTransform. Local publication must be synchronous;
	// preparation failure callbacks run independently of transform consumption.
	// Notify all referencing views, including those waiting on other segments.
	// Their state machines ignore the failure if already Ready; do not mark the
	// readiness ref unrecoverable, which would also reject historical queries.
	go func() {
		for _, callback := range callbacks {
			invokeUnrecoverable(callback)
		}
	}()
}

// CheckTransformReadable validates the pinned instance after shared visibility
// has been reached. Historical MVCCs remain usable while the view is retained.
func (h *sealedSegmentHandle) CheckTransformReadable(timetick uint64) error {
	h.manager.mu.Lock()
	defer h.manager.mu.Unlock()
	if poison := h.state.poison; poison != nil && timetick >= poison.failedTimeTick {
		return viewerror.NewViewInvalidated("segment %d generation %d is poisoned from timetick %d", h.ID(), h.state.generation, poison.failedTimeTick)
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
