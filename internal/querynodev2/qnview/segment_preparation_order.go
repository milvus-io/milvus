package qnview

import "context"

func (s *physicalSegmentState) hasNewerSchema(request segmentLoadRequest) bool {
	for _, other := range s.requests {
		if other.meta.GetSchemaVersion() > request.meta.GetSchemaVersion() {
			return true
		}
	}
	return false
}

// Finish each retained schema's first read snapshot before advancing to a new
// schema. Once captured, that snapshot remains usable across later reopens.
func preferLoadRequest(candidate, current segmentLoadRequest, newer bool) bool {
	if candidate.prepared != current.prepared {
		return !candidate.prepared
	}
	if !candidate.prepared && candidate.meta.GetSchemaVersion() != current.meta.GetSchemaVersion() {
		return candidate.meta.GetSchemaVersion() < current.meta.GetSchemaVersion()
	}
	return newer
}

func sameLoadRequirements(a, b segmentLoadRequest) bool {
	return a.meta.GetSchemaVersion() == b.meta.GetSchemaVersion() &&
		a.meta.GetLoadInfoVersion() == b.meta.GetLoadInfoVersion() &&
		a.manifestVersion == b.manifestVersion
}

// An initial load can fail before a published-schema recovery request arrives.
// Transfer only after the native attempt has returned, preserving each attempt's
// own pending-load accounting until its completion callback runs.
func (m *ViewScopedPhysicalSegmentManager) retryChangedSegmentLoad(previous segmentLoadSubmission) bool {
	m.mu.Lock()
	state := m.segments[previous.segmentID]
	if state == nil || previous.ctx.Err() != nil {
		m.mu.Unlock()
		return false
	}
	request, found := state.loadRequest()
	if !found || state.latestSnapshot == nil ||
		sameLoadRequirements(request, previous.request) {
		m.mu.Unlock()
		return false
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := onceLoadDone(m.trackPendingLoadAttemptLocked(state))
	state.loadCancel, state.loadDone = cancel, done
	state.pendingSnapshot = nil
	submission := segmentLoadSubmission{
		segmentID: previous.segmentID, ctx: ctx,
		request: request, snapshot: *state.latestSnapshot, done: chainLoadDone(done, cancel),
	}
	m.mu.Unlock()
	m.submitSegmentLoad(submission, nil)
	return true
}
