package qnview

import (
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// qnShardView manages all query view state machines for a single shard on a QueryNode.
// All public methods are concurrent-safe via the internal mutex.
type qnShardView struct {
	mu      sync.Mutex
	views   map[qviews.QueryViewVersion]*qnViewEntry
	segMgr  SegmentManager
	onEmpty func() // called (under mu) when the last view entry is removed
}

// qnViewEntry pairs an ApplyView (carrying the OnReport callback) with its state machine.
type qnViewEntry struct {
	handler.ApplyView
	sm *QNQueryViewStateMachine
}

// ApplyViews applies a batch of coord-pushed views atomically.
// Preparing views are processed first to ensure segment ref counts are
// incremented before any Dropped views decrement them, preventing
// unnecessary unload-then-reload of shared segments.
func (s *qnShardView) ApplyViews(views []handler.ApplyView) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range views {
		if views[i].View.State() == qviews.QueryViewStatePreparing {
			s.applyOneLocked(&views[i])
		}
	}
	for i := range views {
		if views[i].View.State() != qviews.QueryViewStatePreparing {
			s.applyOneLocked(&views[i])
		}
	}
}

// applyOneLocked applies a single view. Caller must hold s.mu.
func (s *qnShardView) applyOneLocked(av *handler.ApplyView) {
	key := av.View.QueryViewKey()
	entry, exists := s.views[key.QueryViewVersion]
	pushedState := av.View.State()

	if !exists {
		switch pushedState {
		case qviews.QueryViewStatePreparing:
			// New Preparing view: create SM and acquire segments.
			qnView := av.View.(*qviews.QueryViewAtQueryNode)
			sm := NewQNQueryViewStateMachine(
				qnView.IntoProto().Meta,
				qnView.ViewOfQueryNode(),
			)
			entry = &qnViewEntry{ApplyView: *av, sm: sm}
			s.views[key.QueryViewVersion] = entry

			// Tell SegmentManager to load segments. Callbacks will drive SM progress.
			s.segMgr.Acquire(AcquireSegments{
				Key:        key,
				SegmentIDs: qnView.SegmentIDs(),
				Settings:   qnView.IntoProto().Meta.Settings,
				OnReady: func(readySegments map[int64][]int64) {
					s.notifySegmentsReady(key.QueryViewVersion, readySegments)
				},
				OnUnrecoverable: func() {
					s.notifyUnrecoverable(key.QueryViewVersion)
				},
			})
		case qviews.QueryViewStateDropped:
			// View doesn't exist (e.g., QN restarted). Report Dropped immediately
			// so Coord can finish cleanup.
			if av.OnReport != nil {
				av.OnReport(av.View)
			}
		default:
			// View unknown to this node (e.g., state lost after restart).
			// Report Unrecoverable so Coord can generate a replacement view.
			if av.OnReport != nil {
				pb := av.View.IntoProto()
				pb.Meta.State = viewpb.QueryViewState(qviews.QueryViewStateUnrecoverable)
				av.OnReport(qviews.NewQueryViewAtWorkNodeFromProto(pb))
			}
		}
		return
	}

	// Existing view: replace callback and deliver coord push.
	entry.ApplyView = *av
	entry.sm.OnCoordStateDelivered(pushedState)
	s.consumeReportAndCleanup(key, entry)
}

// notifySegmentsReady is called by SegmentManager callback when segments
// have been loaded. Drives the SM from Preparing → Ready.
func (s *qnShardView) notifySegmentsReady(version qviews.QueryViewVersion, readySegments map[int64][]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[version]
	if !exists {
		return
	}

	entry.sm.OnSegmentsReady(readySegments)
	s.consumeReportAndCleanup(entry.View.QueryViewKey(), entry)
}

// notifyUnrecoverable is called by SegmentManager callback when a fatal error
// occurs during segment loading.
func (s *qnShardView) notifyUnrecoverable(version qviews.QueryViewVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[version]
	if !exists {
		return
	}

	entry.sm.OnUnrecoverable()
	s.consumeReportAndCleanup(entry.View.QueryViewKey(), entry)
}

// notifyDropped is called by the SegmentManager Release callback when segment
// release completes. Drives the SM from Dropping → Dropped.
func (s *qnShardView) notifyDropped(version qviews.QueryViewVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[version]
	if !exists {
		return
	}

	entry.sm.OnDropped()
	s.consumeReportAndCleanup(entry.View.QueryViewKey(), entry)
}

// consumeReportAndCleanup drains pending report and release, invokes callbacks,
// and removes the entry if it has reached Dropped state.
// Caller must hold s.mu.
func (s *qnShardView) consumeReportAndCleanup(key qviews.QueryViewKey, entry *qnViewEntry) {
	report := entry.sm.ConsumeReport()
	if report != nil && entry.OnReport != nil {
		entry.OnReport(qviews.NewQueryViewAtWorkNodeFromProto(report))
	}
	if entry.sm.ConsumeRelease() {
		s.segMgr.Release(ReleaseSegments{
			Key: key,
			OnDropped: func() {
				s.notifyDropped(key.QueryViewVersion)
			},
		})
	}
	if entry.sm.State() == qviews.QueryViewStateDropped {
		delete(s.views, key.QueryViewVersion)
		if len(s.views) == 0 && s.onEmpty != nil {
			s.onEmpty()
		}
	}
}
