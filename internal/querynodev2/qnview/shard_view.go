package qnview

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	qvobserve "github.com/milvus-io/milvus/internal/views/qviews/observe"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// qnShardView manages all query view state machines for a single shard on a QueryNode.
// All public methods are concurrent-safe via the internal mutex.
type qnShardView struct {
	mu       sync.Mutex
	views    map[qviews.QueryViewVersion]*qnViewEntry
	segMgr   SegmentManager
	detached bool
	onEmpty  func(*qnShardView) // called (under mu) when the last view entry is removed
}

// qnViewEntry pairs an ApplyView (carrying the OnReport callback) with its state machine.
type qnViewEntry struct {
	handler.ApplyView
	sm *QNQueryViewStateMachine
}

// ApplyViews applies a batch of coord-pushed views atomically.
// Preparing and Up views are processed first so new serving candidates are
// installed before older views are released.
func (s *qnShardView) ApplyViews(views []handler.ApplyView) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.detached {
		return false
	}

	for i := range views {
		state := views[i].View.State()
		if state == qviews.QueryViewStatePreparing || state == qviews.QueryViewStateUp {
			s.applyOneLocked(&views[i])
		}
	}
	for i := range views {
		state := views[i].View.State()
		if state != qviews.QueryViewStatePreparing && state != qviews.QueryViewStateUp {
			s.applyOneLocked(&views[i])
		}
	}
	return true
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
			qvobserve.Observe(context.TODO(), qvobserve.QueryNodeAcquireSegmentsEvent{
				View:         key,
				SegmentCount: countViewSegments(qnView.ViewOfQueryNode()),
			})

			// Tell SegmentManager to load segments. Callbacks will drive SM progress.
			meta := qnView.IntoProto().Meta
			s.segMgr.Acquire(AcquireSegments{
				Key:  key,
				Meta: proto.Clone(meta).(*viewpb.QueryViewMeta),
				View: proto.Clone(qnView.ViewOfQueryNode()).(*viewpb.QueryViewOfQueryNode),
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
	before := entry.sm.State()
	entry.sm.OnCoordStateDelivered(pushedState)
	qvobserve.Observe(context.TODO(), qvobserve.QueryNodeApplyCoordViewEvent{
		ViewStateTransition: qvobserve.ViewStateTransition{
			CollectionID: entry.sm.Meta().GetCollectionId(),
			View:         key,
			From:         before,
			To:           entry.sm.State(),
		},
	})
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

	key := entry.View.QueryViewKey()
	before := entry.sm.State()
	entry.sm.OnSegmentsReady(readySegments)
	qvobserve.Observe(context.TODO(), qvobserve.QueryNodeSegmentsReadyEvent{
		ViewStateTransition: qvobserve.ViewStateTransition{
			CollectionID: entry.sm.Meta().GetCollectionId(),
			View:         key,
			From:         before,
			To:           entry.sm.State(),
		},
		ReadySegmentCount: countReadySegments(readySegments),
	})
	s.consumeReportAndCleanup(key, entry)
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

	key := entry.View.QueryViewKey()
	before := entry.sm.State()
	entry.sm.OnUnrecoverable()
	qvobserve.Observe(context.TODO(), qvobserve.QueryNodeSegmentUnrecoverableEvent{
		ViewStateTransition: qvobserve.ViewStateTransition{
			CollectionID: entry.sm.Meta().GetCollectionId(),
			View:         key,
			From:         before,
			To:           entry.sm.State(),
		},
	})
	s.consumeReportAndCleanup(key, entry)
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

	key := entry.View.QueryViewKey()
	before := entry.sm.State()
	entry.sm.OnDropped()
	qvobserve.Observe(context.TODO(), qvobserve.QueryNodeReleaseDoneEvent{
		ViewStateTransition: qvobserve.ViewStateTransition{
			CollectionID: entry.sm.Meta().GetCollectionId(),
			View:         key,
			From:         before,
			To:           entry.sm.State(),
		},
	})
	s.consumeReportAndCleanup(key, entry)
}

// consumeReportAndCleanup drains pending report and release, invokes callbacks,
// and removes the entry if it has reached Dropped state.
// Caller must hold s.mu.
func (s *qnShardView) consumeReportAndCleanup(key qviews.QueryViewKey, entry *qnViewEntry) {
	report := entry.sm.ConsumeReport()
	if report != nil && entry.OnReport != nil {
		qvobserve.Observe(context.TODO(), qvobserve.QueryNodeReportViewEvent{
			View:  key,
			State: qviews.QueryViewState(report.GetMeta().GetState()),
		})
		entry.OnReport(qviews.NewQueryViewAtWorkNodeFromProto(report))
	}
	if entry.sm.ConsumeRelease() {
		qvobserve.Observe(context.TODO(), qvobserve.QueryNodeReleaseSegmentsEvent{
			View: key,
		})
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
			s.detached = true
			s.onEmpty(s)
		}
	}
}

func countReadySegments(readySegments map[int64][]int64) int {
	total := 0
	for _, segmentIDs := range readySegments {
		total += len(segmentIDs)
	}
	return total
}

func countViewSegments(view *viewpb.QueryViewOfQueryNode) int {
	total := 0
	for _, partition := range view.GetPartitions() {
		total += len(partition.GetSegmentIds())
	}
	return total
}
