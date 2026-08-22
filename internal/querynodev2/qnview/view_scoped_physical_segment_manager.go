package qnview

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// ViewScopedPhysicalSegmentManager owns one physical sealed segment per
// segment ID and shares it across QueryViews through view-scoped references.
// Segment metadata is supplied exclusively by the revisioned metadata stream.
type ViewScopedPhysicalSegmentManager struct {
	scheduler TaskScheduler
	loader    PhysicalSegmentLoader
	estimator SegmentResourceEstimator
	stream    SegmentLoadInfoStream

	mu       sync.Mutex
	views    map[qviews.QueryViewKey]*physicalViewRef
	segments map[int64]*physicalSegmentState
}

type physicalViewRef struct {
	segments        map[int64]int64
	onLoaded        func([]TransformSegment)
	onSegmentFailed func(int64, error)
	onUnrecoverable func()
}

type physicalSegmentState struct {
	collectionID int64
	segment      TransformSegment
	revision     SegmentLoadInfoRevision
	refs         map[qviews.QueryViewKey]struct{}
	requests     map[qviews.QueryViewKey]physicalLoadRequest

	loading    bool
	loadCancel context.CancelFunc
	loadHandle TaskHandle

	updating     bool
	updateEpoch  uint64
	updateHandle TaskHandle

	pending      *SegmentLoadInfoSnapshot
	subscription SegmentLoadInfoSubscription
}

type physicalLoadRequest struct {
	collection CollectionRuntime
	startAfter uint64
}

func NewViewScopedPhysicalSegmentManager(
	scheduler TaskScheduler,
	loader PhysicalSegmentLoader,
	stream SegmentLoadInfoStream,
	estimator SegmentResourceEstimator,
) *ViewScopedPhysicalSegmentManager {
	return &ViewScopedPhysicalSegmentManager{
		scheduler: scheduler,
		loader:    loader,
		stream:    stream,
		estimator: estimator,
		views:     make(map[qviews.QueryViewKey]*physicalViewRef),
		segments:  make(map[int64]*physicalSegmentState),
	}
}

func (m *ViewScopedPhysicalSegmentManager) Acquire(req AcquirePhysicalSegments) {
	req.Meta = cloneQueryViewMeta(req.Meta)
	req.View = cloneQueryNodeView(req.View)

	type subscriptionRequest struct {
		collectionID int64
		segmentID    int64
		revision     SegmentLoadInfoRevision
		state        *physicalSegmentState
	}
	subscriptions := make([]subscriptionRequest, 0)
	loaded := make([]TransformSegment, 0)

	m.mu.Lock()
	if m.views[req.Key] != nil {
		m.mu.Unlock()
		return
	}
	viewRef := &physicalViewRef{
		segments:        segmentPartitionMap(req.View),
		onLoaded:        req.OnLoaded,
		onSegmentFailed: req.OnSegmentUnrecoverable,
		onUnrecoverable: req.OnUnrecoverable,
	}
	m.views[req.Key] = viewRef
	for segmentID := range viewRef.segments {
		state := m.segments[segmentID]
		if state == nil {
			state = &physicalSegmentState{
				collectionID: req.Meta.GetCollectionId(),
				refs:         make(map[qviews.QueryViewKey]struct{}),
				requests:     make(map[qviews.QueryViewKey]physicalLoadRequest),
			}
			m.segments[segmentID] = state
		}
		state.refs[req.Key] = struct{}{}
		state.requests[req.Key] = physicalLoadRequest{
			collection: req.Collection,
			startAfter: req.Meta.GetTransformStartAfterTimetick(),
		}
		if state.segment != nil {
			loaded = append(loaded, state.segment)
		}
		if m.stream != nil && state.subscription == nil {
			subscriptions = append(subscriptions, subscriptionRequest{
				collectionID: state.collectionID,
				segmentID:    segmentID,
				revision:     state.revision,
				state:        state,
			})
		}
	}
	m.mu.Unlock()

	for _, request := range subscriptions {
		m.subscribe(request.collectionID, request.segmentID, request.revision, request.state)
	}
	if len(viewRef.segments) == 0 || len(loaded) == len(viewRef.segments) {
		m.submitLoaded(req.OnLoaded, loaded)
	}
}

func (m *ViewScopedPhysicalSegmentManager) Release(req ReleaseSegments) {
	var (
		toClose   []SegmentLoadInfoSubscription
		toRelease []TransformSegment
		toWait    []TaskHandle
	)
	m.mu.Lock()
	viewRef := m.views[req.Key]
	delete(m.views, req.Key)
	if viewRef != nil {
		for segmentID := range viewRef.segments {
			state := m.segments[segmentID]
			if state == nil {
				continue
			}
			delete(state.refs, req.Key)
			delete(state.requests, req.Key)
			if len(state.refs) != 0 {
				continue
			}
			if state.loadCancel != nil {
				state.loadCancel()
			}
			if state.loadHandle != nil {
				state.loadHandle.Cancel()
				toWait = append(toWait, state.loadHandle)
			}
			if state.updateHandle != nil {
				state.updateHandle.Cancel()
				toWait = append(toWait, state.updateHandle)
			}
			if state.subscription != nil {
				toClose = append(toClose, state.subscription)
			}
			if state.segment != nil {
				toRelease = append(toRelease, state.segment)
			}
			delete(m.segments, segmentID)
		}
	}
	m.mu.Unlock()

	for _, subscription := range toClose {
		subscription.Close()
	}
	go func() {
		for _, handle := range toWait {
			_ = handle.Wait(context.Background())
		}
		for _, segment := range toRelease {
			_ = segment.Release(context.Background())
		}
		m.submitCallback(req.OnDropped)
	}()
}

func (m *ViewScopedPhysicalSegmentManager) ApplyLoadInfoSnapshot(ctx context.Context, snapshot SegmentLoadInfoSnapshot) {
	snapshot = cloneSegmentLoadInfoSnapshot(snapshot)
	if !validSegmentLoadInfoSnapshot(snapshot) {
		mlog.Warn(ctx, "ignore invalid query view segment metadata snapshot",
			mlog.Int64("collectionID", snapshot.CollectionID),
			mlog.Int64("segmentID", snapshot.SegmentID),
			mlog.Uint64("revision", snapshot.Revision.Revision))
		return
	}

	m.mu.Lock()
	state := m.segments[snapshot.SegmentID]
	if state == nil || len(state.refs) == 0 || state.collectionID != snapshot.CollectionID {
		m.mu.Unlock()
		return
	}
	if snapshot.Revision.Revision <= state.revision.Revision ||
		(state.pending != nil && snapshot.Revision.Revision <= state.pending.Revision.Revision) {
		m.mu.Unlock()
		return
	}
	if state.loading || state.updating {
		state.pending = &snapshot
		m.mu.Unlock()
		return
	}
	if state.segment == nil {
		submission, ok := m.startLoadLocked(state, snapshot)
		m.mu.Unlock()
		if ok {
			m.submitLoad(submission)
		}
		return
	}
	submission, ok := m.startUpdateLocked(state, snapshot)
	m.mu.Unlock()
	if ok {
		m.submitUpdate(submission)
	}
}

type physicalLoadSubmission struct {
	state     *physicalSegmentState
	segmentID int64
	ctx       context.Context
	request   physicalLoadRequest
	snapshot  SegmentLoadInfoSnapshot
}

func (m *ViewScopedPhysicalSegmentManager) startLoadLocked(
	state *physicalSegmentState,
	snapshot SegmentLoadInfoSnapshot,
) (physicalLoadSubmission, bool) {
	request, ok := state.anyRequest()
	if !ok {
		return physicalLoadSubmission{}, false
	}
	// The cancellation function is retained in physicalSegmentState and is
	// invoked by Release or ResetSegment while a load is in flight.
	ctx, cancel := context.WithCancel(context.Background()) // #nosec G118
	state.loading = true
	state.loadCancel = cancel
	state.pending = nil
	return physicalLoadSubmission{
		state:     state,
		segmentID: snapshot.SegmentID,
		ctx:       ctx,
		request:   request,
		snapshot:  snapshot,
	}, true
}

func (m *ViewScopedPhysicalSegmentManager) submitLoad(submission physicalLoadSubmission) {
	task := newSegmentLoadTask(m.loader, m.estimator, SegmentLoadTask{
		Context:                     submission.ctx,
		SegmentID:                   submission.segmentID,
		Collection:                  submission.request.collection,
		TransformStartAfterTimeTick: submission.request.startAfter,
		Snapshot:                    submission.snapshot,
		OnLoaded: func(segment TransformSegment) {
			m.completeLoad(submission, segment)
		},
		OnUnrecoverable: func(err error) {
			m.failLoad(submission, err)
		},
	})
	handle := m.scheduler.Submit(task)
	m.mu.Lock()
	if state := m.segments[submission.segmentID]; state == submission.state && state.loading {
		state.loadHandle = handle
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()
	handle.Cancel()
}

func (m *ViewScopedPhysicalSegmentManager) completeLoad(submission physicalLoadSubmission, segment TransformSegment) {
	if segment == nil {
		m.failLoad(submission, nil)
		return
	}
	var (
		notifications []func()
		update        physicalUpdateSubmission
		hasUpdate     bool
	)
	m.mu.Lock()
	state := m.segments[submission.segmentID]
	if state != submission.state || !state.loading || len(state.refs) == 0 {
		m.mu.Unlock()
		_ = segment.Release(context.Background())
		return
	}
	state.segment = segment
	state.revision = submission.snapshot.Revision
	state.loading = false
	state.loadCancel = nil
	state.loadHandle = nil
	for key := range state.refs {
		if ref := m.views[key]; ref != nil && ref.onLoaded != nil {
			callback := ref.onLoaded
			notifications = append(notifications, func() { callback([]TransformSegment{segment}) })
		}
	}
	if state.pending != nil && state.pending.Revision.Revision > state.revision.Revision {
		update, hasUpdate = m.startUpdateLocked(state, *state.pending)
	}
	m.mu.Unlock()
	for _, notify := range notifications {
		notify()
	}
	if hasUpdate {
		m.submitUpdate(update)
	}
}

func (m *ViewScopedPhysicalSegmentManager) failLoad(submission physicalLoadSubmission, err error) {
	var (
		notifications []func()
		toClose       SegmentLoadInfoSubscription
	)
	m.mu.Lock()
	state := m.segments[submission.segmentID]
	if state != submission.state || !state.loading {
		m.mu.Unlock()
		return
	}
	state.loading = false
	state.loadCancel = nil
	state.loadHandle = nil
	toClose = state.subscription
	for key := range state.refs {
		ref := m.views[key]
		if ref == nil {
			continue
		}
		if ref.onSegmentFailed != nil {
			callback := ref.onSegmentFailed
			notifications = append(notifications, func() { callback(submission.segmentID, err) })
		} else if ref.onUnrecoverable != nil {
			notifications = append(notifications, ref.onUnrecoverable)
		}
	}
	delete(m.segments, submission.segmentID)
	m.mu.Unlock()
	if toClose != nil {
		toClose.Close()
	}
	for _, notify := range notifications {
		notify()
	}
}

type physicalUpdateSubmission struct {
	state    *physicalSegmentState
	epoch    uint64
	segment  TransformSegment
	request  physicalLoadRequest
	snapshot SegmentLoadInfoSnapshot
	current  SegmentLoadInfoRevision
}

func (m *ViewScopedPhysicalSegmentManager) startUpdateLocked(
	state *physicalSegmentState,
	snapshot SegmentLoadInfoSnapshot,
) (physicalUpdateSubmission, bool) {
	request, ok := state.anyRequest()
	if !ok || state.segment == nil || snapshot.Revision.Revision <= state.revision.Revision {
		return physicalUpdateSubmission{}, false
	}
	state.updating = true
	state.updateEpoch++
	state.pending = nil
	return physicalUpdateSubmission{
		state:    state,
		epoch:    state.updateEpoch,
		segment:  state.segment,
		request:  request,
		snapshot: snapshot,
		current:  state.revision,
	}, true
}

func (m *ViewScopedPhysicalSegmentManager) submitUpdate(submission physicalUpdateSubmission) {
	task := newSegmentUpdateTask(m.loader, SegmentUpdateTask{
		Context:    context.Background(),
		Segment:    submission.segment,
		Collection: submission.request.collection,
		Snapshot:   submission.snapshot,
		Current:    submission.current,
		OnUpdated: func(revision SegmentLoadInfoRevision) {
			m.completeUpdate(submission, revision)
		},
		OnFailed: func(error) {
			m.cancelUpdate(submission)
		},
	})
	handle := m.scheduler.Submit(task)
	m.mu.Lock()
	state := m.segments[submission.snapshot.SegmentID]
	if state == submission.state && state.updating && state.updateEpoch == submission.epoch {
		state.updateHandle = handle
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()
	handle.Cancel()
}

func (m *ViewScopedPhysicalSegmentManager) completeUpdate(submission physicalUpdateSubmission, revision SegmentLoadInfoRevision) {
	var (
		next    physicalUpdateSubmission
		hasNext bool
	)
	m.mu.Lock()
	state := m.segments[submission.snapshot.SegmentID]
	if state == submission.state && state.updating && state.updateEpoch == submission.epoch {
		state.revision = revision
		state.updating = false
		state.updateHandle = nil
		if state.pending != nil && state.pending.Revision.Revision > state.revision.Revision {
			next, hasNext = m.startUpdateLocked(state, *state.pending)
		}
	}
	m.mu.Unlock()
	if hasNext {
		m.submitUpdate(next)
	}
}

func (m *ViewScopedPhysicalSegmentManager) cancelUpdate(submission physicalUpdateSubmission) {
	m.mu.Lock()
	state := m.segments[submission.snapshot.SegmentID]
	if state == submission.state && state.updateEpoch == submission.epoch {
		state.updating = false
		state.updateHandle = nil
	}
	m.mu.Unlock()
}

func (m *ViewScopedPhysicalSegmentManager) ResetSegment(segmentID int64) {
	var (
		segment      TransformSegment
		subscription SegmentLoadInfoSubscription
		handles      []TaskHandle
	)
	m.mu.Lock()
	state := m.segments[segmentID]
	if state != nil {
		if state.loadCancel != nil {
			state.loadCancel()
		}
		if state.loadHandle != nil {
			state.loadHandle.Cancel()
			handles = append(handles, state.loadHandle)
		}
		if state.updateHandle != nil {
			state.updateHandle.Cancel()
			handles = append(handles, state.updateHandle)
		}
		segment = state.segment
		subscription = state.subscription
		delete(m.segments, segmentID)
	}
	for _, view := range m.views {
		delete(view.segments, segmentID)
	}
	m.mu.Unlock()
	if subscription != nil {
		subscription.Close()
	}
	go func() {
		for _, handle := range handles {
			_ = handle.Wait(context.Background())
		}
		if segment != nil {
			_ = segment.Release(context.Background())
		}
	}()
}

func (m *ViewScopedPhysicalSegmentManager) subscribe(
	collectionID, segmentID int64,
	revision SegmentLoadInfoRevision,
	expected *physicalSegmentState,
) {
	subscription := m.stream.Subscribe(SegmentLoadInfoSubscriptionOption{
		CollectionID: collectionID,
		SegmentID:    segmentID,
		Revision:     revision,
		Handler: physicalSegmentLoadInfoHandler{
			manager: m,
			state:   expected,
		},
	})
	if subscription == nil {
		return
	}
	m.mu.Lock()
	state := m.segments[segmentID]
	if state == expected && len(state.refs) > 0 && state.subscription == nil {
		state.subscription = subscription
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()
	subscription.Close()
}

type physicalSegmentLoadInfoHandler struct {
	manager *ViewScopedPhysicalSegmentManager
	state   *physicalSegmentState
}

func (h physicalSegmentLoadInfoHandler) Handle(snapshot SegmentLoadInfoSnapshot) error {
	h.manager.mu.Lock()
	current := h.manager.segments[snapshot.SegmentID]
	h.manager.mu.Unlock()
	if current == h.state {
		h.manager.ApplyLoadInfoSnapshot(context.Background(), snapshot)
	}
	return nil
}

func (physicalSegmentLoadInfoHandler) Close() {}

func (s *physicalSegmentState) anyRequest() (physicalLoadRequest, bool) {
	for key := range s.refs {
		request, ok := s.requests[key]
		if ok {
			return request, true
		}
	}
	return physicalLoadRequest{}, false
}

func (m *ViewScopedPhysicalSegmentManager) submitLoaded(callback func([]TransformSegment), loaded []TransformSegment) {
	if callback == nil {
		return
	}
	copyOfLoaded := append([]TransformSegment(nil), loaded...)
	m.scheduler.Submit(schedulerTaskFunc(func(context.Context) error {
		callback(copyOfLoaded)
		return nil
	}))
}

func (m *ViewScopedPhysicalSegmentManager) submitCallback(callback func()) {
	if callback == nil {
		return
	}
	m.scheduler.Submit(schedulerTaskFunc(func(context.Context) error {
		callback()
		return nil
	}))
}

func validSegmentLoadInfoSnapshot(snapshot SegmentLoadInfoSnapshot) bool {
	return !snapshot.Revision.Empty() && snapshot.CollectionID != 0 && snapshot.SegmentID != 0 &&
		snapshot.LoadInfo != nil && snapshot.LoadInfo.GetCollectionID() == snapshot.CollectionID &&
		snapshot.LoadInfo.GetSegmentID() == snapshot.SegmentID
}

func cloneSegmentLoadInfoSnapshot(snapshot SegmentLoadInfoSnapshot) SegmentLoadInfoSnapshot {
	if snapshot.LoadInfo != nil {
		snapshot.LoadInfo = proto.Clone(snapshot.LoadInfo).(*querypb.SegmentLoadInfo)
	}
	if snapshot.IndexInfos != nil {
		indexes := snapshot.IndexInfos
		snapshot.IndexInfos = make([]*indexpb.IndexInfo, 0, len(indexes))
		for _, index := range indexes {
			if index == nil {
				snapshot.IndexInfos = append(snapshot.IndexInfos, nil)
				continue
			}
			snapshot.IndexInfos = append(snapshot.IndexInfos, proto.Clone(index).(*indexpb.IndexInfo))
		}
	}
	return snapshot
}

func cloneQueryViewMeta(meta *viewpb.QueryViewMeta) *viewpb.QueryViewMeta {
	if meta == nil {
		return nil
	}
	return proto.Clone(meta).(*viewpb.QueryViewMeta)
}

func cloneQueryNodeView(view *viewpb.QueryViewOfQueryNode) *viewpb.QueryViewOfQueryNode {
	if view == nil {
		return nil
	}
	return proto.Clone(view).(*viewpb.QueryViewOfQueryNode)
}
