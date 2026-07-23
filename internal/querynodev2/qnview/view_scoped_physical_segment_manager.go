package qnview

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type ViewScopedPhysicalSegmentManager struct {
	nodeScheduler nodescheduler.Scheduler
	scheduler     SegmentLoadScheduler
	watcher       SegmentLoadInfoWatcher

	mu       sync.Mutex
	views    map[qviews.QueryViewKey]*viewRef
	dropping map[qviews.QueryViewKey]*viewRef
	segments map[int64]*physicalSegmentState
	cancels  map[qviews.QueryViewKey]context.CancelFunc
}

type viewRef struct {
	segments        map[int64]int64
	pendingLoads    int
	onDropped       func()
	onLoaded        func([]TransformSegment)
	onSegmentFailed func(segmentID int64, err error)
	onUnrecoverable func()
}

type physicalSegmentState struct {
	segment         TransformSegment
	collectionID    int64
	loading         bool
	pending         bool
	updating        bool
	loadCancel      context.CancelFunc
	loadDone        func()
	refs            map[qviews.QueryViewKey]struct{}
	requests        map[qviews.QueryViewKey]segmentLoadRequest
	revision        SegmentLoadInfoRevision
	pendingSnapshot *SegmentLoadInfoSnapshot
}

type segmentLoadSubmission struct {
	segmentID int64
	ctx       context.Context
	request   segmentLoadRequest
	snapshot  SegmentLoadInfoSnapshot
	done      func()
}

type segmentLoadRequest struct {
	meta                        *viewpb.QueryViewMeta
	collection                  CollectionRuntime
	transformStartAfterTimeTick uint64
}

func NewViewScopedPhysicalSegmentManager(meta QueryViewLoadMetadataProvider, loader PhysicalSegmentLoader, estimators ...SegmentResourceEstimator) *ViewScopedPhysicalSegmentManager {
	return NewViewScopedPhysicalSegmentManagerWithScheduler(NewQueryViewSegmentLoadScheduler(meta, loader, estimators...))
}

func NewViewScopedPhysicalSegmentManagerWithScheduler(scheduler SegmentLoadScheduler) *ViewScopedPhysicalSegmentManager {
	return NewViewScopedPhysicalSegmentManagerWithNodeScheduler(nodescheduler.Get(), scheduler)
}

func NewViewScopedPhysicalSegmentManagerWithSchedulerAndWatcher(scheduler SegmentLoadScheduler, watcher SegmentLoadInfoWatcher) *ViewScopedPhysicalSegmentManager {
	return NewViewScopedPhysicalSegmentManagerWithNodeSchedulerAndWatcher(nodescheduler.Get(), scheduler, watcher)
}

func NewViewScopedPhysicalSegmentManagerWithNodeScheduler(nodeScheduler nodescheduler.Scheduler, scheduler SegmentLoadScheduler) *ViewScopedPhysicalSegmentManager {
	return NewViewScopedPhysicalSegmentManagerWithNodeSchedulerAndWatcher(nodeScheduler, scheduler, nil)
}

func NewViewScopedPhysicalSegmentManagerWithNodeSchedulerAndWatcher(nodeScheduler nodescheduler.Scheduler, scheduler SegmentLoadScheduler, watcher SegmentLoadInfoWatcher) *ViewScopedPhysicalSegmentManager {
	return &ViewScopedPhysicalSegmentManager{
		nodeScheduler: nodeScheduler,
		scheduler:     scheduler,
		watcher:       watcher,
		views:         make(map[qviews.QueryViewKey]*viewRef),
		dropping:      make(map[qviews.QueryViewKey]*viewRef),
		segments:      make(map[int64]*physicalSegmentState),
		cancels:       make(map[qviews.QueryViewKey]context.CancelFunc),
	}
}

func (m *ViewScopedPhysicalSegmentManager) SetSegmentLoadInfoWatcher(watcher SegmentLoadInfoWatcher) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.watcher = watcher
}

func (m *ViewScopedPhysicalSegmentManager) Acquire(req AcquirePhysicalSegments) {
	ctx, cancel := context.WithCancel(context.Background())
	toLoad, ok := m.recordView(req, cancel)
	if !ok {
		cancel()
		return
	}
	m.nodeScheduler.Submit(schedulerTaskFunc(func(context.Context) error {
		m.load(ctx, req, toLoad)
		return nil
	}))
}

func (m *ViewScopedPhysicalSegmentManager) Release(req ReleaseSegments) {
	toUnsubscribe, onDropped := m.removeView(req)
	m.unsubscribeSegments(toUnsubscribe)
	m.submitCallback(onDropped)
}

func (m *ViewScopedPhysicalSegmentManager) ApplyLoadInfoSnapshot(ctx context.Context, snapshot SegmentLoadInfoSnapshot) {
	if ctx == nil {
		ctx = context.Background()
	}
	if snapshot.SegmentID == 0 && snapshot.LoadInfo != nil {
		snapshot.SegmentID = snapshot.LoadInfo.GetSegmentID()
	}
	load, update, ok := m.recordSegmentSnapshot(ctx, snapshot)
	if !ok {
		return
	}
	if load.segmentID != 0 {
		m.submitSegmentLoad(load, nil)
		return
	}
	m.submitSegmentUpdate(update)
}

func (m *ViewScopedPhysicalSegmentManager) recordView(req AcquirePhysicalSegments, cancel context.CancelFunc) ([]segmentLoadSubmission, bool) {
	segmentPartitions := segmentPartitionMap(req.View)
	toLoad := make([]segmentLoadSubmission, 0, len(segmentPartitions))
	toSubscribe := make([]SegmentLoadInfoSubscription, 0, len(segmentPartitions))
	m.mu.Lock()
	if m.views[req.Key] != nil {
		m.mu.Unlock()
		return nil, false
	}
	m.cancels[req.Key] = cancel
	ref := &viewRef{
		segments:        segmentPartitions,
		onLoaded:        req.OnLoaded,
		onSegmentFailed: req.OnSegmentUnrecoverable,
		onUnrecoverable: req.OnUnrecoverable,
	}
	m.views[req.Key] = ref
	for segmentID := range segmentPartitions {
		state, ok := m.segments[segmentID]
		if !ok {
			state = &physicalSegmentState{
				collectionID: req.Meta.GetCollectionId(),
				refs:         make(map[qviews.QueryViewKey]struct{}),
				requests:     make(map[qviews.QueryViewKey]segmentLoadRequest),
			}
			if m.watcher == nil {
				loadCtx, loadCancel := context.WithCancel(context.Background())
				ref.pendingLoads++
				loadDone := onceLoadDone(func() { m.completeLoadAttempts([]qviews.QueryViewKey{req.Key}) })
				state.loading = true
				state.loadCancel = loadCancel
				state.loadDone = loadDone
				toLoad = append(toLoad, segmentLoadSubmission{
					segmentID: segmentID,
					ctx:       loadCtx,
					request:   newSegmentLoadRequest(req),
					done:      chainLoadDone(loadDone, loadCancel),
				})
			} else {
				toSubscribe = append(toSubscribe, SegmentLoadInfoSubscription{
					CollectionID: req.Meta.GetCollectionId(),
					SegmentID:    segmentID,
				})
			}
			m.segments[segmentID] = state
		} else if state.segment == nil && !state.loading && !state.pending && m.watcher == nil {
			loadCtx, loadCancel := context.WithCancel(context.Background())
			ref.pendingLoads++
			loadDone := onceLoadDone(func() { m.completeLoadAttempts([]qviews.QueryViewKey{req.Key}) })
			state.loading = true
			state.loadCancel = loadCancel
			state.loadDone = loadDone
			toLoad = append(toLoad, segmentLoadSubmission{
				segmentID: segmentID,
				ctx:       loadCtx,
				request:   newSegmentLoadRequest(req),
				done:      chainLoadDone(loadDone, loadCancel),
			})
		}
		state.refs[req.Key] = struct{}{}
		state.requests[req.Key] = newSegmentLoadRequest(req)
	}
	m.mu.Unlock()
	m.subscribeSegments(toSubscribe)

	return toLoad, true
}

func (m *ViewScopedPhysicalSegmentManager) load(ctx context.Context, req AcquirePhysicalSegments, toLoad []segmentLoadSubmission) {
	if len(toLoad) > 0 {
		for _, submission := range toLoad {
			if ctx.Err() != nil {
				submission.done()
				continue
			}
			m.submitSegmentLoad(submission, nil)
		}
		return
	}

	loaded, complete := m.collectLoaded(req.View)
	if ctx.Err() != nil {
		return
	}
	if complete && req.OnLoaded != nil {
		req.OnLoaded(loaded)
	}
}

func (m *ViewScopedPhysicalSegmentManager) submitSegmentLoad(submission segmentLoadSubmission, done func()) {
	done = chainLoadDone(done, submission.done)
	if done == nil {
		done = func() {}
	}
	task := SegmentLoadTask{
		Context:                     submission.ctx,
		Meta:                        proto.Clone(submission.request.meta).(*viewpb.QueryViewMeta),
		SegmentID:                   submission.segmentID,
		Collection:                  submission.request.collection,
		TransformStartAfterTimeTick: submission.request.transformStartAfterTimeTick,
		Snapshot:                    submission.snapshot,
		OnFinished:                  done,
		OnLoaded: func(segment TransformSegment) {
			defer done()
			if segment == nil {
				notifications, retries := m.failPhysicalSegmentLoad(submission.segmentID, nil)
				m.submitSegmentLoadSubmissions(retries)
				for _, notify := range notifications {
					notify()
				}
				return
			}
			notifications, retries, kept, subscription := m.completePhysicalSegmentLoad(segment, submission.snapshot.Revision)
			m.submitSegmentLoadSubmissions(retries)
			if !kept {
				_ = segment.Release(context.Background())
				return
			}
			m.subscribeSegments(subscription)
			for _, notify := range notifications {
				notify()
			}
		},
		OnUnrecoverable: func(err error) {
			defer done()
			notifications, retries := m.failPhysicalSegmentLoad(submission.segmentID, err)
			m.submitSegmentLoadSubmissions(retries)
			for _, notify := range notifications {
				notify()
			}
		},
	}
	m.scheduler.Submit(task)
}

func (m *ViewScopedPhysicalSegmentManager) completePhysicalSegmentLoad(segment TransformSegment, revision SegmentLoadInfoRevision) ([]func(), []segmentLoadSubmission, bool, []SegmentLoadInfoSubscription) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.segments[segment.ID()]
	if state == nil {
		return nil, nil, false, nil
	}
	state.segment = segment
	state.loading = false
	state.pending = false
	state.loadCancel = nil
	state.loadDone = nil
	if !revision.Empty() {
		state.revision = revision
	}
	if len(state.refs) == 0 {
		delete(m.segments, segment.ID())
		return nil, nil, false, nil
	}

	notifications := make([]func(), 0, len(state.refs))
	for key := range state.refs {
		ref := m.views[key]
		if ref == nil || ref.onLoaded == nil {
			continue
		}
		loaded := []TransformSegment{segment}
		cb := ref.onLoaded
		notifications = append(notifications, func() {
			cb(loaded)
		})
	}
	subscription := m.subscriptionForStateLocked(segment.ID(), state)
	return notifications, m.collectPendingLoadSubmissionsLocked(), true, subscription
}

func (m *ViewScopedPhysicalSegmentManager) recordSegmentSnapshot(ctx context.Context, snapshot SegmentLoadInfoSnapshot) (segmentLoadSubmission, SegmentUpdateTask, bool) {
	if snapshot.Revision.Empty() {
		return segmentLoadSubmission{}, SegmentUpdateTask{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.segments[snapshot.SegmentID]
	if state == nil || len(state.refs) == 0 {
		return segmentLoadSubmission{}, SegmentUpdateTask{}, false
	}
	if state.segment == nil {
		if state.loading || snapshot.LoadInfo == nil {
			return segmentLoadSubmission{}, SegmentUpdateTask{}, false
		}
		request, ok := state.loadRequest()
		if !ok {
			return segmentLoadSubmission{}, SegmentUpdateTask{}, false
		}
		loadCtx, loadCancel := context.WithCancel(context.Background())
		loadDone := onceLoadDone(m.trackPendingLoadAttemptLocked(state))
		state.loading = true
		state.loadCancel = loadCancel
		state.loadDone = loadDone
		return segmentLoadSubmission{
			segmentID: snapshot.SegmentID,
			ctx:       loadCtx,
			request:   request,
			snapshot:  snapshot,
			done:      chainLoadDone(loadDone, loadCancel),
		}, SegmentUpdateTask{}, true
	}
	task, ok := m.recordSegmentUpdateLocked(ctx, snapshot, state)
	return segmentLoadSubmission{}, task, ok
}

func (m *ViewScopedPhysicalSegmentManager) recordSegmentUpdate(ctx context.Context, snapshot SegmentLoadInfoSnapshot) (SegmentUpdateTask, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.segments[snapshot.SegmentID]
	return m.recordSegmentUpdateLocked(ctx, snapshot, state)
}

func (m *ViewScopedPhysicalSegmentManager) recordSegmentUpdateLocked(ctx context.Context, snapshot SegmentLoadInfoSnapshot, state *physicalSegmentState) (SegmentUpdateTask, bool) {
	if state == nil || state.segment == nil || len(state.refs) == 0 || snapshot.Revision.Empty() || state.revision == snapshot.Revision {
		return SegmentUpdateTask{}, false
	}
	snapshotCopy := snapshot
	state.pendingSnapshot = &snapshotCopy
	if state.updating {
		return SegmentUpdateTask{}, false
	}
	return m.nextSegmentUpdateLocked(ctx, snapshot.SegmentID)
}

func (m *ViewScopedPhysicalSegmentManager) nextSegmentUpdateLocked(ctx context.Context, segmentID int64) (SegmentUpdateTask, bool) {
	state := m.segments[segmentID]
	if state == nil || state.segment == nil || state.pendingSnapshot == nil || len(state.refs) == 0 {
		if state != nil {
			state.updating = false
		}
		return SegmentUpdateTask{}, false
	}
	request, ok := state.loadRequest()
	if !ok || request.collection == nil {
		state.updating = false
		return SegmentUpdateTask{}, false
	}
	snapshot := *state.pendingSnapshot
	state.pendingSnapshot = nil
	state.updating = true
	return SegmentUpdateTask{
		Context:    ctx,
		Segment:    state.segment,
		Collection: request.collection,
		Snapshot:   snapshot,
		Current:    state.revision,
		OnUpdated: func(revision SegmentLoadInfoRevision) {
			m.completeSegmentUpdate(segmentID, revision)
		},
		OnFailed: func(error) {
			m.failSegmentUpdate(segmentID)
		},
	}, true
}

func (m *ViewScopedPhysicalSegmentManager) submitSegmentUpdate(task SegmentUpdateTask) {
	m.scheduler.Update(task)
}

func (m *ViewScopedPhysicalSegmentManager) completeSegmentUpdate(segmentID int64, revision SegmentLoadInfoRevision) {
	var next SegmentUpdateTask
	var subscription []SegmentLoadInfoSubscription
	var ok bool
	m.mu.Lock()
	state := m.segments[segmentID]
	if state != nil {
		state.revision = revision
		state.updating = false
		subscription = m.subscriptionForStateLocked(segmentID, state)
		next, ok = m.nextSegmentUpdateLocked(context.Background(), segmentID)
	}
	m.mu.Unlock()
	m.subscribeSegments(subscription)
	if ok {
		m.submitSegmentUpdate(next)
	}
}

func (m *ViewScopedPhysicalSegmentManager) failSegmentUpdate(segmentID int64) {
	m.mu.Lock()
	if state := m.segments[segmentID]; state != nil {
		state.updating = false
	}
	m.mu.Unlock()
}

func (m *ViewScopedPhysicalSegmentManager) failPhysicalSegmentLoad(segmentID int64, err error) ([]func(), []segmentLoadSubmission) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.segments[segmentID]
	if state == nil {
		return nil, nil
	}
	state.loading = false
	state.loadCancel = nil
	state.loadDone = nil
	if state.segment == nil && len(state.refs) == 0 {
		delete(m.segments, segmentID)
		return nil, nil
	}
	if isSegmentResourceInsufficient(err) && m.hasOtherLoadingSegmentLocked(segmentID) {
		state.pending = true
		return nil, nil
	}

	notifications := make([]func(), 0, len(state.refs))
	for key := range state.refs {
		ref := m.views[key]
		if ref == nil {
			continue
		}
		if ref.onSegmentFailed != nil {
			cb := ref.onSegmentFailed
			notifications = append(notifications, func() {
				cb(segmentID, err)
			})
			continue
		}
		if ref.onUnrecoverable != nil {
			cb := ref.onUnrecoverable
			notifications = append(notifications, func() {
				cb()
			})
		}
	}
	state.refs = make(map[qviews.QueryViewKey]struct{})
	state.requests = make(map[qviews.QueryViewKey]segmentLoadRequest)
	state.loading = false
	state.pending = false
	state.loadCancel = nil
	state.loadDone = nil
	if state.segment == nil {
		delete(m.segments, segmentID)
	}
	return notifications, m.collectPendingLoadSubmissionsLocked()
}

func (m *ViewScopedPhysicalSegmentManager) ResetSegment(segmentID int64) {
	var unsubscribe []SegmentLoadInfoSubscription
	m.mu.Lock()
	if state := m.segments[segmentID]; state != nil {
		if state.loadCancel != nil {
			state.loadCancel()
		}
		unsubscribe = m.subscriptionForStateLocked(segmentID, state)
		delete(m.segments, segmentID)
	}
	for _, ref := range m.views {
		delete(ref.segments, segmentID)
	}
	m.mu.Unlock()
	m.unsubscribeSegments(unsubscribe)
}

func (m *ViewScopedPhysicalSegmentManager) collectLoaded(view *viewpb.QueryViewOfQueryNode) ([]TransformSegment, bool) {
	segments := make([]TransformSegment, 0, len(segmentPartitionMap(view)))

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, partition := range view.GetPartitions() {
		for _, segmentID := range partition.GetSegmentIds() {
			state := m.segments[segmentID]
			if state == nil || state.segment == nil {
				return nil, false
			}
			segments = append(segments, state.segment)
		}
	}
	return segments, true
}

func (m *ViewScopedPhysicalSegmentManager) removeView(req ReleaseSegments) ([]SegmentLoadInfoSubscription, func()) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := req.Key
	ref := m.views[key]
	if ref == nil {
		if cancel, ok := m.cancels[key]; ok {
			cancel()
			delete(m.cancels, key)
		}
		return nil, req.OnDropped
	}
	delete(m.views, key)
	ref.onDropped = req.OnDropped

	toUnsubscribe := make([]SegmentLoadInfoSubscription, 0, len(ref.segments))
	for segmentID := range ref.segments {
		state := m.segments[segmentID]
		if state == nil {
			continue
		}
		delete(state.refs, key)
		delete(state.requests, key)
		if len(state.refs) == 0 {
			if state.segment == nil && state.loading {
				if state.loadCancel != nil {
					state.loadCancel()
				}
				state.loadCancel = nil
				state.loadDone = nil
			}
			toUnsubscribe = append(toUnsubscribe, m.subscriptionForStateLocked(segmentID, state)...)
			delete(m.segments, segmentID)
		}
	}
	if cancel, ok := m.cancels[key]; ok {
		cancel()
		delete(m.cancels, key)
	}
	if ref.pendingLoads == 0 {
		return toUnsubscribe, ref.onDropped
	}
	m.dropping[key] = ref
	return toUnsubscribe, nil
}

func newSegmentLoadRequest(req AcquirePhysicalSegments) segmentLoadRequest {
	return segmentLoadRequest{
		meta:                        req.Meta,
		collection:                  req.Collection,
		transformStartAfterTimeTick: req.Meta.GetTransformStartAfterTimetick(),
	}
}

func isSegmentResourceInsufficient(err error) bool {
	return errors.Is(err, merr.ErrSegmentRequestResourceFailed)
}

func (m *ViewScopedPhysicalSegmentManager) hasOtherLoadingSegmentLocked(segmentID int64) bool {
	for id, state := range m.segments {
		if id != segmentID && state.loading {
			return true
		}
	}
	return false
}

func (m *ViewScopedPhysicalSegmentManager) collectPendingLoadSubmissionsLocked() []segmentLoadSubmission {
	submissions := make([]segmentLoadSubmission, 0)
	for segmentID, state := range m.segments {
		if !state.pending || state.loading || state.segment != nil || len(state.refs) == 0 {
			continue
		}
		request, ok := state.loadRequest()
		if !ok {
			continue
		}
		loadCtx, loadCancel := context.WithCancel(context.Background())
		loadDone := onceLoadDone(m.trackPendingLoadAttemptLocked(state))
		state.pending = false
		state.loading = true
		state.loadCancel = loadCancel
		state.loadDone = loadDone
		submissions = append(submissions, segmentLoadSubmission{
			segmentID: segmentID,
			ctx:       loadCtx,
			request:   request,
			done:      chainLoadDone(loadDone, loadCancel),
		})
	}
	return submissions
}

func (m *ViewScopedPhysicalSegmentManager) trackPendingLoadAttemptLocked(state *physicalSegmentState) func() {
	keys := make([]qviews.QueryViewKey, 0, len(state.refs))
	for key := range state.refs {
		ref := m.views[key]
		if ref == nil {
			continue
		}
		ref.pendingLoads++
		keys = append(keys, key)
	}
	return func() {
		m.completeLoadAttempts(keys)
	}
}

func (m *ViewScopedPhysicalSegmentManager) completeLoadAttempts(keys []qviews.QueryViewKey) {
	callbacks := make([]func(), 0)
	m.mu.Lock()
	for _, key := range keys {
		ref := m.views[key]
		if ref == nil {
			ref = m.dropping[key]
		}
		if ref == nil || ref.pendingLoads == 0 {
			continue
		}
		ref.pendingLoads--
		if ref.pendingLoads == 0 && m.dropping[key] == ref {
			delete(m.dropping, key)
			callbacks = append(callbacks, ref.onDropped)
		}
	}
	m.mu.Unlock()
	for _, callback := range callbacks {
		m.submitCallback(callback)
	}
}

func (m *ViewScopedPhysicalSegmentManager) submitCallback(callback func()) {
	if callback == nil {
		return
	}
	m.nodeScheduler.Submit(schedulerTaskFunc(func(context.Context) error {
		callback()
		return nil
	}))
}

func (s *physicalSegmentState) loadRequest() (segmentLoadRequest, bool) {
	for key := range s.refs {
		request, ok := s.requests[key]
		if ok {
			return request, true
		}
	}
	return segmentLoadRequest{}, false
}

func (m *ViewScopedPhysicalSegmentManager) submitSegmentLoadSubmissions(submissions []segmentLoadSubmission) {
	for _, submission := range submissions {
		m.submitSegmentLoad(submission, nil)
	}
}

func (m *ViewScopedPhysicalSegmentManager) subscriptionForStateLocked(segmentID int64, state *physicalSegmentState) []SegmentLoadInfoSubscription {
	if m.watcher == nil || state == nil || state.collectionID == 0 {
		return nil
	}
	return []SegmentLoadInfoSubscription{{
		CollectionID: state.collectionID,
		SegmentID:    segmentID,
		Revision:     state.revision,
	}}
}

func (m *ViewScopedPhysicalSegmentManager) subscribeSegments(subscriptions []SegmentLoadInfoSubscription) {
	if m.watcher == nil {
		return
	}
	for _, subscription := range subscriptions {
		m.watcher.Subscribe(subscription)
	}
}

func (m *ViewScopedPhysicalSegmentManager) unsubscribeSegments(subscriptions []SegmentLoadInfoSubscription) {
	if m.watcher == nil {
		return
	}
	for _, subscription := range subscriptions {
		m.watcher.Unsubscribe(subscription.CollectionID, subscription.SegmentID)
	}
}

func chainLoadDone(first func(), second func()) func() {
	if first == nil {
		return second
	}
	if second == nil {
		return first
	}
	return func() {
		defer first()
		second()
	}
}

func onceLoadDone(done func()) func() {
	var once sync.Once
	return func() {
		once.Do(done)
	}
}
