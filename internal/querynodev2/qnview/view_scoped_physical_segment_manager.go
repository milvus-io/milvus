package qnview

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type ViewScopedPhysicalSegmentManager struct {
	nodeScheduler nodescheduler.Scheduler
	loader        PhysicalSegmentLoader
	estimator     SegmentResourceEstimator
	stream        SegmentLoadInfoStream

	// Allocated under mu; never reset when a SegmentID is removed or recreated.
	nextLoadGeneration uint64
	mu                 sync.Mutex
	views              map[qviews.QueryViewKey]*viewRef
	dropping           map[qviews.QueryViewKey]*viewRef
	segments           map[int64]*physicalSegmentState
	cancels            map[qviews.QueryViewKey]context.CancelFunc
}

type viewRef struct {
	key             qviews.QueryViewKey
	dropped         bool
	loadInfo        *QueryViewLoadInfo
	onAvailable     func([]TransformSegment)
	available       map[int64]bool
	dataVersion     qviews.DataVersion
	loaded          map[int64]bool
	segments        map[int64]int64
	pendingLoads    int
	onDropped       func()
	onLoaded        func([]TransformSegment)
	onSegmentFailed func(segmentID int64, err error)
	onUnrecoverable func()
}

type physicalSegmentState struct {
	resources         *QueryViewLoadInfo
	appliedResources  *QueryViewLoadInfo
	lastSnapshot      *SegmentLoadInfoSnapshot
	dataVersion       qviews.DataVersion
	acceptedVersion   qviews.DataVersion
	targetVersion     qviews.DataVersion
	subscriptionEpoch uint64
	segment           TransformSegment
	collectionID      int64
	loading           bool
	loadGeneration    uint64
	updating          bool
	updateHandle      nodescheduler.TaskHandle
	updateEpoch       uint64
	loadCancel        context.CancelFunc
	loadDone          func()
	refs              map[qviews.QueryViewKey]struct{}
	requests          map[qviews.QueryViewKey]segmentLoadRequest
	revision          SegmentLoadInfoRevision
	pendingSnapshot   *SegmentLoadInfoSnapshot
	subscription      SegmentLoadInfoSubscription
}

type segmentLoadInfoSubscriptionRequest struct {
	resources    *QueryViewLoadInfo
	dataVersion  qviews.DataVersion
	epoch        uint64
	collectionID int64
	segmentID    int64
	revision     SegmentLoadInfoRevision
	state        *physicalSegmentState
}

type segmentLoadSubmission struct {
	generation uint64
	segmentID  int64
	ctx        context.Context
	request    segmentLoadRequest
	snapshot   SegmentLoadInfoSnapshot
	done       func()
}

type segmentUpdateSubmission struct {
	task  *SegmentUpdateTask
	state *physicalSegmentState
	epoch uint64
}

type segmentLoadRequest struct {
	loadInfo                    *QueryViewLoadInfo
	meta                        *viewpb.QueryViewMeta
	collection                  CollectionRuntime
	transformStartAfterTimeTick uint64
}

func NewViewScopedPhysicalSegmentManager(loader PhysicalSegmentLoader, estimators ...SegmentResourceEstimator) *ViewScopedPhysicalSegmentManager {
	return NewViewScopedPhysicalSegmentManagerWithNodeScheduler(nodescheduler.Get(), loader, estimators...)
}

func NewViewScopedPhysicalSegmentManagerWithNodeScheduler(nodeScheduler nodescheduler.Scheduler, loader PhysicalSegmentLoader, estimators ...SegmentResourceEstimator) *ViewScopedPhysicalSegmentManager {
	return NewViewScopedPhysicalSegmentManagerWithNodeSchedulerAndStream(nodeScheduler, loader, nil, estimators...)
}

func NewViewScopedPhysicalSegmentManagerWithNodeSchedulerAndStream(nodeScheduler nodescheduler.Scheduler, loader PhysicalSegmentLoader, stream SegmentLoadInfoStream, estimators ...SegmentResourceEstimator) *ViewScopedPhysicalSegmentManager {
	var estimator SegmentResourceEstimator
	if len(estimators) > 0 {
		estimator = estimators[0]
	}
	return &ViewScopedPhysicalSegmentManager{
		nodeScheduler: nodeScheduler,
		loader:        loader,
		estimator:     estimator,
		stream:        stream,
		views:         make(map[qviews.QueryViewKey]*viewRef),
		dropping:      make(map[qviews.QueryViewKey]*viewRef),
		segments:      make(map[int64]*physicalSegmentState),
		cancels:       make(map[qviews.QueryViewKey]context.CancelFunc),
	}
}

func (m *ViewScopedPhysicalSegmentManager) Acquire(req AcquirePhysicalSegments) {
	if req.LoadInfo != nil {
		info := CloneQueryViewLoadInfo(*req.LoadInfo)
		req.LoadInfo = &info
	}
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
	toClose, onDropped := m.removeView(req)
	m.closeSubscriptions(toClose)
	m.submitCallback(onDropped)
}

func (m *ViewScopedPhysicalSegmentManager) ApplyLoadInfoSnapshot(ctx context.Context, snapshot SegmentLoadInfoSnapshot) {
	m.applyLoadInfoSnapshot(ctx, snapshot, nil)
}

func (m *ViewScopedPhysicalSegmentManager) applyLoadInfoSnapshot(ctx context.Context, snapshot SegmentLoadInfoSnapshot, expected *physicalSegmentState, subscriptionEpoch ...uint64) {
	if ctx == nil {
		ctx = context.Background()
	}
	if snapshot.SegmentID == 0 && snapshot.LoadInfo != nil {
		snapshot.SegmentID = snapshot.LoadInfo.GetSegmentID()
	}
	load, update, ok := m.recordSegmentSnapshot(ctx, snapshot, expected, subscriptionEpoch...)
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
	toSubscribe := make([]segmentLoadInfoSubscriptionRequest, 0, len(segmentPartitions))
	var toClose []SegmentLoadInfoSubscription
	m.mu.Lock()
	if m.views[req.Key] != nil {
		m.mu.Unlock()
		return nil, false
	}
	m.cancels[req.Key] = cancel
	ref := &viewRef{
		key:             req.Key,
		dataVersion:     queryViewDataVersion(req.Meta),
		loadInfo:        req.LoadInfo,
		onAvailable:     req.OnAvailable,
		available:       make(map[int64]bool),
		loaded:          make(map[int64]bool),
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
			if m.stream == nil {
				loadCtx, loadCancel := context.WithCancel(context.Background())
				ref.pendingLoads++
				loadDone := onceLoadDone(func() { m.completeLoadAttempts([]*viewRef{ref}) })
				m.nextLoadGeneration++
				state.loadGeneration = m.nextLoadGeneration
				state.loading = true
				state.loadCancel = loadCancel
				state.loadDone = loadDone
				toLoad = append(toLoad, segmentLoadSubmission{
					generation: state.loadGeneration,
					segmentID:  segmentID,
					ctx:        loadCtx,
					request:    newSegmentLoadRequest(req),
					done:       chainLoadDone(loadDone, loadCancel),
				})
			}
			m.segments[segmentID] = state
		} else if state.segment == nil && !state.loading && m.stream == nil {
			loadCtx, loadCancel := context.WithCancel(context.Background())
			ref.pendingLoads++
			loadDone := onceLoadDone(func() { m.completeLoadAttempts([]*viewRef{ref}) })
			m.nextLoadGeneration++
			state.loadGeneration = m.nextLoadGeneration
			state.loading = true
			state.loadCancel = loadCancel
			state.loadDone = loadDone
			toLoad = append(toLoad, segmentLoadSubmission{
				generation: state.loadGeneration,
				segmentID:  segmentID,
				ctx:        loadCtx,
				request:    newSegmentLoadRequest(req),
				done:       chainLoadDone(loadDone, loadCancel),
			})
		}
		state.refs[req.Key] = struct{}{}
		state.requests[req.Key] = newSegmentLoadRequest(req)
		resources := unionLoadInfo(state.requests)
		changed := !sameLoadRequirements(state.resources, resources)
		state.resources = resources
		if changed && state.lastSnapshot != nil {
			state.pendingSnapshot = state.lastSnapshot
		}
		if m.stream != nil && (state.subscriptionEpoch == 0 || ref.dataVersion.GT(state.targetVersion) || changed) {
			toClose = append(toClose, m.detachSubscriptionLocked(state)...)
			if ref.dataVersion.GT(state.targetVersion) {
				state.targetVersion = ref.dataVersion
			}
			state.subscriptionEpoch++
			toSubscribe = append(toSubscribe, segmentLoadInfoSubscriptionRequest{
				collectionID: state.collectionID, segmentID: segmentID,
				revision: state.revision, state: state,
				dataVersion: state.targetVersion, epoch: state.subscriptionEpoch, resources: state.resources,
			})
		}
	}
	m.mu.Unlock()
	m.closeSubscriptions(toClose)
	m.subscribeSegments(toSubscribe)
	for id := range segmentPartitions {
		m.submitPendingSegmentUpdate(id)
	}

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

	m.mu.Lock()
	var notifications []func()
	if ctx.Err() == nil {
		for segmentID := range segmentPartitionMap(req.View) {
			if state := m.segments[segmentID]; state != nil {
				notifications = append(notifications, m.loadedNotificationsLocked(state)...)
			}
		}
	}
	m.mu.Unlock()
	for _, notify := range notifications {
		notify()
	}
}

func (m *ViewScopedPhysicalSegmentManager) submitSegmentLoad(submission segmentLoadSubmission, done func()) {
	done = chainLoadDone(done, submission.done)
	if done == nil {
		done = func() {}
	}
	task := newSegmentLoadTask(m.loader, m.estimator, SegmentLoadTask{
		Context:                     submission.ctx,
		SegmentID:                   submission.segmentID,
		Collection:                  submission.request.collection,
		TransformStartAfterTimeTick: submission.request.transformStartAfterTimeTick,
		Snapshot:                    submission.snapshot,
		OnFinished:                  done,
		OnLoaded: func(segment TransformSegment) {
			defer done()
			if segment == nil {
				notifications, retries, subscriptions := m.failPhysicalSegmentLoad(submission, nil)
				m.submitSegmentLoadSubmissions(retries)
				m.closeSubscriptions(subscriptions)
				for _, notify := range notifications {
					notify()
				}
				return
			}
			notifications, retries, kept := m.completePhysicalSegmentLoad(submission, segment)
			m.submitSegmentLoadSubmissions(retries)
			if !kept {
				_ = segment.Release(context.Background())
				return
			}
			m.submitPendingSegmentUpdate(segment.ID())
			for _, notify := range notifications {
				notify()
			}
		},
		OnUnrecoverable: func(err error) {
			defer done()
			notifications, retries, subscriptions := m.failPhysicalSegmentLoad(submission, err)
			m.submitSegmentLoadSubmissions(retries)
			m.closeSubscriptions(subscriptions)
			for _, notify := range notifications {
				notify()
			}
		},
	})
	handle := m.nodeScheduler.Submit(task)
	go func() {
		if handle.Wait(context.Background()) == nil {
			done()
		}
	}()
}

func (m *ViewScopedPhysicalSegmentManager) completePhysicalSegmentLoad(submission segmentLoadSubmission, segment TransformSegment) ([]func(), []segmentLoadSubmission, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.segments[submission.segmentID]
	if state == nil || !state.loading || state.loadGeneration != submission.generation {
		return nil, nil, false
	}
	state.segment = segment
	state.dataVersion = submission.snapshot.DataVersion
	state.appliedResources = submission.snapshot.resources
	state.loading = false
	state.loadCancel = nil
	state.loadDone = nil
	if !submission.snapshot.Revision.Empty() {
		state.revision = submission.snapshot.Revision
	}
	if len(state.refs) == 0 {
		delete(m.segments, segment.ID())
		return nil, nil, false
	}

	return m.loadedNotificationsLocked(state), nil, true
}

func (m *ViewScopedPhysicalSegmentManager) recordSegmentSnapshot(ctx context.Context, snapshot SegmentLoadInfoSnapshot, expected *physicalSegmentState, subscriptionEpoch ...uint64) (segmentLoadSubmission, segmentUpdateSubmission, bool) {
	if snapshot.Revision.Empty() {
		return segmentLoadSubmission{}, segmentUpdateSubmission{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.segments[snapshot.SegmentID]
	if state == nil || len(state.refs) == 0 || (expected != nil && state != expected) {
		return segmentLoadSubmission{}, segmentUpdateSubmission{}, false
	}
	if len(subscriptionEpoch) > 0 && state.subscriptionEpoch != subscriptionEpoch[0] {
		return segmentLoadSubmission{}, segmentUpdateSubmission{}, false
	}
	if state.acceptedVersion.GT(snapshot.DataVersion) {
		return segmentLoadSubmission{}, segmentUpdateSubmission{}, false
	}
	state.acceptedVersion = snapshot.DataVersion
	snapshotCopy := snapshot
	state.lastSnapshot = &snapshotCopy
	if state.segment == nil {
		if state.loading {
			snapshotCopy := snapshot
			state.pendingSnapshot = &snapshotCopy
			return segmentLoadSubmission{}, segmentUpdateSubmission{}, false
		}
		request, ok := state.loadRequest()
		if !ok {
			return segmentLoadSubmission{}, segmentUpdateSubmission{}, false
		}
		planned, ready := planSegmentSnapshot(snapshot, state.resources)
		if !ready {
			return segmentLoadSubmission{}, segmentUpdateSubmission{}, false
		}
		snapshot = planned
		loadCtx, loadCancel := context.WithCancel(context.Background())
		loadDone := onceLoadDone(m.trackPendingLoadAttemptLocked(state))
		state.pendingSnapshot = nil
		m.nextLoadGeneration++
		state.loadGeneration = m.nextLoadGeneration
		state.loading = true
		state.loadCancel = loadCancel
		state.loadDone = loadDone
		return segmentLoadSubmission{
			generation: state.loadGeneration,
			segmentID:  snapshot.SegmentID,
			ctx:        loadCtx,
			request:    request,
			snapshot:   snapshot,
			done:       chainLoadDone(loadDone, loadCancel),
		}, segmentUpdateSubmission{}, true
	}
	task, ok := m.recordSegmentUpdateLocked(ctx, snapshot, state)
	return segmentLoadSubmission{}, task, ok
}

func (m *ViewScopedPhysicalSegmentManager) recordSegmentUpdate(ctx context.Context, snapshot SegmentLoadInfoSnapshot) (segmentUpdateSubmission, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.segments[snapshot.SegmentID]
	return m.recordSegmentUpdateLocked(ctx, snapshot, state)
}

func (m *ViewScopedPhysicalSegmentManager) recordSegmentUpdateLocked(ctx context.Context, snapshot SegmentLoadInfoSnapshot, state *physicalSegmentState) (segmentUpdateSubmission, bool) {
	if state == nil || state.segment == nil || len(state.refs) == 0 || snapshot.Revision.Empty() {
		return segmentUpdateSubmission{}, false
	}
	if state.acceptedVersion.GT(snapshot.DataVersion) || (!state.updating && state.revision == snapshot.Revision && state.dataVersion.GTE(snapshot.DataVersion) && sameLoadRequirements(state.appliedResources, state.resources)) {
		return segmentUpdateSubmission{}, false
	}
	state.acceptedVersion = snapshot.DataVersion
	snapshotCopy := snapshot
	state.pendingSnapshot = &snapshotCopy
	if state.updating {
		return segmentUpdateSubmission{}, false
	}
	return m.nextSegmentUpdateLocked(ctx, snapshot.SegmentID)
}

func (m *ViewScopedPhysicalSegmentManager) nextSegmentUpdateLocked(ctx context.Context, segmentID int64) (segmentUpdateSubmission, bool) {
	state := m.segments[segmentID]
	if state == nil || state.segment == nil || state.updating || state.pendingSnapshot == nil || len(state.refs) == 0 {
		return segmentUpdateSubmission{}, false
	}
	request, ok := state.loadRequest()
	if !ok {
		state.updating = false
		return segmentUpdateSubmission{}, false
	}
	snapshot, ready := planSegmentSnapshot(*state.pendingSnapshot, state.resources)
	if !ready {
		return segmentUpdateSubmission{}, false
	}
	current := state.revision
	if !sameLoadRequirements(state.appliedResources, state.resources) {
		current = SegmentLoadInfoRevision{} // Configuration changes also require Reopen.
	}
	state.pendingSnapshot = nil
	state.updating = true
	state.updateEpoch++
	epoch := state.updateEpoch
	done := onceLoadDone(m.trackPendingLoadAttemptLocked(state))
	task := newSegmentUpdateTask(m.loader, SegmentUpdateTask{
		Context:    ctx,
		Segment:    state.segment,
		Collection: request.collection,
		Snapshot:   snapshot,
		Current:    current,
		OnFinished: done,
		OnUpdated: func(revision SegmentLoadInfoRevision) {
			m.completeSegmentUpdate(segmentID, state, epoch, revision, snapshot)
		},
		OnFailed: func(err error) {
			m.failSegmentUpdate(segmentID, state, epoch, snapshot)
		},
	}, m.estimator)
	return segmentUpdateSubmission{task: task, state: state, epoch: epoch}, true
}

func (m *ViewScopedPhysicalSegmentManager) submitPendingSegmentUpdate(segmentID int64) {
	m.mu.Lock()
	submission, ok := m.nextSegmentUpdateLocked(context.Background(), segmentID)
	m.mu.Unlock()
	if ok {
		m.submitSegmentUpdate(submission)
	}
}

func (m *ViewScopedPhysicalSegmentManager) submitSegmentUpdate(submission segmentUpdateSubmission) {
	handle := m.nodeScheduler.Submit(submission.task)
	go func() {
		if handle.Wait(context.Background()) == nil {
			submission.task.OnFinished()
		}
	}()
	m.mu.Lock()
	state := m.segments[submission.task.Snapshot.SegmentID]
	if state != submission.state || !state.updating || state.updateEpoch != submission.epoch {
		m.mu.Unlock()
		handle.Cancel()
		return
	}
	state.updateHandle = handle
	m.mu.Unlock()
}

func (m *ViewScopedPhysicalSegmentManager) completeSegmentUpdate(segmentID int64, expected *physicalSegmentState, epoch uint64, revision SegmentLoadInfoRevision, snapshot SegmentLoadInfoSnapshot) {
	var notifications []func()
	var next segmentUpdateSubmission
	var ok bool
	m.mu.Lock()
	state := m.segments[segmentID]
	if state == expected && state.updateEpoch == epoch {
		state.revision = revision
		state.dataVersion = snapshot.DataVersion
		state.appliedResources = snapshot.resources
		notifications = m.loadedNotificationsLocked(state)
		state.updating = false
		state.updateHandle = nil
		next, ok = m.nextSegmentUpdateLocked(context.Background(), segmentID)
	}
	m.mu.Unlock()
	if ok {
		m.submitSegmentUpdate(next)
	}
	for _, notify := range notifications {
		notify()
	}
}

func (m *ViewScopedPhysicalSegmentManager) failSegmentUpdate(segmentID int64, expected *physicalSegmentState, epoch uint64, attempted SegmentLoadInfoSnapshot) {
	var notifications []func()
	var next segmentUpdateSubmission
	var ok bool
	m.mu.Lock()
	if state := m.segments[segmentID]; state == expected && state.updateEpoch == epoch {
		state.updating = false
		state.updateHandle = nil
		for key := range state.refs {
			ref := m.views[key]
			if ref != nil && (!state.dataVersion.GTE(ref.dataVersion) || !coversLoadRequirements(state.appliedResources, ref.loadInfo)) && attempted.DataVersion.GTE(ref.dataVersion) && coversLoadRequirements(attempted.resources, ref.loadInfo) && ref.onUnrecoverable != nil {
				notifications = append(notifications, ref.onUnrecoverable)
			}
		}
		next, ok = m.nextSegmentUpdateLocked(context.Background(), segmentID)
	}
	m.mu.Unlock()
	for _, notify := range notifications {
		notify()
	}
	if ok {
		m.submitSegmentUpdate(next)
	}
}

func (m *ViewScopedPhysicalSegmentManager) cancelSegmentUpdateLocked(state *physicalSegmentState) {
	if state == nil {
		return
	}
	state.updateEpoch++
	state.updating = false
	state.pendingSnapshot = nil
	if state.updateHandle != nil {
		state.updateHandle.Cancel()
		state.updateHandle = nil
	}
}

func (m *ViewScopedPhysicalSegmentManager) failPhysicalSegmentLoad(submission segmentLoadSubmission, err error) ([]func(), []segmentLoadSubmission, []SegmentLoadInfoSubscription) {
	m.mu.Lock()
	defer m.mu.Unlock()

	segmentID := submission.segmentID
	state := m.segments[segmentID]
	if state == nil || !state.loading || state.loadGeneration != submission.generation {
		return nil, nil, nil
	}
	state.loading = false
	state.loadCancel = nil
	state.loadDone = nil
	if state.segment == nil && len(state.refs) == 0 {
		subscription := m.detachSubscriptionLocked(state)
		delete(m.segments, segmentID)
		return nil, nil, subscription
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
	state.pendingSnapshot = nil
	state.loadCancel = nil
	state.loadDone = nil
	subscription := m.detachSubscriptionLocked(state)
	if state.segment == nil {
		delete(m.segments, segmentID)
	}
	return notifications, nil, subscription
}

func (m *ViewScopedPhysicalSegmentManager) ResetSegment(segmentID int64) {
	var subscriptions []SegmentLoadInfoSubscription
	m.mu.Lock()
	if state := m.segments[segmentID]; state != nil {
		if state.loadCancel != nil {
			state.loadCancel()
		}
		m.cancelSegmentUpdateLocked(state)
		subscriptions = m.detachSubscriptionLocked(state)
		delete(m.segments, segmentID)
	}
	for _, ref := range m.views {
		delete(ref.segments, segmentID)
	}
	m.mu.Unlock()
	m.closeSubscriptions(subscriptions)
}

// Each view is notified once per segment, after its minimum data version is
// physically available. Transform readiness is checked by the caller separately.
func (m *ViewScopedPhysicalSegmentManager) loadedNotificationsLocked(state *physicalSegmentState) []func() {
	var notifications []func()
	if state.segment == nil {
		return nil
	}
	for key := range state.refs {
		ref := m.views[key]
		if ref == nil {
			continue
		}
		if !ref.available[state.segment.ID()] && ref.onAvailable != nil {
			ref.available[state.segment.ID()] = true
			cb, segment := ref.onAvailable, state.segment
			notifications = append(notifications, func() { cb([]TransformSegment{segment}) })
		}
		if ref.loaded[state.segment.ID()] || !state.dataVersion.GTE(ref.dataVersion) || !coversLoadRequirements(state.appliedResources, ref.loadInfo) {
			continue
		}
		if ref.onLoaded != nil {
			ref.loaded[state.segment.ID()] = true
			cb, segment := ref.onLoaded, state.segment
			notifications = append(notifications, func() { cb([]TransformSegment{segment}) })
		}
	}
	return notifications
}

func queryViewDataVersion(meta *viewpb.QueryViewMeta) qviews.DataVersion {
	version := meta.GetVersion().GetDataVersion()
	return qviews.DataVersion{StreamingVersion: version.GetStreamingVersion(), CompactVersion: version.GetCompactVersion()}
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
	ref.dropped = true

	toClose := make([]SegmentLoadInfoSubscription, 0, len(ref.segments))
	for segmentID := range ref.segments {
		state := m.segments[segmentID]
		if state == nil {
			continue
		}
		delete(state.refs, key)
		delete(state.requests, key)
		state.resources = unionLoadInfo(state.requests)
		if len(state.refs) == 0 {
			m.cancelSegmentUpdateLocked(state)
			if state.segment == nil && state.loading {
				if state.loadCancel != nil {
					state.loadCancel()
				}
				state.loadCancel = nil
				state.loadDone = nil
			}
			toClose = append(toClose, m.detachSubscriptionLocked(state)...)
			delete(m.segments, segmentID)
		}
	}
	if cancel, ok := m.cancels[key]; ok {
		cancel()
		delete(m.cancels, key)
	}
	if ref.pendingLoads == 0 {
		return toClose, ref.onDropped
	}
	m.dropping[key] = ref
	return toClose, nil
}

func newSegmentLoadRequest(req AcquirePhysicalSegments) segmentLoadRequest {
	return segmentLoadRequest{
		loadInfo: req.LoadInfo, meta: req.Meta,
		collection:                  req.Collection,
		transformStartAfterTimeTick: req.Meta.GetTransformStartAfterTimetick(),
	}
}

func (m *ViewScopedPhysicalSegmentManager) trackPendingLoadAttemptLocked(state *physicalSegmentState) func() {
	refs := make([]*viewRef, 0, len(state.refs))
	for key := range state.refs {
		ref := m.views[key]
		if ref == nil {
			continue
		}
		ref.pendingLoads++
		refs = append(refs, ref)
	}
	return func() {
		m.completeLoadAttempts(refs)
	}
}

func (m *ViewScopedPhysicalSegmentManager) completeLoadAttempts(refs []*viewRef) {
	callbacks := make([]func(), 0)
	m.mu.Lock()
	for _, ref := range refs {
		if ref.pendingLoads == 0 {
			continue
		}
		ref.pendingLoads--
		if ref.pendingLoads == 0 && ref.dropped {
			if m.dropping[ref.key] == ref {
				delete(m.dropping, ref.key)
			}
			callbacks = append(callbacks, ref.onDropped)
			ref.onDropped = nil
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

func (m *ViewScopedPhysicalSegmentManager) detachSubscriptionLocked(state *physicalSegmentState) []SegmentLoadInfoSubscription {
	if state == nil || state.subscription == nil {
		return nil
	}
	subscription := state.subscription
	state.subscription = nil
	return []SegmentLoadInfoSubscription{subscription}
}

func (m *ViewScopedPhysicalSegmentManager) subscribeSegments(requests []segmentLoadInfoSubscriptionRequest) {
	if m.stream == nil {
		return
	}
	for _, request := range requests {
		var loadInfo QueryViewLoadInfo
		if request.resources != nil {
			loadInfo = CloneQueryViewLoadInfo(*request.resources)
		}
		subscription := m.stream.Subscribe(SegmentLoadInfoSubscriptionOption{
			LoadInfo:     loadInfo,
			CollectionID: request.collectionID,
			SegmentID:    request.segmentID,
			Revision:     request.revision,
			DataVersion:  request.dataVersion,
			Handler: physicalSegmentLoadInfoHandler{
				manager: m,
				state:   request.state,
				epoch:   request.epoch,
			},
		})
		if subscription == nil {
			continue
		}
		keep := false
		m.mu.Lock()
		if state := m.segments[request.segmentID]; state == request.state && state.subscriptionEpoch == request.epoch && len(state.refs) > 0 && state.subscription == nil {
			state.subscription = subscription
			keep = true
		}
		m.mu.Unlock()
		if !keep {
			subscription.Close()
		}
	}
}

func (m *ViewScopedPhysicalSegmentManager) closeSubscriptions(subscriptions []SegmentLoadInfoSubscription) {
	for _, subscription := range subscriptions {
		subscription.Close()
	}
}

type physicalSegmentLoadInfoHandler struct {
	epoch   uint64
	manager *ViewScopedPhysicalSegmentManager
	state   *physicalSegmentState
}

func (h physicalSegmentLoadInfoHandler) Handle(snapshot SegmentLoadInfoSnapshot) error {
	h.manager.applyLoadInfoSnapshot(context.Background(), snapshot, h.state, h.epoch)
	return nil
}

func (physicalSegmentLoadInfoHandler) Close() {}

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
