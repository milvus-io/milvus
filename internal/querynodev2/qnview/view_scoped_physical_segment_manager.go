package qnview

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type ViewScopedPhysicalSegmentManager struct {
	nodeScheduler nodescheduler.Scheduler
	loader        PhysicalSegmentLoader
	estimator     SegmentResourceEstimator
	stream        SegmentLoadInfoStream

	mu                  sync.Mutex
	views               map[qviews.QueryViewKey]*viewRef
	dropping            map[qviews.QueryViewKey]*viewRef
	segments            map[int64]*physicalSegmentState
	pendingLoadSegments map[int64]struct{}
	cancels             map[qviews.QueryViewKey]context.CancelFunc
}

type viewRef struct {
	pendingAcquire  bool
	segments        map[int64]int64
	pendingLoads    int
	ready           map[int64]bool
	onDropped       func()
	onLoaded        func([]TransformSegment)
	onSegmentFailed func(segmentID int64, err error)
	onUnrecoverable func()
}

type physicalSegmentState struct {
	segment                TransformSegment
	collectionID           int64
	loading                bool
	updating               bool
	updateHandle           nodescheduler.TaskHandle
	updateEpoch            uint64
	loadCancel             context.CancelFunc
	loadDone               func()
	refs                   map[qviews.QueryViewKey]struct{}
	requests               map[qviews.QueryViewKey]segmentLoadRequest
	revision               SegmentLoadInfoRevision
	pendingSnapshot        *SegmentLoadInfoSnapshot
	latestSnapshot         *SegmentLoadInfoSnapshot
	appliedMeta            *viewpb.QueryViewMeta
	appliedManifestVersion int64
	subscription           SegmentLoadInfoSubscription
}

type segmentLoadInfoSubscriptionRequest struct {
	collectionID int64
	segmentID    int64
	revision     SegmentLoadInfoRevision
	state        *physicalSegmentState
}

type segmentLoadSubmission struct {
	segmentID int64
	ctx       context.Context
	request   segmentLoadRequest
	snapshot  SegmentLoadInfoSnapshot
	done      func()
}

type segmentUpdateSubmission struct {
	task            *SegmentUpdateTask
	state           *physicalSegmentState
	epoch           uint64
	unpinCollection func()
}

type segmentLoadRequest struct {
	prepared                    bool
	meta                        *viewpb.QueryViewMeta
	collection                  CollectionRuntime
	transformStartAfterTimeTick uint64
	manifestVersion             int64
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
		nodeScheduler:       nodeScheduler,
		loader:              loader,
		estimator:           estimator,
		stream:              stream,
		views:               make(map[qviews.QueryViewKey]*viewRef),
		dropping:            make(map[qviews.QueryViewKey]*viewRef),
		segments:            make(map[int64]*physicalSegmentState),
		pendingLoadSegments: make(map[int64]struct{}),
		cancels:             make(map[qviews.QueryViewKey]context.CancelFunc),
	}
}

func (m *ViewScopedPhysicalSegmentManager) Retain(req AcquirePhysicalSegments) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.views[req.Key] != nil {
		return
	}
	ref := &viewRef{segments: segmentPartitionMap(req.View), ready: make(map[int64]bool), pendingAcquire: true}
	m.views[req.Key] = ref
	for segmentID := range ref.segments {
		if state := m.segments[segmentID]; state != nil {
			state.refs[req.Key] = struct{}{}
		}
	}
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
	callback := req.OnDropped
	var loadsDone chan struct{}
	if callback != nil {
		loadsDone = make(chan struct{})
		req.OnDropped = func() { close(loadsDone) }
	}
	toClose, onDropped := m.removeView(req)
	m.submitCallback(onDropped)
	go func() {
		// A metadata callback may be waiting for the caller's shard mutex.
		// Close waits for that callback, so it must run after Release returns.
		m.closeSubscriptions(toClose)
		if callback != nil {
			<-loadsDone
			callback()
		}
	}()
}

func (m *ViewScopedPhysicalSegmentManager) ApplyLoadInfoSnapshot(ctx context.Context, snapshot SegmentLoadInfoSnapshot) {
	m.applyLoadInfoSnapshot(ctx, snapshot, nil)
}

func (m *ViewScopedPhysicalSegmentManager) applyLoadInfoSnapshot(ctx context.Context, snapshot SegmentLoadInfoSnapshot, expected *physicalSegmentState) {
	if ctx == nil {
		ctx = context.Background()
	}
	if snapshot.SegmentID == 0 && snapshot.LoadInfo != nil {
		snapshot.SegmentID = snapshot.LoadInfo.GetSegmentID()
	}
	load, update, ok := m.recordSegmentSnapshot(ctx, snapshot, expected)
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
	m.mu.Lock()
	ref := m.views[req.Key]
	if ref != nil && !ref.pendingAcquire {
		m.mu.Unlock()
		return nil, false
	}
	m.cancels[req.Key] = cancel
	if ref == nil {
		ref = &viewRef{segments: segmentPartitions, ready: make(map[int64]bool)}
		m.views[req.Key] = ref
	}
	ref.pendingAcquire = false
	ref.onLoaded = req.OnLoaded
	ref.onSegmentFailed = req.OnSegmentUnrecoverable
	ref.onUnrecoverable = req.OnUnrecoverable
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
				loadDone := onceLoadDone(func() { m.completeLoadAttempts([]qviews.QueryViewKey{req.Key}) })
				state.loading = true
				state.loadCancel = loadCancel
				state.loadDone = loadDone
				toLoad = append(toLoad, segmentLoadSubmission{
					segmentID: segmentID,
					ctx:       loadCtx,
					request:   newSegmentLoadRequest(req, segmentID),
					done:      chainLoadDone(loadDone, loadCancel),
				})
			} else {
				toSubscribe = append(toSubscribe, segmentLoadInfoSubscriptionRequest{
					collectionID: req.Meta.GetCollectionId(),
					segmentID:    segmentID,
					revision:     state.revision,
					state:        state,
				})
			}
			m.segments[segmentID] = state
		} else if _, pending := m.pendingLoadSegments[segmentID]; state.segment == nil && !state.loading && !pending && m.stream == nil {
			loadCtx, loadCancel := context.WithCancel(context.Background())
			ref.pendingLoads++
			loadDone := onceLoadDone(func() { m.completeLoadAttempts([]qviews.QueryViewKey{req.Key}) })
			state.loading = true
			state.loadCancel = loadCancel
			state.loadDone = loadDone
			toLoad = append(toLoad, segmentLoadSubmission{
				segmentID: segmentID,
				ctx:       loadCtx,
				request:   newSegmentLoadRequest(req, segmentID),
				done:      chainLoadDone(loadDone, loadCancel),
			})
		}
		state.refs[req.Key] = struct{}{}
		state.requests[req.Key] = newSegmentLoadRequest(req, segmentID)
		if state.segment != nil && state.latestSnapshot != nil && !state.satisfies(state.requests[req.Key]) && state.pendingSnapshot == nil {
			state.pendingSnapshot = state.latestSnapshot
		}
	}
	var notifications []func()
	for segmentID := range segmentPartitions {
		notifications = append(notifications, m.readyNotificationsLocked(segmentID)...)
	}
	m.mu.Unlock()
	for _, notify := range notifications {
		notify()
	}
	m.subscribeSegments(toSubscribe)
	for segmentID := range segmentPartitions {
		m.submitPendingSegmentUpdate(segmentID)
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

	if ctx.Err() != nil {
		return
	}
	m.mu.Lock()
	var notifications []func()
	for segmentID := range segmentPartitionMap(req.View) {
		notifications = append(notifications, m.readyNotificationsLocked(segmentID)...)
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
				notifications, retries, subscriptions := m.failPhysicalSegmentLoad(submission.segmentID, submission.snapshot, nil)
				m.submitSegmentLoadSubmissions(retries)
				m.closeSubscriptions(subscriptions)
				for _, notify := range notifications {
					notify()
				}
				return
			}
			notifications, retries, kept := m.completePhysicalSegmentLoad(segment, submission.snapshot.Revision, submission.request.meta, snapshotManifestVersion(submission.snapshot))
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
			if m.retryChangedSegmentLoad(submission) {
				return
			}
			notifications, retries, subscriptions := m.failPhysicalSegmentLoad(submission.segmentID, submission.snapshot, err)
			m.submitSegmentLoadSubmissions(retries)
			m.closeSubscriptions(subscriptions)
			for _, notify := range notifications {
				notify()
			}
		},
	})
	m.nodeScheduler.Submit(task)
}

func (m *ViewScopedPhysicalSegmentManager) completePhysicalSegmentLoad(segment TransformSegment, revision SegmentLoadInfoRevision, meta *viewpb.QueryViewMeta, manifestVersion int64) ([]func(), []segmentLoadSubmission, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.segments[segment.ID()]
	if state == nil {
		return nil, nil, false
	}
	state.segment = segment
	state.appliedMeta = meta
	state.appliedManifestVersion = manifestVersion
	state.loading = false
	delete(m.pendingLoadSegments, segment.ID())
	state.loadCancel = nil
	state.loadDone = nil
	if !revision.Empty() {
		state.revision = revision
	}
	if len(state.refs) == 0 {
		delete(m.segments, segment.ID())
		return nil, nil, false
	}

	notifications := m.readyNotificationsLocked(segment.ID())
	request, _ := state.loadRequest()
	if !state.satisfies(request) && state.pendingSnapshot == nil {
		state.pendingSnapshot = state.latestSnapshot
	}
	if state.pendingSnapshot != nil && state.pendingSnapshot.Revision == state.revision && state.satisfies(request) {
		state.pendingSnapshot = nil
	}
	return notifications, m.collectPendingLoadSubmissionsLocked(), true
}

func (m *ViewScopedPhysicalSegmentManager) recordSegmentSnapshot(ctx context.Context, snapshot SegmentLoadInfoSnapshot, expected *physicalSegmentState) (segmentLoadSubmission, segmentUpdateSubmission, bool) {
	if snapshot.Revision.Empty() {
		return segmentLoadSubmission{}, segmentUpdateSubmission{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.segments[snapshot.SegmentID]
	if state == nil || len(state.refs) == 0 || (expected != nil && state != expected) {
		return segmentLoadSubmission{}, segmentUpdateSubmission{}, false
	}
	snapshotCopy := snapshot
	state.latestSnapshot = &snapshotCopy
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
		loadCtx, loadCancel := context.WithCancel(context.Background())
		loadDone := onceLoadDone(m.trackPendingLoadAttemptLocked(state))
		delete(m.pendingLoadSegments, snapshot.SegmentID)
		state.pendingSnapshot = nil
		state.loading = true
		state.loadCancel = loadCancel
		state.loadDone = loadDone
		return segmentLoadSubmission{
			segmentID: snapshot.SegmentID,
			ctx:       loadCtx,
			request:   request,
			snapshot:  snapshot,
			done:      chainLoadDone(loadDone, loadCancel),
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
	request, _ := state.loadRequest()
	if !state.updating && state.revision == snapshot.Revision && state.satisfies(request) {
		return segmentUpdateSubmission{}, false
	}
	snapshotCopy := snapshot
	state.pendingSnapshot = &snapshotCopy
	if state.updating {
		return segmentUpdateSubmission{}, false
	}
	return m.nextSegmentUpdateLocked(ctx, snapshot.SegmentID)
}

func (m *ViewScopedPhysicalSegmentManager) nextSegmentUpdateLocked(ctx context.Context, segmentID int64) (segmentUpdateSubmission, bool) {
	state := m.segments[segmentID]
	if state != nil && state.updating {
		return segmentUpdateSubmission{}, false
	}
	if state == nil || state.segment == nil || state.pendingSnapshot == nil || len(state.refs) == 0 {
		if state != nil {
			state.updating = false
		}
		return segmentUpdateSubmission{}, false
	}
	request, ok := state.loadRequest()
	if state.pendingSnapshot.Revision == state.revision && state.satisfies(request) {
		state.pendingSnapshot = nil
		state.updating = false
		return segmentUpdateSubmission{}, false
	}
	if !ok || request.collection == nil {
		state.updating = false
		return segmentUpdateSubmission{}, false
	}
	// A newer view can arrive before its watched metadata. Do not repeatedly
	// reopen the old snapshot or acknowledge readiness for a base it lacks.
	if snapshotManifestVersion(*state.pendingSnapshot) < request.manifestVersion {
		return segmentUpdateSubmission{}, false
	}
	snapshot := *state.pendingSnapshot
	state.pendingSnapshot = nil
	state.updating = true
	state.updateEpoch++
	epoch := state.updateEpoch
	task := newSegmentUpdateTask(m.loader, SegmentUpdateTask{
		Context:     ctx,
		Segment:     state.segment,
		Collection:  request.collection,
		Snapshot:    snapshot,
		Current:     state.revision,
		ForceReopen: !state.satisfies(request),
		OnUpdated: func(revision SegmentLoadInfoRevision) {
			m.completeSegmentUpdate(segmentID, state, epoch, revision, request.meta, snapshotManifestVersion(snapshot))
		},
		OnFailed: func(error) {
			m.failSegmentUpdate(segmentID, state, epoch)
		},
		OnRetry: func() bool {
			return m.supersedeFailedSegmentUpdate(segmentID, state, epoch, request, snapshot.Revision)
		},
	}, m.estimator)
	return segmentUpdateSubmission{task: task, state: state, epoch: epoch, unpinCollection: request.collection.Pin()}, true
}

// A snapshot that lacks a newly materialized column may never become loadable.
// Transfer only after the attempt has returned, so native updates never overlap.
func (m *ViewScopedPhysicalSegmentManager) supersedeFailedSegmentUpdate(segmentID int64, expected *physicalSegmentState, epoch uint64, previous segmentLoadRequest, revision SegmentLoadInfoRevision) bool {
	m.mu.Lock()
	state := m.segments[segmentID]
	if state != expected || state.updateEpoch != epoch {
		m.mu.Unlock()
		return true
	}
	request, _ := state.loadRequest()
	requirementsChanged := request.meta.GetSchemaVersion() != previous.meta.GetSchemaVersion() ||
		request.meta.GetLoadInfoVersion() != previous.meta.GetLoadInfoVersion() ||
		request.manifestVersion != previous.manifestVersion
	if !requirementsChanged && (state.pendingSnapshot == nil || state.pendingSnapshot.Revision == revision) {
		m.mu.Unlock()
		return false
	}
	state.updating = false
	state.updateHandle = nil
	if state.pendingSnapshot == nil {
		state.pendingSnapshot = state.latestSnapshot
	}
	notifications := m.readyNotificationsLocked(segmentID)
	submission, ok := m.nextSegmentUpdateLocked(context.Background(), segmentID)
	m.mu.Unlock()
	if ok {
		m.submitSegmentUpdate(submission)
	}
	for _, notify := range notifications {
		notify()
	}
	return true
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
	// Cancel can discard a queued task without executing it. Wait covers both
	// that path and a running native update, and includes every delayed retry.
	go func() {
		_ = handle.Wait(context.Background())
		submission.unpinCollection()
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

func (m *ViewScopedPhysicalSegmentManager) completeSegmentUpdate(segmentID int64, expected *physicalSegmentState, epoch uint64, revision SegmentLoadInfoRevision, meta *viewpb.QueryViewMeta, manifestVersion int64) {
	var next segmentUpdateSubmission
	var ok bool
	var notifications []func()
	m.mu.Lock()
	state := m.segments[segmentID]
	if state == expected && state.updateEpoch == epoch {
		state.revision = revision
		state.appliedMeta = meta
		state.appliedManifestVersion = manifestVersion
		state.updating = false
		state.updateHandle = nil
		notifications = m.readyNotificationsLocked(segmentID)
		if request, found := state.loadRequest(); found && !state.satisfies(request) && state.pendingSnapshot == nil {
			state.pendingSnapshot = state.latestSnapshot
		}
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

func (m *ViewScopedPhysicalSegmentManager) failSegmentUpdate(segmentID int64, expected *physicalSegmentState, epoch uint64) {
	m.mu.Lock()
	if state := m.segments[segmentID]; state == expected && state.updateEpoch == epoch {
		state.updating = false
		state.updateHandle = nil
	}
	m.mu.Unlock()
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

func (m *ViewScopedPhysicalSegmentManager) failPhysicalSegmentLoad(segmentID int64, snapshot SegmentLoadInfoSnapshot, err error) ([]func(), []segmentLoadSubmission, []SegmentLoadInfoSubscription) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.segments[segmentID]
	if state == nil {
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
	if isSegmentResourceInsufficient(err) && m.hasOtherLoadingSegmentLocked(segmentID) {
		m.pendingLoadSegments[segmentID] = struct{}{}
		if state.pendingSnapshot == nil {
			snapshotCopy := snapshot
			state.pendingSnapshot = &snapshotCopy
		}
		return nil, nil, nil
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
	delete(m.pendingLoadSegments, segmentID)
	state.pendingSnapshot = nil
	state.loadCancel = nil
	state.loadDone = nil
	subscription := m.detachSubscriptionLocked(state)
	if state.segment == nil {
		delete(m.segments, segmentID)
	}
	return notifications, m.collectPendingLoadSubmissionsLocked(), subscription
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
		delete(m.pendingLoadSegments, segmentID)
		delete(m.segments, segmentID)
	}
	for _, ref := range m.views {
		delete(ref.segments, segmentID)
	}
	m.mu.Unlock()
	m.closeSubscriptions(subscriptions)
}

// readyNotificationsLocked joins physical metadata preparation with each view's
// requirements. A loaded pointer alone is not a readiness certificate.
func (m *ViewScopedPhysicalSegmentManager) readyNotificationsLocked(segmentID int64) []func() {
	state := m.segments[segmentID]
	if state == nil || state.segment == nil || state.updating {
		return nil
	}
	var notifications []func()
	for key, request := range state.requests {
		ref := m.views[key]
		if ref == nil || ref.ready[segmentID] || !state.satisfies(request) {
			continue
		}
		if state.pendingSnapshot != nil && state.pendingSnapshot.Revision != state.revision && !state.hasNewerSchema(request) {
			continue
		}
		if cb := ref.onLoaded; cb != nil {
			segment := state.segment
			if provider, ok := UnwrapTransformSegment(segment).(SealedReadStateProvider); ok {
				read, err := provider.PinReadState()
				if err != nil {
					if failed := ref.onSegmentFailed; failed != nil {
						notifications = append(notifications, func() { failed(segmentID, err) })
					} else if failed := ref.onUnrecoverable; failed != nil {
						notifications = append(notifications, failed)
					}
					continue
				}
				segment = &preparedReadSegment{TransformSegment: segment, read: read}
			}
			notifications = append(notifications, func() { cb([]TransformSegment{segment}) })
		}
		ref.ready[segmentID] = true
		request.prepared = true
		state.requests[key] = request
	}
	return notifications
}

func (s *physicalSegmentState) satisfies(request segmentLoadRequest) bool {
	meta := request.meta
	return s.appliedManifestVersion >= request.manifestVersion && s.appliedMeta.GetSchemaVersion() >= meta.GetSchemaVersion() &&
		s.appliedMeta.GetLoadInfoVersion() == meta.GetLoadInfoVersion()
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

	toClose := make([]SegmentLoadInfoSubscription, 0, len(ref.segments))
	for segmentID := range ref.segments {
		state := m.segments[segmentID]
		if state == nil {
			continue
		}
		delete(state.refs, key)
		delete(state.requests, key)
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
			delete(m.pendingLoadSegments, segmentID)
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

func newSegmentLoadRequest(req AcquirePhysicalSegments, segmentID int64) segmentLoadRequest {
	start := req.Meta.GetTransformStartAfterTimetick()
	var manifestVersion int64
	for _, partition := range req.View.GetPartitions() {
		for idx, id := range partition.GetSegmentIds() {
			if id == segmentID && idx < len(partition.GetSegmentManifestVersions()) {
				manifestVersion = partition.GetSegmentManifestVersions()[idx]
			}
			if id == segmentID && idx < len(partition.GetSegmentTransformStartAfterTimeticks()) {
				if cursor := partition.GetSegmentTransformStartAfterTimeticks()[idx]; cursor != 0 {
					start = cursor
				}
			}
		}
	}
	return segmentLoadRequest{
		meta:                        req.Meta,
		collection:                  req.Collection,
		transformStartAfterTimeTick: start,
		manifestVersion:             manifestVersion,
	}
}

func snapshotManifestVersion(snapshot SegmentLoadInfoSnapshot) int64 {
	_, version, _ := packed.UnmarshalManifestPath(snapshot.LoadInfo.GetManifestPath())
	return version
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
	submissions := make([]segmentLoadSubmission, 0, len(m.pendingLoadSegments))
	for segmentID := range m.pendingLoadSegments {
		state := m.segments[segmentID]
		if state == nil {
			delete(m.pendingLoadSegments, segmentID)
			continue
		}
		if state.loading || state.segment != nil || len(state.refs) == 0 {
			continue
		}
		request, ok := state.loadRequest()
		if !ok {
			continue
		}
		loadCtx, loadCancel := context.WithCancel(context.Background())
		loadDone := onceLoadDone(m.trackPendingLoadAttemptLocked(state))
		delete(m.pendingLoadSegments, segmentID)
		state.loading = true
		snapshot := *state.pendingSnapshot
		state.pendingSnapshot = nil
		state.loadCancel = loadCancel
		state.loadDone = loadDone
		submissions = append(submissions, segmentLoadSubmission{
			segmentID: segmentID,
			ctx:       loadCtx,
			request:   request,
			snapshot:  snapshot,
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
	var latest segmentLoadRequest
	var version qviews.QueryViewVersion
	var found bool
	for key := range s.refs {
		request, ok := s.requests[key]
		// LoadInfoVersion is a content hash, not an ordered revision. Use the
		// actual view publication order within each schema. Unprepared serving
		// schemas go first; a retained read snapshot no longer blocks the target.
		if ok && (!found || preferLoadRequest(request, latest, key.QueryViewVersion.GT(version))) {
			latest, version, found = request, key.QueryViewVersion, true
		}
	}
	return latest, found
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
		subscription := m.stream.Subscribe(SegmentLoadInfoSubscriptionOption{
			CollectionID: request.collectionID,
			SegmentID:    request.segmentID,
			Revision:     request.revision,
			Handler: physicalSegmentLoadInfoHandler{
				manager: m,
				state:   request.state,
			},
		})
		if subscription == nil {
			continue
		}
		keep := false
		m.mu.Lock()
		if state := m.segments[request.segmentID]; state == request.state && len(state.refs) > 0 && state.subscription == nil {
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
	manager *ViewScopedPhysicalSegmentManager
	state   *physicalSegmentState
}

func (h physicalSegmentLoadInfoHandler) Handle(snapshot SegmentLoadInfoSnapshot) error {
	h.manager.applyLoadInfoSnapshot(context.Background(), snapshot, h.state)
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
