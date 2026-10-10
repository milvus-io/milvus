package qnview

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	qvobserve "github.com/milvus-io/milvus/internal/views/qviews/observe"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// QueryViewSegmentManager owns the View references and Segment instances used
// by physical preparation, Transform catch-up, and query execution.
type QueryViewSegmentManager struct {
	scheduler          nodescheduler.Scheduler
	loader             PhysicalSegmentLoader
	estimator          SegmentResourceEstimator
	stream             SegmentLoadInfoStream
	nextLoadGeneration uint64
	buffer             TransformLogBuffer
	collections        QueryViewCollectionRuntimeManager

	mu         sync.Mutex
	generation uint64
	views      map[qviews.QueryViewKey]*queryViewRef
	segments   map[int64]*segmentState
}

// QueryViewSegmentManagerConfig supplies execution dependencies, not resource owners.
type QueryViewSegmentManagerConfig struct {
	Scheduler      nodescheduler.Scheduler
	Loader         PhysicalSegmentLoader
	Estimator      SegmentResourceEstimator
	LoadInfoStream SegmentLoadInfoStream
	Buffer         TransformLogBuffer
	Collections    QueryViewCollectionRuntimeManager
}

func NewQueryViewSegmentManager(cfg QueryViewSegmentManagerConfig) *QueryViewSegmentManager {
	m := &QueryViewSegmentManager{
		scheduler:   cfg.Scheduler,
		loader:      cfg.Loader,
		estimator:   cfg.Estimator,
		stream:      cfg.LoadInfoStream,
		buffer:      cfg.Buffer,
		collections: cfg.Collections,
		views:       make(map[qviews.QueryViewKey]*queryViewRef),
		segments:    make(map[int64]*segmentState),
	}
	return m
}

func (m *QueryViewSegmentManager) Acquire(req AcquireSegments) {
	req = cloneAcquireSegments(req)
	m.acquire(req)
}

func (m *QueryViewSegmentManager) Release(req ReleaseSegments) {
	m.release(req)
}

type transformSegmentLoadState int

const (
	transformSegmentWaiting transformSegmentLoadState = iota
	transformSegmentLoading
	transformSegmentCatchingUp
	transformSegmentLoaded
)

type queryViewRef struct {
	ctx             context.Context
	prepared        bool
	pendingLoads    int
	releaseReady    bool
	onDropped       func()
	loadInfo        *QueryViewLoadInfo
	dataVersion     qviews.DataVersion
	onAvailable     func([]TransformSegment)
	available       map[int64]bool
	loaded          map[int64]bool
	onLoaded        func([]TransformSegment)
	onSegmentFailed func(int64, error)

	states          map[int64]*segmentState
	queryRefs       int
	dropping        bool
	physicalReady   map[int64]bool
	cancel          context.CancelFunc
	transformGuard  *retainedTransformGuard
	collectionGuard CollectionRuntimeGuard
	segments        map[int64]int64
	onUnrecoverable func()
	unrecoverable   bool
	pendingReleases []ReleaseSegments
}

// segmentState is the sole physical and Transform identity for one instance.
// View references, query handles, and asynchronous tasks all pin this state.
type segmentState struct {
	taskRefs       int
	cleanupPending bool // Unregistration outside mu must finish before native release.

	resources         *QueryViewLoadInfo
	appliedResources  *QueryViewLoadInfo
	lastSnapshot      *SegmentLoadInfoSnapshot
	dataVersion       qviews.DataVersion
	acceptedVersion   qviews.DataVersion
	targetVersion     qviews.DataVersion
	subscriptionEpoch uint64
	collectionID      int64
	loading           bool
	loadGeneration    uint64
	updating          bool
	updateHandle      nodescheduler.TaskHandle
	updateEpoch       uint64
	loadCancel        context.CancelFunc
	requests          map[qviews.QueryViewKey]segmentLoadRequest
	revision          SegmentLoadInfoRevision
	pendingSnapshot   *SegmentLoadInfoSnapshot
	subscription      SegmentLoadInfoSubscription

	replayGuard   *retainedTransformGuard
	replayStart   uint64
	state         transformSegmentLoadState
	generation    uint64
	poison        *segmentPoison
	segment       TransformSegment
	reg           TransformRegistration
	catchupCancel context.CancelFunc
	queryRefs     int
	refs          map[qviews.QueryViewKey]struct{}
	waiters       map[qviews.QueryViewKey]transformSegmentWaiter
}

// A task retains the state identity it was created for. Segment IDs can be
// reused after a view drops while an older asynchronous task is still returning.
type segmentCatchupTask struct {
	ctx     context.Context
	segment TransformSegment
	state   *segmentState
}

type transformSegmentWaiter struct {
	key             qviews.QueryViewKey
	partitionID     int64
	segmentID       int64
	onReady         func(map[int64][]int64)
	onUnrecoverable func()
}

func (m *QueryViewSegmentManager) acquire(req AcquireSegments) {
	ctx, cancel := context.WithCancel(context.Background())
	ref, ok := m.recordPendingAcquire(ctx, req, cancel)
	if !ok {
		cancel()
		return
	}
	// Buffer subscription can wait on a remote SN. The reference is already
	// visible to Release and to overlapping views before this work begins.
	go func() {
		view := qviews.NewQueryViewAtQueryNode(req.Meta, req.View).(*qviews.QueryViewAtQueryNode)
		guard, err := m.buffer.Acquire(ctx, view)
		if err != nil {
			m.failView(req.Key, ref)
			return
		}
		m.mu.Lock()
		if m.views[req.Key] != ref || ref.unrecoverable {
			m.mu.Unlock()
			guard.Release()
			return
		}
		previous := ref.transformGuard
		ref.transformGuard = newRetainedTransformGuard(guard, req.Meta.GetTransformStartAfterTimetick())
		m.mu.Unlock()
		if previous != nil {
			previous.Release()
		}
		m.scheduler.Submit(schedulerTaskFunc(func(schedulerCtx context.Context) error {
			ctx, stop := mergeTaskContext(schedulerCtx, ctx)
			defer stop()
			return m.continueAcquire(ctx, req, ref, view, cancel)
		}))
	}()
}

func (m *QueryViewSegmentManager) submitCallback(callback func()) {
	if callback == nil {
		return
	}
	m.scheduler.Submit(schedulerTaskFunc(func(context.Context) error {
		callback()
		return nil
	}))
}

func (m *QueryViewSegmentManager) continueAcquire(ctx context.Context, req AcquireSegments, ref *queryViewRef, view *qviews.QueryViewAtQueryNode, cancel context.CancelFunc) error {
	collectionGuard, retryable, err := m.acquireCollectionRuntime(ctx, view)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		if retryable {
			return nodescheduler.ErrDelay
		}
		m.failView(req.Key, ref)
		return nil
	}

	physicalRefSegments, noAssignedSegments, current := m.activateAcquire(req, ref, collectionGuard)
	if !current {
		cancel()
		if collectionGuard != nil {
			collectionGuard.Release()
		}
		return nil
	}

	if noAssignedSegments {
		if req.OnReady != nil {
			req.OnReady(map[int64][]int64{})
		}
		return nil
	}
	viewToLoad := filterViewSegments(req.View, physicalRefSegments)

	var loadInfo *QueryViewLoadInfo
	if provider, ok := collectionGuard.(QueryViewLoadInfoProvider); ok {
		info := provider.LoadInfo()
		loadInfo = &info
	}
	m.preparePhysical(segmentPreparationRequest{
		Context:     ref.ctx,
		expected:    ref,
		LoadInfo:    loadInfo,
		OnAvailable: func(loaded []TransformSegment) { m.onViewPhysicalAvailable(ref, loaded) },
		Key:         req.Key,
		Meta:        proto.Clone(req.Meta).(*viewpb.QueryViewMeta),
		View:        viewToLoad,
		Collection:  collectionGuard,
		OnLoaded: func(loaded []TransformSegment) {
			m.onViewPhysicalLoaded(req.Key, ref, loaded)
		},
		OnSegmentUnrecoverable: func(segmentID int64, err error) {
			m.failViewSegment(ref, segmentID, err)
		},
		OnUnrecoverable: func() {
			m.failView(req.Key, ref)
		},
	})
	return nil
}

func (m *QueryViewSegmentManager) acquireCollectionRuntime(ctx context.Context, view *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
	if m.collections == nil {
		return nil, false, nil
	}
	return m.collections.Acquire(ctx, view)
}

func (m *QueryViewSegmentManager) recordPendingAcquire(ctx context.Context, req AcquireSegments, cancel context.CancelFunc) (*queryViewRef, bool) {
	segmentPartitions := segmentPartitionMap(req.View)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.views[req.Key] != nil {
		return nil, false
	}
	ref := &queryViewRef{
		ctx:             ctx,
		available:       make(map[int64]bool),
		loaded:          make(map[int64]bool),
		cancel:          cancel,
		segments:        segmentPartitions,
		physicalReady:   make(map[int64]bool),
		states:          make(map[int64]*segmentState),
		onUnrecoverable: req.OnUnrecoverable,
	}
	// Keep an existing channel subscription alive during asynchronous handoff.
	// The new view installs its own retention frontier before preparing data.
	for key, previous := range m.views {
		guard := previous.transformGuard
		if key.ShardID.VChannel == req.Key.ShardID.VChannel && guard != nil && (ref.transformGuard == nil || guard.startAfter < ref.transformGuard.startAfter) {
			ref.transformGuard = guard
		}
	}
	if ref.transformGuard != nil {
		ref.transformGuard.retain()
	}
	m.views[req.Key] = ref
	for segmentID, partitionID := range segmentPartitions {
		state := m.segments[segmentID]
		if state == nil {
			state = &segmentState{
				state:        transformSegmentWaiting,
				collectionID: req.Meta.GetCollectionId(),
				requests:     make(map[qviews.QueryViewKey]segmentLoadRequest),
				refs:         make(map[qviews.QueryViewKey]struct{}),
				waiters:      make(map[qviews.QueryViewKey]transformSegmentWaiter),
			}
			m.segments[segmentID] = state
		}
		state.refs[req.Key] = struct{}{}
		ref.states[segmentID] = state
		state.waiters[req.Key] = transformSegmentWaiter{
			key:             req.Key,
			partitionID:     partitionID,
			segmentID:       segmentID,
			onReady:         req.OnReady,
			onUnrecoverable: req.OnUnrecoverable,
		}
	}
	ref.onAvailable = func(loaded []TransformSegment) { m.onViewPhysicalAvailable(ref, loaded) }
	ref.onSegmentFailed = func(segmentID int64, err error) { m.failViewSegment(ref, segmentID, err) }
	return ref, true
}

func (m *QueryViewSegmentManager) activateAcquire(req AcquireSegments, ref *queryViewRef, collectionGuard CollectionRuntimeGuard) ([]int64, bool, bool) {
	physicalRefSegments := make([]int64, 0)
	var supersededGuards []*retainedTransformGuard

	m.mu.Lock()
	if m.views[req.Key] != ref || ref.unrecoverable {
		m.mu.Unlock()
		return nil, false, false
	}
	ref.collectionGuard = collectionGuard
	ref.onUnrecoverable = req.OnUnrecoverable
	for segmentID := range ref.segments {
		// Preparation uses the instance registered synchronously by Acquire.
		physicalRefSegments = append(physicalRefSegments, segmentID)
		state := ref.states[segmentID]
		waiter := transformSegmentWaiter{
			key:             req.Key,
			partitionID:     ref.segments[segmentID],
			segmentID:       segmentID,
			onReady:         req.OnReady,
			onUnrecoverable: req.OnUnrecoverable,
		}
		if state.poison != nil {
			go m.notifyUnrecoverable(req.Key, req.OnUnrecoverable)
			continue
		}

		if state.state == transformSegmentWaiting {
			state.state = transformSegmentLoading
		}
		if state.reg == nil && (state.replayGuard == nil || req.Meta.GetTransformStartAfterTimetick() < state.replayStart) {
			if state.replayGuard != nil {
				supersededGuards = append(supersededGuards, state.replayGuard)
			}
			state.replayGuard = ref.transformGuard.retain()
			state.replayStart = req.Meta.GetTransformStartAfterTimetick()
		}
		state.waiters[req.Key] = waiter
	}
	m.mu.Unlock()
	for _, guard := range supersededGuards {
		guard.Release()
	}

	return physicalRefSegments, len(ref.segments) == 0, true
}

func invokeUnrecoverable(cb func()) {
	if cb != nil {
		cb()
	}
}

func (m *QueryViewSegmentManager) onViewPhysicalAvailable(ref *queryViewRef, segments []TransformSegment) {
	m.mu.Lock()
	states := make(map[int64]*segmentState, len(segments))
	for _, segment := range segments {
		states[segment.ID()] = ref.states[segment.ID()]
	}
	m.mu.Unlock()
	m.onPhysicalLoaded(segments, states)
}

func (m *QueryViewSegmentManager) failViewSegment(ref *queryViewRef, segmentID int64, err error) {
	m.mu.Lock()
	state := ref.states[segmentID]
	m.mu.Unlock()
	if state != nil {
		m.failSegment(segmentID, state, err)
	}
}

// Physical callbacks are view-scoped: a shared transform-ready instance alone
// does not prove that this view's data version and load configuration are ready.
func (m *QueryViewSegmentManager) onViewPhysicalLoaded(key qviews.QueryViewKey, expected *queryViewRef, segments []TransformSegment) {
	m.onViewPhysicalAvailable(expected, segments)
	m.mu.Lock()
	if m.views[key] != expected || expected.unrecoverable {
		m.mu.Unlock()
		return
	}
	for _, segment := range segments {
		expected.physicalReady[segment.ID()] = true
	}
	m.mu.Unlock()
	var ready []transformSegmentWaiter
	m.mu.Lock()
	if m.views[key] == expected && !expected.unrecoverable {
		for _, segment := range segments {
			state := m.segments[segment.ID()]
			if state != nil && state.state == transformSegmentLoaded && state.poison == nil {
				if waiter, ok := state.waiters[key]; ok {
					ready = append(ready, waiter)
					delete(state.waiters, key)
				}
			}
		}
	}
	m.mu.Unlock()
	for _, waiter := range ready {
		waiter.reportReady()
	}
}

func (m *QueryViewSegmentManager) onPhysicalLoaded(segments []TransformSegment, expected ...map[int64]*segmentState) {
	for _, segment := range segments {
		if segment == nil {
			continue
		}
		var states []*segmentState
		if len(expected) > 0 {
			states = append(states, expected[0][segment.ID()])
		}
		if _, task := m.markPhysicalLoaded(segment, states...); task.state != nil {
			m.registerAndCatchup(task)
		}
	}
}

func (m *QueryViewSegmentManager) markPhysicalLoaded(segment TransformSegment, expected ...*segmentState) (bool, segmentCatchupTask) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.segments[segment.ID()]
	if state == nil || len(state.refs) == 0 || (len(expected) > 0 && state != expected[0]) {
		return false, segmentCatchupTask{}
	}
	if state.state == transformSegmentLoaded || state.state == transformSegmentCatchingUp {
		return true, segmentCatchupTask{}
	}
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // owned by state; canceled on completion, failure or detach
	m.generation++
	state.generation = m.generation
	state.segment = segment
	state.state = transformSegmentCatchingUp
	state.catchupCancel = cancel
	state.taskRefs++
	return true, segmentCatchupTask{ctx: ctx, segment: segment, state: state}
}

func (m *QueryViewSegmentManager) registerAndCatchup(task segmentCatchupTask) {
	if err := task.ctx.Err(); err != nil {
		m.completeCatchup(task, err)
		return
	}
	reg, err := m.buffer.RegisterSegment(task.ctx, &observedTransformSegment{TransformSegment: task.segment, manager: m, state: task.state})
	if err != nil {
		m.completeCatchup(task, err)
		return
	}
	if !m.storeRegistration(task, reg) {
		reg.Unregister()
		m.completeCatchup(task, context.Canceled)
		return
	}
	reg.Catchup(task.ctx, func(err error) { m.completeCatchup(task, err) })
}

func (m *QueryViewSegmentManager) completeCatchup(task segmentCatchupTask, err error) {
	defer func() {
		m.mu.Lock()
		task.state.taskRefs--
		m.mu.Unlock()
		m.releaseSegmentState(task.state)
	}()
	if err != nil {
		m.failSegment(task.segment.ID(), task.state, err)
		return
	}
	for _, waiter := range m.markSegmentReady(task) {
		waiter.reportReady()
	}
}

func (m *QueryViewSegmentManager) storeRegistration(task segmentCatchupTask, reg TransformRegistration) bool {
	m.mu.Lock()
	state := m.segments[task.segment.ID()]
	if state != task.state || len(state.refs) == 0 {
		m.mu.Unlock()
		return false
	}
	state.reg = reg
	guard := state.replayGuard
	state.replayGuard = nil
	m.mu.Unlock()
	if guard != nil {
		guard.Release()
	}
	return true
}

func (m *QueryViewSegmentManager) markSegmentReady(task segmentCatchupTask) []transformSegmentWaiter {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.segments[task.segment.ID()]
	if state != task.state || state.state != transformSegmentCatchingUp {
		return nil
	}
	state.catchupCancel()
	state.catchupCancel = nil
	state.state = transformSegmentLoaded
	if state.poison != nil {
		// Poison publication owns failure notification for the waiting views.
		return nil
	}
	waiters := make([]transformSegmentWaiter, 0, len(state.waiters))
	for key, waiter := range state.waiters {
		if ref := m.views[key]; ref == nil || ref.unrecoverable || !ref.physicalReady[task.segment.ID()] {
			continue
		}
		waiters = append(waiters, waiter)
		delete(state.waiters, key)
	}
	return waiters
}

func (m *QueryViewSegmentManager) failSegment(segmentID int64, expected *segmentState, err error) {
	m.mu.Lock()
	state := m.segments[segmentID]
	if state == nil || (expected != nil && state != expected) {
		m.mu.Unlock()
		return
	}
	reg := state.reg
	guard := state.replayGuard
	state.replayGuard = nil
	cancel := state.catchupCancel
	// Hold a cleanup reference until transform unregistration completes, even
	// if the last query handle is released concurrently. Existing handles keep
	// this retired state alive independently of a replacement in m.segments.
	state.queryRefs++
	// Failed preparation keeps the owning View references until Release.
	waiters := make([]transformSegmentWaiter, 0, len(state.waiters))
	for _, waiter := range state.waiters {
		if ref := m.views[waiter.key]; ref != nil && !ref.unrecoverable {
			ref.unrecoverable = true
			waiters = append(waiters, waiter)
		}
	}
	m.cancelSegmentUpdateLocked(state)
	if state.loadCancel != nil {
		state.loadCancel()
	}
	subscriptions := m.detachSubscriptionLocked(state)
	delete(m.segments, segmentID)
	m.mu.Unlock()

	m.closeSubscriptions(subscriptions)
	if guard != nil {
		guard.Release()
	}
	if cancel != nil {
		cancel()
	}
	if reg != nil {
		reg.Unregister()
	}
	m.releaseSealedSegmentHandle(segmentID, state)
	if err == nil {
		err = errors.New("segment became unrecoverable")
	}
	for _, waiter := range waiters {
		qvobserve.Observe(context.TODO(), qvobserve.QueryNodeSegmentFailureEvent{
			View:      waiter.key,
			SegmentID: segmentID,
			Err:       err,
		})
		invokeUnrecoverable(waiter.onUnrecoverable)
	}
}

func (m *QueryViewSegmentManager) failView(key qviews.QueryViewKey, expected ...*queryViewRef) {
	m.mu.Lock()
	ref := m.views[key]
	if ref == nil || ref.unrecoverable || (len(expected) > 0 && ref != expected[0]) {
		m.mu.Unlock()
		return
	}
	ref.unrecoverable = true
	cb := ref.onUnrecoverable
	for segmentID := range ref.segments {
		if state := ref.states[segmentID]; state != nil {
			delete(state.waiters, key)
		}
	}
	m.mu.Unlock()

	if cb != nil {
		cb()
	}
}

func (m *QueryViewSegmentManager) notifyUnrecoverable(key qviews.QueryViewKey, cb func()) {
	m.mu.Lock()
	ref := m.views[key]
	if ref == nil || ref.unrecoverable {
		m.mu.Unlock()
		return
	}
	ref.unrecoverable = true
	for segmentID := range ref.segments {
		if state := ref.states[segmentID]; state != nil {
			delete(state.waiters, key)
		}
	}
	m.mu.Unlock()
	invokeUnrecoverable(cb)
}

func (m *QueryViewSegmentManager) release(req ReleaseSegments) {
	m.mu.Lock()
	if ref := m.views[req.Key]; ref != nil && ref.queryRefs > 0 {
		ref.dropping = true
		onDropped := req.OnDropped
		req.OnDropped = nil
		ref.pendingReleases = append(ref.pendingReleases, req)
		m.mu.Unlock()
		// Query handles retain the view's load requirements until they finish.
		// The view itself can leave the routing/state-machine registry now.
		m.submitCallback(onDropped)
		return
	}
	detached := m.detachViewLocked(req.Key)
	m.mu.Unlock()
	m.finishViewRelease(detached, req.OnDropped)
}

func (m *QueryViewSegmentManager) finishViewRelease(detached viewDetach, onDropped func()) {
	detached.releaseTransform()
	detached.unregister()
	m.closeSubscriptions(detached.subscriptions)
	m.mu.Lock()
	for _, state := range detached.segments {
		state.cleanupPending = false
	}
	m.mu.Unlock()
	complete := func() {
		detached.releaseSegments()
		detached.releaseCollection()
		m.submitCallback(onDropped)
	}
	m.mu.Lock()
	if detached.ref != nil {
		detached.ref.releaseReady = true
		if detached.ref.pendingLoads > 0 {
			detached.ref.onDropped = complete
			m.mu.Unlock()
			return
		}
	}
	m.mu.Unlock()
	complete()
}

type viewGuards struct {
	transform  *retainedTransformGuard
	collection CollectionRuntimeGuard
}

type viewDetach struct {
	manager       *QueryViewSegmentManager
	ref           *queryViewRef
	subscriptions []SegmentLoadInfoSubscription
	guards        viewGuards
	replayGuards  []*retainedTransformGuard
	cancels       []context.CancelFunc
	regs          []TransformRegistration
	segments      []*segmentState
}

func (d viewDetach) releaseTransform() {
	if d.guards.transform != nil {
		d.guards.transform.Release()
	}
}

func (d viewDetach) releaseCollection() {
	if d.guards.collection != nil {
		d.guards.collection.Release()
	}
}

func (d viewDetach) unregister() {
	for _, guard := range d.replayGuards {
		guard.Release()
	}
	for _, cancel := range d.cancels {
		if cancel != nil {
			cancel()
		}
	}
	for _, reg := range d.regs {
		if reg != nil {
			reg.Unregister()
		}
	}
}

func (d viewDetach) releaseSegments() {
	for _, state := range d.segments {
		d.manager.releaseSegmentState(state)
	}
}

func (m *QueryViewSegmentManager) detachViewLocked(key qviews.QueryViewKey) viewDetach {
	ref := m.views[key]
	if ref == nil {
		return viewDetach{}
	}
	delete(m.views, key)
	ref.dropping = true
	if ref.cancel != nil {
		ref.cancel()
	}
	detached := viewDetach{
		ref: ref, manager: m,
		guards: viewGuards{transform: ref.transformGuard, collection: ref.collectionGuard},
	}
	for segmentID := range ref.segments {
		state := ref.states[segmentID]
		if state == nil {
			continue
		}
		delete(state.refs, key)
		delete(state.requests, key)
		state.resources = unionLoadInfo(state.requests)
		delete(state.waiters, key)
		if len(state.refs) == 0 {
			m.cancelSegmentUpdateLocked(state)
			if state.loadCancel != nil {
				state.loadCancel()
			}
			detached.subscriptions = append(detached.subscriptions, m.detachSubscriptionLocked(state)...)
			if state.replayGuard != nil {
				detached.replayGuards = append(detached.replayGuards, state.replayGuard)
				state.replayGuard = nil
			}
			if state.catchupCancel != nil {
				detached.cancels = append(detached.cancels, state.catchupCancel)
				state.catchupCancel = nil
			}
			if state.reg != nil {
				detached.regs = append(detached.regs, state.reg)
				state.reg = nil
			}
			state.cleanupPending = true
			detached.segments = append(detached.segments, state)
			if m.segments[segmentID] == state {
				delete(m.segments, segmentID)
			}
		}
	}
	return detached
}

func (m *QueryViewSegmentManager) releaseSegmentState(state *segmentState) {
	m.mu.Lock()
	var segment TransformSegment
	if len(state.refs) == 0 && state.queryRefs == 0 && state.taskRefs == 0 && !state.cleanupPending {
		segment = state.segment
		state.segment = nil
	}
	m.mu.Unlock()
	m.releaseDetachedSegment(segment)
}

func (m *QueryViewSegmentManager) releaseDetachedSegment(segment TransformSegment) {
	if segment != nil {
		_ = segment.Release(context.Background())
	}
}

func (w transformSegmentWaiter) reportReady() {
	if w.onReady != nil {
		w.onReady(map[int64][]int64{w.partitionID: {w.segmentID}})
	}
}

func cloneAcquireSegments(req AcquireSegments) AcquireSegments {
	out := req
	if req.Meta != nil {
		out.Meta = proto.Clone(req.Meta).(*viewpb.QueryViewMeta)
	}
	if req.View != nil {
		out.View = proto.Clone(req.View).(*viewpb.QueryViewOfQueryNode)
	}
	return out
}
