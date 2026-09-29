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

// QueryViewSegmentReadinessManager turns physically loaded segments into
// QueryView-ready segments by registering them with the TransformLogBuffer and
// waiting for catch-up.
type QueryViewSegmentReadinessManager struct {
	scheduler    nodescheduler.Scheduler
	physical     PhysicalSegmentManager
	buffer       TransformLogBuffer
	collections  QueryViewCollectionRuntimeManager
	catchupTasks chan segmentCatchupTask

	mu         sync.Mutex
	generation uint64
	views      map[qviews.QueryViewKey]*transformViewRef
	segments   map[int64]*transformSegmentState
}

func NewQueryViewSegmentReadinessManagerWithScheduler(
	scheduler nodescheduler.Scheduler,
	physical PhysicalSegmentManager,
	buffer TransformLogBuffer,
	catchupConcurrency int,
	collections ...QueryViewCollectionRuntimeManager,
) *QueryViewSegmentReadinessManager {
	if catchupConcurrency <= 0 {
		panic("query view segment catch-up concurrency must be positive")
	}
	var collectionManager QueryViewCollectionRuntimeManager
	if len(collections) > 0 {
		collectionManager = collections[0]
	}
	m := &QueryViewSegmentReadinessManager{
		scheduler:    scheduler,
		physical:     physical,
		buffer:       buffer,
		collections:  collectionManager,
		catchupTasks: make(chan segmentCatchupTask, 1024),
		views:        make(map[qviews.QueryViewKey]*transformViewRef),
		segments:     make(map[int64]*transformSegmentState),
	}
	for i := 0; i < catchupConcurrency; i++ {
		go m.catchupWorker()
	}
	return m
}

func (m *QueryViewSegmentReadinessManager) Acquire(req AcquireSegments) {
	req = cloneAcquireSegments(req)
	m.acquire(req)
}

func (m *QueryViewSegmentReadinessManager) Release(req ReleaseSegments) {
	m.release(req)
}

type transformSegmentLoadState int

const (
	transformSegmentWaiting transformSegmentLoadState = iota
	transformSegmentLoading
	transformSegmentCatchingUp
	transformSegmentLoaded
)

type transformViewRef struct {
	cancel                 context.CancelFunc
	transformGuard         TransformLogGuard
	collectionGuard        CollectionRuntimeGuard
	segments               map[int64]int64
	onUnrecoverable        func()
	onPoisoned             func(*viewpb.PoisonedSegment)
	unrecoverable          bool
	physicalAcquirePending bool
	pendingReleases        []ReleaseSegments
}

type transformSegmentState struct {
	state         transformSegmentLoadState
	generation    uint64
	poison        *viewpb.PoisonedSegment
	poisonErr     error
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
	state   *transformSegmentState
}

type transformSegmentWaiter struct {
	key             qviews.QueryViewKey
	partitionID     int64
	segmentID       int64
	onReady         func(map[int64][]int64)
	onUnrecoverable func()
}

func (m *QueryViewSegmentReadinessManager) acquire(req AcquireSegments) {
	ctx, cancel := context.WithCancel(context.Background())
	view := qviews.NewQueryViewAtQueryNode(req.Meta, req.View).(*qviews.QueryViewAtQueryNode)
	guard, err := m.buffer.Acquire(ctx, view)
	if err != nil {
		cancel()
		m.submitCallback(req.OnUnrecoverable)
		return
	}

	ref, ok := m.recordPendingAcquire(req, cancel, guard)
	if !ok {
		cancel()
		guard.Release()
		return
	}
	m.scheduler.Submit(schedulerTaskFunc(func(schedulerCtx context.Context) error {
		ctx, stop := mergeTaskContext(schedulerCtx, ctx)
		defer stop()
		return m.continueAcquire(ctx, req, ref, view, cancel)
	}))
}

func (m *QueryViewSegmentReadinessManager) submitCallback(callback func()) {
	if callback == nil {
		return
	}
	m.scheduler.Submit(schedulerTaskFunc(func(context.Context) error {
		callback()
		return nil
	}))
}

func (m *QueryViewSegmentReadinessManager) continueAcquire(ctx context.Context, req AcquireSegments, ref *transformViewRef, view *qviews.QueryViewAtQueryNode, cancel context.CancelFunc) error {
	collectionGuard, retryable, err := m.acquireCollectionRuntime(ctx, view)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		if retryable {
			return nodescheduler.ErrDelay
		}
		cancel()
		if detached, current := m.detachViewIfCurrent(req.Key, ref); current {
			detached.releaseTransform()
			detached.unregister()
			detached.releaseSegments()
			invokeUnrecoverable(req.OnUnrecoverable)
			return err
		}
		return nil
	}

	readyNow, physicalRefSegments, noAssignedSegments, current := m.activateAcquire(req, ref, collectionGuard)
	if !current {
		cancel()
		collectionGuard.Release()
		return nil
	}

	if noAssignedSegments {
		if req.OnReady != nil {
			req.OnReady(map[int64][]int64{})
		}
		return nil
	}
	viewToLoad := filterViewSegments(req.View, physicalRefSegments)

	m.physical.Acquire(AcquirePhysicalSegments{
		Key:        req.Key,
		Meta:       proto.Clone(req.Meta).(*viewpb.QueryViewMeta),
		View:       viewToLoad,
		Collection: collectionGuard,
		OnLoaded: func(loaded []TransformSegment) {
			m.onPhysicalLoaded(loaded)
		},
		OnSegmentUnrecoverable: func(segmentID int64, err error) {
			m.failSegment(segmentID, nil, err)
		},
		OnUnrecoverable: func() {
			m.failView(req.Key)
		},
	})
	if m.finishPhysicalAcquire(req.Key, ref) {
		for _, waiter := range readyNow {
			waiter.reportReady()
		}
	}
	return nil
}

func (m *QueryViewSegmentReadinessManager) acquireCollectionRuntime(ctx context.Context, view *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
	if m.collections == nil {
		return nil, false, nil
	}
	return m.collections.Acquire(ctx, view)
}

func (m *QueryViewSegmentReadinessManager) recordPendingAcquire(req AcquireSegments, cancel context.CancelFunc, guard TransformLogGuard) (*transformViewRef, bool) {
	segmentPartitions := segmentPartitionMap(req.View)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.views[req.Key] != nil {
		return nil, false
	}
	ref := &transformViewRef{
		cancel:          cancel,
		transformGuard:  guard,
		segments:        segmentPartitions,
		onUnrecoverable: req.OnUnrecoverable,
		onPoisoned:      req.OnPoisoned,
	}
	m.views[req.Key] = ref
	for segmentID, partitionID := range segmentPartitions {
		state := m.segments[segmentID]
		if state == nil {
			state = &transformSegmentState{
				state:   transformSegmentWaiting,
				refs:    make(map[qviews.QueryViewKey]struct{}),
				waiters: make(map[qviews.QueryViewKey]transformSegmentWaiter),
			}
			m.segments[segmentID] = state
		}
		state.refs[req.Key] = struct{}{}
		state.waiters[req.Key] = transformSegmentWaiter{
			key:             req.Key,
			partitionID:     partitionID,
			segmentID:       segmentID,
			onReady:         req.OnReady,
			onUnrecoverable: req.OnUnrecoverable,
		}
	}
	return ref, true
}

func (m *QueryViewSegmentReadinessManager) detachViewIfCurrent(key qviews.QueryViewKey, ref *transformViewRef) (transformViewDetach, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.views[key] != ref {
		return transformViewDetach{}, false
	}
	return m.detachViewLocked(key), true
}

func (m *QueryViewSegmentReadinessManager) activateAcquire(req AcquireSegments, ref *transformViewRef, collectionGuard CollectionRuntimeGuard) ([]transformSegmentWaiter, []int64, bool, bool) {
	readyNow := make([]transformSegmentWaiter, 0)
	physicalRefSegments := make([]int64, 0)

	m.mu.Lock()
	if m.views[req.Key] != ref {
		m.mu.Unlock()
		return nil, nil, false, false
	}
	ref.physicalAcquirePending = len(ref.segments) > 0
	ref.collectionGuard = collectionGuard
	ref.onUnrecoverable = req.OnUnrecoverable
	for segmentID := range ref.segments {
		// Every view owns a physical reference, including loaded/catching-up segments.
		// The physical manager deduplicates loading and owns metadata subscriptions.
		physicalRefSegments = append(physicalRefSegments, segmentID)
		state := m.segments[segmentID]
		if state == nil {
			state = &transformSegmentState{
				state:   transformSegmentWaiting,
				refs:    make(map[qviews.QueryViewKey]struct{}),
				waiters: make(map[qviews.QueryViewKey]transformSegmentWaiter),
			}
			m.segments[segmentID] = state
			state.refs[req.Key] = struct{}{}
		}
		waiter := transformSegmentWaiter{
			key:             req.Key,
			partitionID:     ref.segments[segmentID],
			segmentID:       segmentID,
			onReady:         req.OnReady,
			onUnrecoverable: req.OnUnrecoverable,
		}
		if state.poison != nil {
			poison := state.poison
			go func() {
				if req.OnPoisoned != nil {
					req.OnPoisoned(poison)
				}
				m.notifyUnrecoverable(req.Key, req.OnUnrecoverable)
			}()
			continue
		}
		if state.state == transformSegmentLoaded {
			readyNow = append(readyNow, waiter)
			delete(state.waiters, req.Key)
			continue
		}
		if state.state == transformSegmentWaiting {
			state.state = transformSegmentLoading
		}
		state.waiters[req.Key] = waiter
	}
	m.mu.Unlock()

	return readyNow, physicalRefSegments, len(ref.segments) == 0, true
}

func invokeUnrecoverable(cb func()) {
	if cb != nil {
		cb()
	}
}

func (m *QueryViewSegmentReadinessManager) onPhysicalLoaded(segments []TransformSegment) {
	for _, segment := range segments {
		if segment == nil {
			continue
		}
		if kept, task := m.markPhysicalLoaded(segment); task.state != nil {
			m.scheduleCatchup(task)
		} else if !kept {
			_ = segment.Release(context.Background())
		}
	}
}

func (m *QueryViewSegmentReadinessManager) scheduleCatchup(task segmentCatchupTask) {
	m.catchupTasks <- task
}

func (m *QueryViewSegmentReadinessManager) catchupWorker() {
	for task := range m.catchupTasks {
		m.registerAndCatchup(task)
	}
}

func (m *QueryViewSegmentReadinessManager) markPhysicalLoaded(segment TransformSegment) (bool, segmentCatchupTask) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.segments[segment.ID()]
	if state == nil || len(state.refs) == 0 {
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
	return true, segmentCatchupTask{ctx: ctx, segment: segment, state: state}
}

func (m *QueryViewSegmentReadinessManager) registerAndCatchup(task segmentCatchupTask) {
	if task.ctx.Err() != nil {
		return
	}
	reg, err := m.buffer.RegisterSegment(task.ctx, &observedTransformSegment{TransformSegment: task.segment, manager: m, state: task.state})
	if err != nil {
		m.failSegment(task.segment.ID(), task.state, err)
		return
	}
	if !m.storeRegistration(task, reg) {
		reg.Unregister()
		return
	}
	if err := reg.WaitCatchup(task.ctx); err != nil {
		reg.Unregister()
		m.failSegment(task.segment.ID(), task.state, err)
		return
	}
	for _, waiter := range m.markSegmentReady(task) {
		waiter.reportReady()
	}
}

func (m *QueryViewSegmentReadinessManager) storeRegistration(task segmentCatchupTask, reg TransformRegistration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.segments[task.segment.ID()]
	if state != task.state || len(state.refs) == 0 {
		return false
	}
	state.reg = reg
	return true
}

func (m *QueryViewSegmentReadinessManager) markSegmentReady(task segmentCatchupTask) []transformSegmentWaiter {
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
		if m.views[key] == nil {
			continue
		}
		waiters = append(waiters, waiter)
	}
	state.waiters = make(map[qviews.QueryViewKey]transformSegmentWaiter)
	return waiters
}

func (m *QueryViewSegmentReadinessManager) failSegment(segmentID int64, expected *transformSegmentState, err error) {
	m.mu.Lock()
	state := m.segments[segmentID]
	if state == nil || (expected != nil && state != expected) {
		m.mu.Unlock()
		return
	}
	reg := state.reg
	cancel := state.catchupCancel
	// Hold a cleanup reference until transform unregistration completes, even
	// if the last query handle is released concurrently. Existing handles keep
	// this retired state alive independently of a replacement in m.segments.
	state.queryRefs++
	state.refs = nil
	waiters := make([]transformSegmentWaiter, 0, len(state.waiters))
	for _, waiter := range state.waiters {
		waiters = append(waiters, waiter)
	}
	delete(m.segments, segmentID)
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if reg != nil {
		reg.Unregister()
	}
	m.releaseSealedSegmentHandle(segmentID, state)
	if resetter, ok := m.physical.(PhysicalSegmentResetter); ok {
		resetter.ResetSegment(segmentID)
	}
	if err == nil {
		err = errors.New("segment became unrecoverable")
	}
	for _, waiter := range waiters {
		qvobserve.Observe(context.TODO(), qvobserve.QueryNodeSegmentFailureEvent{
			View:      waiter.key,
			SegmentID: segmentID,
			Err:       err,
		})
		m.notifyUnrecoverable(waiter.key, waiter.onUnrecoverable)
	}
}

func (m *QueryViewSegmentReadinessManager) failView(key qviews.QueryViewKey) {
	m.mu.Lock()
	ref := m.views[key]
	if ref == nil || ref.unrecoverable {
		m.mu.Unlock()
		return
	}
	ref.unrecoverable = true
	cb := ref.onUnrecoverable
	for segmentID := range ref.segments {
		if state := m.segments[segmentID]; state != nil {
			delete(state.waiters, key)
		}
	}
	m.mu.Unlock()

	if cb != nil {
		cb()
	}
}

func (m *QueryViewSegmentReadinessManager) notifyUnrecoverable(key qviews.QueryViewKey, cb func()) {
	m.mu.Lock()
	ref := m.views[key]
	if ref == nil || ref.unrecoverable {
		m.mu.Unlock()
		return
	}
	ref.unrecoverable = true
	for segmentID := range ref.segments {
		if state := m.segments[segmentID]; state != nil {
			delete(state.waiters, key)
		}
	}
	m.mu.Unlock()
	invokeUnrecoverable(cb)
}

// finishPhysicalAcquire orders a concurrent release after physical reference
// registration without holding the readiness lock across an external call.
func (m *QueryViewSegmentReadinessManager) finishPhysicalAcquire(key qviews.QueryViewKey, ref *transformViewRef) bool {
	m.mu.Lock()
	ref.physicalAcquirePending = false
	releases := ref.pendingReleases
	ref.pendingReleases = nil
	current := m.views[key] == ref
	m.mu.Unlock()
	for _, req := range releases {
		m.release(req)
	}
	return current && len(releases) == 0
}

func (m *QueryViewSegmentReadinessManager) release(req ReleaseSegments) {
	m.mu.Lock()
	if ref := m.views[req.Key]; ref != nil && ref.physicalAcquirePending {
		ref.pendingReleases = append(ref.pendingReleases, req)
		m.mu.Unlock()
		return
	}
	detached := m.detachViewLocked(req.Key)
	m.mu.Unlock()
	detached.releaseTransform()
	detached.unregister()
	detached.releaseSegments()
	m.physical.Release(ReleaseSegments{
		Key: req.Key,
		OnDropped: func() {
			detached.releaseCollection()
			if req.OnDropped != nil {
				req.OnDropped()
			}
		},
	})
}

type transformViewGuards struct {
	transform  TransformLogGuard
	collection CollectionRuntimeGuard
}

type transformViewDetach struct {
	guards   transformViewGuards
	cancels  []context.CancelFunc
	regs     []TransformRegistration
	segments []TransformSegment
}

func (d transformViewDetach) releaseTransform() {
	if d.guards.transform != nil {
		d.guards.transform.Release()
	}
}

func (d transformViewDetach) releaseCollection() {
	if d.guards.collection != nil {
		d.guards.collection.Release()
	}
}

func (d transformViewDetach) unregister() {
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

func (d transformViewDetach) releaseSegments() {
	for _, segment := range d.segments {
		if segment != nil {
			_ = segment.Release(context.Background())
		}
	}
}

func (m *QueryViewSegmentReadinessManager) detachViewLocked(key qviews.QueryViewKey) transformViewDetach {
	ref := m.views[key]
	if ref == nil {
		return transformViewDetach{}
	}
	delete(m.views, key)
	if ref.cancel != nil {
		ref.cancel()
	}
	detached := transformViewDetach{
		guards: transformViewGuards{transform: ref.transformGuard, collection: ref.collectionGuard},
	}
	for segmentID := range ref.segments {
		state := m.segments[segmentID]
		if state == nil {
			continue
		}
		delete(state.refs, key)
		delete(state.waiters, key)
		if len(state.refs) == 0 {
			if state.catchupCancel != nil {
				detached.cancels = append(detached.cancels, state.catchupCancel)
				state.catchupCancel = nil
			}
			if state.reg != nil {
				detached.regs = append(detached.regs, state.reg)
				state.reg = nil
			}
			if state.segment != nil {
				if state.queryRefs > 0 {
					continue
				}
				detached.segments = append(detached.segments, state.segment)
				delete(m.segments, segmentID)
				continue
			}
			delete(m.segments, segmentID)
		}
	}
	return detached
}

func (m *QueryViewSegmentReadinessManager) releaseDetachedSegment(segment TransformSegment) {
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
