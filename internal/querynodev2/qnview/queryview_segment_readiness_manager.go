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
	catchupTasks chan TransformSegment

	mu       sync.Mutex
	views    map[qviews.QueryViewKey]*transformViewRef
	segments map[int64]*transformSegmentState
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
		catchupTasks: make(chan TransformSegment, 1024),
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
	cancel          context.CancelFunc
	transformGuard  TransformLogGuard
	collectionGuard CollectionRuntimeGuard
	segments        map[int64]int64
	onUnrecoverable func()
	unrecoverable   bool
}

type transformSegmentState struct {
	state         transformSegmentLoadState
	segment       TransformSegment
	reg           TransformRegistration
	catchupCancel context.CancelFunc
	queryRefs     int
	refs          map[qviews.QueryViewKey]struct{}
	waiters       map[qviews.QueryViewKey]transformSegmentWaiter
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
		return m.continueAcquire(req, ref, view, ctx, cancel)
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

func (m *QueryViewSegmentReadinessManager) continueAcquire(req AcquireSegments, ref *transformViewRef, view *qviews.QueryViewAtQueryNode, ctx context.Context, cancel context.CancelFunc) error {
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

	for _, waiter := range readyNow {
		waiter.reportReady()
	}
	if noAssignedSegments && req.OnReady != nil {
		req.OnReady(map[int64][]int64{})
	}
	if noAssignedSegments {
		return nil
	}
	if len(physicalRefSegments) == 0 {
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
			m.failSegment(segmentID, err)
		},
		OnUnrecoverable: func() {
			m.failView(req.Key)
		},
	})
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
	ref.collectionGuard = collectionGuard
	ref.onUnrecoverable = req.OnUnrecoverable
	for segmentID := range ref.segments {
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
		if state.state == transformSegmentLoaded {
			readyNow = append(readyNow, waiter)
			delete(state.waiters, req.Key)
			continue
		}
		if state.state == transformSegmentWaiting {
			state.state = transformSegmentLoading
		}
		if state.state == transformSegmentLoading {
			physicalRefSegments = append(physicalRefSegments, segmentID)
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
		if kept, schedule := m.markPhysicalLoaded(segment); schedule {
			m.scheduleCatchup(segment)
		} else if !kept {
			_ = segment.Release(context.Background())
		}
	}
}

func (m *QueryViewSegmentReadinessManager) scheduleCatchup(segment TransformSegment) {
	m.catchupTasks <- segment
}

func (m *QueryViewSegmentReadinessManager) catchupWorker() {
	for segment := range m.catchupTasks {
		m.registerAndCatchup(segment)
	}
}

func (m *QueryViewSegmentReadinessManager) markPhysicalLoaded(segment TransformSegment) (bool, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.segments[segment.ID()]
	if state == nil || len(state.refs) == 0 {
		return false, false
	}
	if state.state == transformSegmentLoaded || state.state == transformSegmentCatchingUp {
		return true, false
	}
	state.segment = segment
	state.state = transformSegmentCatchingUp
	return true, true
}

func (m *QueryViewSegmentReadinessManager) registerAndCatchup(segment TransformSegment) {
	reg, err := m.buffer.RegisterSegment(context.Background(), segment)
	if err != nil {
		m.failSegment(segment.ID(), err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	if !m.storeRegistration(segment.ID(), segment, reg, cancel) {
		cancel()
		reg.Unregister()
		return
	}
	if err := reg.WaitCatchup(ctx); err != nil {
		cancel()
		reg.Unregister()
		m.failSegment(segment.ID(), err)
		return
	}
	cancel()
	for _, waiter := range m.markSegmentReady(segment.ID()) {
		waiter.reportReady()
	}
}

func (m *QueryViewSegmentReadinessManager) storeRegistration(segmentID int64, segment TransformSegment, reg TransformRegistration, cancel context.CancelFunc) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.segments[segmentID]
	if state == nil || state.segment != segment || len(state.refs) == 0 {
		return false
	}
	state.reg = reg
	state.catchupCancel = cancel
	state.state = transformSegmentCatchingUp
	return true
}

func (m *QueryViewSegmentReadinessManager) markSegmentReady(segmentID int64) []transformSegmentWaiter {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.segments[segmentID]
	if state == nil || state.state != transformSegmentCatchingUp {
		return nil
	}
	state.state = transformSegmentLoaded
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

func (m *QueryViewSegmentReadinessManager) failSegment(segmentID int64, err error) {
	m.mu.Lock()
	state := m.segments[segmentID]
	if state == nil {
		m.mu.Unlock()
		return
	}
	reg := state.reg
	cancel := state.catchupCancel
	segment := state.segment
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
	if segment != nil {
		_ = segment.Release(context.Background())
	}
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

func (m *QueryViewSegmentReadinessManager) release(req ReleaseSegments) {
	detached := m.detachView(req.Key)
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

func (m *QueryViewSegmentReadinessManager) detachView(key qviews.QueryViewKey) transformViewDetach {
	m.mu.Lock()
	detached := m.detachViewLocked(key)
	m.mu.Unlock()
	return detached
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
