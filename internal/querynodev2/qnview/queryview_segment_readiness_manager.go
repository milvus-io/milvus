package qnview

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const transformCatchupWorkerCount = 4

// QueryViewSegmentReadinessManager turns physically loaded segments into
// QueryView-ready segments by registering them with TransformLog and waiting
// for catch-up. It deliberately depends only on injected resource interfaces.
type QueryViewSegmentReadinessManager struct {
	scheduler   TaskScheduler
	physical    PhysicalSegmentManager
	buffer      TransformLogBuffer
	collections QueryViewCollectionRuntimeManager
	catchups    chan TransformSegment

	mu       sync.Mutex
	views    map[qviews.QueryViewKey]*transformViewRef
	segments map[int64]*transformSegmentState
}

func NewQueryViewSegmentReadinessManager(
	scheduler TaskScheduler,
	physical PhysicalSegmentManager,
	buffer TransformLogBuffer,
	collections QueryViewCollectionRuntimeManager,
) *QueryViewSegmentReadinessManager {
	m := &QueryViewSegmentReadinessManager{
		scheduler:   scheduler,
		physical:    physical,
		buffer:      buffer,
		collections: collections,
		catchups:    make(chan TransformSegment, 1024),
		views:       make(map[qviews.QueryViewKey]*transformViewRef),
		segments:    make(map[int64]*transformSegmentState),
	}
	for range transformCatchupWorkerCount {
		go m.catchupWorker()
	}
	return m
}

func (m *QueryViewSegmentReadinessManager) Acquire(req AcquireSegments) {
	req = cloneAcquireSegments(req)
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
		taskCtx, stop := mergeTaskContext(schedulerCtx, ctx)
		defer stop()
		return m.continueAcquire(taskCtx, req, ref, view)
	}))
}

func (m *QueryViewSegmentReadinessManager) continueAcquire(
	ctx context.Context,
	req AcquireSegments,
	ref *transformViewRef,
	view *qviews.QueryViewAtQueryNode,
) error {
	collectionGuard, retryable, err := m.collections.Acquire(ctx, view)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		if retryable {
			return merr.Wrap(err, "acquire query view collection runtime")
		}
		if detached, current := m.detachViewIfCurrent(req.Key, ref); current {
			detached.releaseTransform()
			detached.unregister()
			invoke(req.OnUnrecoverable)
		}
		return nil
	}

	ready, physicalIDs, current := m.activateAcquire(req, ref, collectionGuard)
	if !current {
		collectionGuard.Release()
		return nil
	}
	for _, waiter := range ready {
		waiter.reportReady()
	}
	if len(ref.segments) == 0 {
		if req.OnReady != nil {
			req.OnReady(map[int64][]int64{})
		}
		return nil
	}
	if len(physicalIDs) == 0 {
		return nil
	}
	m.physical.Acquire(AcquirePhysicalSegments{
		Key:        req.Key,
		Meta:       proto.Clone(req.Meta).(*viewpb.QueryViewMeta),
		View:       filterViewSegments(req.View, physicalIDs),
		Collection: collectionGuard,
		OnLoaded:   m.onPhysicalLoaded,
		OnSegmentUnrecoverable: func(segmentID int64, err error) {
			m.failSegment(segmentID, err)
		},
		OnUnrecoverable: func() { m.failView(req.Key) },
	})
	return nil
}

func (m *QueryViewSegmentReadinessManager) Release(req ReleaseSegments) {
	detached := m.detachView(req.Key)
	detached.releaseTransform()
	detached.unregister()
	m.physical.Release(ReleaseSegments{
		Key: req.Key,
		OnDropped: func() {
			detached.releaseCollection()
			invoke(req.OnDropped)
		},
	})
}

type transformSegmentLoadState uint8

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
	registration  TransformRegistration
	catchupCancel context.CancelFunc
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

func (m *QueryViewSegmentReadinessManager) recordPendingAcquire(
	req AcquireSegments,
	cancel context.CancelFunc,
	guard TransformLogGuard,
) (*transformViewRef, bool) {
	partitions := segmentPartitionMap(req.View)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.views[req.Key] != nil {
		return nil, false
	}
	ref := &transformViewRef{
		cancel:          cancel,
		transformGuard:  guard,
		segments:        partitions,
		onUnrecoverable: req.OnUnrecoverable,
	}
	m.views[req.Key] = ref
	for segmentID, partitionID := range partitions {
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
		state.waiters[req.Key] = newTransformWaiter(req, partitionID, segmentID)
	}
	return ref, true
}

func newTransformWaiter(req AcquireSegments, partitionID, segmentID int64) transformSegmentWaiter {
	return transformSegmentWaiter{
		key:             req.Key,
		partitionID:     partitionID,
		segmentID:       segmentID,
		onReady:         req.OnReady,
		onUnrecoverable: req.OnUnrecoverable,
	}
}

func (m *QueryViewSegmentReadinessManager) activateAcquire(
	req AcquireSegments,
	ref *transformViewRef,
	collection CollectionRuntimeGuard,
) ([]transformSegmentWaiter, []int64, bool) {
	ready := make([]transformSegmentWaiter, 0)
	physicalIDs := make([]int64, 0)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.views[req.Key] != ref {
		return nil, nil, false
	}
	ref.collectionGuard = collection
	for segmentID, partitionID := range ref.segments {
		state := m.segments[segmentID]
		waiter := newTransformWaiter(req, partitionID, segmentID)
		if state.state == transformSegmentLoaded {
			ready = append(ready, waiter)
			delete(state.waiters, req.Key)
		} else {
			if state.state == transformSegmentWaiting {
				state.state = transformSegmentLoading
			}
			state.waiters[req.Key] = waiter
		}
		// Every view must acquire its own physical-manager reference, including
		// views that reuse an already loaded or catching-up segment.
		physicalIDs = append(physicalIDs, segmentID)
	}
	return ready, physicalIDs, true
}

func (m *QueryViewSegmentReadinessManager) onPhysicalLoaded(loaded []TransformSegment) {
	for _, segment := range loaded {
		if segment == nil {
			continue
		}
		_, schedule := m.markPhysicalLoaded(segment)
		if schedule {
			m.catchups <- segment
		}
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

func (m *QueryViewSegmentReadinessManager) catchupWorker() {
	for segment := range m.catchups {
		m.registerAndCatchup(segment)
	}
}

func (m *QueryViewSegmentReadinessManager) registerAndCatchup(segment TransformSegment) {
	registration, err := m.buffer.RegisterSegment(context.Background(), segment)
	if err != nil {
		m.failSegment(segment.ID(), err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	if !m.storeRegistration(segment, registration, cancel) {
		cancel()
		registration.Unregister()
		return
	}
	if err := registration.WaitCatchup(ctx); err != nil {
		cancel()
		registration.Unregister()
		m.failSegment(segment.ID(), err)
		return
	}
	cancel()
	for _, waiter := range m.markSegmentReady(segment.ID()) {
		waiter.reportReady()
	}
}

func (m *QueryViewSegmentReadinessManager) storeRegistration(
	segment TransformSegment,
	registration TransformRegistration,
	cancel context.CancelFunc,
) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.segments[segment.ID()]
	if state == nil || state.segment != segment || len(state.refs) == 0 {
		return false
	}
	state.registration = registration
	state.catchupCancel = cancel
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
		if m.views[key] != nil {
			waiters = append(waiters, waiter)
		}
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
	waiters := make([]transformSegmentWaiter, 0, len(state.waiters))
	for _, waiter := range state.waiters {
		waiters = append(waiters, waiter)
	}
	delete(m.segments, segmentID)
	m.mu.Unlock()

	if state.catchupCancel != nil {
		state.catchupCancel()
	}
	if state.registration != nil {
		state.registration.Unregister()
	}
	if resetter, ok := m.physical.(PhysicalSegmentResetter); ok {
		resetter.ResetSegment(segmentID)
	}
	mlog.Warn(context.TODO(), "query view segment became unrecoverable",
		mlog.Int64("segmentID", segmentID), mlog.Err(err))
	for _, waiter := range waiters {
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
	for segmentID := range ref.segments {
		if state := m.segments[segmentID]; state != nil {
			delete(state.waiters, key)
		}
	}
	callback := ref.onUnrecoverable
	m.mu.Unlock()
	invoke(callback)
}

func (m *QueryViewSegmentReadinessManager) notifyUnrecoverable(key qviews.QueryViewKey, callback func()) {
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
	invoke(callback)
}

type transformViewDetach struct {
	transform  TransformLogGuard
	collection CollectionRuntimeGuard
	cancels    []context.CancelFunc
	regs       []TransformRegistration
}

func (m *QueryViewSegmentReadinessManager) detachViewIfCurrent(key qviews.QueryViewKey, expected *transformViewRef) (transformViewDetach, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.views[key] != expected {
		return transformViewDetach{}, false
	}
	return m.detachViewLocked(key), true
}

func (m *QueryViewSegmentReadinessManager) detachView(key qviews.QueryViewKey) transformViewDetach {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.detachViewLocked(key)
}

func (m *QueryViewSegmentReadinessManager) detachViewLocked(key qviews.QueryViewKey) transformViewDetach {
	ref := m.views[key]
	if ref == nil {
		return transformViewDetach{}
	}
	delete(m.views, key)
	ref.cancel()
	detached := transformViewDetach{transform: ref.transformGuard, collection: ref.collectionGuard}
	for segmentID := range ref.segments {
		state := m.segments[segmentID]
		if state == nil {
			continue
		}
		delete(state.refs, key)
		delete(state.waiters, key)
		if len(state.refs) != 0 {
			continue
		}
		detached.cancels = appendIfNotNil(detached.cancels, state.catchupCancel)
		if state.registration != nil {
			detached.regs = append(detached.regs, state.registration)
		}
		delete(m.segments, segmentID)
	}
	return detached
}

func appendIfNotNil(dst []context.CancelFunc, cancel context.CancelFunc) []context.CancelFunc {
	if cancel != nil {
		return append(dst, cancel)
	}
	return dst
}

func (d transformViewDetach) releaseTransform() {
	if d.transform != nil {
		d.transform.Release()
	}
}

func (d transformViewDetach) releaseCollection() {
	if d.collection != nil {
		d.collection.Release()
	}
}

func (d transformViewDetach) unregister() {
	for _, cancel := range d.cancels {
		cancel()
	}
	for _, registration := range d.regs {
		registration.Unregister()
	}
}

func (m *QueryViewSegmentReadinessManager) submitCallback(callback func()) {
	if callback != nil {
		m.scheduler.Submit(schedulerTaskFunc(func(context.Context) error {
			callback()
			return nil
		}))
	}
}

func (w transformSegmentWaiter) reportReady() {
	if w.onReady != nil {
		w.onReady(map[int64][]int64{w.partitionID: {w.segmentID}})
	}
}

func invoke(callback func()) {
	if callback != nil {
		callback()
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
