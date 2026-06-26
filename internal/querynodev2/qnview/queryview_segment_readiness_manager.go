package qnview

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"google.golang.org/protobuf/proto"
)

// QueryViewSegmentReadinessManager turns physically loaded segments into
// QueryView-ready segments by registering them with the TransformLogBuffer and
// waiting for catch-up.
type QueryViewSegmentReadinessManager struct {
	physical    PhysicalSegmentManager
	buffer      TransformLogBuffer
	collections QueryViewCollectionRuntimeManager

	mu       sync.Mutex
	views    map[qviews.QueryViewKey]*transformViewRef
	segments map[int64]*transformSegmentState
}

func NewQueryViewSegmentReadinessManager(physical PhysicalSegmentManager, buffer TransformLogBuffer, collections ...QueryViewCollectionRuntimeManager) *QueryViewSegmentReadinessManager {
	var collectionManager QueryViewCollectionRuntimeManager
	if len(collections) > 0 {
		collectionManager = collections[0]
	}
	return &QueryViewSegmentReadinessManager{
		physical:    physical,
		buffer:      buffer,
		collections: collectionManager,
		views:       make(map[qviews.QueryViewKey]*transformViewRef),
		segments:    make(map[int64]*transformSegmentState),
	}
}

func (m *QueryViewSegmentReadinessManager) Acquire(req AcquireSegments) {
	req = cloneAcquireSegments(req)
	go m.acquire(req)
}

func (m *QueryViewSegmentReadinessManager) Release(req ReleaseSegments) {
	go m.release(req)
}

type transformSegmentLoadState int

const (
	transformSegmentLoading transformSegmentLoadState = iota
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
		invokeUnrecoverable(req.OnUnrecoverable)
		return
	}
	collectionGuard, err := m.acquireCollectionRuntime(ctx, view)
	if err != nil {
		cancel()
		guard.Release()
		invokeUnrecoverable(req.OnUnrecoverable)
		return
	}

	readyNow, segmentsToLoad, noAssignedSegments := m.recordAcquire(req, cancel, guard, collectionGuard)
	for _, waiter := range readyNow {
		waiter.reportReady()
	}
	if noAssignedSegments && req.OnReady != nil {
		req.OnReady(map[int64][]int64{})
	}
	if noAssignedSegments || len(segmentsToLoad) == 0 {
		return
	}
	viewToLoad := filterViewSegments(req.View, segmentsToLoad)

	m.physical.Acquire(AcquirePhysicalSegments{
		Key:        req.Key,
		Meta:       proto.Clone(req.Meta).(*viewpb.QueryViewMeta),
		View:       viewToLoad,
		Collection: collectionGuard,
		OnLoaded: func(loaded []TransformSegment) {
			m.onPhysicalLoaded(loaded)
		},
		OnSegmentUnrecoverable: func(segmentID int64, err error) {
			m.failSegment(segmentID)
		},
		OnUnrecoverable: func() {
			m.failView(req.Key)
		},
	})
}

func (m *QueryViewSegmentReadinessManager) acquireCollectionRuntime(ctx context.Context, view *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, error) {
	if m.collections == nil {
		return nil, nil
	}
	return m.collections.Acquire(ctx, view)
}

func (m *QueryViewSegmentReadinessManager) recordAcquire(req AcquireSegments, cancel context.CancelFunc, guard TransformLogGuard, collectionGuard CollectionRuntimeGuard) ([]transformSegmentWaiter, []int64, bool) {
	segmentPartitions := segmentPartitionMap(req.View)
	readyNow := make([]transformSegmentWaiter, 0)
	segmentsToLoad := make([]int64, 0)

	var oldDetach transformViewDetach
	m.mu.Lock()
	if old := m.views[req.Key]; old != nil {
		oldDetach = m.detachViewLocked(req.Key)
	}
	m.views[req.Key] = &transformViewRef{
		cancel:          cancel,
		transformGuard:  guard,
		collectionGuard: collectionGuard,
		segments:        segmentPartitions,
		onUnrecoverable: req.OnUnrecoverable,
	}
	for segmentID, partitionID := range segmentPartitions {
		state := m.segments[segmentID]
		if state == nil {
			state = &transformSegmentState{
				state:   transformSegmentLoading,
				refs:    make(map[qviews.QueryViewKey]struct{}),
				waiters: make(map[qviews.QueryViewKey]transformSegmentWaiter),
			}
			m.segments[segmentID] = state
			segmentsToLoad = append(segmentsToLoad, segmentID)
		}
		state.refs[req.Key] = struct{}{}
		waiter := transformSegmentWaiter{
			key:             req.Key,
			partitionID:     partitionID,
			segmentID:       segmentID,
			onReady:         req.OnReady,
			onUnrecoverable: req.OnUnrecoverable,
		}
		if state.state == transformSegmentLoaded {
			readyNow = append(readyNow, waiter)
			continue
		}
		state.waiters[req.Key] = waiter
	}
	m.mu.Unlock()

	for _, segment := range oldDetach.segments {
		_ = segment.Release(context.Background())
	}
	oldDetach.guards.release()
	return readyNow, segmentsToLoad, len(segmentPartitions) == 0
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
		if m.markPhysicalLoaded(segment) {
			go m.registerAndCatchup(segment)
		} else {
			_ = segment.Release(context.Background())
		}
	}
}

func (m *QueryViewSegmentReadinessManager) markPhysicalLoaded(segment TransformSegment) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.segments[segment.ID()]
	if state == nil || len(state.refs) == 0 {
		return false
	}
	if state.state == transformSegmentLoaded || state.state == transformSegmentCatchingUp {
		return false
	}
	state.segment = segment
	state.state = transformSegmentCatchingUp
	return true
}

func (m *QueryViewSegmentReadinessManager) registerAndCatchup(segment TransformSegment) {
	reg, err := m.buffer.RegisterSegment(context.Background(), segment)
	if err != nil {
		m.failSegment(segment.ID())
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
		m.failSegment(segment.ID())
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

func (m *QueryViewSegmentReadinessManager) failSegment(segmentID int64) {
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
	for _, segment := range detached.segments {
		_ = segment.Release(context.Background())
	}
	m.physical.Release(ReleaseSegments{
		Key: req.Key,
		OnDropped: func() {
			detached.guards.release()
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
	segments []TransformSegment
}

func (g transformViewGuards) release() {
	if g.transform != nil {
		g.transform.Release()
	}
	if g.collection != nil {
		g.collection.Release()
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
				state.catchupCancel()
			}
			if state.reg != nil {
				state.reg.Unregister()
			}
			if state.segment != nil {
				detached.segments = append(detached.segments, state.segment)
			}
			delete(m.segments, segmentID)
		}
	}
	return detached
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
