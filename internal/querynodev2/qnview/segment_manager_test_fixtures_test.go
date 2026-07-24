//go:build test && dynamic

package qnview

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type fakeTransformSegment struct {
	id           int64
	vchannel     string
	partitionID  int64
	startAfter   uint64
	applied      uint64
	released     bool
	waitErr      error
	waitTimetick uint64
	waitCalled   bool
}

func (s *fakeTransformSegment) ID() int64 {
	return s.id
}

func (s *fakeTransformSegment) VChannel() string {
	if s.vchannel == "" {
		return testVChannel
	}
	return s.vchannel
}

func (s *fakeTransformSegment) PartitionID() int64 {
	return s.partitionID
}

func (s *fakeTransformSegment) TransformStartAfterTimeTick() uint64 {
	return s.startAfter
}

func (s *fakeTransformSegment) ApplyTransform(context.Context, *streamingpb.TransformLogEntry) error {
	return nil
}

func (s *fakeTransformSegment) AppliedTransformTimeTick() uint64 {
	return s.applied
}

func (s *fakeTransformSegment) WaitTransformApplied(_ context.Context, timetick uint64) error {
	s.waitCalled = true
	s.waitTimetick = timetick
	return s.waitErr
}

func (s *fakeTransformSegment) Release(context.Context) error {
	s.released = true
	return nil
}

type fakeReadableTransformSegment struct {
	fakeTransformSegment
	querySegment segments.Segment
	collection   *segments.Collection
}

func (s *fakeReadableTransformSegment) QuerySegment() segments.Segment {
	return s.querySegment
}

func (s *fakeReadableTransformSegment) Collection() *segments.Collection {
	return s.collection
}

type fakeTransformRegistration struct {
	waitCh       chan struct{}
	waitErr      error
	unregistered bool
}

func newFakeTransformRegistration() *fakeTransformRegistration {
	return &fakeTransformRegistration{waitCh: make(chan struct{})}
}

func (r *fakeTransformRegistration) WaitCatchup(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.waitCh:
		return r.waitErr
	}
}

func (r *fakeTransformRegistration) Unregister() {
	r.unregistered = true
}

type fakeTransformLogBuffer struct {
	mu               sync.Mutex
	acquireView      *qviews.QueryViewAtQueryNode
	acquireErr       error
	guard            *fakeTransformLogGuard
	registerSegments []int64
	registerErr      error
	regs             []*fakeTransformRegistration
}

func (b *fakeTransformLogBuffer) Acquire(_ context.Context, view *qviews.QueryViewAtQueryNode) (TransformLogGuard, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.acquireView = view
	if b.acquireErr != nil {
		return nil, b.acquireErr
	}
	if b.guard == nil {
		b.guard = &fakeTransformLogGuard{}
	}
	return b.guard, nil
}

func (b *fakeTransformLogBuffer) RegisterSegment(_ context.Context, segment TransformSegment) (TransformRegistration, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.registerErr != nil {
		return nil, b.registerErr
	}
	reg := newFakeTransformRegistration()
	b.registerSegments = append(b.registerSegments, segment.ID())
	b.regs = append(b.regs, reg)
	return reg, nil
}

type fakeTransformLogGuard struct {
	mu           sync.Mutex
	released     bool
	waitCalled   bool
	waitTimetick uint64
	waitErr      error
}

func (g *fakeTransformLogGuard) WaitTransformVisible(_ context.Context, timetick uint64) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.waitCalled = true
	g.waitTimetick = timetick
	return g.waitErr
}

func (g *fakeTransformLogGuard) Release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.released = true
}

type fakeQueryViewCollectionRuntimeManager struct {
	mu           sync.Mutex
	acquireView  *qviews.QueryViewAtQueryNode
	acquireErr   error
	acquireErrs  []error
	retryable    []bool
	acquireCalls int
	guard        *fakeCollectionRuntimeGuard
}

func (m *fakeQueryViewCollectionRuntimeManager) Acquire(_ context.Context, view *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acquireView = view
	idx := m.acquireCalls
	m.acquireCalls++
	if idx < len(m.acquireErrs) && m.acquireErrs[idx] != nil {
		return nil, idx < len(m.retryable) && m.retryable[idx], m.acquireErrs[idx]
	}
	if m.acquireErr != nil {
		return nil, false, m.acquireErr
	}
	if m.guard == nil {
		m.guard = &fakeCollectionRuntimeGuard{}
	}
	return m.guard, false, nil
}

type fakeCollectionRuntimeGuard struct {
	mu             sync.Mutex
	released       bool
	collectionID   int64
	databaseName   string
	schema         *schemapb.CollectionSchema
	schemaVersion  int64
	updatedIndexes []*indexpb.IndexInfo
	updateErr      error
}

func (g *fakeCollectionRuntimeGuard) Release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.released = true
}

func (g *fakeCollectionRuntimeGuard) CollectionID() int64 {
	return g.collectionID
}

func (g *fakeCollectionRuntimeGuard) DatabaseName() string {
	return g.databaseName
}

func (g *fakeCollectionRuntimeGuard) Schema() *schemapb.CollectionSchema {
	return g.schema
}

func (g *fakeCollectionRuntimeGuard) SchemaVersion() int64 {
	return g.schemaVersion
}

func (g *fakeCollectionRuntimeGuard) CCollection() *segcore.CCollection {
	return nil
}

func (g *fakeCollectionRuntimeGuard) UpdateIndexMeta(_ context.Context, indexes []*indexpb.IndexInfo) error {
	g.updatedIndexes = append([]*indexpb.IndexInfo(nil), indexes...)
	return g.updateErr
}

type fakePhysicalSegmentManager struct {
	acquire func(AcquirePhysicalSegments)
	release func(ReleaseSegments)
}

func (m fakePhysicalSegmentManager) Acquire(req AcquirePhysicalSegments) {
	m.acquire(req)
}

func (m fakePhysicalSegmentManager) Release(req ReleaseSegments) {
	m.release(req)
}

func (m fakePhysicalSegmentManager) ApplyLoadInfoSnapshot(context.Context, SegmentLoadInfoSnapshot) {}

type instantTransformRegistration struct{}

func (instantTransformRegistration) WaitCatchup(context.Context) error {
	return nil
}

func (instantTransformRegistration) Unregister() {}

type interleavingTransformLogBuffer struct {
	mu                sync.Mutex
	acquireCalls      int
	secondAcquire     chan struct{}
	registeredChannel map[int64]string
}

func newInterleavingTransformLogBuffer() *interleavingTransformLogBuffer {
	return &interleavingTransformLogBuffer{
		secondAcquire:     make(chan struct{}),
		registeredChannel: make(map[int64]string),
	}
}

func (b *interleavingTransformLogBuffer) Acquire(_ context.Context, _ *qviews.QueryViewAtQueryNode) (TransformLogGuard, error) {
	b.mu.Lock()
	b.acquireCalls++
	if b.acquireCalls == 2 {
		close(b.secondAcquire)
	}
	isFirst := b.acquireCalls == 1
	b.mu.Unlock()

	if isFirst {
		select {
		case <-b.secondAcquire:
		case <-time.After(50 * time.Millisecond):
		}
	}
	return instantTransformGuard{}, nil
}

func (b *interleavingTransformLogBuffer) RegisterSegment(_ context.Context, segment TransformSegment) (TransformRegistration, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.registeredChannel[segment.ID()] = segment.VChannel()
	return instantTransformRegistration{}, nil
}

type instantTransformGuard struct{}

func (instantTransformGuard) WaitTransformVisible(context.Context, uint64) error {
	return nil
}

func (instantTransformGuard) Release() {}

type fakeQueryViewLoadMetadataProvider struct {
	mu             sync.Mutex
	describeCalled bool
	loadInfoCalled []int64
	collection     *milvuspb.DescribeCollectionResponse
	loadInfos      []*querypb.SegmentLoadInfo
	loadIndexInfos []*indexpb.IndexInfo
	err            error
}

func (p *fakeQueryViewLoadMetadataProvider) DescribeCollection(context.Context, int64) (*milvuspb.DescribeCollectionResponse, error) {
	p.describeCalled = true
	return p.collection, p.err
}

func (p *fakeQueryViewLoadMetadataProvider) GetQueryViewLoadInfo(context.Context, int64, QueryViewLoadInfoVersion) (QueryViewLoadInfo, error) {
	return QueryViewLoadInfo{IndexInfos: p.loadIndexInfos}, p.err
}

type fakePhysicalLoader struct {
	mu                sync.Mutex
	loadInfos         []*querypb.SegmentLoadInfo
	collections       []CollectionRuntime
	updateSnapshots   []SegmentLoadInfoSnapshot
	updateActions     []SegmentUpdateAction
	released          []int64
	loaded            TransformSegment
	loadErr           error
	updateErr         error
	releaseErr        error
	loadFn            func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error)
	loadFnWithContext func(context.Context, *querypb.SegmentLoadInfo, CollectionRuntime) (TransformSegment, error)
	updateFn          func(segment TransformSegment, collection CollectionRuntime, snapshot SegmentLoadInfoSnapshot, action SegmentUpdateAction) error
}

func (l *fakePhysicalLoader) Load(ctx context.Context, info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
	l.mu.Lock()
	l.loadInfos = append(l.loadInfos, info)
	l.collections = append(l.collections, collection)
	l.mu.Unlock()
	if l.loadFnWithContext != nil {
		return l.loadFnWithContext(ctx, info, collection)
	}
	if l.loadFn != nil {
		return l.loadFn(info, collection)
	}
	return l.loaded, l.loadErr
}

func (l *fakePhysicalLoader) Update(_ context.Context, segment TransformSegment, collection CollectionRuntime, snapshot SegmentLoadInfoSnapshot, action SegmentUpdateAction) error {
	l.mu.Lock()
	l.updateSnapshots = append(l.updateSnapshots, snapshot)
	l.updateActions = append(l.updateActions, action)
	l.collections = append(l.collections, collection)
	l.mu.Unlock()
	if l.updateFn != nil {
		return l.updateFn(segment, collection, snapshot, action)
	}
	return l.updateErr
}

func (l *fakePhysicalLoader) Release(_ context.Context, segmentIDs []int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.released = append(l.released, segmentIDs...)
	return l.releaseErr
}

func newTestNodeScheduler(t *testing.T) nodescheduler.Scheduler {
	nodeScheduler := nodescheduler.New(4)
	t.Cleanup(nodeScheduler.Close)
	return nodeScheduler
}

func submitTestSegmentLoadTask(t *testing.T, loader PhysicalSegmentLoader, task SegmentLoadTask, estimators ...SegmentResourceEstimator) nodescheduler.TaskHandle {
	var estimator SegmentResourceEstimator
	if len(estimators) > 0 {
		estimator = estimators[0]
	}
	return newTestNodeScheduler(t).Submit(newSegmentLoadTask(loader, estimator, task))
}

func submitTestSegmentUpdateTask(t *testing.T, loader PhysicalSegmentLoader, task SegmentUpdateTask) nodescheduler.TaskHandle {
	return newTestNodeScheduler(t).Submit(newSegmentUpdateTask(loader, task))
}

type fakeResourceReservation struct {
	released bool
}

func (r *fakeResourceReservation) Release() {
	r.released = true
}

type fakeSegmentResourceEstimator struct {
	infos        []*querypb.SegmentLoadInfo
	collections  []CollectionRuntime
	reservations []*fakeResourceReservation
	err          error
}

func (e *fakeSegmentResourceEstimator) Reserve(_ context.Context, info *querypb.SegmentLoadInfo, collection CollectionRuntime) (ResourceReservation, error) {
	e.infos = append(e.infos, info)
	e.collections = append(e.collections, collection)
	if e.err != nil {
		return nil, e.err
	}
	reservation := &fakeResourceReservation{}
	e.reservations = append(e.reservations, reservation)
	return reservation, nil
}

type fakeNodeScheduler struct {
	tasks   []*SegmentLoadTask
	updates []*SegmentUpdateTask
}

func (s *fakeNodeScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	switch task := task.(type) {
	case schedulerTaskFunc:
		_ = task.Execute(context.Background())
	case *SegmentLoadTask:
		s.tasks = append(s.tasks, task)
	case *SegmentUpdateTask:
		s.updates = append(s.updates, task)
	}
	return noopNodeTaskHandle{}
}

func newTestQueryViewSegmentReadinessManager(t *testing.T, physical PhysicalSegmentManager, buffer TransformLogBuffer, collections ...QueryViewCollectionRuntimeManager) *QueryViewSegmentReadinessManager {
	t.Helper()
	scheduler := nodescheduler.New(4)
	t.Cleanup(scheduler.Close)
	return NewQueryViewSegmentReadinessManagerWithScheduler(scheduler, physical, buffer, collections...)
}

func newTestViewScopedPhysicalSegmentManager(t *testing.T, scheduler nodescheduler.Scheduler, watchers ...SegmentLoadInfoWatcher) *ViewScopedPhysicalSegmentManager {
	t.Helper()
	if len(watchers) > 0 {
		return NewViewScopedPhysicalSegmentManagerWithNodeSchedulerAndWatcher(scheduler, &fakePhysicalLoader{}, watchers[0])
	}
	return NewViewScopedPhysicalSegmentManagerWithNodeScheduler(scheduler, &fakePhysicalLoader{})
}

func newTestViewScopedPhysicalSegmentManagerWithLoader(t *testing.T, loader PhysicalSegmentLoader, watchers ...SegmentLoadInfoWatcher) *ViewScopedPhysicalSegmentManager {
	t.Helper()
	nodeScheduler := newTestNodeScheduler(t)
	if len(watchers) > 0 {
		return NewViewScopedPhysicalSegmentManagerWithNodeSchedulerAndWatcher(nodeScheduler, loader, watchers[0])
	}
	return NewViewScopedPhysicalSegmentManagerWithNodeScheduler(nodeScheduler, loader)
}

type fakeSegmentLoadInfoWatcher struct {
	subscriptions   []SegmentLoadInfoSubscription
	unsubscriptions []SegmentLoadInfoSubscription
}

func (w *fakeSegmentLoadInfoWatcher) Subscribe(subscription SegmentLoadInfoSubscription) {
	w.subscriptions = append(w.subscriptions, subscription)
}

func (w *fakeSegmentLoadInfoWatcher) Unsubscribe(collectionID int64, segmentID int64) {
	w.unsubscriptions = append(w.unsubscriptions, SegmentLoadInfoSubscription{
		CollectionID: collectionID,
		SegmentID:    segmentID,
	})
}

func (w *fakeSegmentLoadInfoWatcher) Close() {}
