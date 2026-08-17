package qnview

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type testTaskScheduler struct{}

func (testTaskScheduler) Submit(task Task) TaskHandle {
	ctx, cancel := context.WithCancel(context.Background())
	handle := &testTaskHandle{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(handle.done)
		for ctx.Err() == nil {
			if err := task.Execute(ctx); err == nil {
				return
			}
			timer := time.NewTimer(time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return
			}
		}
	}()
	return handle
}

type testTaskHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (h *testTaskHandle) Cancel() { h.cancel() }
func (h *testTaskHandle) Wait(ctx context.Context) error {
	select {
	case <-h.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type testTransformSegment struct {
	id          int64
	partitionID int64
	released    atomic.Int32
}

func (s *testTransformSegment) ID() int64                           { return s.id }
func (s *testTransformSegment) VChannel() string                    { return "by-dev-rootcoord-dml_0v0" }
func (s *testTransformSegment) PartitionID() int64                  { return s.partitionID }
func (s *testTransformSegment) TransformStartAfterTimeTick() uint64 { return 0 }
func (s *testTransformSegment) AppliedTransformTimeTick() uint64    { return 0 }
func (s *testTransformSegment) WaitTransformApplied(context.Context, uint64) error {
	return nil
}

func (s *testTransformSegment) Release(context.Context) error {
	s.released.Add(1)
	return nil
}

type testTransformGuard struct{ released atomic.Bool }

func (*testTransformGuard) WaitTransformVisible(context.Context, uint64) error { return nil }
func (g *testTransformGuard) Release()                                         { g.released.Store(true) }

type testTransformRegistration struct {
	catchup      chan struct{}
	unregistered atomic.Bool
	err          error
}

func (r *testTransformRegistration) WaitCatchup(ctx context.Context) error {
	select {
	case <-r.catchup:
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (r *testTransformRegistration) Unregister() { r.unregistered.Store(true) }

type testTransformBuffer struct {
	guard       *testTransformGuard
	reg         *testTransformRegistration
	acquireErr  error
	registerErr error
}

type rotatingTransformBuffer struct {
	mu     sync.Mutex
	guards []*testTransformGuard
}

func (b *rotatingTransformBuffer) Acquire(context.Context, *qviews.QueryViewAtQueryNode) (TransformLogGuard, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	guard := &testTransformGuard{}
	b.guards = append(b.guards, guard)
	return guard, nil
}

func (*rotatingTransformBuffer) RegisterSegment(context.Context, TransformSegment) (TransformRegistration, error) {
	return nil, merr.WrapErrServiceInternalMsg("unexpected registration")
}

func (b *testTransformBuffer) Acquire(context.Context, *qviews.QueryViewAtQueryNode) (TransformLogGuard, error) {
	return b.guard, b.acquireErr
}

func (b *testTransformBuffer) RegisterSegment(context.Context, TransformSegment) (TransformRegistration, error) {
	return b.reg, b.registerErr
}

type testCollectionRuntimeGuard struct {
	collectionID int64
	released     atomic.Bool
}

func (g *testCollectionRuntimeGuard) CollectionID() int64              { return g.collectionID }
func (*testCollectionRuntimeGuard) DatabaseName() string               { return "default" }
func (*testCollectionRuntimeGuard) Schema() *schemapb.CollectionSchema { return nil }
func (*testCollectionRuntimeGuard) SchemaVersion() int64               { return 0 }
func (*testCollectionRuntimeGuard) CCollection() *segcore.CCollection  { return nil }
func (*testCollectionRuntimeGuard) PinnedCollection() *segments.Collection {
	return nil
}
func (g *testCollectionRuntimeGuard) Release() { g.released.Store(true) }

type testCollectionRuntimeManager struct {
	guard     *testCollectionRuntimeGuard
	retryable bool
	err       error
}

func (m testCollectionRuntimeManager) Acquire(context.Context, *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
	return m.guard, m.retryable, m.err
}

type testPhysicalManager struct {
	acquire func(AcquirePhysicalSegments)
	release func(ReleaseSegments)
	reset   chan int64
}

func (m testPhysicalManager) Acquire(req AcquirePhysicalSegments) { m.acquire(req) }
func (m testPhysicalManager) Release(req ReleaseSegments)         { m.release(req) }
func (testPhysicalManager) ApplyLoadInfoSnapshot(context.Context, SegmentLoadInfoSnapshot) {
}

func (m *testPhysicalManager) ResetSegment(segmentID int64) {
	if m.reset != nil {
		m.reset <- segmentID
	}
}

func TestQueryViewSegmentReadinessWaitsForTransformCatchup(t *testing.T) {
	meta, view, key := testQueryView()
	segment := &testTransformSegment{id: 100, partitionID: 10}
	guard := &testCollectionRuntimeGuard{collectionID: 1}
	transformGuard := &testTransformGuard{}
	registration := &testTransformRegistration{catchup: make(chan struct{})}
	ready := make(chan map[int64][]int64, 1)
	dropped := make(chan struct{})

	physical := testPhysicalManager{
		acquire: func(req AcquirePhysicalSegments) { req.OnLoaded([]TransformSegment{segment}) },
		release: func(req ReleaseSegments) { req.OnDropped() },
	}
	manager := NewQueryViewSegmentReadinessManager(
		testTaskScheduler{}, physical,
		&testTransformBuffer{guard: transformGuard, reg: registration},
		testCollectionRuntimeManager{guard: guard},
	)
	manager.Acquire(AcquireSegments{
		Key: key, Meta: meta, View: view,
		OnReady:         func(value map[int64][]int64) { ready <- value },
		OnUnrecoverable: func() { t.Error("unexpected unrecoverable callback") },
	})
	assert.Never(t, func() bool { return len(ready) != 0 }, 20*time.Millisecond, time.Millisecond)
	close(registration.catchup)
	select {
	case value := <-ready:
		assert.Equal(t, []int64{100}, value[10])
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ready callback")
	}

	manager.Release(ReleaseSegments{Key: key, OnDropped: func() { close(dropped) }})
	select {
	case <-dropped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dropped callback")
	}
	assert.True(t, transformGuard.released.Load())
	assert.True(t, registration.unregistered.Load())
	assert.True(t, guard.released.Load())
}

func TestQueryViewSegmentReadinessFailurePaths(t *testing.T) {
	meta, view, key := testQueryView()

	t.Run("transform guard acquire", func(t *testing.T) {
		unrecoverable := make(chan struct{})
		physicalCalled := atomic.Bool{}
		manager := NewQueryViewSegmentReadinessManager(
			testTaskScheduler{},
			testPhysicalManager{
				acquire: func(AcquirePhysicalSegments) { physicalCalled.Store(true) },
				release: func(req ReleaseSegments) { req.OnDropped() },
			},
			&testTransformBuffer{acquireErr: merr.WrapErrServiceNotReadyMsg("test acquire failure")},
			testCollectionRuntimeManager{},
		)
		manager.Acquire(AcquireSegments{
			Key: key, Meta: meta, View: view,
			OnUnrecoverable: func() { close(unrecoverable) },
		})
		select {
		case <-unrecoverable:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for transform acquire failure")
		}
		assert.False(t, physicalCalled.Load())
	})

	t.Run("collection runtime", func(t *testing.T) {
		unrecoverable := make(chan struct{})
		transformGuard := &testTransformGuard{}
		manager := NewQueryViewSegmentReadinessManager(
			testTaskScheduler{},
			testPhysicalManager{
				acquire: func(AcquirePhysicalSegments) { t.Error("unexpected physical acquire") },
				release: func(req ReleaseSegments) { req.OnDropped() },
			},
			&testTransformBuffer{guard: transformGuard},
			testCollectionRuntimeManager{err: merr.WrapErrCollectionNotFound(1)},
		)
		manager.Acquire(AcquireSegments{
			Key: key, Meta: meta, View: view,
			OnUnrecoverable: func() { close(unrecoverable) },
		})
		select {
		case <-unrecoverable:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for collection failure")
		}
		assert.True(t, transformGuard.released.Load())
	})

	t.Run("transform registration resets physical segment", func(t *testing.T) {
		unrecoverable := make(chan struct{})
		reset := make(chan int64, 1)
		physical := &testPhysicalManager{
			acquire: func(req AcquirePhysicalSegments) {
				req.OnLoaded([]TransformSegment{&testTransformSegment{id: 100, partitionID: 10}})
			},
			release: func(req ReleaseSegments) { req.OnDropped() },
			reset:   reset,
		}
		manager := NewQueryViewSegmentReadinessManager(
			testTaskScheduler{}, physical,
			&testTransformBuffer{
				guard:       &testTransformGuard{},
				registerErr: merr.WrapErrServiceNotReadyMsg("test registration failure"),
			},
			testCollectionRuntimeManager{guard: &testCollectionRuntimeGuard{collectionID: 1}},
		)
		manager.Acquire(AcquireSegments{
			Key: key, Meta: meta, View: view,
			OnUnrecoverable: func() { close(unrecoverable) },
		})
		select {
		case segmentID := <-reset:
			assert.Equal(t, int64(100), segmentID)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for physical reset")
		}
		select {
		case <-unrecoverable:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for registration failure")
		}
		manager.Release(ReleaseSegments{Key: key})
	})

	t.Run("physical manager rejects whole view", func(t *testing.T) {
		unrecoverable := make(chan struct{})
		physical := testPhysicalManager{
			acquire: func(req AcquirePhysicalSegments) { req.OnUnrecoverable() },
			release: func(req ReleaseSegments) { req.OnDropped() },
		}
		manager := NewQueryViewSegmentReadinessManager(
			testTaskScheduler{}, physical,
			&testTransformBuffer{guard: &testTransformGuard{}},
			testCollectionRuntimeManager{guard: &testCollectionRuntimeGuard{collectionID: 1}},
		)
		manager.Acquire(AcquireSegments{
			Key: key, Meta: meta, View: view,
			OnUnrecoverable: func() { close(unrecoverable) },
		})
		select {
		case <-unrecoverable:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for whole-view failure")
		}
		manager.Release(ReleaseSegments{Key: key})
	})
}

func TestQueryViewSegmentReadinessCatchupFailure(t *testing.T) {
	meta, view, key := testQueryView()
	unrecoverable := make(chan struct{})
	reset := make(chan int64, 1)
	physical := &testPhysicalManager{
		acquire: func(req AcquirePhysicalSegments) {
			req.OnLoaded([]TransformSegment{&testTransformSegment{id: 100, partitionID: 10}})
		},
		release: func(req ReleaseSegments) { req.OnDropped() },
		reset:   reset,
	}
	catchup := make(chan struct{})
	close(catchup)
	registration := &testTransformRegistration{
		catchup: catchup,
		err:     merr.WrapErrServiceNotReadyMsg("test catchup failure"),
	}
	manager := NewQueryViewSegmentReadinessManager(
		testTaskScheduler{}, physical,
		&testTransformBuffer{guard: &testTransformGuard{}, reg: registration},
		testCollectionRuntimeManager{guard: &testCollectionRuntimeGuard{collectionID: 1}},
	)
	manager.Acquire(AcquireSegments{
		Key: key, Meta: meta, View: view,
		OnUnrecoverable: func() { close(unrecoverable) },
	})
	select {
	case <-unrecoverable:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for catchup failure")
	}
	assert.True(t, registration.unregistered.Load())
	select {
	case segmentID := <-reset:
		assert.Equal(t, int64(100), segmentID)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for catchup reset")
	}
	manager.Release(ReleaseSegments{Key: key})
}

type retryOnceCollectionRuntimeManager struct {
	guard *testCollectionRuntimeGuard
	calls atomic.Int32
}

func (m *retryOnceCollectionRuntimeManager) Acquire(context.Context, *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
	if m.calls.Add(1) == 1 {
		return nil, true, merr.WrapErrServiceNotReadyMsg("test retry")
	}
	return m.guard, false, nil
}

func TestQueryViewSegmentReadinessRetriesCollectionRuntime(t *testing.T) {
	meta, view, key := testQueryView()
	collections := &retryOnceCollectionRuntimeManager{guard: &testCollectionRuntimeGuard{collectionID: 1}}
	physicalCalled := make(chan struct{}, 1)
	manager := NewQueryViewSegmentReadinessManager(
		testTaskScheduler{},
		testPhysicalManager{
			acquire: func(AcquirePhysicalSegments) { physicalCalled <- struct{}{} },
			release: func(req ReleaseSegments) { req.OnDropped() },
		},
		&testTransformBuffer{guard: &testTransformGuard{}},
		collections,
	)
	manager.Acquire(AcquireSegments{Key: key, Meta: meta, View: view})
	select {
	case <-physicalCalled:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for retried collection acquire")
	}
	assert.Equal(t, int32(2), collections.calls.Load())
	manager.Release(ReleaseSegments{Key: key})
}

func TestQueryViewSegmentReadinessIgnoresDuplicateAcquire(t *testing.T) {
	meta, view, key := testQueryView()
	buffer := &rotatingTransformBuffer{}
	physicalCalled := make(chan struct{}, 1)
	manager := NewQueryViewSegmentReadinessManager(
		testTaskScheduler{},
		testPhysicalManager{
			acquire: func(AcquirePhysicalSegments) { physicalCalled <- struct{}{} },
			release: func(req ReleaseSegments) { req.OnDropped() },
		},
		buffer,
		testCollectionRuntimeManager{guard: &testCollectionRuntimeGuard{collectionID: 1}},
	)
	req := AcquireSegments{Key: key, Meta: meta, View: view}
	manager.Acquire(req)
	select {
	case <-physicalCalled:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first physical acquire")
	}
	manager.Acquire(req)
	require.Eventually(t, func() bool {
		buffer.mu.Lock()
		defer buffer.mu.Unlock()
		return len(buffer.guards) == 2 && buffer.guards[1].released.Load()
	}, time.Second, time.Millisecond)
	manager.Release(ReleaseSegments{Key: key})
}

type testPhysicalLoader struct {
	mu            sync.Mutex
	loads         []SegmentLoadInfoRevision
	updates       []SegmentLoadInfoRevision
	segment       *testTransformSegment
	loadErr       error
	updateErr     error
	loadStarted   chan struct{}
	loadBlock     chan struct{}
	updateStarted chan struct{}
	updateBlock   chan struct{}
}

func (l *testPhysicalLoader) Load(ctx context.Context, info *querypb.SegmentLoadInfo, _ CollectionRuntime) (TransformSegment, error) {
	l.mu.Lock()
	l.loads = append(l.loads, SegmentLoadInfoRevision{Revision: uint64(info.GetNumOfRows())})
	l.mu.Unlock()
	if l.loadStarted != nil {
		close(l.loadStarted)
	}
	if l.loadBlock != nil {
		select {
		case <-l.loadBlock:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return l.segment, l.loadErr
}

func (l *testPhysicalLoader) Update(ctx context.Context, _ TransformSegment, _ CollectionRuntime, snapshot SegmentLoadInfoSnapshot, _ SegmentUpdateAction) error {
	l.mu.Lock()
	l.updates = append(l.updates, snapshot.Revision)
	l.mu.Unlock()
	if l.updateStarted != nil {
		close(l.updateStarted)
	}
	if l.updateBlock != nil {
		select {
		case <-l.updateBlock:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return l.updateErr
}

type testSegmentLoadInfoStream struct {
	mu      sync.Mutex
	handler SegmentLoadInfoEventHandler
}

func (s *testSegmentLoadInfoStream) Subscribe(option SegmentLoadInfoSubscriptionOption) SegmentLoadInfoSubscription {
	s.mu.Lock()
	s.handler = option.Handler
	s.mu.Unlock()
	return &testSubscription{collectionID: option.CollectionID, segmentID: option.SegmentID}
}
func (*testSegmentLoadInfoStream) Close() {}

type testSubscription struct {
	collectionID int64
	segmentID    int64
	closed       atomic.Bool
}

func (s *testSubscription) CollectionID() int64 { return s.collectionID }
func (s *testSubscription) SegmentID() int64    { return s.segmentID }
func (*testSubscription) Error() error          { return nil }
func (s *testSubscription) Close()              { s.closed.Store(true) }

func TestPhysicalSegmentManagerRejectsRevisionRollback(t *testing.T) {
	meta, view, key := testQueryView()
	stream := &testSegmentLoadInfoStream{}
	loader := &testPhysicalLoader{segment: &testTransformSegment{id: 100, partitionID: 10}}
	manager := NewViewScopedPhysicalSegmentManager(testTaskScheduler{}, loader, stream, nil)
	loaded := make(chan struct{}, 1)
	dropped := make(chan struct{})
	manager.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: view,
		Collection: &testCollectionRuntimeGuard{collectionID: 1},
		OnLoaded:   func([]TransformSegment) { loaded <- struct{}{} },
	})
	require.Eventually(t, func() bool {
		stream.mu.Lock()
		defer stream.mu.Unlock()
		return stream.handler != nil
	}, time.Second, time.Millisecond)

	stream.mu.Lock()
	handler := stream.handler
	stream.mu.Unlock()
	require.NoError(t, handler.Handle(testSnapshot(2)))
	select {
	case <-loaded:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for physical load")
	}
	require.NoError(t, handler.Handle(testSnapshot(1)))
	require.NoError(t, handler.Handle(testSnapshot(3)))
	require.Eventually(t, func() bool {
		loader.mu.Lock()
		defer loader.mu.Unlock()
		return len(loader.updates) == 1
	}, time.Second, time.Millisecond)
	loader.mu.Lock()
	assert.Equal(t, []SegmentLoadInfoRevision{{Revision: 3}}, loader.updates)
	loader.mu.Unlock()

	meta.Version.QueryVersion++
	secondKey := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	reused := make(chan []TransformSegment, 1)
	manager.Acquire(AcquirePhysicalSegments{
		Key: secondKey, Meta: meta, View: view,
		Collection: &testCollectionRuntimeGuard{collectionID: 1},
		OnLoaded:   func(segments []TransformSegment) { reused <- segments },
	})
	select {
	case segments := <-reused:
		require.Len(t, segments, 1)
		assert.Same(t, loader.segment, segments[0])
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shared physical segment")
	}
	manager.Release(ReleaseSegments{Key: secondKey})

	manager.Release(ReleaseSegments{Key: key, OnDropped: func() { close(dropped) }})
	select {
	case <-dropped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for physical release")
	}
}

func TestPhysicalSegmentManagerFailureAndCancellation(t *testing.T) {
	meta, view, key := testQueryView()

	t.Run("terminal load failure", func(t *testing.T) {
		stream := &testSegmentLoadInfoStream{}
		loadErr := merr.WrapErrServiceInternalMsg("test load failure")
		loader := &testPhysicalLoader{loadErr: loadErr}
		manager := NewViewScopedPhysicalSegmentManager(testTaskScheduler{}, loader, stream, nil)
		failed := make(chan error, 1)
		manager.Acquire(AcquirePhysicalSegments{
			Key: key, Meta: meta, View: view,
			Collection:             &testCollectionRuntimeGuard{collectionID: 1},
			OnSegmentUnrecoverable: func(_ int64, err error) { failed <- err },
		})
		require.Eventually(t, func() bool {
			stream.mu.Lock()
			defer stream.mu.Unlock()
			return stream.handler != nil
		}, time.Second, time.Millisecond)
		stream.mu.Lock()
		handler := stream.handler
		stream.mu.Unlock()
		require.NoError(t, handler.Handle(testSnapshot(1)))
		select {
		case err := <-failed:
			require.ErrorIs(t, err, loadErr)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for load failure")
		}
	})

	t.Run("release cancels and waits for load", func(t *testing.T) {
		stream := &testSegmentLoadInfoStream{}
		loader := &testPhysicalLoader{
			segment:     &testTransformSegment{id: 100, partitionID: 10},
			loadStarted: make(chan struct{}),
			loadBlock:   make(chan struct{}),
		}
		manager := NewViewScopedPhysicalSegmentManager(testTaskScheduler{}, loader, stream, nil)
		manager.Acquire(AcquirePhysicalSegments{
			Key: key, Meta: meta, View: view,
			Collection: &testCollectionRuntimeGuard{collectionID: 1},
		})
		require.Eventually(t, func() bool {
			stream.mu.Lock()
			defer stream.mu.Unlock()
			return stream.handler != nil
		}, time.Second, time.Millisecond)
		stream.mu.Lock()
		handler := stream.handler
		stream.mu.Unlock()
		require.NoError(t, handler.Handle(testSnapshot(1)))
		select {
		case <-loader.loadStarted:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for load start")
		}
		dropped := make(chan struct{})
		manager.Release(ReleaseSegments{Key: key, OnDropped: func() { close(dropped) }})
		select {
		case <-dropped:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for canceled load release")
		}
	})
}

func TestPhysicalSegmentManagerEmptyViewAndReset(t *testing.T) {
	meta, view, key := testQueryView()
	empty := &viewpb.QueryViewOfQueryNode{NodeId: view.GetNodeId()}
	manager := NewViewScopedPhysicalSegmentManager(
		testTaskScheduler{}, &testPhysicalLoader{}, &testSegmentLoadInfoStream{}, nil)
	loadedEmpty := make(chan []TransformSegment, 1)
	manager.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: empty,
		OnLoaded: func(segments []TransformSegment) { loadedEmpty <- segments },
	})
	select {
	case loaded := <-loadedEmpty:
		assert.Empty(t, loaded)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for empty view readiness")
	}
	manager.Release(ReleaseSegments{Key: key})

	meta.Version.QueryVersion++
	key = qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	stream := &testSegmentLoadInfoStream{}
	segment := &testTransformSegment{id: 100, partitionID: 10}
	manager = NewViewScopedPhysicalSegmentManager(
		testTaskScheduler{}, &testPhysicalLoader{segment: segment}, stream, nil)
	loaded := make(chan struct{}, 1)
	manager.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: view,
		Collection: &testCollectionRuntimeGuard{collectionID: 1},
		OnLoaded:   func([]TransformSegment) { loaded <- struct{}{} },
	})
	require.Eventually(t, func() bool {
		stream.mu.Lock()
		defer stream.mu.Unlock()
		return stream.handler != nil
	}, time.Second, time.Millisecond)
	stream.mu.Lock()
	handler := stream.handler
	stream.mu.Unlock()
	require.NoError(t, handler.Handle(testSnapshot(1)))
	select {
	case <-loaded:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for segment load")
	}
	manager.ResetSegment(100)
	require.Eventually(t, func() bool { return segment.released.Load() == 1 }, time.Second, time.Millisecond)
}

func TestPhysicalSegmentManagerResetCancelsUpdate(t *testing.T) {
	meta, view, key := testQueryView()
	stream := &testSegmentLoadInfoStream{}
	segment := &testTransformSegment{id: 100, partitionID: 10}
	loader := &testPhysicalLoader{
		segment:       segment,
		updateStarted: make(chan struct{}),
		updateBlock:   make(chan struct{}),
	}
	manager := NewViewScopedPhysicalSegmentManager(testTaskScheduler{}, loader, stream, nil)
	loaded := make(chan struct{}, 1)
	manager.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: view,
		Collection: &testCollectionRuntimeGuard{collectionID: 1},
		OnLoaded:   func([]TransformSegment) { loaded <- struct{}{} },
	})
	require.Eventually(t, func() bool {
		stream.mu.Lock()
		defer stream.mu.Unlock()
		return stream.handler != nil
	}, time.Second, time.Millisecond)
	stream.mu.Lock()
	handler := stream.handler
	stream.mu.Unlock()
	require.NoError(t, handler.Handle(testSnapshot(1)))
	select {
	case <-loaded:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial load")
	}
	require.NoError(t, handler.Handle(testSnapshot(2)))
	select {
	case <-loader.updateStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for update start")
	}
	manager.ResetSegment(100)
	require.Eventually(t, func() bool { return segment.released.Load() == 1 }, time.Second, time.Millisecond)
}

func TestResourceTypeAndViewFilterHelpers(t *testing.T) {
	assert.Equal(t, QueryViewLoadInfoVersion(7), QueryViewLoadInfoVersionFromProto(7))
	action := SegmentUpdateReopen | SegmentUpdateLoadIndex
	assert.True(t, action.Has(SegmentUpdateReopen))
	assert.True(t, action.Has(SegmentUpdateLoadIndex))
	assert.False(t, action.Has(SegmentUpdateAction(1<<7)))

	_, view, _ := testQueryView()
	view.Partitions[0].SegmentIds = []int64{100, 101}
	filtered := filterViewSegments(view, []int64{101})
	require.Len(t, filtered.GetPartitions(), 1)
	assert.Equal(t, []int64{101}, filtered.GetPartitions()[0].GetSegmentIds())
	all := filterViewSegments(view, []int64{100, 101})
	assert.Equal(t, view.GetPartitions(), all.GetPartitions())

	snapshot := testSnapshot(1)
	snapshot.IndexInfos = []*indexpb.IndexInfo{nil, {IndexName: "idx"}}
	cloned := cloneSegmentLoadInfoSnapshot(snapshot)
	require.Len(t, cloned.IndexInfos, 2)
	assert.Nil(t, cloned.IndexInfos[0])
	assert.NotSame(t, snapshot.IndexInfos[1], cloned.IndexInfos[1])
	assert.Nil(t, cloneQueryViewMeta(nil))
	assert.Nil(t, cloneQueryNodeView(nil))
	assert.Equal(t, SegmentUpdateNone, classifySegmentUpdate(SegmentLoadInfoRevision{Revision: 1}, SegmentLoadInfoRevision{}))
	assert.Equal(t, SegmentUpdateNone, classifySegmentUpdate(SegmentLoadInfoRevision{Revision: 1}, SegmentLoadInfoRevision{Revision: 1}))
	physicalSegmentLoadInfoHandler{}.Close()
}

func TestAdditionalResourcePreparationBranches(t *testing.T) {
	t.Run("empty readiness view", func(t *testing.T) {
		meta, view, key := testQueryView()
		view.Partitions = nil
		ready := make(chan map[int64][]int64, 1)
		manager := NewQueryViewSegmentReadinessManager(
			testTaskScheduler{},
			testPhysicalManager{
				acquire: func(AcquirePhysicalSegments) { t.Error("unexpected physical acquire") },
				release: func(req ReleaseSegments) { req.OnDropped() },
			},
			&testTransformBuffer{guard: &testTransformGuard{}},
			testCollectionRuntimeManager{guard: &testCollectionRuntimeGuard{collectionID: 1}},
		)
		manager.Acquire(AcquireSegments{Key: key, Meta: meta, View: view, OnReady: func(value map[int64][]int64) { ready <- value }})
		select {
		case value := <-ready:
			assert.Empty(t, value)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for empty readiness view")
		}
		manager.Release(ReleaseSegments{Key: key})
	})

	t.Run("no-op update", func(t *testing.T) {
		updated := make(chan SegmentLoadInfoRevision, 1)
		task := newSegmentUpdateTask(&testPhysicalLoader{}, SegmentUpdateTask{
			Current:  SegmentLoadInfoRevision{Revision: 2},
			Snapshot: testSnapshot(2),
			OnUpdated: func(revision SegmentLoadInfoRevision) {
				updated <- revision
			},
		})
		require.NoError(t, task.Execute(context.Background()))
		assert.Equal(t, SegmentLoadInfoRevision{Revision: 2}, <-updated)
	})

	t.Run("invalid snapshot", func(t *testing.T) {
		manager := NewViewScopedPhysicalSegmentManager(testTaskScheduler{}, &testPhysicalLoader{}, nil, nil)
		manager.ApplyLoadInfoSnapshot(context.Background(), SegmentLoadInfoSnapshot{})
		manager.ApplyLoadInfoSnapshot(context.Background(), testSnapshot(1))
		manager.Release(ReleaseSegments{})
	})
}

type testResourceReservation struct{ released atomic.Bool }

func (r *testResourceReservation) Release() { r.released.Store(true) }

type testResourceEstimator struct {
	reservation *testResourceReservation
	err         error
}

type testIndexUpdatingRuntime struct {
	*testCollectionRuntimeGuard
	err   error
	calls atomic.Int32
}

func (r *testIndexUpdatingRuntime) UpdateIndexMeta(context.Context, []*indexpb.IndexInfo) error {
	r.calls.Add(1)
	return r.err
}

func (e testResourceEstimator) Reserve(context.Context, *querypb.SegmentLoadInfo, CollectionRuntime) (ResourceReservation, error) {
	return e.reservation, e.err
}

func TestSegmentLoadTaskReservationAndTransformStart(t *testing.T) {
	segment := &testTransformSegment{id: 100, partitionID: 10}
	loader := &testPhysicalLoader{segment: segment}
	reservation := &testResourceReservation{}
	runtime := &testIndexUpdatingRuntime{
		testCollectionRuntimeGuard: &testCollectionRuntimeGuard{collectionID: 1},
	}
	var loaded TransformSegment
	finished := false
	snapshot := testSnapshot(1)
	snapshot.IndexInfos = []*indexpb.IndexInfo{{IndexName: "idx"}}
	task := newSegmentLoadTask(loader, testResourceEstimator{reservation: reservation}, SegmentLoadTask{
		SegmentID:                   100,
		Collection:                  runtime,
		TransformStartAfterTimeTick: 99,
		Snapshot:                    snapshot,
		OnLoaded:                    func(segment TransformSegment) { loaded = segment },
		OnFinished:                  func() { finished = true },
	})

	require.NoError(t, task.Execute(context.Background()))
	require.NotNil(t, loaded)
	assert.Equal(t, uint64(99), loaded.TransformStartAfterTimeTick())
	assert.Same(t, segment, UnwrapTransformSegment(loaded))
	assert.True(t, reservation.released.Load())
	assert.Equal(t, int32(1), runtime.calls.Load())
	assert.True(t, finished)
}

func TestSegmentLoadTaskFailureClassification(t *testing.T) {
	t.Run("missing snapshot is terminal", func(t *testing.T) {
		var failed error
		task := newSegmentLoadTask(&testPhysicalLoader{}, nil, SegmentLoadTask{
			SegmentID:       100,
			OnUnrecoverable: func(err error) { failed = err },
		})
		require.NoError(t, task.Execute(context.Background()))
		require.Error(t, failed)
	})

	t.Run("resource pressure asks scheduler to retry", func(t *testing.T) {
		resourceErr := merr.WrapErrSegmentRequestResourceFailed("Memory")
		loader := &testPhysicalLoader{loadErr: resourceErr}
		called := false
		task := newSegmentLoadTask(loader, nil, SegmentLoadTask{
			SegmentID:       100,
			Snapshot:        testSnapshot(1),
			OnUnrecoverable: func(error) { called = true },
		})
		err := task.Execute(context.Background())
		require.ErrorIs(t, err, merr.ErrSegmentRequestResourceFailed)
		assert.False(t, called)
	})

	t.Run("collection index update failure is terminal", func(t *testing.T) {
		updateErr := merr.WrapErrServiceInternalMsg("test index metadata failure")
		runtime := &testIndexUpdatingRuntime{
			testCollectionRuntimeGuard: &testCollectionRuntimeGuard{collectionID: 1},
			err:                        updateErr,
		}
		var failed error
		task := newSegmentLoadTask(&testPhysicalLoader{}, nil, SegmentLoadTask{
			SegmentID:  100,
			Collection: runtime,
			Snapshot: SegmentLoadInfoSnapshot{
				CollectionID: 1,
				SegmentID:    100,
				Revision:     SegmentLoadInfoRevision{Revision: 1},
				LoadInfo:     &querypb.SegmentLoadInfo{CollectionID: 1, SegmentID: 100},
				IndexInfos:   []*indexpb.IndexInfo{{IndexName: "idx"}},
			},
			OnUnrecoverable: func(err error) { failed = err },
		})
		require.NoError(t, task.Execute(context.Background()))
		require.ErrorIs(t, failed, updateErr)
		assert.Equal(t, int32(1), runtime.calls.Load())
	})

	t.Run("reservation failure is terminal", func(t *testing.T) {
		reserveErr := merr.WrapErrServiceInternalMsg("test reserve failure")
		var failed error
		task := newSegmentLoadTask(&testPhysicalLoader{}, testResourceEstimator{err: reserveErr}, SegmentLoadTask{
			SegmentID:       100,
			Snapshot:        testSnapshot(1),
			OnUnrecoverable: func(err error) { failed = err },
		})
		require.NoError(t, task.Execute(context.Background()))
		require.ErrorIs(t, failed, reserveErr)
	})
}

func TestSegmentUpdateTaskRetriesAndCancels(t *testing.T) {
	updateErr := merr.WrapErrServiceNotReadyMsg("test update retry")
	loader := &testPhysicalLoader{updateErr: updateErr}
	updated := false
	task := newSegmentUpdateTask(loader, SegmentUpdateTask{
		Segment:  &testTransformSegment{id: 100},
		Current:  SegmentLoadInfoRevision{Revision: 1},
		Snapshot: testSnapshot(2),
		OnUpdated: func(SegmentLoadInfoRevision) {
			updated = true
		},
	})
	require.ErrorIs(t, task.Execute(context.Background()), merr.ErrServiceNotReady)
	assert.False(t, updated)

	loader.updateErr = nil
	require.NoError(t, task.Execute(context.Background()))
	assert.True(t, updated)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var failed error
	canceled := newSegmentUpdateTask(loader, SegmentUpdateTask{
		Context:  ctx,
		Snapshot: testSnapshot(3),
		OnFailed: func(err error) { failed = err },
	})
	require.NoError(t, canceled.Execute(context.Background()))
	require.ErrorIs(t, failed, context.Canceled)
}

type scriptedEventSource struct {
	mu          sync.Mutex
	after       []SegmentLoadInfoRevision
	readers     []SegmentLoadInfoEventReader
	shouldRetry bool
}

func (s *scriptedEventSource) Open(ctx context.Context, _, _ int64, after SegmentLoadInfoRevision) (SegmentLoadInfoEventReader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.after = append(s.after, after)
	if len(s.readers) == 0 {
		return contextEventReader{ctx: ctx}, nil
	}
	reader := s.readers[0]
	s.readers = s.readers[1:]
	return reader, nil
}
func (s *scriptedEventSource) Retryable(error) bool { return s.shouldRetry }

type scriptedEventReader struct {
	snapshots []SegmentLoadInfoSnapshot
	err       error
}

func (r *scriptedEventReader) Recv() (SegmentLoadInfoSnapshot, error) {
	if len(r.snapshots) == 0 {
		return SegmentLoadInfoSnapshot{}, r.err
	}
	snapshot := r.snapshots[0]
	r.snapshots = r.snapshots[1:]
	return snapshot, nil
}
func (*scriptedEventReader) Close() {}

type contextEventReader struct{ ctx context.Context }

func (r contextEventReader) Recv() (SegmentLoadInfoSnapshot, error) {
	<-r.ctx.Done()
	return SegmentLoadInfoSnapshot{}, r.ctx.Err()
}
func (contextEventReader) Close() {}

type collectingEventHandler struct {
	values chan SegmentLoadInfoSnapshot
}

func (h collectingEventHandler) Handle(snapshot SegmentLoadInfoSnapshot) error {
	h.values <- snapshot
	return nil
}
func (collectingEventHandler) Close() {}

type closeTrackingEventHandler struct {
	closed *atomic.Bool
}

func (closeTrackingEventHandler) Handle(SegmentLoadInfoSnapshot) error { return nil }
func (h closeTrackingEventHandler) Close()                             { h.closed.Store(true) }

func TestSegmentLoadInfoStreamReconnectsFromAcceptedRevision(t *testing.T) {
	retryErr := merr.WrapErrServiceNotReadyMsg("test stream disconnected")
	source := &scriptedEventSource{shouldRetry: true, readers: []SegmentLoadInfoEventReader{
		&scriptedEventReader{snapshots: []SegmentLoadInfoSnapshot{testSnapshot(1)}, err: retryErr},
		&scriptedEventReader{snapshots: []SegmentLoadInfoSnapshot{testSnapshot(2)}, err: retryErr},
	}}
	stream := NewReconnectingSegmentLoadInfoStream(context.Background(), source, time.Millisecond)
	handler := collectingEventHandler{values: make(chan SegmentLoadInfoSnapshot, 2)}
	subscription := stream.Subscribe(SegmentLoadInfoSubscriptionOption{
		CollectionID: 1, SegmentID: 100, Handler: handler,
	})
	defer stream.Close()
	for expected := uint64(1); expected <= 2; expected++ {
		select {
		case snapshot := <-handler.values:
			assert.Equal(t, expected, snapshot.Revision.Revision)
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for revision %d", expected)
		}
	}
	subscription.Close()
	source.mu.Lock()
	require.GreaterOrEqual(t, len(source.after), 2)
	assert.Equal(t, SegmentLoadInfoRevision{Revision: 1}, source.after[1])
	source.mu.Unlock()
}

func TestSegmentLoadInfoStreamExposesTerminalSubscriptionError(t *testing.T) {
	terminalErr := merr.WrapErrCollectionNotFound(1)
	source := &scriptedEventSource{readers: []SegmentLoadInfoEventReader{
		&scriptedEventReader{err: terminalErr},
	}}
	stream := NewReconnectingSegmentLoadInfoStream(context.Background(), source, time.Millisecond)
	subscription := stream.Subscribe(SegmentLoadInfoSubscriptionOption{
		CollectionID: 1,
		SegmentID:    100,
		Handler:      collectingEventHandler{values: make(chan SegmentLoadInfoSnapshot, 1)},
	})
	assert.Equal(t, int64(1), subscription.CollectionID())
	assert.Equal(t, int64(100), subscription.SegmentID())
	require.Eventually(t, func() bool {
		return subscription.Error() != nil
	}, time.Second, time.Millisecond)
	require.ErrorIs(t, subscription.Error(), merr.ErrCollectionNotFound)
	subscription.Close()
	stream.Close()
	stream.Close()
}

func TestSegmentLoadInfoStreamSubscribeAfterClose(t *testing.T) {
	// Intentionally verify that the public constructor normalizes a nil context.
	stream := NewReconnectingSegmentLoadInfoStream(nil, &scriptedEventSource{}, 0) //nolint:staticcheck
	stream.Close()
	closed := &atomic.Bool{}
	subscription := stream.Subscribe(SegmentLoadInfoSubscriptionOption{
		CollectionID: 1,
		SegmentID:    100,
		Handler:      closeTrackingEventHandler{closed: closed},
	})
	subscription.Close()
	assert.True(t, closed.Load())
}

func testQueryView() (*viewpb.QueryViewMeta, *viewpb.QueryViewOfQueryNode, qviews.QueryViewKey) {
	meta := &viewpb.QueryViewMeta{
		CollectionId: 1,
		ReplicaId:    2,
		Vchannel:     "by-dev-rootcoord-dml_0v0",
		Version: &viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
			QueryVersion: 1,
		},
		LoadInfoVersion: 7,
	}
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     3,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{100}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	return meta, view, key
}

func testSnapshot(revision uint64) SegmentLoadInfoSnapshot {
	return SegmentLoadInfoSnapshot{
		CollectionID: 1,
		SegmentID:    100,
		Revision:     SegmentLoadInfoRevision{Revision: revision},
		LoadInfo: &querypb.SegmentLoadInfo{
			CollectionID: 1,
			SegmentID:    100,
			PartitionID:  10,
			NumOfRows:    int64(revision),
		},
	}
}
