package qnview

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func acquireGenerationView(m *QueryViewSegmentReadinessManager, version int64) (qviews.QueryViewKey, *viewpb.QueryViewOfQueryNode, chan struct{}) {
	meta := buildHandlerTestMeta(version)
	view := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	ready := make(chan struct{}, 1)
	m.Acquire(AcquireSegments{Key: key, Meta: meta, View: view, OnReady: func(map[int64][]int64) { ready <- struct{}{} }})
	return key, view, ready
}

func waitGenerationEvent(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out")
	}
}

func TestCatchupCallbackKeepsReplacementGeneration(t *testing.T) {
	for _, stage := range []string{"registration-failure", "catchup-failure", "catchup-success"} {
		t.Run(stage, func(t *testing.T) {
			oldSegment := &fakeTransformSegment{id: 1000, partitionID: 10}
			newSegment := &fakeTransformSegment{id: 1000, partitionID: 10}
			oldReg, newReg := &lifetimeRegistration{}, &lifetimeRegistration{}
			oldEntered, finishOld, oldFinished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			newEntered, finishNew := make(chan struct{}), make(chan struct{})
			var newReleased atomic.Bool
			patchLifetime(t, mockey.Mock((*fakeTransformSegment).Release).To(func(s *fakeTransformSegment, _ context.Context) error {
				if s == newSegment {
					newReleased.Store(true)
				}
				return nil
			}).Build())
			patchLifetime(t, mockey.Mock((*fakeTransformLogBuffer).Acquire).Return(instantTransformGuard{}, nil).Build())
			patchLifetime(t, mockey.Mock((*fakeTransformLogBuffer).RegisterSegment).To(func(_ *fakeTransformLogBuffer, _ context.Context, s TransformSegment) (TransformRegistration, error) {
				if UnwrapTransformSegment(s) == oldSegment {
					if stage == "registration-failure" {
						close(oldEntered)
						<-finishOld
						return nil, assert.AnError
					}
					return oldReg, nil
				}
				return newReg, nil
			}).Build())
			patchLifetime(t, mockey.Mock((*lifetimeRegistration).Unregister).Return().Build())
			patchLifetime(t, mockey.Mock((*lifetimeRegistration).WaitCatchup).To(func(r *lifetimeRegistration, ctx context.Context) error {
				if r == newReg {
					close(newEntered)
					<-finishNew
					return nil
				}
				close(oldEntered)
				<-ctx.Done()
				<-finishOld
				if stage == "catchup-success" {
					return nil
				}
				return ctx.Err()
			}).Build())
			patchLifetime(t, mockey.Mock(fakePhysicalSegmentManager.Acquire).To(func(_ fakePhysicalSegmentManager, req AcquirePhysicalSegments) {
				s := newSegment
				if req.Key.QueryViewVersion.QueryVersion == 1 {
					s = oldSegment
				}
				req.OnLoaded([]TransformSegment{s})
			}).Build())
			patchLifetime(t, mockey.Mock(fakePhysicalSegmentManager.Release).To(func(_ fakePhysicalSegmentManager, req ReleaseSegments) { req.OnDropped() }).Build())
			var origin func(*QueryViewSegmentReadinessManager, segmentCatchupTask)
			patchLifetime(t, mockey.Mock((*QueryViewSegmentReadinessManager).registerAndCatchup).To(func(m *QueryViewSegmentReadinessManager, task segmentCatchupTask) {
				origin(m, task)
				if task.segment == oldSegment {
					close(oldFinished)
				}
			}).Origin(&origin).Build())
			sched := nodescheduler.New(4)
			t.Cleanup(sched.Close)
			mgr := NewQueryViewSegmentReadinessManagerWithScheduler(sched, fakePhysicalSegmentManager{}, &fakeTransformLogBuffer{}, 2)
			first, _, _ := acquireGenerationView(mgr, 1)
			waitGenerationEvent(t, oldEntered)
			releaseLifetimeView(t, mgr, first)
			second, view, readySecond := acquireGenerationView(mgr, 2)
			waitGenerationEvent(t, newEntered)
			if stage == "catchup-success" {
				close(finishOld)
				waitGenerationEvent(t, oldFinished)
				select {
				case <-readySecond:
					t.Error("old catchup made replacement ready before its own catchup")
				default:
				}
			}
			close(finishNew)
			waitGenerationEvent(t, readySecond)
			handles, err := mgr.AcquireSealedSegmentHandles(context.Background(), second, view)
			require.NoError(t, err)
			defer handles[0].Release()
			if stage != "catchup-success" {
				close(finishOld)
				waitGenerationEvent(t, oldFinished)
			}
			assert.False(t, newReleased.Load(), "stale callback released a query's segment")
			mgr.mu.Lock()
			state := mgr.segments[1000]
			mgr.mu.Unlock()
			require.NotNil(t, state)
			assert.Same(t, newSegment, state.segment)
			releaseLifetimeView(t, mgr, second)
			assert.False(t, newReleased.Load(), "query handle must keep segment after view drops")
			handles[0].Release()
			assert.True(t, newReleased.Load())
		})
	}
}

func TestFailedSegmentWaitsForItsOwnQueryHandles(t *testing.T) {
	var oldReleased, newReleased atomic.Bool
	oldSegment := &fakeTransformSegment{id: 1000, partitionID: 10}
	newSegment := &fakeTransformSegment{id: 1000, partitionID: 10}
	patchLifetime(t, mockey.Mock((*fakeTransformSegment).Release).To(func(s *fakeTransformSegment, _ context.Context) error {
		if UnwrapTransformSegment(s) == oldSegment {
			oldReleased.Store(true)
		} else {
			newReleased.Store(true)
		}
		return nil
	}).Build())
	patchLifetime(t, mockey.Mock((*fakeTransformLogBuffer).Acquire).Return(instantTransformGuard{}, nil).Build())
	patchLifetime(t, mockey.Mock((*fakeTransformLogBuffer).RegisterSegment).Return(instantTransformRegistration{}, nil).Build())
	patchLifetime(t, mockey.Mock(fakePhysicalSegmentManager.Acquire).To(func(_ fakePhysicalSegmentManager, req AcquirePhysicalSegments) {
		s := newSegment
		if req.Key.QueryViewVersion.QueryVersion == 1 {
			s = oldSegment
		}
		req.OnLoaded([]TransformSegment{s})
	}).Build())
	patchLifetime(t, mockey.Mock(fakePhysicalSegmentManager.Release).To(func(_ fakePhysicalSegmentManager, req ReleaseSegments) { req.OnDropped() }).Build())
	sched := nodescheduler.New(4)
	t.Cleanup(sched.Close)
	mgr := NewQueryViewSegmentReadinessManagerWithScheduler(sched, fakePhysicalSegmentManager{}, &fakeTransformLogBuffer{}, 2)
	first, view, ready := acquireGenerationView(mgr, 1)
	waitGenerationEvent(t, ready)
	handles, err := mgr.AcquireSealedSegmentHandles(context.Background(), first, view)
	require.NoError(t, err)
	defer handles[0].Release()
	moreHandles, err := mgr.AcquireSealedSegmentHandles(context.Background(), first, view)
	require.NoError(t, err)
	defer moreHandles[0].Release()
	mgr.mu.Lock()
	oldState := mgr.segments[1000]
	mgr.mu.Unlock()
	mgr.failSegment(1000, oldState, assert.AnError)
	assert.False(t, oldReleased.Load(), "failure must not release successful query tasks")
	releaseLifetimeView(t, mgr, first)
	second, view, ready := acquireGenerationView(mgr, 2)
	waitGenerationEvent(t, ready)
	newHandles, err := mgr.AcquireSealedSegmentHandles(context.Background(), second, view)
	require.NoError(t, err)
	defer newHandles[0].Release()
	handles[0].Release()
	assert.False(t, oldReleased.Load())
	moreHandles[0].Release()
	assert.True(t, oldReleased.Load())
	assert.False(t, newReleased.Load())
	mgr.mu.Lock()
	newRefs := mgr.segments[1000].queryRefs
	mgr.mu.Unlock()
	assert.Equal(t, 1, newRefs, "old handles must not decrement replacement query refs")
	releaseLifetimeView(t, mgr, second)
	assert.False(t, newReleased.Load())
	newHandles[0].Release()
	assert.True(t, newReleased.Load())
}

func TestFailedSegmentUnregistersBeforeLastHandleRelease(t *testing.T) {
	segment := &fakeTransformSegment{id: 1000, partitionID: 10}
	var released atomic.Bool
	entered, resume, failed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	patchLifetime(t, mockey.Mock((*fakeTransformSegment).Release).To(func(*fakeTransformSegment, context.Context) error {
		released.Store(true)
		return nil
	}).Build())
	patchLifetime(t, mockey.Mock((*lifetimeRegistration).Unregister).To(func(*lifetimeRegistration) {
		close(entered)
		<-resume
	}).Build())
	state := &transformSegmentState{state: transformSegmentLoaded, segment: segment, queryRefs: 1, reg: &lifetimeRegistration{}}
	manager := &QueryViewSegmentReadinessManager{segments: map[int64]*transformSegmentState{1000: state}}
	handle := &sealedSegmentHandle{view: &transformViewRef{queryRefs: 1}, manager: manager, segmentID: 1000, segment: segment, state: state}
	go func() {
		manager.failSegment(1000, state, assert.AnError)
		close(failed)
	}()
	waitGenerationEvent(t, entered)
	handle.Release()
	assert.False(t, released.Load(), "segment must survive until transform unregistration completes")
	close(resume)
	waitGenerationEvent(t, failed)
	assert.True(t, released.Load())
}
