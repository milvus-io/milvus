package qnview

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestPhysicalLoadCallbackKeepsReplacementGeneration(t *testing.T) {
	for _, failOld := range []bool{false, true} {
		name := "late-success"
		if failOld {
			name = "late-cancel"
		}
		t.Run(name, func(t *testing.T) {
			oldSegment := &fakeTransformSegment{id: 1000, partitionID: 10}
			newSegment := &fakeTransformSegment{id: 1000, partitionID: 10}
			oldEntered, continueOld := make(chan struct{}), make(chan struct{})
			var calls, failures, closes, staleNotifications atomic.Int32
			var oldReleased atomic.Bool
			patchLifetime(t, mockey.Mock((*fakeTransformSegment).Release).To(func(s *fakeTransformSegment, _ context.Context) error {
				if s == oldSegment {
					oldReleased.Store(true)
				}
				return nil
			}).Build())
			patchLifetime(t, mockey.Mock((*fakePhysicalLoader).Load).To(func(_ *fakePhysicalLoader, ctx context.Context, _ *querypb.SegmentLoadInfo, _ CollectionRuntime) (TransformSegment, error) {
				if calls.Add(1) == 1 {
					close(oldEntered)
					<-continueOld
					if failOld {
						return nil, ctx.Err()
					}
					return oldSegment, nil
				}
				return newSegment, nil
			}).Build())
			patchLifetime(t, mockey.Mock((*fakeSegmentLoadInfoStream).Subscribe).To(func(_ *fakeSegmentLoadInfoStream, opt SegmentLoadInfoSubscriptionOption) SegmentLoadInfoSubscription {
				_ = opt.Handler.Handle(SegmentLoadInfoSnapshot{DataVersion: opt.DataVersion, CollectionID: testCollectionID, SegmentID: 1000, Revision: SegmentLoadInfoRevision{Revision: 1}, LoadInfo: &querypb.SegmentLoadInfo{SegmentID: 1000}})
				return &lifetimeSubscription{}
			}).Build())
			patchLifetime(t, mockey.Mock((*lifetimeSubscription).Close).To(func(*lifetimeSubscription) { closes.Add(1) }).Build())
			sched := nodescheduler.New(4)
			t.Cleanup(sched.Close)
			phys := NewViewScopedPhysicalSegmentManagerWithNodeSchedulerAndStream(sched, &fakePhysicalLoader{}, &fakeSegmentLoadInfoStream{})
			acquire := func(version int64) (qviews.QueryViewKey, chan struct{}) {
				meta := buildHandlerTestMeta(version)
				view := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
				key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
				ready := make(chan struct{}, 3)
				phys.Acquire(AcquirePhysicalSegments{Key: key, Meta: meta, View: view, Collection: &fakeCollectionRuntimeGuard{collectionID: testCollectionID}, OnLoaded: func(segments []TransformSegment) {
					for _, segment := range segments {
						if version == 2 && segment == oldSegment {
							staleNotifications.Add(1)
						}
					}
					ready <- struct{}{}
				}, OnSegmentUnrecoverable: func(int64, error) { failures.Add(1) }})
				return key, ready
			}
			first, _ := acquire(1)
			waitGenerationEvent(t, oldEntered)
			dropped := make(chan struct{})
			phys.Release(ReleaseSegments{Key: first, OnDropped: func() { close(dropped) }})
			second, ready := acquire(2)
			waitGenerationEvent(t, ready)
			close(continueOld)
			waitGenerationEvent(t, dropped)
			phys.mu.Lock()
			state := phys.segments[1000]
			actual := state.segment
			refs := len(state.refs)
			phys.mu.Unlock()
			assert.Equal(t, int32(1), closes.Load(), "old callback must not close replacement subscription")
			assert.Equal(t, !failOld, oldReleased.Load(), "late successful result must release its own segment")
			assert.Zero(t, staleNotifications.Load(), "old result must not notify replacement view")
			assert.Same(t, newSegment, actual, "late old load must not overwrite replacement segment")
			assert.Equal(t, 1, refs, "old cancellation must not remove replacement references")
			assert.Equal(t, int32(0), failures.Load(), "old cancellation must not fail replacement view")
			done := make(chan struct{})
			phys.Release(ReleaseSegments{Key: second, OnDropped: func() { close(done) }})
			waitGenerationEvent(t, done)
		})
	}
}
