package qnview

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestPendingReplacementRetainsReadySegmentsBeforeOldRelease(t *testing.T) {
	catchup := mockey.Mock((*fakeTransformRegistration).WaitCatchup).Return(nil).Build()
	defer catchup.UnPatch()
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	shared := &fakeTransformSegment{id: 1000, partitionID: 10}
	exclusive := &fakeTransformSegment{id: 1001, partitionID: 10}
	loads := 0
	acquire := mockey.Mock(fakePhysicalSegmentManager.Acquire).To(func(_ fakePhysicalSegmentManager, req AcquirePhysicalSegments) {
		loads++
		req.OnLoaded([]TransformSegment{shared, exclusive})
	}).Build()
	defer acquire.UnPatch()
	release := mockey.Mock(fakePhysicalSegmentManager.Release).To(func(_ fakePhysicalSegmentManager, req ReleaseSegments) { req.OnDropped() }).Build()
	defer release.UnPatch()
	manager := NewQueryViewSegmentReadinessManagerWithScheduler(scheduler, fakePhysicalSegmentManager{}, &fakeTransformLogBuffer{}, 4, nil)
	meta, view := buildHandlerTestMeta(1), buildHandlerTestQNView(1)
	view.Partitions = view.Partitions[:1]
	oldKey := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	ready := make(chan int64, 4)
	onReady := func(segments map[int64][]int64) {
		for _, ids := range segments {
			for _, id := range ids {
				ready <- id
			}
		}
	}
	manager.Acquire(AcquireSegments{Key: oldKey, Meta: meta, View: view, OnReady: onReady, OnUnrecoverable: func() { t.Error("unexpected failure") }})
	waitReady := func(n int) {
		for range n {
			select {
			case <-ready:
			case <-time.After(time.Second * 5):
				t.Fatal("segments not ready")
			}
		}
	}
	waitReady(2)
	// Make the collection-runtime step run only after the old view is released.
	entered, resume := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(resume) })
	scheduler.Submit(qnTaskFunc(func(context.Context) error { close(entered); <-resume; return nil }))
	<-entered
	meta = buildHandlerTestMeta(1)
	meta.Version.QueryVersion = 2
	view = buildHandlerTestQNView(1)
	view.Partitions = view.Partitions[:1]
	view.Partitions[0].SegmentIds = []int64{1000}
	replacement := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	manager.Acquire(AcquireSegments{Key: replacement, Meta: meta, View: view, OnReady: onReady})
	dropped := make(chan struct{})
	manager.Release(ReleaseSegments{Key: oldKey, OnDropped: func() { close(dropped) }})
	<-dropped
	require.False(t, shared.released, "pending replacement must already own the loaded segment")
	require.True(t, exclusive.released, "unshared successful loads must be released")
	unblock.Do(func() { close(resume) })
	waitReady(1)
	require.Equal(t, 2, loads, "each view must acquire its physical reference")
	replacementDropped := make(chan struct{})
	manager.Release(ReleaseSegments{Key: replacement, OnDropped: func() { close(replacementDropped) }})
	<-replacementDropped
	require.True(t, shared.released)
}
