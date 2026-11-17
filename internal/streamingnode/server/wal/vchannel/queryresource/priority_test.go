package queryresource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestManagerBuildPriorityInheritanceAndWithdrawal(t *testing.T) {
	for _, withdraw := range []bool{false, true} {
		t.Run(map[bool]string{false: "inherit", true: "withdraw"}[withdraw], func(t *testing.T) {
			mockey.PatchConvey("shared build follows current dependencies", t, func() {
				scheduler := nodescheduler.New(1)
				defer scheduler.Close()
				dispatcher := NewDispatcher(1)
				defer dispatcher.Close()
				started, release := make(chan struct{}), make(chan struct{})
				scheduler.Submit(queryResourceTaskFunc(func(context.Context) error { close(started); <-release; return nil }))
				<-started
				order := make(chan int64, 2)
				mockey.Mock((*QueryRuntime).Initialize).To(func(_ *QueryRuntime, _ context.Context, view walview.VChannelWALView) error {
					order <- view.CollectionID
					return nil
				}).Build()
				newManager := func() *Manager {
					return NewManager(Config{Scheduler: scheduler, Dispatcher: dispatcher, Builders: []QueryRuntimeModuleBuilder{versionedQueryRuntimeModuleBuilder{}}})
				}
				a, b := newManager(), newManager()
				defer a.Close()
				defer b.Close()
				acquire := func(m *Manager, collection, version int64, priority int32) qviews.QueryViewKey {
					meta, key := testManagerQueryViewMetaAndKey(version)
					meta.Control = &viewpb.QueryViewControl{PreparationPriority: priority}
					m.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta}, func(*viewpb.QueryViewMeta) (walview.VChannelWALView, bool) {
						return walview.VChannelWALView{CollectionID: collection}, true
					})
					return key
				}
				acquire(a, 1, 1, -20)
				acquire(b, 2, 1, -15)
				high := acquire(a, 1, 2, 10)
				if withdraw {
					a.Release(snview.ReleaseResource{Key: high})
				}
				a.mu.Lock()
				buildA := a.task
				a.mu.Unlock()
				b.mu.Lock()
				buildB := b.task
				b.mu.Unlock()
				close(release)
				_, err := buildA.Result()
				require.NoError(t, err)
				_, err = buildB.Result()
				require.NoError(t, err)
				if withdraw {
					require.EqualValues(t, 2, <-order)
					require.EqualValues(t, 1, <-order)
				} else {
					require.EqualValues(t, 1, <-order)
					require.EqualValues(t, 2, <-order)
				}
			})
		})
	}
}

func TestManagerReadyTasksUseImmutableViewPriority(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()
	started, release := make(chan struct{}), make(chan struct{})
	scheduler.Submit(queryResourceTaskFunc(func(context.Context) error { close(started); <-release; return nil }))
	<-started
	order := make(chan int64, 2)
	manager := NewManager(Config{Scheduler: scheduler, Dispatcher: dispatcher, Builders: []QueryRuntimeModuleBuilder{versionedQueryRuntimeModuleBuilder{
		prepare: func(_ context.Context, version qviews.DataVersion) error {
			order <- version.StreamingVersion
			return nil
		},
	}}})
	defer manager.Close()
	done := make(chan struct{}, 2)
	for i, priority := range []int32{-10, 10} {
		meta, key := testManagerQueryViewMetaAndKey(int64(i + 1))
		meta.Control = &viewpb.QueryViewControl{PreparationPriority: priority}
		manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta, OnReady: func() { done <- struct{}{} }}, testManagerViewBuilder)
		// Same-identity retransmission must not replace the captured priority.
		meta.Control.PreparationPriority = -priority
		manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta}, testManagerViewBuilder)
	}
	close(release)
	<-done
	<-done
	require.EqualValues(t, 2, <-order)
	require.EqualValues(t, 1, <-order)
	manager.mu.Lock()
	require.Zero(t, manager.buildPriorityLocked(), "prepared views do not boost new preparation")
	manager.mu.Unlock()
}
