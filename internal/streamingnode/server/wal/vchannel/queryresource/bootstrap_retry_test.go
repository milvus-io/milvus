package queryresource

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestBootstrapRetryCapturesFreshSnapshotAndLiveEvents(t *testing.T) {
	scheduler := &capturedNodeScheduler{}
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()
	manager := NewManager(Config{Scheduler: scheduler, Dispatcher: dispatcher, Builders: []QueryRuntimeModuleBuilder{versionedQueryRuntimeModuleBuilder{prepare: func(context.Context, qviews.DataVersion) error { return nil }}}})
	defer manager.Close()
	var owner sync.Mutex
	observed := uint64(10)
	captures := 0
	viewBuilder := func(*viewpb.QueryViewMeta) (walview.VChannelWALView, bool) {
		captures++
		return walview.VChannelWALView{BaseGrowingTimeTick: observed, WithResourceEventLock: func(fn func()) { owner.Lock(); defer owner.Unlock(); fn() }}, true
	}
	var prepared []*versionedQueryRuntimeModule
	var snapshots, applied []uint64
	var runtimes []*QueryRuntime
	patches := []*mockey.Mocker{
		mockey.Mock((*versionedQueryRuntimeModule).Prepare).To(func(module *versionedQueryRuntimeModule, ctx context.Context, view walview.VChannelWALView) error {
			prepared = append(prepared, module)
			manager.mu.Lock()
			runtimes = append(runtimes, manager.runtime)
			manager.mu.Unlock()
			snapshots = append(snapshots, view.BaseGrowingTimeTick)
			owner.Lock()
			observed++
			tick := observed
			manager.ObserveEvent(ctx, walview.VChannelResourceEvent{Barrier: func() { applied = append(applied, tick) }})
			owner.Unlock()
			if len(prepared) == 1 {
				return context.DeadlineExceeded
			}
			return nil
		}).Build(),
	}
	defer func() {
		manager.Close()
		for _, p := range patches {
			p.UnPatch()
		}
	}()
	meta, key := testManagerQueryViewMetaAndKey(1)
	ready := 0
	owner.Lock()
	manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta, OnReady: func() { ready++ }}, viewBuilder)
	owner.Unlock()
	task := scheduler.task
	require.True(t, errors.Is(task.Execute(context.Background()), nodescheduler.ErrDelay))
	require.Equal(t, queryRuntimeClosed, runtimes[0].state)
	require.Empty(t, applied)
	_, ok := manager.QueryRuntime(key)
	require.False(t, ok)
	// An event while no runtime is installed is represented by the next capture.
	owner.Lock()
	observed = 12
	manager.ObserveEvent(context.Background(), walview.VChannelResourceEvent{})
	owner.Unlock()
	require.NoError(t, task.Execute(context.Background()))
	require.Equal(t, 2, captures)
	require.Equal(t, []uint64{10, 12}, snapshots)
	require.NotSame(t, prepared[0], prepared[1])
	require.Equal(t, []uint64{13}, applied)
	require.NoError(t, scheduler.task.Execute(context.Background()))
	require.Equal(t, 1, ready)
	manager.Close()
	for _, runtime := range runtimes {
		require.Equal(t, queryRuntimeClosed, runtime.state)
	}
}

func TestBootstrapRetriesWithoutNewWALEvents(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"deadline", context.DeadlineExceeded},
		{"transport_eof", merr.Wrap(merr.ErrIoUnexpectEOF, "read BM25 stats")},
		{"storage_throttled", merr.ErrIoTooManyRequests},
		{"rpc_unavailable", status.Error(codes.Unavailable, "coordinator unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheduler := nodescheduler.New(1)
			defer scheduler.Close()
			dispatcher := NewDispatcher(1)
			defer dispatcher.Close()
			manager := NewManager(Config{Scheduler: scheduler, Dispatcher: dispatcher, Builders: []QueryRuntimeModuleBuilder{versionedQueryRuntimeModuleBuilder{prepare: func(context.Context, qviews.DataVersion) error { return nil }}}})
			var attempts atomic.Int32
			patch := mockey.Mock((*versionedQueryRuntimeModule).Prepare).To(func(*versionedQueryRuntimeModule, context.Context, walview.VChannelWALView) error {
				if attempts.Add(1) < 3 {
					return tc.err
				}
				return nil
			}).Build()
			defer patch.UnPatch()
			defer manager.Close()
			ready := make(chan struct{})
			meta, key := testManagerQueryViewMetaAndKey(1)
			manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta, OnReady: func() { close(ready) }}, testManagerViewBuilder)
			select {
			case <-ready:
			case <-time.After(5 * time.Second):
				t.Fatal("retry needed an external WAL event")
			}
			require.Equal(t, int32(3), attempts.Load())
		})
	}
}

func TestBootstrapRetryDoesNotResurrectReleasedView(t *testing.T) {
	scheduler := &capturedNodeScheduler{}
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()
	manager := NewManager(Config{Scheduler: scheduler, Dispatcher: dispatcher})
	defer manager.Close()
	patch := mockey.Mock((*QueryRuntime).Initialize).Return(context.DeadlineExceeded).Build()
	defer patch.UnPatch()
	meta, key := testManagerQueryViewMetaAndKey(1)
	manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta}, testManagerViewBuilder)
	oldTask := scheduler.task
	require.True(t, errors.Is(oldTask.Execute(context.Background()), nodescheduler.ErrDelay))
	manager.Release(snview.ReleaseResource{Key: key})
	manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta}, testManagerViewBuilder)
	current := manager.runtime
	require.ErrorIs(t, oldTask.Execute(context.Background()), context.Canceled)
	require.Same(t, current, manager.runtime)
}

func TestBootstrapPermanentFailureIsUnrecoverable(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"data_integrity", merr.ErrDataIntegrity},
		{"bm25_decode", merr.WrapErrSerializationFailed(io.ErrUnexpectedEOF, "decode BM25 stats")},
		{"invalid_resource_response", merr.WrapErrServiceInternalMsg("query view resource version mismatch")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheduler := &capturedNodeScheduler{}
			dispatcher := NewDispatcher(1)
			defer dispatcher.Close()
			manager := NewManager(Config{
				Scheduler: scheduler, Dispatcher: dispatcher,
				Builders: []QueryRuntimeModuleBuilder{versionedQueryRuntimeModuleBuilder{}},
			})
			defer manager.Close()
			patch := mockey.Mock((*versionedQueryRuntimeModule).Prepare).Return(tc.err).Build()
			defer patch.UnPatch()
			rejected, ready := false, false
			meta, key := testManagerQueryViewMetaAndKey(1)
			manager.AcquireLocked(snview.AcquireResource{
				Key: key, Meta: meta,
				OnReady: func() { ready = true }, OnUnrecoverable: func() { rejected = true },
			}, testManagerViewBuilder)
			runtime := manager.runtime
			err := scheduler.task.Execute(context.Background())
			require.ErrorIs(t, err, tc.err)
			require.NotErrorIs(t, err, nodescheduler.ErrDelay)
			require.NoError(t, scheduler.task.Execute(context.Background()))
			require.True(t, rejected)
			require.False(t, ready)
			require.Equal(t, queryRuntimeClosed, runtime.state)
			require.Nil(t, manager.runtime)
			require.Nil(t, manager.task)
			require.ErrorIs(t, manager.err, tc.err)
		})
	}
}

func TestResolvedEmptyLoadScopeIsNotUnrestricted(t *testing.T) {
	manager := NewManager(Config{LoadInfoProvider: fakeLoadInfoProvider{}})
	view, err := manager.resolveLoadInfo(context.Background(), walview.VChannelWALView{LoadInfoVersion: 1})
	require.NoError(t, err)
	require.NotNil(t, view.PartitionIDs)
	require.Empty(t, view.PartitionIDs)
}
