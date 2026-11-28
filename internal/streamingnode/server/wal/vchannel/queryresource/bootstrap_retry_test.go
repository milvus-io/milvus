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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
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
		{"canceled_dependency", context.Canceled},
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
			var rejected, ready atomic.Bool
			meta, key := testManagerQueryViewMetaAndKey(1)
			manager.AcquireLocked(snview.AcquireResource{
				Key: key, Meta: meta,
				OnReady: func() { ready.Store(true) }, OnUnrecoverable: func() { rejected.Store(true) },
			}, testManagerViewBuilder)
			runtime := manager.runtime
			err := scheduler.task.Execute(context.Background())
			require.ErrorIs(t, err, tc.err)
			require.NotErrorIs(t, err, nodescheduler.ErrDelay)
			require.Eventually(t, rejected.Load, time.Second, time.Millisecond)
			require.False(t, ready.Load())
			require.Equal(t, queryRuntimeClosed, runtime.state)
			require.Nil(t, manager.runtime)
			require.Nil(t, manager.task)
			require.ErrorIs(t, manager.err, tc.err)
		})
	}
}

func TestResolvedEmptyLoadScopeIsNotUnrestricted(t *testing.T) {
	manager := NewManager(Config{LoadInfoProvider: fakeLoadInfoProvider{}})
	view, err := manager.resolveLoadInfo(context.Background(), walview.VChannelWALView{LoadInfoVersion: 1, Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100}}}})
	require.NoError(t, err)
	require.NotNil(t, view.PartitionIDs)
	require.Empty(t, view.PartitionIDs)
}

func TestReplicaPreparationRetriesSameVersion(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()

	var captures, initial, readiness atomic.Int32
	var rejected atomic.Bool
	module := versionedQueryRuntimeModuleBuilder{prepare: func(context.Context, qviews.DataVersion) error {
		switch readiness.Add(1) {
		case 1:
			return merr.ErrDataIntegrity
		case 2:
			return context.Canceled // dependency canceled its request while the registration was live
		default:
			return nil
		}
	}}
	patch := mockey.Mock((*versionedQueryRuntimeModule).Prepare).To(func(*versionedQueryRuntimeModule, context.Context, walview.VChannelWALView) error {
		if initial.Add(1) == 1 {
			return merr.ErrDataIntegrity
		}
		return nil
	}).Build()
	defer patch.UnPatch()
	manager := NewManager(Config{
		Scheduler: scheduler, Dispatcher: dispatcher, Builders: []QueryRuntimeModuleBuilder{module},
		RetryFailedPreparation: func() bool { return true },
		CaptureView: func(_ context.Context, _ *viewpb.QueryViewMeta, install func(walview.VChannelWALView)) error {
			captures.Add(1)
			install(walview.VChannelWALView{})
			return nil
		},
	})
	defer manager.Close()

	meta, key := testManagerQueryViewMetaAndKey(1)
	ready := make(chan struct{})
	manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta, OnReady: func() { close(ready) }, OnUnrecoverable: func() { rejected.Store(true) }}, nil)
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("the same RO registration never became Ready")
	}
	require.False(t, rejected.Load())
	require.EqualValues(t, 2, captures.Load())
	require.EqualValues(t, 3, readiness.Load())
}

func TestReplicaBuildFailureAcrossRoleChanges(t *testing.T) {
	scheduler := &capturedNodeScheduler{}
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()
	var writer atomic.Bool
	fail := true
	patch := mockey.Mock((*versionedQueryRuntimeModule).Prepare).To(func(*versionedQueryRuntimeModule, context.Context, walview.VChannelWALView) error {
		if fail {
			return merr.ErrDataIntegrity
		}
		return nil
	}).Build()
	defer patch.UnPatch()
	manager := NewManager(Config{
		Scheduler: scheduler, Dispatcher: dispatcher,
		Builders:               []QueryRuntimeModuleBuilder{versionedQueryRuntimeModuleBuilder{prepare: func(context.Context, qviews.DataVersion) error { return nil }}},
		RetryFailedPreparation: func() bool { return !writer.Load() },
	})
	defer manager.Close()
	meta, key := testManagerQueryViewMetaAndKey(1)
	var rejected atomic.Bool
	manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta, OnUnrecoverable: func() { rejected.Store(true) }}, testManagerViewBuilder)
	require.True(t, errors.Is(scheduler.task.Execute(t.Context()), nodescheduler.ErrDelay))

	writer.Store(true)
	err := scheduler.task.Execute(t.Context())
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.False(t, errors.Is(err, nodescheduler.ErrDelay))
	require.Eventually(t, rejected.Load, time.Second, time.Millisecond)

	// Even if another failed reference remains, demotion can rebuild from a fresh snapshot.
	writer.Store(false)
	fail = false
	nextMeta, nextKey := testManagerQueryViewMetaAndKey(2)
	ready := false
	require.True(t, manager.TryAcquireLocked(snview.AcquireResource{Key: nextKey, Meta: nextMeta, OnReady: func() { ready = true }}, testManagerViewBuilder))
	require.NoError(t, scheduler.task.Execute(t.Context()))
	require.NoError(t, scheduler.task.Execute(t.Context()))
	require.True(t, ready)
}

// The older failed view is the sole owner of that DataVersion. Releasing it
// would advance reclamation to the newer serving view before its RO retry.
func TestReplicaRetryPreservesOlderReadBase(t *testing.T) {
	scheduler := &capturedNodeScheduler{}
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()
	reader := false
	module := versionedQueryRuntimeModuleBuilder{prepare: func(_ context.Context, version qviews.DataVersion) error {
		if version.StreamingVersion == 1 && !reader {
			return merr.ErrDataIntegrity
		}
		return nil
	}}
	manager := NewManager(Config{
		Scheduler: scheduler, Dispatcher: dispatcher,
		Builders:               []QueryRuntimeModuleBuilder{module},
		RetryFailedPreparation: func() bool { return reader },
	})
	defer manager.Close()

	meta, key := testManagerQueryViewMetaAndKey(1)
	oldReady, failed := 0, 0
	manager.AcquireLocked(snview.AcquireResource{Meta: meta, Key: key, OnReady: func() { oldReady++ }, OnUnrecoverable: func() { failed++ }}, testManagerViewBuilder)
	require.NoError(t, scheduler.task.Execute(t.Context()))
	oldTask := scheduler.task
	require.NoError(t, oldTask.Execute(t.Context()))
	require.Equal(t, 1, failed)

	newMeta, newer := testManagerQueryViewMetaAndKey(2)
	newer.ShardID.ReplicaID++
	newMeta.ReplicaId = newer.ShardID.ReplicaID
	newerReady := false
	manager.AcquireLocked(snview.AcquireResource{Meta: newMeta, Key: newer, OnReady: func() { newerReady = true }}, testManagerViewBuilder)
	require.NoError(t, scheduler.task.Execute(t.Context()))
	require.True(t, newerReady)
	original := manager.refs[key].resources

	reader = true
	ready := false
	require.True(t, manager.TryAcquireLocked(snview.AcquireResource{
		Meta: meta, Key: key, Retry: true, OnReady: func() { ready = true },
	}, testManagerViewBuilder))
	require.Same(t, original, manager.refs[key].resources)
	oldest, ok := manager.OldestDataVersion()
	require.True(t, ok)
	require.Equal(t, key.QueryViewVersion.DataVersion, oldest)
	require.NoError(t, oldTask.Execute(t.Context()))
	require.Zero(t, oldReady)
	require.False(t, manager.refs[key].prepared)
	require.NoError(t, scheduler.task.Execute(t.Context()))
	require.True(t, ready)
	require.True(t, manager.refs[newer].prepared)
}

func TestReplicaPromotionRetriesCanceledDependency(t *testing.T) {
	scheduler := &capturedNodeScheduler{}
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()
	reader := true
	attempts := 0
	module := versionedQueryRuntimeModuleBuilder{prepare: func(context.Context, qviews.DataVersion) error {
		attempts++
		if attempts <= 2 {
			return context.Canceled
		}
		return nil
	}}
	manager := NewManager(Config{
		Scheduler: scheduler, Dispatcher: dispatcher,
		Builders:               []QueryRuntimeModuleBuilder{module},
		RetryFailedPreparation: func() bool { return reader },
	})
	defer manager.Close()
	meta, key := testManagerQueryViewMetaAndKey(1)
	ready := false
	manager.AcquireLocked(snview.AcquireResource{Meta: meta, Key: key, OnReady: func() { ready = true }}, testManagerViewBuilder)
	require.NoError(t, scheduler.task.Execute(t.Context()))
	task := scheduler.task
	require.True(t, errors.Is(task.Execute(t.Context()), nodescheduler.ErrDelay))
	reader = false
	require.True(t, errors.Is(task.Execute(t.Context()), nodescheduler.ErrDelay))
	require.False(t, ready)
	require.NoError(t, task.Execute(t.Context()))
	require.True(t, ready)
}
