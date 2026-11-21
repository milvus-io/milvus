package queryresource

import (
	"context"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/growingruntime"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestManagerSharesDefaultBuildersAndLazilyAllocatesRefs(t *testing.T) {
	first := NewManager(Config{})
	second := NewManager(Config{})

	require.Len(t, first.builders, 1)
	require.Len(t, second.builders, 1)
	assert.Same(t, &first.builders[0], &second.builders[0])
	assert.Nil(t, first.refs)
	assert.Nil(t, second.refs)
}

func TestManagerBuildNotifiesAllCurrentRefsWithoutWaiterGoroutines(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	manager := NewManager(Config{
		Scheduler:  scheduler,
		Dispatcher: dispatcher,
		Builders: []QueryRuntimeModuleBuilder{gatedQueryRuntimeModuleBuilder{
			started: started,
			release: release,
		}},
	})

	ready1 := make(chan struct{})
	ready2 := make(chan struct{})
	meta1, key1 := testManagerQueryViewMetaAndKey(1)
	manager.AcquireLocked(snview.AcquireResource{Key: key1, Meta: meta1, OnReady: func() { close(ready1) }}, testManagerViewBuilder)
	<-started

	meta2, key2 := testManagerQueryViewMetaAndKey(2)
	manager.AcquireLocked(snview.AcquireResource{Key: key2, Meta: meta2, OnReady: func() { close(ready2) }}, testManagerViewBuilder)
	close(release)

	require.Eventually(t, func() bool {
		select {
		case <-ready1:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		select {
		case <-ready2:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func TestManagerDefersBuildUntilViewBecomesReady(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()
	manager := NewManager(Config{
		Scheduler:  scheduler,
		Dispatcher: dispatcher,
	})

	ready := make(chan struct{})
	meta, key := testManagerQueryViewMetaAndKey(1)
	buildReady := false
	build := func(*viewpb.QueryViewMeta) (walview.VChannelWALView, bool) {
		return walview.VChannelWALView{}, buildReady
	}

	require.NotPanics(t, func() {
		manager.AcquireLocked(snview.AcquireResource{
			Key:     key,
			Meta:    meta,
			OnReady: func() { close(ready) },
		}, build)
	})
	select {
	case <-ready:
		t.Fatal("query resource became ready while the WAL view was unavailable")
	case <-time.After(20 * time.Millisecond):
	}
	_, ok := manager.QueryRuntime(key)
	require.False(t, ok)

	buildReady = true
	manager.TryBuildLocked(build)
	require.Eventually(t, func() bool {
		select {
		case <-ready:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	_, ok = manager.QueryRuntime(key)
	require.True(t, ok)
}

func TestManagerWaitsForExactDataVersionBeforeReady(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()

	prepareVersion2 := make(chan struct{})
	manager := NewManager(Config{
		Scheduler:  scheduler,
		Dispatcher: dispatcher,
		Builders: []QueryRuntimeModuleBuilder{versionedQueryRuntimeModuleBuilder{
			prepare: func(ctx context.Context, version qviews.DataVersion) error {
				if version.StreamingVersion != 2 {
					return nil
				}
				select {
				case <-prepareVersion2:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		}},
	})

	ready1 := make(chan struct{})
	meta1, key1 := testManagerQueryViewMetaAndKey(1)
	manager.AcquireLocked(snview.AcquireResource{Key: key1, Meta: meta1, OnReady: func() { close(ready1) }}, testManagerViewBuilder)
	require.Eventually(t, func() bool {
		select {
		case <-ready1:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)

	ready2 := make(chan struct{})
	meta2, key2 := testManagerQueryViewMetaAndKey(2)
	manager.AcquireLocked(snview.AcquireResource{Key: key2, Meta: meta2, OnReady: func() { close(ready2) }}, testManagerViewBuilder)
	select {
	case <-ready2:
		t.Fatal("query view became ready before its data version was prepared")
	case <-time.After(20 * time.Millisecond):
	}
	close(prepareVersion2)
	require.Eventually(t, func() bool {
		select {
		case <-ready2:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func TestManagerRetriesDataVersionPreparationBeforeReady(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()

	var attempts atomic.Int32
	manager := NewManager(Config{
		Scheduler:  scheduler,
		Dispatcher: dispatcher,
		Builders: []QueryRuntimeModuleBuilder{versionedQueryRuntimeModuleBuilder{
			prepare: func(context.Context, qviews.DataVersion) error {
				if attempts.Add(1) == 1 {
					return merr.WrapErrServiceUnavailableMsg("BM25 stats are not ready")
				}
				return nil
			},
		}},
	})

	ready := make(chan struct{})
	meta, key := testManagerQueryViewMetaAndKey(1)
	manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta, OnReady: func() { close(ready) }}, testManagerViewBuilder)
	require.Eventually(t, func() bool {
		select {
		case <-ready:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.GreaterOrEqual(t, attempts.Load(), int32(2))
}

func TestManagerReportsPermanentDataVersionPreparationFailure(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()

	var attempts atomic.Int32
	manager := NewManager(Config{
		Scheduler:  scheduler,
		Dispatcher: dispatcher,
		Builders: []QueryRuntimeModuleBuilder{versionedQueryRuntimeModuleBuilder{
			prepare: func(context.Context, qviews.DataVersion) error {
				attempts.Add(1)
				return merr.WrapErrSerializationFailed(io.ErrUnexpectedEOF, "corrupt BM25 stats")
			},
		}},
	})
	defer manager.Close()

	ready := make(chan struct{})
	unrecoverable := make(chan struct{})
	meta, key := testManagerQueryViewMetaAndKey(1)
	manager.AcquireLocked(snview.AcquireResource{
		Key:             key,
		Meta:            meta,
		OnReady:         func() { close(ready) },
		OnUnrecoverable: func() { close(unrecoverable) },
	}, testManagerViewBuilder)

	select {
	case <-unrecoverable:
	case <-time.After(time.Second):
		t.Fatal("permanent BM25 preparation failure was not reported")
	}
	select {
	case <-ready:
		t.Fatal("query view became ready with corrupt BM25 stats")
	default:
	}
	require.Equal(t, int32(1), attempts.Load())
}

func TestManagerDoesNotReadyViewDroppedDuringPreparation(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()

	started := make(chan struct{})
	finish := make(chan struct{})
	released := make(chan struct{})
	manager := NewManager(Config{
		Scheduler:  scheduler,
		Dispatcher: dispatcher,
		Builders: []QueryRuntimeModuleBuilder{versionedQueryRuntimeModuleBuilder{
			prepare: func(ctx context.Context, version qviews.DataVersion) error {
				if version.StreamingVersion != 2 {
					return nil
				}
				close(started)
				select {
				case <-finish:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		}},
	})

	ready1 := make(chan struct{})
	meta1, key1 := testManagerQueryViewMetaAndKey(1)
	manager.AcquireLocked(snview.AcquireResource{Key: key1, Meta: meta1, OnReady: func() { close(ready1) }}, testManagerViewBuilder)
	require.Eventually(t, func() bool {
		select {
		case <-ready1:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)

	meta2, key2 := testManagerQueryViewMetaAndKey(2)
	manager.AcquireLocked(snview.AcquireResource{Key: key2, Meta: meta2, OnReady: func() {}}, testManagerViewBuilder)
	<-started
	manager.Release(snview.ReleaseResource{Key: key2, OnDropped: func() { close(released) }})
	close(finish)
	require.Eventually(t, func() bool {
		select {
		case <-released:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func TestManagerKeepsDataVersionWhileAnotherQueryViewReferencesIt(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()

	var releases atomic.Int32
	manager := NewManager(Config{
		Scheduler:  scheduler,
		Dispatcher: dispatcher,
		Builders: []QueryRuntimeModuleBuilder{versionedQueryRuntimeModuleBuilder{
			prepare: func(context.Context, qviews.DataVersion) error { return nil },
			release: func(qviews.DataVersion) { releases.Add(1) },
		}},
	})

	meta1, key1 := testManagerQueryViewMetaAndKey(1)
	ready1 := make(chan struct{})
	manager.AcquireLocked(snview.AcquireResource{Key: key1, Meta: meta1, OnReady: func() { close(ready1) }}, testManagerViewBuilder)
	require.Eventually(t, func() bool {
		select {
		case <-ready1:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)

	key2 := key1
	key2.QueryViewVersion.QueryVersion = 2
	meta2 := proto.Clone(meta1).(*viewpb.QueryViewMeta)
	meta2.Version.QueryVersion = 2
	ready2 := make(chan struct{})
	manager.AcquireLocked(snview.AcquireResource{Key: key2, Meta: meta2, OnReady: func() { close(ready2) }}, testManagerViewBuilder)
	require.Eventually(t, func() bool {
		select {
		case <-ready2:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)

	manager.Release(snview.ReleaseResource{Key: key1})
	require.Equal(t, int32(0), releases.Load())
}

func TestResourceCallbacksRunWhileSchedulerIsFull(t *testing.T) {
	for _, action := range []string{"reject", "release"} {
		t.Run(action, func(t *testing.T) {
			scheduler := nodescheduler.New(1)
			defer scheduler.Close()
			started := make(chan struct{})
			scheduler.Submit(queryResourceTaskFunc(func(ctx context.Context) error {
				close(started)
				<-ctx.Done()
				return nil
			}))
			<-started
			manager := NewManager(Config{Scheduler: scheduler})
			defer manager.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			var callerMu sync.Mutex
			returned, notified := make(chan struct{}), make(chan struct{})
			callback := func() {
				callerMu.Lock()
				defer callerMu.Unlock()
				close(notified)
			}
			go func() {
				// Model a shard calling the resource manager while holding the
				// same lock its completion callback needs to acquire.
				callerMu.Lock()
				defer callerMu.Unlock()
				if action == "reject" {
					manager.Reject(snview.AcquireResource{OnUnrecoverable: callback})
				} else {
					manager.Release(snview.ReleaseResource{OnDropped: callback})
				}
				close(returned)
			}()
			select {
			case <-returned:
			case <-ctx.Done():
				t.Fatal("resource notification reentered the caller's lock")
			}
			select {
			case <-notified:
			case <-ctx.Done():
				t.Fatal("resource notification waited for a scheduler worker")
			}
		})
	}
}

func TestManagerCloseWaitsForRunningBuild(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()

	started := make(chan struct{})
	stopped := make(chan struct{})
	manager := NewManager(Config{
		Scheduler:  scheduler,
		Dispatcher: dispatcher,
		Builders: []QueryRuntimeModuleBuilder{blockingQueryRuntimeModuleBuilder{
			started: started,
			stopped: stopped,
		}},
	})
	version := qviews.QueryViewVersion{
		DataVersion:  qviews.DataVersion{StreamingVersion: 1},
		QueryVersion: 1,
	}
	key := qviews.QueryViewKey{
		ShardID:          qviews.ShardID{ReplicaID: 1, VChannel: "v1"},
		QueryViewVersion: version,
	}
	manager.AcquireLocked(snview.AcquireResource{
		Key: key,
		Meta: &viewpb.QueryViewMeta{
			ReplicaId: 1,
			Vchannel:  "v1",
			Version:   version.IntoProto(),
		},
	}, func(*viewpb.QueryViewMeta) (walview.VChannelWALView, bool) {
		return walview.VChannelWALView{}, true
	})
	<-started

	manager.Close()
	select {
	case <-stopped:
	default:
		t.Fatal("manager close returned before the running build stopped")
	}
}

func TestManagerResolveLoadInfoAppliesLoadInfoAndIndexInfos(t *testing.T) {
	provider := fakeLoadInfoProvider{
		loadInfo: QueryViewLoadInfo{
			PartitionIDs:   []int64{10},
			LoadFields:     loadFields(100),
			RequiredFields: loadFields(100, 101),
			IndexInfos: []*indexpb.IndexInfo{
				{CollectionID: 1, FieldID: 101, IndexName: "sparse_inverted"},
			},
		},
	}
	manager := NewManager(Config{LoadInfoProvider: provider})

	view, err := manager.resolveLoadInfo(context.Background(), walview.VChannelWALView{
		CollectionID:    1,
		LoadInfoVersion: 7,
	})
	require.NoError(t, err)
	require.Equal(t, []int64{10}, view.PartitionIDs)
	require.Equal(t, loadFields(100), view.LoadFields)
	require.Equal(t, loadFields(100, 101), view.RequiredFields)
	require.Len(t, view.IndexInfos, 1)
	require.Equal(t, int64(101), view.IndexInfos[0].GetFieldID())
}

func TestManagerClassifiesLoadInfoPreparationFailure(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		retryable bool
	}{
		{"coord_not_ready", merr.ErrServiceNotReady, true},
		{"collection_not_loaded", merr.WrapErrCollectionNotLoaded(1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheduler := &capturedNodeScheduler{}
			dispatcher := NewDispatcher(1)
			defer dispatcher.Close()
			manager := NewManager(Config{
				Scheduler:        scheduler,
				Dispatcher:       dispatcher,
				LoadInfoProvider: fakeLoadInfoProvider{err: tc.err},
			})
			defer manager.Close()
			meta, key := testManagerQueryViewMetaAndKey(1)
			meta.LoadInfoVersion = 7
			var rejected atomic.Bool
			manager.AcquireLocked(snview.AcquireResource{
				Key: key, Meta: meta, OnUnrecoverable: func() { rejected.Store(true) },
			}, func(meta *viewpb.QueryViewMeta) (walview.VChannelWALView, bool) {
				return walview.VChannelWALView{
					CollectionID:    1,
					LoadInfoVersion: meta.GetLoadInfoVersion(),
				}, true
			})

			runtime := manager.runtime
			err := scheduler.task.Execute(context.Background())
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, tc.retryable, errors.Is(err, nodescheduler.ErrDelay))
			require.Equal(t, queryRuntimeClosed, runtime.state)
			require.Nil(t, manager.runtime)
			if tc.retryable {
				require.NotNil(t, manager.task)
				require.False(t, rejected.Load())
			} else {
				require.Nil(t, manager.task)
				require.Eventually(t, rejected.Load, time.Second, time.Millisecond)
			}
		})
	}
}

func loadFields(fieldIDs ...int64) []*viewpb.LoadFieldConfig {
	fields := make([]*viewpb.LoadFieldConfig, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		fields = append(fields, &viewpb.LoadFieldConfig{FieldId: fieldID})
	}
	return fields
}

type fakeLoadInfoProvider struct {
	loadInfo QueryViewLoadInfo
	err      error
}

type capturedNodeScheduler struct {
	task nodescheduler.Task
}

func (s *capturedNodeScheduler) Submit(task nodescheduler.Task, opts ...nodescheduler.Option) nodescheduler.TaskHandle {
	s.task = task
	return noopTaskHandle{}
}

type noopTaskHandle struct{}

func (noopTaskHandle) Cancel() {}

func (noopTaskHandle) Wait(context.Context) error { return nil }

func (p fakeLoadInfoProvider) QueryViewLoadInfo(context.Context, int64, uint64) (QueryViewLoadInfo, error) {
	return p.loadInfo, p.err
}

type blockingQueryRuntimeModuleBuilder struct {
	started chan struct{}
	stopped chan struct{}
}

func (b blockingQueryRuntimeModuleBuilder) NewRuntime() (QueryRuntimeModule, error) {
	return &blockingQueryRuntimeModule{started: b.started, stopped: b.stopped}, nil
}

type blockingQueryRuntimeModule struct {
	started chan struct{}
	stopped chan struct{}
}

func (m *blockingQueryRuntimeModule) Prepare(ctx context.Context, _ walview.VChannelWALView) error {
	close(m.started)
	<-ctx.Done()
	close(m.stopped)
	return ctx.Err()
}

func (*blockingQueryRuntimeModule) ApplyLiveEvent(context.Context, walview.VChannelResourceEvent) error {
	return nil
}

func (*blockingQueryRuntimeModule) Advance(qviews.DataVersion) {}

func (*blockingQueryRuntimeModule) Close() {}

type gatedQueryRuntimeModuleBuilder struct {
	started chan struct{}
	release chan struct{}
}

func (b gatedQueryRuntimeModuleBuilder) NewRuntime() (QueryRuntimeModule, error) {
	return &gatedQueryRuntimeModule{started: b.started, release: b.release}, nil
}

type gatedQueryRuntimeModule struct {
	started chan struct{}
	release chan struct{}
}

func (m *gatedQueryRuntimeModule) Prepare(ctx context.Context, _ walview.VChannelWALView) error {
	close(m.started)
	select {
	case <-m.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*gatedQueryRuntimeModule) ApplyLiveEvent(context.Context, walview.VChannelResourceEvent) error {
	return nil
}

func (*gatedQueryRuntimeModule) Advance(qviews.DataVersion) {}

func (*gatedQueryRuntimeModule) Close() {}

type versionedQueryRuntimeModuleBuilder struct {
	prepare func(context.Context, qviews.DataVersion) error
	release func(qviews.DataVersion)
}

func (b versionedQueryRuntimeModuleBuilder) NewRuntime() (QueryRuntimeModule, error) {
	return &versionedQueryRuntimeModule{prepare: b.prepare, release: b.release}, nil
}

type versionedQueryRuntimeModule struct {
	prepare func(context.Context, qviews.DataVersion) error
	release func(qviews.DataVersion)
}

func (*versionedQueryRuntimeModule) Prepare(context.Context, walview.VChannelWALView) error {
	return nil
}

func (*versionedQueryRuntimeModule) ApplyLiveEvent(context.Context, walview.VChannelResourceEvent) error {
	return nil
}

func (*versionedQueryRuntimeModule) Advance(qviews.DataVersion) {}

func (m *versionedQueryRuntimeModule) BindView(meta *viewpb.QueryViewMeta) QueryViewResource {
	return &versionedQueryViewResource{module: m, meta: meta}
}

type versionedQueryViewResource struct {
	module *versionedQueryRuntimeModule
	meta   *viewpb.QueryViewMeta
}

func (v *versionedQueryViewResource) Prepare(ctx context.Context, _ LoadInfoProvider) error {
	return v.module.PrepareSchemaDataVersion(ctx, v.meta)
}
func (*versionedQueryViewResource) Release() {}

func (m *versionedQueryRuntimeModule) PrepareSchemaDataVersion(ctx context.Context, meta *viewpb.QueryViewMeta) error {
	return m.prepare(ctx, qviews.FromProtoDataVersion(meta.GetVersion().GetDataVersion()))
}

func (m *versionedQueryRuntimeModule) ReleaseDataVersion(version qviews.DataVersion) {
	if m.release != nil {
		m.release(version)
	}
}

func (*versionedQueryRuntimeModule) Close() {}

func testManagerQueryViewMetaAndKey(streamingVersion int64) (*viewpb.QueryViewMeta, qviews.QueryViewKey) {
	version := qviews.QueryViewVersion{
		DataVersion:  qviews.DataVersion{StreamingVersion: streamingVersion},
		QueryVersion: 1,
	}
	meta := &viewpb.QueryViewMeta{
		ReplicaId: 1,
		Vchannel:  "v1",
		Version:   version.IntoProto(),
	}
	return meta, qviews.QueryViewKey{
		ShardID:          qviews.ShardID{ReplicaID: 1, VChannel: "v1"},
		QueryViewVersion: version,
	}
}

func testManagerViewBuilder(*viewpb.QueryViewMeta) (walview.VChannelWALView, bool) {
	return walview.VChannelWALView{}, true
}

func TestManagerRejectsTruncatedBootstrapWithoutPanicking(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()
	manager := NewManager(Config{Scheduler: scheduler, Dispatcher: dispatcher})
	defer manager.Close()
	patch := mockey.Mock((*QueryRuntime).Initialize).Return(wal.ErrTransformLogStartPointTruncated).Build()
	defer patch.UnPatch()
	meta, key := testManagerQueryViewMetaAndKey(1)
	rejected := make(chan struct{})
	manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta, OnUnrecoverable: func() { close(rejected) }}, testManagerViewBuilder)
	select {
	case <-rejected:
	case <-time.After(time.Second):
		t.Fatal("unrecoverable view was not reported")
	}
	_, ok := manager.QueryRuntime(key)
	require.False(t, ok)
}

func TestManagerDropWaitsForSharedResourceTransition(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	runtime := NewQueryRuntime()
	meta1, key1 := testManagerQueryViewMetaAndKey(1)
	meta2, key2 := testManagerQueryViewMetaAndKey(2)
	manager := NewManager(Config{Scheduler: scheduler})
	manager.runtime = runtime
	manager.refs = map[qviews.QueryViewKey]queryViewRef{key1: {meta: meta1}, key2: {meta: meta2}}
	defer manager.Close()
	var canDrop, attempted, dropped atomic.Bool
	patch := mockey.Mock((*QueryRuntime).BeforeRelease).To(func(_ *QueryRuntime, _ context.Context, version qviews.DataVersion) error {
		require.Equal(t, key2.QueryViewVersion.DataVersion, version)
		attempted.Store(true)
		if !canDrop.Load() {
			return nodescheduler.ErrDelay
		}
		return nil
	}).Build()
	defer patch.UnPatch()
	manager.Release(snview.ReleaseResource{Key: key1, OnDropped: func() { dropped.Store(true) }})
	require.Eventually(t, attempted.Load, time.Second, time.Millisecond)
	require.False(t, dropped.Load())
	canDrop.Store(true)
	require.Eventually(t, dropped.Load, time.Second, time.Millisecond)
	_, ok := manager.QueryRuntime(key2)
	require.True(t, ok)
}

func TestManagerReleaseWaitsForInitialPreparation(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()
	started, canceled, finish := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(finish) })
	defer unblock()
	var closes atomic.Int32
	prepare := mockey.Mock((*growingruntime.Runtime).Prepare).To(func(_ *growingruntime.Runtime, ctx context.Context, _ walview.VChannelWALView) error {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-finish // Simulates a C call that cannot be interrupted by cancellation.
		return ctx.Err()
	}).Build()
	defer prepare.UnPatch()
	closeModule := mockey.Mock((*growingruntime.Runtime).Close).To(func(*growingruntime.Runtime) {
		closes.Add(1)
	}).Build()
	defer closeModule.UnPatch()
	manager := NewManager(Config{
		Scheduler: scheduler, Dispatcher: dispatcher,
		Builders: []QueryRuntimeModuleBuilder{NewGrowingRuntimeModuleBuilder(nil)},
	})
	meta, key := testManagerQueryViewMetaAndKey(1)
	var ready atomic.Int32
	manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta, OnReady: func() { ready.Add(1) }}, testManagerViewBuilder)
	<-started
	dropped := make(chan struct{})
	manager.Release(snview.ReleaseResource{Key: key, OnDropped: func() {
		assert.Equal(t, int32(1), closes.Load())
		close(dropped)
	}})
	<-canceled
	require.Zero(t, closes.Load(), "cancellation must not release resources still used by Prepare")
	closed := make(chan struct{})
	go func() { manager.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close did not wait for a detached build")
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not drain the detached build")
	}
	select {
	case <-dropped:
	case <-time.After(time.Second):
		t.Fatal("Release did not report Dropped")
	}
	require.Equal(t, int32(1), closes.Load())
	require.Zero(t, ready.Load())
}

func TestManagerReleaseCancelsQueuedBuild(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()
	started, finish := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(finish) })
	defer unblock()
	scheduler.Submit(queryResourceTaskFunc(func(context.Context) error {
		close(started)
		<-finish
		return nil
	}))
	<-started
	var prepares, closes atomic.Int32
	prepare := mockey.Mock((*growingruntime.Runtime).Prepare).To(func(*growingruntime.Runtime, context.Context, walview.VChannelWALView) error {
		prepares.Add(1)
		return nil
	}).Build()
	defer prepare.UnPatch()
	closeModule := mockey.Mock((*growingruntime.Runtime).Close).To(func(*growingruntime.Runtime) { closes.Add(1) }).Build()
	defer closeModule.UnPatch()
	manager := NewManager(Config{
		Scheduler: scheduler, Dispatcher: dispatcher,
		Builders: []QueryRuntimeModuleBuilder{NewGrowingRuntimeModuleBuilder(nil)},
	})
	meta, key := testManagerQueryViewMetaAndKey(1)
	manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta}, testManagerViewBuilder)
	dropped := make(chan struct{})
	manager.Release(snview.ReleaseResource{Key: key, OnDropped: func() { close(dropped) }})
	// A new view can build immediately, without inheriting the canceled task.
	meta2, key2 := testManagerQueryViewMetaAndKey(2)
	ready := make(chan struct{})
	manager.AcquireLocked(snview.AcquireResource{Key: key2, Meta: meta2, OnReady: func() { close(ready) }}, testManagerViewBuilder)
	unblock()
	select {
	case <-dropped:
	case <-time.After(time.Second):
		t.Fatal("canceling a queued build must not wait for Execute")
	}
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("new view inherited the canceled build")
	}
	_, ok := manager.QueryRuntime(key2)
	require.True(t, ok)
	require.Equal(t, int32(1), prepares.Load(), "only the new build should execute")
	require.Equal(t, int32(1), closes.Load(), "old cleanup must not close the new runtime")
	manager.Close()
	require.Equal(t, int32(2), closes.Load())
}

// Component fixtures register references directly; production binds them during
// AcquireLocked or successful runtime initialization.
func (m *Manager) prepareReady(ctx context.Context, key qviews.QueryViewKey, onReady func()) error {
	m.mu.Lock()
	ref, runtime := m.refs[key], m.runtime
	if runtime != nil && ref.resources == nil {
		ref.resources = runtime.bindView(ref.meta)
		m.refs[key] = ref
	}
	m.mu.Unlock()
	return m.prepareViewRef(ctx, key, ref, runtime, onReady)
}

type gatedMutationBuilder struct {
	release chan struct{}
	entered chan struct{}
}

func (b gatedMutationBuilder) NewRuntime() (QueryRuntimeModule, error) {
	return &gatedMutationModule{builder: b}, nil
}

type gatedMutationModule struct {
	recordingModule
	builder gatedMutationBuilder
}

func (m *gatedMutationModule) ApplyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent) error {
	if event.Message != nil {
		close(m.builder.entered)
		<-m.builder.release
	}
	return nil
}

func TestLiveApplyFailureRetiresOnlyFailedGeneration(t *testing.T) {
	scheduler := nodescheduler.New(2)
	defer scheduler.Close()
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()
	builder := gatedMutationBuilder{make(chan struct{}), make(chan struct{})}
	manager := NewManager(Config{Scheduler: scheduler, Dispatcher: dispatcher, Builders: []QueryRuntimeModuleBuilder{builder, NewGrowingRuntimeModuleBuilder(nil)}})
	defer manager.Close()
	wait := func(c chan struct{}) {
		t.Helper()
		select {
		case <-c:
		case <-time.After(3 * time.Second):
			t.Fatal("missing lifecycle callback")
		}
	}
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	require.NoError(t, initcore.InitLocalChunkManager(filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole)))
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	require.NoError(t, initcore.InitTieredStorage(paramtable.Get()))
	schema := mock_segcore.GenTestCollectionSchema("runtime-replacement", schemapb.DataType_Int64, false)
	collection, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{CollectionID: 1, Schema: schema})
	require.NoError(t, err)
	defer collection.Release()
	insert, err := mock_segcore.GenInsertMsg(collection, 10, 100, 1)
	require.NoError(t, err)
	var k, field int64
	for _, f := range schema.Fields {
		if f.GetIsPrimaryKey() {
			for _, data := range insert.FieldsData {
				if data.FieldId == f.FieldID {
					k = data.GetScalars().GetLongData().Data[0]
				}
			}
		}
		if f.DataType == schemapb.DataType_Int32 {
			field = f.FieldID
		}
	}
	insert.ShardName = "v1"
	insert.CollectionID, insert.PartitionID, insert.SegmentID = 1, 10, 100
	ins := message.NewInsertMessageBuilderV1().WithVChannel("v1").WithHeader(&message.InsertMessageHeader{CollectionId: 1, Partitions: []*messagespb.PartitionSegmentAssignment{{PartitionId: 10, Rows: 1, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 100}}}}).WithBody(insert.InsertRequest).MustBuildMutable().WithTimeTick(10).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(10))
	updateBody := func(value int32) *messagespb.UpdateMessageBody {
		return &messagespb.UpdateMessageBody{PartitionId: 10, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{k}}}}, Assignments: []*messagespb.UpdateAssignment{{FieldId: field, Literal: &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ScalarValue{ScalarValue: &schemapb.ValueField{Data: &schemapb.ValueField_IntData{IntData: value}}}}}}}
	}
	update := message.NewUpdateMessageBuilderV2().WithVChannel("v1").WithHeader(&messagespb.UpdateMessageHeader{CollectionId: 1}).WithBody(updateBody(216)).MustBuildMutable().WithTimeTick(40).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(40))
	summary := walsummary.NewManager(walsummary.ManagerConfig{})
	summary.ObserveMessage(t.Context(), ins)
	stream := walsummary.NewStream(summary)
	defer stream.Close()
	var through atomic.Uint64
	var captures atomic.Int32
	capture := func(*viewpb.QueryViewMeta) (walview.VChannelWALView, bool) {
		captures.Add(1)
		return walview.VChannelWALView{CollectionID: 1, VChannel: "v1", Schema: schema, BaseGrowingTimeTick: max(10, through.Load()), BaseTransformTimeTick: through.Load(), TransformLogStream: stream, SegmentSnapshot: walview.VisibleSegmentSnapshot{Segments: []walview.VisibleSegment{{SegmentID: 100, PartitionID: 10, Schema: schema, Data: walview.SegmentSnapshotData{InsertMessages: []message.ImmutableMessage{ins}}}}}}, true
	}
	acquire := func(version int64, floor uint64) (qviews.QueryViewKey, chan struct{}) {
		meta, key := testManagerQueryViewMetaAndKey(version)
		meta.ReadAdmissionFloor = &viewpb.QueryPlanMVCC{GrowingTimetick: floor}
		ready, failed := make(chan struct{}), make(chan struct{})
		manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta, OnReady: func() { close(ready) }, OnRuntimeFailed: func() { close(failed) }}, capture)
		wait(ready)
		return key, failed
	}
	first, failed1 := acquire(1, 0)
	second, failed2 := acquire(2, 0)
	old, ok := manager.QueryRuntime(first)
	require.True(t, ok)
	handles, err := old.AcquireGrowingSegmentHandles(t.Context(), first.QueryViewVersion.DataVersion, 0, nil)
	require.NoError(t, err)
	require.Len(t, handles, 1)
	defer handles[0].Release()
	// Corrupt only the old resident store: the retained WAL contains the valid
	// value. The real native applier will reject the conflicting replay.
	require.NoError(t, handles[0].Segment().ApplyUpdates(t.Context(), 40, 0, updateBody(999)))
	summary.ObserveMessage(t.Context(), update)
	through.Store(40)
	manager.ObserveEvent(t.Context(), walview.VChannelResourceEvent{Message: update})
	wait(builder.entered)
	var ran atomic.Bool
	manager.ObserveEvent(t.Context(), walview.VChannelResourceEvent{Barrier: func() { ran.Store(true) }})
	close(builder.release)
	wait(failed1)
	wait(failed2)
	require.False(t, ran.Load())
	_, ok = manager.QueryRuntime(first)
	require.False(t, ok)
	third, failed3 := acquire(3, 40)
	current, ok := manager.QueryRuntime(third)
	require.True(t, ok)
	require.NotSame(t, old, current)
	require.EqualValues(t, 2, captures.Load())
	restored, err := current.AcquireGrowingSegmentHandles(t.Context(), third.QueryViewVersion.DataVersion, 40, nil)
	require.NoError(t, err)
	require.Len(t, restored, 1)
	defer restored[0].Release()
	encoded, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Limit: 1}}, OutputFieldIds: []int64{field}})
	require.NoError(t, err)
	plan, err := segcore.NewRetrievePlan(restored[0].Collection(), encoded, 40, 100, 0, 0, 0)
	require.NoError(t, err)
	defer plan.Delete()
	result, err := restored[0].Segment().RetrieveByOffsets(t.Context(), &segcore.RetrievePlanWithOffsets{RetrievePlan: plan, Offsets: []int64{0}})
	require.NoError(t, err)
	defer result.Release()
	values, err := result.GetResult()
	require.NoError(t, err)
	var actual []int32
	for _, data := range values.GetFieldsData() {
		if data.FieldId == field {
			actual = data.GetScalars().GetIntData().GetData()
		}
	}
	require.Equal(t, []int32{216}, actual)
	restored[0].Release()
	handles[0].Release()
	// The replacement has now excluded G < 40. A late View on this same
	// DataVersion must fail preparation while the serving View stays usable.
	meta, late := testManagerQueryViewMetaAndKey(3)
	late.QueryViewVersion.QueryVersion++
	meta.Version = late.QueryViewVersion.IntoProto()
	meta.ReadAdmissionFloor = &viewpb.QueryPlanMVCC{GrowingTimetick: 39}
	prepared := make(chan bool, 1)
	manager.AcquireLocked(snview.AcquireResource{
		Key: late, Meta: meta,
		OnReady: func() { prepared <- true }, OnUnrecoverable: func() { prepared <- false },
	}, capture)
	select {
	case ready := <-prepared:
		require.False(t, ready)
	case <-time.After(3 * time.Second):
		t.Fatal("late View did not finish preparation")
	}
	healthy, err := current.AcquireGrowingSegmentHandles(t.Context(), third.QueryViewVersion.DataVersion, 40, nil)
	require.NoError(t, err)
	require.Len(t, healthy, 1)
	healthy[0].Release()
	manager.runtimeFailed(old)
	select {
	case <-failed3:
		t.Fatal("obsolete runtime failed replacement")
	default:
	}
	for _, key := range []qviews.QueryViewKey{first, second, third, late} {
		dropped := make(chan struct{})
		manager.Release(snview.ReleaseResource{Key: key, OnDropped: func() { close(dropped) }})
		wait(dropped)
	}
}
