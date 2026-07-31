package queryresource

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
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

func TestManagerReleasesDataVersionPreparedAfterViewWasDropped(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()

	started := make(chan struct{})
	finish := make(chan struct{})
	released := make(chan struct{})
	var prepared atomic.Bool
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
					prepared.Store(true)
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
			release: func(version qviews.DataVersion) {
				if version.StreamingVersion == 2 && prepared.Load() {
					select {
					case <-released:
					default:
						close(released)
					}
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
	manager.Release(snview.ReleaseResource{Key: key2})
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

func TestManagerReleaseQueuesDroppedCallbackInNodeScheduler(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	manager := NewManager(Config{Scheduler: scheduler})

	started := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	blocker := scheduler.Submit(queryResourceTaskFunc(func(context.Context) error {
		close(started)
		<-release
		return nil
	}))
	<-started

	dropped := make(chan struct{})
	manager.Release(snview.ReleaseResource{
		Key:       qviews.QueryViewKey{},
		OnDropped: func() { close(dropped) },
	})
	select {
	case <-dropped:
		t.Fatal("drop callback bypassed node scheduler")
	case <-time.After(20 * time.Millisecond):
	}

	close(release)
	require.NoError(t, blocker.Wait(context.Background()))
	require.Eventually(t, func() bool {
		select {
		case <-dropped:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
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
			PartitionIDs: []int64{10},
			LoadFields:   loadFields(100, 101),
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
	require.Equal(t, loadFields(100, 101), view.LoadFields)
	require.Len(t, view.IndexInfos, 1)
	require.Equal(t, int64(101), view.IndexInfos[0].GetFieldID())
}

func TestManagerResolveLoadInfoFailureDelaysBuild(t *testing.T) {
	scheduler := &capturedNodeScheduler{}
	manager := NewManager(Config{
		Scheduler: scheduler,
		LoadInfoProvider: fakeLoadInfoProvider{
			err: merr.WrapErrCollectionNotLoaded(1),
		},
	})
	meta, key := testManagerQueryViewMetaAndKey(1)
	meta.LoadInfoVersion = 7
	manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta}, func(meta *viewpb.QueryViewMeta) (walview.VChannelWALView, bool) {
		return walview.VChannelWALView{
			CollectionID:    1,
			LoadInfoVersion: meta.GetLoadInfoVersion(),
		}, true
	})

	require.NotNil(t, scheduler.task)
	require.NotPanics(t, func() {
		err := scheduler.task.Execute(context.Background())
		require.Error(t, err)
		require.True(t, errors.Is(err, nodescheduler.ErrDelay))
	})
}

func loadFields(fieldIDs ...int64) []*messagespb.LoadFieldConfig {
	fields := make([]*messagespb.LoadFieldConfig, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		fields = append(fields, &messagespb.LoadFieldConfig{FieldId: fieldID})
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

func (s *capturedNodeScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
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

func (*blockingQueryRuntimeModule) ApplyLiveEvent(context.Context, walview.VChannelResourceEvent) {}

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

func (*gatedQueryRuntimeModule) ApplyLiveEvent(context.Context, walview.VChannelResourceEvent) {}

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

func (*versionedQueryRuntimeModule) ApplyLiveEvent(context.Context, walview.VChannelResourceEvent) {}

func (*versionedQueryRuntimeModule) Advance(qviews.DataVersion) {}

func (m *versionedQueryRuntimeModule) PrepareDataVersion(ctx context.Context, version qviews.DataVersion) error {
	return m.prepare(ctx, version)
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
