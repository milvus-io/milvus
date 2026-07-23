package queryresource

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

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
