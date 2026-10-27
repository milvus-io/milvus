package queryresource

import (
	"context"
	"path"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestPartitionStatsSharedOwnership(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	first := &storage.PartitionStatsSnapshot{Version: 7, SegmentStats: map[int64]storage.SegmentStats{101: {NumRows: 10}}}
	data, err := storage.SerializePartitionStatsSnapshot(first)
	require.NoError(t, err)
	require.NoError(t, cm.Write(t.Context(), path.Join(cm.RootPath(), "part_stats/1/2/p_1v0/7"), data))
	manager := &Manager{}
	req := snviewAcquireStats{1, "p_1v0", []*viewpb.ClusteredPartitionStats{{PartitionId: 2, Version: 7}}}
	a := manager.acquirePartitionStatsLocked(req)
	b := manager.acquirePartitionStatsLocked(req)
	require.Same(t, a[0], b[0])
	require.Equal(t, 2, a[0].refs)
	require.NoError(t, a[0].prepare(t.Context(), cm))
	// A ready shared version performs no further object read.
	require.NoError(t, b[0].prepare(t.Context(), nil))
	old := collectPartitionStatsSnapshots(a)
	require.Equal(t, 10, old[2].SegmentStats[101].NumRows)
	next := manager.acquirePartitionStatsLocked(snviewAcquireStats{1, "p_1v0", []*viewpb.ClusteredPartitionStats{{PartitionId: 2, Version: 8}}})
	require.NotSame(t, a[0], next[0])
	require.Nil(t, next[0].snapshot)
	manager.releasePartitionStatsLocked(a)
	require.NoError(t, b[0].ctx.Err())
	manager.releasePartitionStatsLocked(b)
	require.ErrorIs(t, a[0].ctx.Err(), context.Canceled)
	require.Len(t, manager.partitionStats, 1)
	manager.releasePartitionStatsLocked(next)
	require.Empty(t, manager.partitionStats)
	_, err = cm.Read(t.Context(), path.Join(cm.RootPath(), "part_stats/1/2/p_1v0/7"))
	require.NoError(t, err)
}

func TestPartitionStatsLoadingCancellationAndRetry(t *testing.T) {
	mockey.PatchConvey("last release cancels load without resurrecting it", t, func() {
		cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
		entered, canceled := make(chan struct{}), make(chan struct{})
		mockey.Mock((*storage.LocalChunkManager).Read).To(func(_ *storage.LocalChunkManager, ctx context.Context, _ string) ([]byte, error) {
			close(entered)
			<-ctx.Done()
			close(canceled)
			return nil, ctx.Err()
		}).Build()
		manager := &Manager{}
		req := snviewAcquireStats{1, "p_1v0", []*viewpb.ClusteredPartitionStats{{PartitionId: 2, Version: 7}}}
		refs := manager.acquirePartitionStatsLocked(req)
		done := make(chan error, 1)
		go func() { done <- refs[0].prepare(t.Context(), cm) }()
		<-entered
		require.ErrorIs(t, refs[0].prepare(t.Context(), cm), nodescheduler.ErrDelay)
		manager.releasePartitionStatsLocked(refs)
		<-canceled
		require.ErrorIs(t, <-done, context.Canceled)
		require.Nil(t, refs[0].snapshot)
		replacement := manager.acquirePartitionStatsLocked(req)
		require.NotSame(t, refs[0], replacement[0])
		manager.releasePartitionStatsLocked(replacement)
	})
}

func TestPartitionStatsInvalidFileIsNotReady(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	manager := &Manager{}
	refs := manager.acquirePartitionStatsLocked(snviewAcquireStats{1, "p_1v0", []*viewpb.ClusteredPartitionStats{{PartitionId: 2, Version: 7}}})
	object := path.Join(cm.RootPath(), "part_stats/1/2/p_1v0/7")
	for _, data := range []string{`{`, `{}`} {
		require.NoError(t, cm.Write(t.Context(), object, []byte(data)))
		require.Error(t, refs[0].prepare(t.Context(), cm))
		require.Nil(t, refs[0].snapshot)
	}
	require.NoError(t, cm.Write(t.Context(), object, []byte(`{"Version":0,"segmentStats":{}}`)))
	require.NoError(t, refs[0].prepare(t.Context(), cm))
	require.Equal(t, int64(7), refs[0].snapshot.Version)
	manager.releasePartitionStatsLocked(refs)
}

func TestPartitionStatsBlocksReadinessUntilRetrySucceeds(t *testing.T) {
	mockey.PatchConvey("statistics readiness uses the runtime scheduler retry contract", t, func() {
		cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
		scheduler := nodescheduler.New(1)
		defer scheduler.Close()
		manager := NewManager(Config{ChunkManager: cm, Scheduler: scheduler})
		manager.runtime = NewQueryRuntime()
		defer manager.Close()
		require.NoError(t, manager.runtime.Initialize(t.Context(), walview.VChannelWALView{}))
		meta, key := testManagerQueryViewMetaAndKey(1)
		manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta, PartitionStats: []*viewpb.ClusteredPartitionStats{{PartitionId: 2, Version: 7}}}, nil)
		ready := false
		transient := true
		mockey.Mock((*storage.LocalChunkManager).Read).To(func(_ *storage.LocalChunkManager, _ context.Context, _ string) ([]byte, error) {
			if transient {
				return nil, merr.WrapErrServiceUnavailableMsg("storage temporarily unavailable")
			}
			return []byte(`{"segmentStats":{}}`), nil
		}).Build()
		require.True(t, errors.Is(manager.prepareReady(t.Context(), key, func() { ready = true }), nodescheduler.ErrDelay))
		require.False(t, ready)
		require.Nil(t, manager.refs[key].statsSnapshots)
		transient = false
		require.NoError(t, manager.prepareReady(t.Context(), key, func() { ready = true }))
		require.True(t, ready)
		require.NotNil(t, manager.refs[key].statsSnapshots)
		resources := manager.refs[key].statsResources
		require.Len(t, resources, 1)
		require.Equal(t, int64(7), resources[0].snapshot.Version)
		manager.Close()
		require.Empty(t, manager.partitionStats)
		require.ErrorIs(t, resources[0].ctx.Err(), context.Canceled)
	})
}

// No asynchronous ready callback is installed: these tests drive each preparation
// explicitly while using the real acquisition, release and snapshot resources.
func newPartitionStatsTestManager(t *testing.T) *Manager {
	t.Helper()
	scheduler := nodescheduler.New(1)
	t.Cleanup(scheduler.Close)
	manager := NewManager(Config{ChunkManager: storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir())), Scheduler: scheduler})
	manager.runtime = NewQueryRuntime()
	require.NoError(t, manager.runtime.Initialize(t.Context(), walview.VChannelWALView{}))
	t.Cleanup(manager.Close)
	return manager
}

func TestPartitionStatsViewCacheSharedAndVersioned(t *testing.T) {
	mockey.PatchConvey("queries borrow a prepared immutable mapping", t, func() {
		manager := newPartitionStatsTestManager(t)
		reads := 0
		mockey.Mock((*storage.LocalChunkManager).Read).To(func(_ *storage.LocalChunkManager, _ context.Context, _ string) ([]byte, error) {
			reads++
			return []byte(`{"segmentStats":{}}`), nil
		}).Build()
		mockey.Mock((*viewResources).readRuntime).To(func(r *viewResources) (*ReadRuntime, bool) {
			return &ReadRuntime{QueryRuntime: r.runtime}, true
		}).Build()
		meta1, key1 := testManagerQueryViewMetaAndKey(1)
		meta2, key2 := testManagerQueryViewMetaAndKey(2)
		meta3, key3 := testManagerQueryViewMetaAndKey(3)
		manager.AcquireLocked(snview.AcquireResource{Key: key1, Meta: meta1, PartitionStats: []*viewpb.ClusteredPartitionStats{{PartitionId: 2, Version: 7}}}, nil)
		manager.AcquireLocked(snview.AcquireResource{Key: key2, Meta: meta2, PartitionStats: []*viewpb.ClusteredPartitionStats{{PartitionId: 2, Version: 7}}}, nil)
		manager.AcquireLocked(snview.AcquireResource{Key: key3, Meta: meta3, PartitionStats: []*viewpb.ClusteredPartitionStats{{PartitionId: 2, Version: 8}}}, nil)
		_, ok := manager.ReadRuntime(key1)
		require.False(t, ok, "unpublished statistics cannot serve queries")
		for _, key := range []qviews.QueryViewKey{key1, key2, key3} {
			require.NoError(t, manager.prepareReady(t.Context(), key, func() {
				_, ok := manager.ReadRuntime(key)
				require.True(t, ok, "publish the map before the readiness callback")
			}))
		}
		require.Equal(t, 2, reads, "same object version loads once across Views")
		first, ok := manager.ReadRuntime(key1)
		require.True(t, ok)
		second, ok := manager.ReadRuntime(key2)
		require.True(t, ok)
		third, ok := manager.ReadRuntime(key3)
		require.True(t, ok)
		require.Same(t, first.partitionStats[2], second.partitionStats[2])
		require.NotSame(t, first.partitionStats[2], third.partitionStats[2])
		require.Equal(t, int64(7), first.partitionStats[2].Version)
		require.Equal(t, int64(8), third.partitionStats[2].Version)
		require.NoError(t, manager.prepareReady(t.Context(), key1, func() {}))
		again, ok := manager.ReadRuntime(key1)
		require.True(t, ok)
		require.True(t, reflect.ValueOf(first.partitionStats) == reflect.ValueOf(again.partitionStats), "repeated preparation must reuse the published map")

		// A query must not take the resource mutex or rebuild a snapshot map.
		resource := manager.refs[key1].statsResources[0]
		resource.mu.Lock()
		readDone := make(chan *ReadRuntime, 1)
		go func() { read, _ := manager.ReadRuntime(key1); readDone <- read }()
		func() {
			defer resource.mu.Unlock()
			select {
			case read := <-readDone:
				require.NotNil(t, read)
				require.True(t, reflect.ValueOf(first.partitionStats) == reflect.ValueOf(read.partitionStats))
			case <-time.After(time.Second):
				t.Fatal("query read blocked on the statistics resource mutex")
			}
		}()
		manager.Release(snview.ReleaseResource{Key: key1})
		_, ok = manager.ReadRuntime(key1)
		require.False(t, ok)
		require.NoError(t, resource.ctx.Err(), "second View still owns version 7")
		manager.Release(snview.ReleaseResource{Key: key2})
		require.ErrorIs(t, resource.ctx.Err(), context.Canceled)
		require.Len(t, manager.partitionStats, 1)
		manager.Close()
		require.Empty(t, manager.partitionStats)
		// Teardown removes ownership without mutating the Go-owned snapshots.
		require.Equal(t, int64(7), first.partitionStats[2].Version)
		require.Equal(t, int64(8), third.partitionStats[2].Version)
	})
}

func TestPartitionStatsCachePublicationRejectsReleasedRegistration(t *testing.T) {
	mockey.PatchConvey("a late preparation cannot publish into a replacement View reference", t, func() {
		manager := newPartitionStatsTestManager(t)
		mockey.Mock((*storage.LocalChunkManager).Read).Return([]byte(`{"segmentStats":{}}`), nil).Build()
		meta, key := testManagerQueryViewMetaAndKey(1)
		req := snview.AcquireResource{Key: key, Meta: meta, PartitionStats: []*viewpb.ClusteredPartitionStats{{PartitionId: 2, Version: 7}}}
		manager.AcquireLocked(req, nil)
		// A second reference retains the runtime and the old DataVersion across release.
		anchorMeta := proto.Clone(meta).(*viewpb.QueryViewMeta)
		anchorKey := key
		anchorKey.ShardID.ReplicaID++
		anchorMeta.ReplicaId = anchorKey.ShardID.ReplicaID
		manager.AcquireLocked(snview.AcquireResource{Key: anchorKey, Meta: anchorMeta}, nil)
		entered, resume := make(chan struct{}), make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(resume) })
		patch := mockey.Mock((*QueryRuntime).PrepareIndexMeta).To(func(_ *QueryRuntime, _ context.Context, _ *viewpb.QueryViewMeta, _ LoadInfoProvider) error {
			close(entered)
			<-resume
			return nil
		}).Build()
		done := make(chan error, 1)
		var notified atomic.Bool
		go func() { done <- manager.prepareReady(t.Context(), key, func() { notified.Store(true) }) }()
		<-entered
		oldResource := manager.refs[key].statsResources[0]
		manager.Release(snview.ReleaseResource{Key: key})
		manager.AcquireLocked(req, nil)
		replacement := manager.refs[key]
		require.NotSame(t, oldResource, replacement.statsResources[0])
		once.Do(func() { close(resume) })
		require.NoError(t, <-done)
		require.False(t, notified.Load())
		require.Nil(t, manager.refs[key].statsSnapshots)
		patch.UnPatch()
		require.NoError(t, manager.prepareReady(t.Context(), key, func() { notified.Store(true) }))
		require.True(t, notified.Load())
		require.Same(t, replacement.statsResources[0].snapshot, manager.refs[key].statsSnapshots[2])
	})
}

func TestPartitionStatsEmptyScopeAllocatesNoSnapshotMap(t *testing.T) {
	manager := newPartitionStatsTestManager(t)
	meta, key := testManagerQueryViewMetaAndKey(1)
	manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta}, nil)
	require.NoError(t, manager.prepareReady(t.Context(), key, func() {}))
	require.Nil(t, manager.refs[key].statsSnapshots)
	require.Empty(t, manager.partitionStats)
}

func TestPartitionStatsReleaseCancellationDoesNotFailReadyTask(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		replace    bool
		cancelTask bool
	}{
		{name: "released"},
		{name: "same-key-replaced", replace: true},
		{name: "task-canceled", replace: true, cancelTask: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			mockey.PatchConvey("a canceled obsolete load is not a scheduler failure", t, func() {
				manager := newPartitionStatsTestManager(t)
				meta, key := testManagerQueryViewMetaAndKey(1)
				var unrecoverable atomic.Bool
				req := snview.AcquireResource{
					Key: key, Meta: meta,
					PartitionStats:  []*viewpb.ClusteredPartitionStats{{PartitionId: 2, Version: 7}},
					OnUnrecoverable: func() { unrecoverable.Store(true) },
				}
				manager.AcquireLocked(req, nil)
				// Keep the runtime and DataVersion alive, isolating registration identity
				// from runtime replacement during a same-key reacquisition.
				anchorMeta := proto.Clone(meta).(*viewpb.QueryViewMeta)
				anchorKey := key
				anchorKey.ShardID.ReplicaID++
				anchorMeta.ReplicaId = anchorKey.ShardID.ReplicaID
				manager.AcquireLocked(snview.AcquireResource{Key: anchorKey, Meta: anchorMeta}, nil)
				old := manager.refs[key]
				entered, canceled, resume := make(chan struct{}), make(chan struct{}), make(chan struct{})
				unblock := sync.OnceFunc(func() { close(resume) })
				defer unblock()
				var reads atomic.Int32
				mockey.Mock((*storage.LocalChunkManager).Read).To(func(_ *storage.LocalChunkManager, ctx context.Context, _ string) ([]byte, error) {
					if reads.Add(1) == 1 {
						close(entered)
						<-ctx.Done()
						close(canceled)
						<-resume
						return nil, ctx.Err()
					}
					return []byte(`{"segmentStats":{}}`), nil
				}).Build()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var oldReady atomic.Bool
				done := make(chan error, 1)
				go func() { done <- manager.prepareReady(ctx, key, func() { oldReady.Store(true) }) }()
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("statistics load did not start")
				}
				manager.Release(snview.ReleaseResource{Key: key})
				select {
				case <-canceled:
				case <-time.After(time.Second):
					t.Fatal("last release did not cancel loading")
				}
				if scenario.replace {
					manager.AcquireLocked(req, nil)
					require.NotSame(t, old.meta, manager.refs[key].meta)
					require.NotSame(t, old.statsResources[0], manager.refs[key].statsResources[0])
				}
				if scenario.cancelTask {
					cancel()
				}
				unblock()
				select {
				case err := <-done:
					if scenario.cancelTask {
						require.ErrorIs(t, err, context.Canceled)
					} else {
						require.NoError(t, err)
					}
				case <-time.After(time.Second):
					t.Fatal("canceled readiness task did not finish")
				}
				require.False(t, oldReady.Load())
				require.False(t, unrecoverable.Load())
				require.Nil(t, old.statsResources[0].snapshot)
				if scenario.replace {
					require.Nil(t, manager.refs[key].statsSnapshots, "old load must not publish into replacement")
					ready := false
					require.NoError(t, manager.prepareReady(t.Context(), key, func() { ready = true }))
					require.True(t, ready)
					require.Len(t, manager.partitionStats, 1)
					require.Equal(t, 1, manager.refs[key].statsResources[0].refs)
					require.NotNil(t, manager.refs[key].statsSnapshots[2])
				} else {
					require.NotContains(t, manager.refs, key)
					require.Empty(t, manager.partitionStats)
				}
			})
		})
	}
}

func TestPartitionStatsCurrentRegistrationCancellationRemainsVisible(t *testing.T) {
	mockey.PatchConvey("internal cancellation of a live registration is not swallowed", t, func() {
		manager := newPartitionStatsTestManager(t)
		meta, key := testManagerQueryViewMetaAndKey(1)
		manager.AcquireLocked(snview.AcquireResource{Key: key, Meta: meta, PartitionStats: []*viewpb.ClusteredPartitionStats{{PartitionId: 2, Version: 7}}}, nil)
		mockey.Mock((*storage.LocalChunkManager).Read).Return(nil, context.Canceled).Build()
		ready := false
		err := manager.prepareReady(t.Context(), key, func() { ready = true })
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, ready)
		require.NoError(t, manager.refs[key].statsResources[0].ctx.Err())
		require.Nil(t, manager.refs[key].statsSnapshots)
	})
}
