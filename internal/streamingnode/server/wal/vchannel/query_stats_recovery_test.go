package vchannel

import (
	"context"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestRecoveredViewWaitsForPersistedPartitionStats(t *testing.T) {
	mockey.PatchConvey("recovery must load the persisted statistics version before serving", t, func() {
		paramtable.Init()
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
		const vchannel = "p_1v0"
		persisted := &viewpb.QueryViewOfShard{
			Meta: &viewpb.QueryViewMeta{
				CollectionId: 1,
				Vchannel:     vchannel,
				ReplicaId:    1,
				State:        viewpb.QueryViewState_QueryViewStateUp,
				Version:      &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 10}, QueryVersion: 2},
			},
			StreamingNode: &viewpb.QueryViewOfStreamingNode{PartitionStats: []*viewpb.ClusteredPartitionStats{{PartitionId: 2, Version: 7}}},
		}
		etcdClient, root := kvfactory.GetEtcdAndPath()
		store := etcdkv.NewEtcdKV(etcdClient, path.Join(root, "partition-stats-recovery-"+uuid.NewString()))
		defer func() { require.NoError(t, store.RemoveWithPrefix(context.Background(), "")) }()
		catalog := catalogkv.NewCataLog(store)
		require.NoError(t, catalog.SaveQueryViews(ctx, "p", []*viewpb.QueryViewOfShard{persisted}))
		recovered, err := catalog.ListQueryViews(ctx, "p")
		require.NoError(t, err)
		require.Len(t, recovered, 1)
		require.True(t, proto.Equal(persisted, recovered[0]))

		scheduler := nodescheduler.New(1)
		defer scheduler.Close()
		dispatcher := queryresource.NewDispatcher(1)
		defer dispatcher.Close()
		manager := queryresource.NewManager(queryresource.Config{
			ChunkManager: cm, Scheduler: scheduler, Dispatcher: dispatcher,
			// This test isolates immutable statistics from growing-segment I/O.
			Builders: []queryresource.QueryRuntimeModuleBuilder{nil},
		})
		defer manager.Close()
		mockey.Mock((*PChannelRecoveryManager).Acquire).To(func(_ *PChannelRecoveryManager, req snview.AcquireResource) {
			manager.AcquireLocked(req, func(_ *viewpb.QueryViewMeta) (walview.VChannelWALView, bool) {
				return walview.VChannelWALView{}, true
			})
		}).Build()
		mockey.Mock((*PChannelRecoveryManager).Release).To(func(_ *PChannelRecoveryManager, req snview.ReleaseResource) { manager.Release(req) }).Build()

		require.NoError(t, cm.Write(ctx, path.Join(cm.RootPath(), "part_stats/1/2/p_1v0/7"), []byte(`{"Version":0,"segmentStats":{"101":{"numRows":10}}}`)))
		// A newer object is deliberately present; recovery must never substitute it.
		require.NoError(t, cm.Write(ctx, path.Join(cm.RootPath(), "part_stats/1/2/p_1v0/8"), []byte(`{"segmentStats":{}}`)))
		entered := make(chan string, 1)
		resume := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(resume) })
		defer unblock()
		var read func(*storage.LocalChunkManager, context.Context, string) ([]byte, error)
		mockey.Mock((*storage.LocalChunkManager).Read).Origin(&read).To(func(cm *storage.LocalChunkManager, ctx context.Context, object string) ([]byte, error) {
			entered <- object
			select {
			case <-resume:
				return read(cm, ctx, object)
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).Build()
		h := snview.RecoverPChannelSNQueryViewHandler(ctx, "p", catalog, &PChannelRecoveryManager{}, recovered)
		defer h.CloseForHandoff()
		select {
		case object := <-entered:
			require.Equal(t, path.Join(cm.RootPath(), "part_stats/1/2/p_1v0/7"), object)
		case <-ctx.Done():
			t.Fatal("recovery did not load its persisted statistics")
		}
		shard := qviews.NewShardIDFromQVMeta(persisted.Meta)
		waiting, stopWaiting := context.WithTimeout(ctx, 50*time.Millisecond)
		lease, err := h.AcquireLatestUpView(waiting, shard)
		stopWaiting()
		require.ErrorIs(t, err, context.DeadlineExceeded, "UpRecovering cannot serve before statistics finish loading")
		require.Nil(t, lease)
		unblock()
		lease, err = h.AcquireLatestUpView(ctx, shard)
		require.NoError(t, err)
		defer lease.Release()
		require.Equal(t, qviews.FromProtoQueryViewVersion(persisted.Meta.Version), lease.Version)
		require.True(t, proto.Equal(persisted.StreamingNode, lease.View.StreamingNode))
		select {
		case object := <-entered:
			t.Fatalf("unexpected additional statistics load: %s", object)
		default:
		}
	})
}
