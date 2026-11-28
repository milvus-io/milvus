package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metacache/metacachetest"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	coordMocks "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/types"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type loadInfoIndexSourceForTest struct{ types.MixCoord }

func (*loadInfoIndexSourceForTest) QueryViewCollectionIndexInfos(int64, ...int64) []*indexpb.IndexInfo {
	panic("patch with mockey")
}

func TestQueryViewLoadInfoInitializationAndRecovery(t *testing.T) {
	channels := mockey.Mock(channel.GetClusterChannels).Return(message.ClusterChannels{Channels: []string{"p1", "p2"}}).Build()
	defer channels.UnPatch()
	client, _ := kvfactory.GetEtcdAndPath()
	storage := etcdkv.NewEtcdKV(client, "query-view-load-info-"+uuid.NewString())
	t.Cleanup(func() { require.NoError(t, storage.RemoveWithPrefix(context.Background(), "")) })
	ctx := t.Context()
	snapshot := &viewpb.CollectionLoadConfig{
		CollectionID: 1, Version: 9,
		PartitionIds: []int64{10}, RequiredFields: []*viewpb.LoadFieldConfig{{FieldId: 100, IndexIds: []int64{200}}},
	}
	require.NoError(t, queryview.NewCollectionLoadInfoCatalog(storage).Save(ctx, snapshot))
	indexes := []*indexpb.IndexInfo{{FieldID: 100, IndexID: 200}, {FieldID: 101, IndexID: 201}}
	patch := mockey.Mock((*loadInfoIndexSourceForTest).QueryViewCollectionIndexInfos).To(func(_ *loadInfoIndexSourceForTest, id int64, ids ...int64) []*indexpb.IndexInfo {
		require.EqualValues(t, 1, id)
		require.Equal(t, []int64{200}, ids)
		return indexes
	}).Build()
	defer patch.UnPatch()

	for _, withIndexSource := range []bool{true, false} {
		s := &Server{ctx: ctx, kv: storage, nodeMgr: session.NewNodeManager(), metaView: metacachetest.NewMetaStore()}
		if withIndexSource {
			s.mixCoord = &loadInfoIndexSourceForTest{}
		}
		require.NoError(t, s.initMeta())
		require.Empty(t, s.meta.GetAll(ctx))
		// QueryView runtime owns load recovery; initMeta only restores resource groups.
		deps := newDefaultQViewsRuntimeDependencies(storage, client, nil, s.mixCoord)
		require.Equal(t, []string{"p1", "p2"}, deps.pchannels)
		t.Cleanup(func() {
			deps.queryNodeManager.Close()
			deps.streamingNodeHandler.Close()
			deps.streamingCoordClient.Close()
		})
		configs, err := loadmgr.RecoverLoadConfigStore(ctx, deps.loadConfigDependencies)
		require.NoError(t, err)
		configs.StopPlanning(1) // the durable Release callback replays after recovery
		s.qviewsRuntime = &qviewsRuntime{loadConfigStore: configs}
		s.UpdateStateCode(commonpb.StateCode_Healthy)
		resp, err := s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: 9})
		require.NoError(t, err)
		if withIndexSource {
			require.NoError(t, merr.Error(resp.Status))
			require.EqualValues(t, 9, resp.Version)
			require.Equal(t, []int64{10}, resp.PartitionIDs)
			require.Len(t, resp.IndexInfoList, 1)
			require.EqualValues(t, 200, resp.IndexInfoList[0].IndexID)
		} else {
			require.ErrorIs(t, merr.Error(resp.Status), merr.ErrServiceNotReady)
		}
		resp, err = s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1})
		require.NoError(t, err)
		require.ErrorIs(t, merr.Error(resp.Status), merr.ErrCollectionNotLoaded,
			"recovering historical scopes must not recreate current load state")
	}
}

// The initial channel snapshot must coexist with the runtime's metadata sources.
func TestDefaultQViewsRuntimeSources(t *testing.T) {
	mockey.PatchConvey("initial topology and metadata callbacks", t, func() {
		mockey.Mock(channel.GetClusterChannels).Return(message.ClusterChannels{Channels: []string{"p"}}).Build()
		allocate := mockey.Mock((*coordMocks.MixCoord).AllocID).Return(&rootcoordpb.AllocIDResponse{Status: merr.Success(), ID: 42}, nil).Build()
		describe := mockey.Mock((*meta.CoordinatorBroker).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
			VirtualChannelNames: []string{"p_1v0"},
		}, nil).Build()
		client, _ := kvfactory.GetEtcdAndPath()
		deps := newDefaultQViewsRuntimeDependencies(nil, client, nil, &coordMocks.MixCoord{})
		defer deps.queryNodeManager.Close()
		defer deps.streamingCoordClient.Close()
		defer deps.streamingNodeHandler.Close()
		require.Equal(t, []string{"p"}, deps.pchannels)
		version, err := deps.loadConfigDependencies.AllocateVersion(t.Context())
		require.NoError(t, err)
		require.EqualValues(t, 42, version)
		channels, err := deps.vchannels(t.Context(), 1)
		require.NoError(t, err)
		require.Equal(t, []string{"p_1v0"}, channels)
		allocate.Return(nil, context.Canceled)
		describe.Return(nil, context.Canceled)
		_, err = deps.loadConfigDependencies.AllocateVersion(t.Context())
		require.ErrorIs(t, err, context.Canceled)
		_, err = deps.vchannels(t.Context(), 1)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestReconcileSchemaVersionProvider(t *testing.T) {
	const collectionID int64 = 1
	balances := cache.New(nil)
	t.Cleanup(balances.Close)
	balances.PublishLoadConfig(collectionID, &loadmgr.LoadConfig{CollectionID: collectionID}, 1)
	for _, tc := range []struct {
		name             string
		published        int32
		pending          *schemapb.CollectionSchema
		pendingErr       error
		absent, dropping bool
		wantVersion      int32
		wantErr          error
	}{
		{name: "steady state", published: 3, wantVersion: 3},
		{name: "legacy version zero"},
		{name: "pending target", published: 3, pending: &schemapb.CollectionSchema{Version: 4}, wantVersion: 4},
		{name: "publication overtakes pending read", published: 5, pending: &schemapb.CollectionSchema{Version: 4}, wantVersion: 5},
		{name: "broadcaster recovering", published: 3, pendingErr: merr.ErrServiceNotReady, wantErr: merr.ErrServiceNotReady},
		{name: "missing collection", absent: true, wantErr: merr.ErrCollectionNotFound},
		{name: "dropping collection", dropping: true, wantErr: merr.ErrCollectionNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := metacachetest.NewMetaStore()
			state := etcdpb.CollectionState_CollectionCreated
			if tc.dropping {
				state = etcdpb.CollectionState_CollectionDropping
			}
			if !tc.absent {
				store.PutCollection(&metacache.CollectionInfo{
					ID: collectionID, State: state, Schema: &schemapb.CollectionSchema{Version: tc.published},
				})
			}
			collections := meta.NewCollectionManager(nil, store)
			var steps []string
			patch := mockey.Mock(broadcast.GetPendingSchemaTarget).To(func(id int64) (*broadcaster.SchemaTarget, error) {
				require.Equal(t, collectionID, id)
				steps = append(steps, "pending")
				if tc.pending == nil {
					return nil, tc.pendingErr
				}
				return &broadcaster.SchemaTarget{CollectionID: id, Schema: tc.pending}, nil
			}).Build()
			t.Cleanup(func() { patch.UnPatch() })
			readVersion := reconcileSchemaVersionProvider(balances, func(id int64) (*metacache.CollectionInfo, error) {
				steps = append(steps, "read")
				return collections.GetCollectionInfo(id)
			}, func(id int64, version int32, current func() bool) {
				require.True(t, current())
				require.Equal(t, collectionID, id)
				require.Equal(t, tc.published, version)
				steps = append(steps, "publish")
			})

			version, err := readVersion(t.Context(), collectionID)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantVersion, version)
			if tc.absent || tc.dropping {
				require.Equal(t, []string{"pending", "read"}, steps)
			} else {
				require.Equal(t, []string{"pending", "read", "publish"}, steps)
			}
		})
	}
}

func TestSchemaReadAcrossCollectionReload(t *testing.T) {
	ctx := t.Context()
	registry, err := coordview.RecoverShardViewRegistry(ctx, &fakeQueryViewCatalog{}, nil)
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	balances := cache.New(nil)
	t.Cleanup(balances.Close)
	cfg := &loadmgr.LoadConfig{CollectionID: 1, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}}}
	balances.PublishLoadConfig(1, cfg, 1)
	pending := mockey.Mock(broadcast.GetPendingSchemaTarget).Return(&broadcaster.SchemaTarget{Schema: &schemapb.CollectionSchema{Version: 4}}, nil).Build()
	t.Cleanup(func() { pending.UnPatch() })
	readVersion := reconcileSchemaVersionProvider(balances, func(id int64) (*metacache.CollectionInfo, error) {
		balances.PublishLoadConfig(id, nil, 0)
		require.NoError(t, registry.RequestCollectionRelease(ctx, id))
		balances.PublishLoadConfig(id, cfg, 2)
		return &metacache.CollectionInfo{Schema: &schemapb.CollectionSchema{Version: 2}}, nil
	}, registry.PublishCollectionSchemaIf)
	target, err := readVersion(ctx, 1)
	require.NoError(t, err)
	require.EqualValues(t, 4, target)
	manager := registry.Ensure(qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"})
	require.NoError(t, manager.RequestReplicaRetirement(ctx, 2, func() bool {
		t.Fatal("the previous load's metadata read seeded the new manager")
		return false
	}))
	readVersion = reconcileSchemaVersionProvider(balances, func(int64) (*metacache.CollectionInfo, error) {
		return &metacache.CollectionInfo{Schema: &schemapb.CollectionSchema{Version: 3}}, nil
	}, registry.PublishCollectionSchemaIf)
	_, err = readVersion(ctx, 1)
	require.NoError(t, err)
	current := false
	require.NoError(t, manager.RequestReplicaRetirement(ctx, 3, func() bool { current = true; return false }))
	require.True(t, current)
}
