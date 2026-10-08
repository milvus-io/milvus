package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	mixclient "github.com/milvus-io/milvus/internal/distributed/mixcoord/client"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Exercise the real public Proxy -> QueryCoord status path. Only transport,
// metadata lookup and state snapshots are mocked, using runtime patches.
func TestQueryViewLoadProgressPublicAPIs(t *testing.T) {
	paramtable.Init()
	previousCache := meta.GlobalFailedLoadCache
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()
	t.Cleanup(func() { meta.GlobalFailedLoadCache = previousCache })
	ctx := context.Background()
	cfg := &loadmgr.LoadConfig{CollectionID: 1, PartitionIDs: []int64{2}, LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 100}}, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}}}
	version := uint64(2)
	stats := map[qviews.ShardID]*coordview.ShardStats{}
	var duringSnapshot func()
	var describeErr error
	patches := []*mockey.Mocker{
		mockey.Mock((*loadmgr.LoadConfigStore).Contains).To(func(_ *loadmgr.LoadConfigStore, _ int64) bool { return cfg != nil }).Build(),
		mockey.Mock((*loadmgr.LoadConfigStore).GetConfigWithVersion).To(func(_ *loadmgr.LoadConfigStore, _ int64) (*loadmgr.LoadConfig, uint64) { return cfg, version }).Build(),
		mockey.Mock((*loadmgr.LoadConfigStore).Snapshot).To(func(_ *loadmgr.LoadConfigStore) *loadmgr.LoadConfigSnapshot {
			return loadmgr.NewLoadConfigSnapshotWithVersions(version, map[int64]*loadmgr.LoadConfig{1: cfg}, map[int64]uint64{1: version})
		}).Build(),
		mockey.Mock((*meta.CoordinatorBroker).DescribeCollection).To(func(_ *meta.CoordinatorBroker, _ context.Context, _ int64) (*milvuspb.DescribeCollectionResponse, error) {
			return &milvuspb.DescribeCollectionResponse{VirtualChannelNames: []string{"v0", "v1"}}, describeErr
		}).Build(),
		mockey.Mock((*coordview.ShardViewRegistry).SnapshotForShards).To(func(_ *coordview.ShardViewRegistry, _ []qviews.ShardID) *coordview.ShardViewSnapshot {
			snapshot := coordview.NewShardViewSnapshot(1, stats)
			if duringSnapshot != nil {
				duringSnapshot()
			}
			return snapshot
		}).Build(),
		mockey.Mock((*proxy.MetaCache).GetCollectionID).Return(int64(1), nil).Build(),
		mockey.Mock((*proxy.MetaCache).GetPartitionID).Return(int64(2), nil).Build(),
		mockey.Mock((*proxy.Proxy).GetMetaCache).Return(&proxy.MetaCache{}).Build(),
	}
	for _, patch := range patches {
		t.Cleanup(func() { patch.UnPatch() })
	}
	s := &Server{broker: &meta.CoordinatorBroker{}, qviewsRuntime: &qviewsRuntime{loadConfigStore: &loadmgr.LoadConfigStore{}, shardViewRegistry: &coordview.ShardViewRegistry{}}}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	for _, patch := range []*mockey.Mocker{
		mockey.Mock((*mixclient.Client).ShowLoadCollections).To(func(_ *mixclient.Client, ctx context.Context, req *querypb.ShowCollectionsRequest, _ ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
			return s.ShowLoadCollections(ctx, req)
		}).Build(),
		mockey.Mock((*mixclient.Client).ShowLoadPartitions).To(func(_ *mixclient.Client, ctx context.Context, req *querypb.ShowPartitionsRequest, _ ...grpc.CallOption) (*querypb.ShowPartitionsResponse, error) {
			return s.ShowLoadPartitions(ctx, req)
		}).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
	p := &proxy.Proxy{}
	p.SetMixCoordClient(&mixclient.Client{})
	p.UpdateStateCode(commonpb.StateCode_Healthy)
	check := func(want int64) {
		for _, partitions := range [][]string{nil, {"p1"}} {
			progress, err := p.GetLoadingProgress(ctx, &milvuspb.GetLoadingProgressRequest{CollectionName: "collection", PartitionNames: partitions})
			require.NoError(t, err)
			require.NoError(t, merr.Error(progress.GetStatus()))
			require.Equal(t, want, progress.GetProgress())
			state, err := p.GetLoadState(ctx, &milvuspb.GetLoadStateRequest{CollectionName: "collection", PartitionNames: partitions})
			require.NoError(t, err)
			require.NoError(t, merr.Error(state.GetStatus()))
			expected := commonpb.LoadState_LoadStateLoading
			if want == 100 {
				expected = commonpb.LoadState_LoadStateLoaded
			}
			require.Equal(t, expected, state.GetState())
		}
		response, err := s.ShowLoadCollections(ctx, &querypb.ShowCollectionsRequest{CollectionIDs: []int64{1}})
		require.NoError(t, err)
		require.Equal(t, []bool{want == 100}, response.GetQueryServiceAvailable())
	}
	up := func(replica int64, channel string) {
		stats[qviews.ShardID{ReplicaID: replica, VChannel: channel}] = &coordview.ShardStats{UpVersion: &qviews.QueryViewVersion{}, UpLoadInfoVersion: version}
	}
	up(10, "v0")
	check(50)
	up(10, "v1")
	check(100)
	cfg = cfg.Clone()
	cfg.Replicas = append(cfg.Replicas, &loadmgr.ReplicaAssignment{ReplicaID: 11})
	version++
	check(0)
	up(10, "v0")
	up(10, "v1")
	check(50)
	up(11, "v0")
	check(75)
	up(11, "v1")
	check(100)
	cfg = cfg.Clone()
	cfg.LoadFields = append(cfg.LoadFields, &messagespb.LoadFieldConfig{FieldId: 101})
	version++
	check(0)
	for _, r := range cfg.Replicas {
		for _, c := range []string{"v0", "v1"} {
			up(r.ReplicaID, c)
		}
	}
	check(100)
	duringSnapshot = func() { version++ }
	check(0)
	duringSnapshot = nil
	// Release and release/reload may occur after reading the shard snapshot.
	// Exercise both public routes; an old complete view must never escape as Loaded.
	saved := cfg.Clone()
	for _, partitions := range [][]string{nil, {"p1"}} {
		for _, reload := range []bool{false, true} {
			cfg = saved.Clone()
			version++
			for _, replica := range cfg.Replicas {
				for _, channel := range []string{"v0", "v1"} {
					up(replica.ReplicaID, channel)
				}
			}
			duringSnapshot = func() {
				cfg = nil
				version++
				if reload {
					cfg = saved.Clone()
					version++
				}
			}
			state, err := p.GetLoadState(ctx, &milvuspb.GetLoadStateRequest{CollectionName: "collection", PartitionNames: partitions})
			require.NoError(t, err)
			require.NoError(t, merr.Error(state.GetStatus()))
			expected := commonpb.LoadState_LoadStateNotLoad
			if reload {
				expected = commonpb.LoadState_LoadStateLoading
			}
			require.Equal(t, expected, state.GetState())
			duringSnapshot = nil
		}
	}

	describeErr = merr.WrapErrServiceUnavailableMsg("metadata unavailable")
	response, err := s.ShowLoadCollections(ctx, &querypb.ShowCollectionsRequest{CollectionIDs: []int64{1}})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(response.GetStatus()), merr.ErrServiceUnavailable)
	response2, err := s.ShowLoadPartitions(ctx, &querypb.ShowPartitionsRequest{CollectionID: 1})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(response2.GetStatus()), merr.ErrServiceUnavailable)
}
