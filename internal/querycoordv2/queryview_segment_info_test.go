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
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type segmentInfoMixCoord struct{ types.MixCoord }

func (m *segmentInfoMixCoord) GetQueryViewSegmentLoadInfos(ctx context.Context, cid int64, ids []int64) ([]*querypb.SegmentLoadInfo, []*indexpb.IndexInfo, error) {
	return m.MixCoord.GetQueryViewSegmentLoadInfos(ctx, cid, ids)
}

func TestQueryViewSegmentInfo(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cfg := &loadmgr.LoadConfig{CollectionID: 1, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}, {ReplicaID: 20}, {ReplicaID: 30}}}
	version := uint64(1)
	stats := map[qviews.ShardID]*coordview.ShardStats{}
	var onRead func()
	var readErr error
	reads := 0
	patches := []*mockey.Mocker{
		mockey.Mock((*loadmgr.LoadConfigStore).GetConfigWithVersion).To(func(_ *loadmgr.LoadConfigStore, cid int64) (*loadmgr.LoadConfig, uint64) {
			if cid != 1 {
				return nil, 0
			}
			return cfg, version
		}).Build(),
		mockey.Mock((*loadmgr.LoadConfigStore).Snapshot).To(func(_ *loadmgr.LoadConfigStore) *loadmgr.LoadConfigSnapshot {
			return loadmgr.NewLoadConfigSnapshot(version, map[int64]*loadmgr.LoadConfig{1: cfg})
		}).Build(),
		mockey.Mock((*coordview.ShardViewRegistry).SnapshotForCollection).To(func(_ *coordview.ShardViewRegistry, _ int64) *coordview.ShardViewSnapshot {
			return coordview.NewShardViewSnapshot(1, stats)
		}).Build(),
		mockey.Mock((*segmentInfoMixCoord).GetQueryViewSegmentLoadInfos).To(func(_ *segmentInfoMixCoord, _ context.Context, cid int64, ids []int64) ([]*querypb.SegmentLoadInfo, []*indexpb.IndexInfo, error) {
			reads++
			result := make([]*querypb.SegmentLoadInfo, 0, len(ids))
			for _, id := range ids {
				result = append(result, &querypb.SegmentLoadInfo{SegmentID: id, CollectionID: cid, NumOfRows: 100, IsSorted: true, StorageVersion: 3, IndexInfos: []*querypb.FieldIndexInfo{{IndexID: 7, IndexName: "FLAT"}}})
			}
			if onRead != nil {
				onRead()
			}
			return result, nil, readErr
		}).Build(),
	}
	for _, p := range patches {
		t.Cleanup(func() { p.UnPatch() })
	}
	s := &Server{mixCoord: &segmentInfoMixCoord{}, qviewsRuntime: &qviewsRuntime{loadConfigStore: &loadmgr.LoadConfigStore{}, shardViewRegistry: &coordview.ShardViewRegistry{}}}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	up := func(replica int64, channel string, compact, query int64, ids ...int64) {
		segments := map[int64]*coordview.SegmentStats{}
		for _, id := range ids {
			segments[id] = &coordview.SegmentStats{SegmentID: id, PartitionID: 2, Nodes: map[int64]coordview.SegmentState{replica: coordview.SegmentStateUp}}
		}
		stats[qviews.ShardID{ReplicaID: replica, VChannel: channel}] = &coordview.ShardStats{UpVersion: &qviews.QueryViewVersion{DataVersion: qviews.DataVersion{StreamingVersion: 1, CompactVersion: compact}, QueryVersion: query}, Segments: segments}
	}
	get := func(cid int64, ids ...int64) ([]*querypb.SegmentInfo, error) {
		resp, err := s.GetLoadSegmentInfo(ctx, &querypb.GetSegmentInfoRequest{CollectionID: cid, SegmentIDs: ids})
		require.NoError(t, err)
		return resp.GetInfos(), merr.Error(resp.GetStatus())
	}
	up(10, "v0", 1, 100, 101, 102) // Older replica has a much larger QueryVersion.
	up(20, "v0", 2, 1, 103)        // Compacted child replaces both parents.
	up(30, "v0", 2, 4, 103)
	up(10, "v1", 1, 1, 104) // A different shard must not be filtered by v0's version.
	up(40, "v0", 3, 1, 105) // Removed replica is not part of the current config.
	stats[qviews.ShardID{ReplicaID: 20, VChannel: "v0"}].Segments[999] = &coordview.SegmentStats{SegmentID: 999, Nodes: map[int64]coordview.SegmentState{20: coordview.SegmentStateReady, 21: coordview.SegmentStatePreparing}}
	infos, err := get(1)
	require.NoError(t, err)
	require.Len(t, infos, 2)
	require.Equal(t, int64(103), infos[0].GetSegmentID())
	require.Equal(t, []int64{20, 30}, infos[0].GetNodeIds())
	require.Equal(t, []int64{20, 30}, infos[0].GetReplicaIds())
	require.Equal(t, int64(100), infos[0].GetNumRows())
	require.Zero(t, infos[0].GetMemSize())
	require.Equal(t, "FLAT", infos[0].GetIndexName())
	require.Equal(t, int64(3), infos[0].GetStorageVersion())
	require.Equal(t, int64(104), infos[1].GetSegmentID())
	infos, err = get(1, 104)
	require.NoError(t, err)
	require.Len(t, infos, 1)
	infos, err = get(0, 103)
	require.NoError(t, err)
	require.Len(t, infos, 1)
	_, err = get(1, 101)
	require.ErrorIs(t, err, merr.ErrSegmentNotLoaded)
	infos, err = get(2)
	require.NoError(t, err)
	require.Empty(t, infos)
	_, err = get(2, 103)
	require.ErrorIs(t, err, merr.ErrSegmentNotLoaded)

	t.Run("public Proxy API", func(t *testing.T) {
		for _, patch := range []*mockey.Mocker{
			mockey.Mock((*proxy.MetaCache).GetCollectionID).Return(int64(1), nil).Build(),
			mockey.Mock((*proxy.Proxy).GetMetaCache).Return(&proxy.MetaCache{}).Build(),
			mockey.Mock((*mixclient.Client).GetLoadSegmentInfo).To(func(_ *mixclient.Client, ctx context.Context, req *querypb.GetSegmentInfoRequest, _ ...grpc.CallOption) (*querypb.GetSegmentInfoResponse, error) {
				return s.GetLoadSegmentInfo(ctx, req)
			}).Build(),
		} {
			t.Cleanup(func() { patch.UnPatch() })
		}
		p := &proxy.Proxy{}
		p.SetMixCoordClient(&mixclient.Client{})
		p.UpdateStateCode(commonpb.StateCode_Healthy)
		resp, err := p.GetQuerySegmentInfo(ctx, &milvuspb.GetQuerySegmentInfoRequest{CollectionName: "collection"})
		require.NoError(t, err)
		require.NoError(t, merr.Error(resp.GetStatus()))
		require.Len(t, resp.GetInfos(), 2)
		require.Equal(t, int64(103), resp.Infos[0].GetSegmentID())
		require.Equal(t, []int64{20, 30}, resp.Infos[0].GetNodeIds())
		require.Equal(t, int64(100), resp.Infos[0].GetNumRows())
	})
	t.Run("handoff during metadata read", func(t *testing.T) {
		reads = 0
		onRead = func() { up(20, "v0", 3, 1, 106); onRead = nil }
		infos, err := get(1)
		require.NoError(t, err)
		require.Equal(t, 2, reads)
		require.Equal(t, int64(106), infos[1].GetSegmentID())
	})
	t.Run("metadata failure is not empty success", func(t *testing.T) {
		readErr = merr.WrapErrServiceUnavailableMsg("metadata unavailable")
		defer func() { readErr = nil }()
		_, err := get(1)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	})
	t.Run("release during read", func(t *testing.T) {
		saved := cfg
		defer func() { cfg = saved; onRead = nil }()
		onRead = func() { cfg = nil; version++ }
		infos, err := get(1)
		require.NoError(t, err)
		require.Empty(t, infos)
	})
	t.Run("reload during read", func(t *testing.T) {
		reads = 0
		onRead = func() { version += 2; up(20, "v0", 4, 1, 107); onRead = nil }
		infos, err := get(1)
		require.NoError(t, err)
		require.Equal(t, 2, reads)
		require.Equal(t, int64(107), infos[1].GetSegmentID())
	})
	t.Run("continuous changes are retriable", func(t *testing.T) {
		reads = 0
		onRead = func() { version++ }
		defer func() { onRead = nil }()
		_, err := get(1)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		require.Equal(t, 3, reads)
	})
	t.Run("canceled request", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		_, err := s.queryViewSegmentInfo(ctx, &querypb.GetSegmentInfoRequest{CollectionID: 1})
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("latest empty view replaces older segments", func(t *testing.T) {
		up(20, "v0", 5, 1)
		infos, err := get(1)
		require.NoError(t, err)
		require.Len(t, infos, 1)
		require.Equal(t, int64(104), infos[0].GetSegmentID())
	})
	t.Run("no up view", func(t *testing.T) {
		stats = map[qviews.ShardID]*coordview.ShardStats{{ReplicaID: 10, VChannel: "v0"}: {Segments: map[int64]*coordview.SegmentStats{}}}
		before := reads
		infos, err := get(1)
		require.NoError(t, err)
		require.Empty(t, infos)
		require.Equal(t, before, reads)
	})
}
