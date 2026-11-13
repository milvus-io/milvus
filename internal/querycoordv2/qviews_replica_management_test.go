package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestQueryViewReplicaAllocationWithoutLegacyManager(t *testing.T) {
	allocated := int64(100)
	s := &Server{idAllocator: func() (int64, error) { allocated++; return allocated, nil }}
	replicas, err := s.generateQViewsReplicaConfigs(t.Context(), nil, qviewsExpectedLoadConfig{ReplicaNumber: map[string]int{"rg": 2}})
	require.NoError(t, err)
	require.Len(t, replicas, 2)
	require.EqualValues(t, 101, replicas[0].ReplicaId)
	require.EqualValues(t, 102, replicas[1].ReplicaId)
	current := &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{
		{ReplicaID: 101, ResourceGroup: "rg"}, {ReplicaID: 102, ResourceGroup: "rg"},
	}}
	replicas, err = s.generateQViewsReplicaConfigs(t.Context(), current, qviewsExpectedLoadConfig{ReplicaNumber: map[string]int{"other": 2}})
	require.NoError(t, err)
	require.EqualValues(t, 102, allocated, "moving replicas reuses persisted IDs")
	require.EqualValues(t, 101, replicas[0].ReplicaId)
	require.Equal(t, "other", replicas[0].ResourceGroupName)
}

func TestQueryViewReplicaManagementWithoutLegacyManager(t *testing.T) {
	s, _ := newHandoffServer(t)
	// A nil legacy manager makes accidental reads fail rather than returning a
	// coincidentally matching cache populated by the fixture.
	s.meta = &meta.Meta{ResourceManager: &meta.ResourceManager{}}
	cfg := s.qviewsRuntime.loadConfigStore.GetConfig(1).Clone()
	cfg.Replicas = []*loadmgr.ReplicaAssignment{{ReplicaID: 11, ResourceGroup: "a"}, {ReplicaID: 12, ResourceGroup: "b"}}
	cfg.UserSpecifiedReplicaMode = true
	require.NoError(t, s.qviewsRuntime.loadConfigStore.Put(t.Context(), cfg))
	version := s.qviewsRuntime.loadConfigStore.GetConfigVersion(1)
	s.qviewsRuntime.resources = &resource.Resources{VChannels: func(context.Context, int64) ([]string, error) { return []string{"v0", "v1"}, nil }}
	v := &qviews.QueryViewVersion{QueryVersion: 1}
	stats := map[qviews.ShardID]*coordview.ShardStats{
		{ReplicaID: 11, VChannel: "v0"}: {UpVersion: v, UpLoadInfoVersion: version, ResidentNodes: []int64{1}},
		{ReplicaID: 11, VChannel: "v1"}: {UpVersion: v, UpLoadInfoVersion: version, ResidentNodes: []int64{1}},
		{ReplicaID: 12, VChannel: "v0"}: {UpVersion: v, UpLoadInfoVersion: version, ResidentNodes: []int64{2}},
	}
	patch := mockey.Mock((*coordview.ShardViewRegistry).SnapshotForCollection).To(func(*coordview.ShardViewRegistry, int64) *coordview.ShardViewSnapshot {
		return coordview.NewShardViewSnapshot(1, stats)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	rgPatch := mockey.Mock((*meta.ResourceManager).GetResourceGroupByNodeID).To(func(*meta.ResourceManager, int64) string { return "a" }).Build()
	t.Cleanup(func() { rgPatch.UnPatch() })
	replicas := s.GetInternalReplicasByCollection(t.Context(), 1)
	require.Len(t, replicas, 2)
	require.Equal(t, []int64{1}, replicas[0].GetNodes(), "node membership is deduplicated across channels")
	require.True(t, s.IsCollectionUserSpecifiedReplicaMode(t.Context(), 1))
	errors := s.CheckReplicasServiceable(t.Context(), 1)
	require.Len(t, errors, 1)
	require.Contains(t, errors, int64(12), "a missing expected shard is not serving")
	stats[qviews.ShardID{ReplicaID: 12, VChannel: "v1"}] = &coordview.ShardStats{UpVersion: v, UpLoadInfoVersion: version - 1}
	require.Contains(t, s.CheckReplicasServiceable(t.Context(), 1), int64(12), "old load version is not ready")
	stats[qviews.ShardID{ReplicaID: 12, VChannel: "v1"}].UpLoadInfoVersion = version
	require.NoError(t, s.CheckAllReplicasServiceable(t.Context(), 1))
	loaded, outgoing, incoming := s.qviewsResourceGroupStats("a")
	require.EqualValues(t, 1, loaded[1])
	require.Zero(t, outgoing[1])
	require.EqualValues(t, 1, incoming[1])
	value, err := s.qviewsReplicasJSON(t.Context())
	require.NoError(t, err)
	require.Contains(t, value, "a")
}
