package balancer

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestManualBalance(t *testing.T) {
	cfg := cfgFor(1, 10, []int64{100}, nil)
	c := newTestCache(cfg)
	id := cacheShard(1, 10)
	dv := qviews.DataVersion{StreamingVersion: 1}
	publishTestData(c, 1, dv, map[int64]*SegmentDataView{
		101: {SegmentID: 101, LogicalSize: 10, ManifestVersion: 8, TransformStartAfterTimetick: 55},
		102: {SegmentID: 102, LogicalSize: 20, ManifestVersion: 9, TransformStartAfterTimetick: 56},
		201: {SegmentID: 201, LogicalSize: 1000, ManifestVersion: 10},
	}, &viewpb.DataViewOfShard{Vchannel: id.VChannel, Partitions: []*viewpb.DataViewOfPartition{
		{PartitionId: 100, SegmentIds: []int64{101, 102}},
		{PartitionId: 200, SegmentIds: []int64{201}},
	}})
	publishTestNodes(c, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"}, &NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"})
	stats := upStats(dv, placement(101, 100, 1, coordview.SegmentStateUp), placement(102, 100, 1, coordview.SegmentStateUp))
	stats.UpSchemaVersion = 7
	c.PublishShard(id, stats)
	c.MarkReady()
	policy := NewDefaultBalancePolicy()
	policy.Plan(c, []qviews.ShardID{id}) // Establish the real replica ownership layout.
	b := NewDefaultBalancer(c, &coordview.ShardViewRegistry{}, policy, nil)
	manager := &coordview.ShardViewManager{}
	var accepted []*viewpb.QueryViewOfShard
	source := qviews.NewQueryViewAtCoordBuilder(10, syntheticDataView(cfg, dv, c.GetCollection(1).DataView().Shard(id.VChannel)), id.VChannel).
		SetAssignments(map[int64]map[int64][]int64{1: {100: {101, 102}}}).SetSchemaVersion(7).SetLoadInfoVersion(1)
	for _, patch := range []*mockey.Mocker{
		mockey.Mock((*coordview.ShardViewRegistry).Get).Return(manager).Build(),
		mockey.Mock((*coordview.ShardViewRegistry).NodeShards).Return([]qviews.ShardID{id}).Build(),
		mockey.Mock((*coordview.ShardViewRegistry).CollectionShards).Return([]qviews.ShardID{id}).Build(),
		mockey.Mock((*coordview.ShardViewManager).PlacementBuilder).To(func(*coordview.ShardViewManager) *qviews.QueryViewAtCoordBuilder {
			return qviews.NewQueryViewAtCoordBuilderFromView(source.Build())
		}).Build(),
		mockey.Mock((*coordview.ShardViewManager).AddManualPreparing).To(func(_ *coordview.ShardViewManager, _ context.Context, builder *qviews.QueryViewAtCoordBuilder, _ qviews.QueryViewVersion) (*coordview.Preparation, error) {
			builder.SetQueryVersion(2)
			accepted = append(accepted, builder.Build())
			return &coordview.Preparation{Version: qviews.FromProtoQueryViewVersion(builder.Build().GetMeta().GetVersion())}, nil
		}).Build(),
		mockey.Mock((*coordview.Preparation).Wait).To(func(_ *coordview.Preparation, _ context.Context) coordview.PreparationResult {
			require.True(t, b.reconcileMu.TryLock(), "manual waits must not block reconciliation")
			b.reconcileMu.Unlock()
			return coordview.PreparationResult{State: qviews.QueryViewStateUp}
		}).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
	for _, tc := range []struct {
		name                   string
		source                 int64
		destinations, segments []int64
		err                    error
		message                string
	}{
		{"unknown source", 99, []int64{2}, []int64{101}, merr.ErrNodeNotFound, "source node not found in any replica"},
		{"unknown destination", 1, []int64{99}, []int64{101}, merr.ErrNodeNotFound, "destination node not found in the same replica"},
		{"one missing segment rejects whole request", 1, []int64{2}, []int64{101, 999}, merr.ErrSegmentNotFound, "segment not found in source node"},
		{"all missing segments", 1, []int64{2}, []int64{998, 999}, merr.ErrSegmentNotFound, "segment not found in source node"},
		{"unloaded partition", 1, []int64{2}, []int64{201}, merr.ErrSegmentNotFound, "segment not found in source node"},
		{"source only", 1, []int64{1}, []int64{101}, merr.ErrParameterInvalid, "no destination query node other than source"},
		{"missing segment before source-only destination", 1, []int64{1}, []int64{999}, merr.ErrSegmentNotFound, "segment not found in source node"},
		{"mixed selection before source-only destination", 1, []int64{1}, []int64{101, 999}, merr.ErrSegmentNotFound, "segment not found in source node"},
		{"mixed selection before invalid destination", 1, []int64{99}, []int64{101, 999}, merr.ErrSegmentNotFound, "segment not found in source node"},
		{"mixed selection with automatic destination", 1, nil, []int64{101, 999}, merr.ErrSegmentNotFound, "segment not found in source node"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := b.MoveSegments(t.Context(), 1, tc.source, tc.destinations, tc.segments)
			require.ErrorIs(t, err, tc.err)
			require.ErrorContains(t, err, tc.message)
			require.Equal(t, map[int64]int64{101: 1, 102: 1}, flattenAssignments(source.Build()))
			require.Empty(t, accepted)
		})
	}
	moves, err := b.MoveSegments(t.Context(), 1, 1, []int64{2}, []int64{101})
	require.NoError(t, err)
	require.Len(t, accepted, 1)
	require.Equal(t, qviews.QueryViewStateUp, moves[id].State)
	require.Equal(t, int64(2), moves[id].Version.QueryVersion)
	require.Equal(t, map[int64]int64{101: 2, 102: 1}, flattenAssignments(accepted[0]))
	require.Equal(t, int32(7), accepted[0].GetMeta().GetSchemaVersion())
	require.Equal(t, uint64(1), accepted[0].GetMeta().GetLoadInfoVersion())
	for _, node := range accepted[0].QueryNode {
		for _, partition := range node.Partitions {
			if node.NodeId == 2 {
				require.Equal(t, []int64{8}, partition.SegmentManifestVersions)
			}
		}
	}
	accepted = nil
	_, err = b.MoveSegments(t.Context(), 1, 1, []int64{2}, []int64{101, 102})
	require.NoError(t, err)
	require.Len(t, accepted, 1)
	require.Equal(t, map[int64]int64{101: 2, 102: 2}, flattenAssignments(accepted[0]))
	accepted = nil
	_, err = b.MoveSegments(t.Context(), 1, 1, nil, nil)
	require.NoError(t, err)
	require.Equal(t, map[int64]int64{101: 2, 102: 2}, flattenAssignments(accepted[0]))
	accepted = nil
	results, err := b.ManualPlace(t.Context(), PlacementScope{NodeID: 1}, func(_ map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder) error { return nil })
	require.NoError(t, err)
	require.False(t, results[id].Changed)
	require.Empty(t, accepted)
	results, err = b.ManualPlace(t.Context(), PlacementScope{NodeID: 1}, func(builders map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder) error {
		delete(builders, id)
		return nil
	})
	require.NoError(t, err)
	require.Empty(t, results)
	require.Empty(t, accepted)
	rejected := merr.WrapErrParameterInvalidMsg("operation rejected the selected placement")
	_, err = b.ManualPlace(t.Context(), PlacementScope{NodeID: 1}, func(builders map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder) error {
		builders[id].SetAssignments(map[int64]map[int64][]int64{2: {100: {101, 102}}})
		return rejected
	})
	require.Equal(t, rejected, err)
	require.Empty(t, accepted)
	for name, modify := range map[string]func(*viewpb.QueryViewOfShard){
		"removed segment":   func(v *viewpb.QueryViewOfShard) { v.QueryNode[0].Partitions[0].SegmentIds = []int64{101} },
		"duplicate segment": func(v *viewpb.QueryViewOfShard) { v.QueryNode[0].Partitions[0].SegmentIds = []int64{101, 101, 102} },
		"new segment": func(v *viewpb.QueryViewOfShard) {
			v.QueryNode[0].Partitions[0].SegmentIds = append(v.QueryNode[0].Partitions[0].SegmentIds, 999)
		},
		"changed partition":        func(v *viewpb.QueryViewOfShard) { v.QueryNode[0].Partitions[0].PartitionId = 200 },
		"changed manifest":         func(v *viewpb.QueryViewOfShard) { v.QueryNode[0].Partitions[0].SegmentManifestVersions[0]++ },
		"changed data version":     func(v *viewpb.QueryViewOfShard) { v.Meta.Version.DataVersion.StreamingVersion++ },
		"changed schema":           func(v *viewpb.QueryViewOfShard) { v.Meta.SchemaVersion++ },
		"changed load config":      func(v *viewpb.QueryViewOfShard) { v.Meta.LoadInfoVersion++ },
		"changed transform cursor": func(v *viewpb.QueryViewOfShard) { v.Meta.TransformStartAfterTimetick++ },
		"changed partition stats": func(v *viewpb.QueryViewOfShard) {
			v.StreamingNode.PartitionStats = []*viewpb.ClusteredPartitionStats{{PartitionId: 100, Version: 42}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := b.ManualPlace(t.Context(), PlacementScope{NodeID: 1}, func(builders map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder) error {
				modified := proto.Clone(builders[id].Build()).(*viewpb.QueryViewOfShard)
				modify(modified)
				builders[id] = qviews.NewQueryViewAtCoordBuilderFromView(modified)
				return nil
			})
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Empty(t, accepted, "invalid data must be rejected before publishing")
		})
	}
	c.PublishNode(2, &NodeInfo{NodeID: 2, Alive: true, Stopping: true, ResourceGroup: "rg1"})
	_, err = b.MoveSegments(t.Context(), 1, 1, []int64{2}, []int64{101})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)

	// A single-node replica has no automatic destination. Missing source data
	// must still take precedence, including when the request also selects data
	// that is present. Neither failure may publish a partial migration.
	policy.Plan(c, []qviews.ShardID{id})
	pl, err := b.Placement(t.Context(), 1)
	require.NoError(t, err)
	require.Equal(t, []int64{1}, pl.Nodes[id.ReplicaID])
	_, err = b.MoveSegments(t.Context(), 1, 1, nil, []int64{101, 999})
	require.ErrorIs(t, err, merr.ErrSegmentNotFound)
	require.ErrorContains(t, err, "segment not found in source node")
	require.Empty(t, accepted)
	_, err = b.MoveSegments(t.Context(), 1, 1, nil, []int64{101})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Empty(t, accepted)
}

func TestManualPlacePartialSubmission(t *testing.T) {
	c := newTestCache(cfgFor(1, 10, nil, nil))
	c.PublishLoadConfig(2, cfgFor(2, 20, nil, nil), 1)
	ids := []qviews.ShardID{cacheShard(1, 10), cacheShard(2, 20)}
	managers := make(map[qviews.ShardID]*coordview.ShardViewManager)
	sources := make(map[*coordview.ShardViewManager]*qviews.QueryViewAtCoordBuilder)
	for i, id := range ids {
		collection := int64(i + 1)
		data := cacheData(collection, id.VChannel, 100)
		c.PublishDataView(collection, data)
		c.PublishShard(id, upStats(data.DataVersion, placement(collection*1000, 1, 1, coordview.SegmentStateUp)))
		manager := &coordview.ShardViewManager{}
		managers[id] = manager
		sources[manager] = qviews.NewQueryViewAtCoordBuilder(id.ReplicaID, syntheticDataView(c.GetCollection(collection).LoadConfig(), data.DataVersion, data.Shard(id.VChannel)), id.VChannel).
			SetAssignments(map[int64]map[int64][]int64{1: {1: {collection * 1000}}}).SetLoadInfoVersion(1)
	}
	publishTestNodes(c, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"}, &NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"})
	c.MarkReady()
	policy := NewDefaultBalancePolicy()
	policy.Plan(c, ids)
	b := NewDefaultBalancer(c, &coordview.ShardViewRegistry{}, policy, nil)
	accepted := 0
	for _, patch := range []*mockey.Mocker{
		mockey.Mock((*coordview.ShardViewRegistry).NodeShards).Return(ids).Build(),
		mockey.Mock((*coordview.ShardViewRegistry).Get).To(func(_ *coordview.ShardViewRegistry, id qviews.ShardID) *coordview.ShardViewManager {
			return managers[id]
		}).Build(),
		mockey.Mock((*coordview.ShardViewManager).PlacementBuilder).To(func(m *coordview.ShardViewManager) *qviews.QueryViewAtCoordBuilder {
			return qviews.NewQueryViewAtCoordBuilderFromView(sources[m].Build())
		}).Build(),
		mockey.Mock((*coordview.ShardViewManager).AddManualPreparing).To(func(m *coordview.ShardViewManager, _ context.Context, builder *qviews.QueryViewAtCoordBuilder, _ qviews.QueryViewVersion) (*coordview.Preparation, error) {
			if m == managers[ids[1]] {
				return nil, merr.WrapErrServiceUnavailableMsg("concurrent release")
			}
			accepted++
			return &coordview.Preparation{Version: qviews.QueryViewVersion{DataVersion: builder.DataVersion(), QueryVersion: 2}}, nil
		}).Build(),
		mockey.Mock((*coordview.Preparation).Wait).Return(coordview.PreparationResult{State: qviews.QueryViewStateUp}).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
	_, err := b.ManualPlace(t.Context(), PlacementScope{NodeID: 1}, func(builders map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder) error {
		builders[ids[0]].SetAssignments(map[int64]map[int64][]int64{2: {1: {1000}}})
		builders[ids[1]].SetAssignments(nil)
		return nil
	})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Zero(t, accepted, "validate all changed shards before the first submission")
	results, err := b.MoveSegments(t.Context(), 0, 1, []int64{2}, nil)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Equal(t, 1, accepted)
	require.Len(t, results, 2)
	require.True(t, results[ids[0]].Changed)
	require.Equal(t, qviews.QueryViewStateUp, results[ids[0]].State)
	require.NoError(t, results[ids[0]].Err)
	require.False(t, results[ids[1]].Changed)
	require.ErrorIs(t, results[ids[1]].Err, merr.ErrServiceUnavailable)
	// The callback can discard a shard whose submission would fail. ManualPlace
	// does not add a LoadConfig policy gate to these captured snapshots.
	accepted = 0
	results, err = b.ManualPlace(t.Context(), PlacementScope{NodeID: 1}, func(builders map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder) error {
		delete(builders, ids[1])
		builders[ids[0]].SetAssignments(map[int64]map[int64][]int64{2: {1: {1000}}})
		c.PublishLoadConfig(1, nil, 2)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, accepted)
	require.Len(t, results, 1)
	require.Equal(t, qviews.QueryViewStateUp, results[ids[0]].State)
}

func TestManualBalancePreservesReplicaIsolation(t *testing.T) {
	cfg := cfgFor(1, 10, nil, nil)
	cfg.Replicas = append(cfg.Replicas, &loadmgr.ReplicaAssignment{ReplicaID: 11, ResourceGroup: "rg1"})
	c := newTestCache(cfg)
	id := cacheShard(1, 10)
	c.PublishDataView(1, cacheData(1, id.VChannel, 100))
	publishTestNodes(c, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"}, &NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"})
	c.MarkReady()
	policy := NewDefaultBalancePolicy()
	policy.Plan(c, []qviews.ShardID{id})
	b := NewDefaultBalancer(c, &coordview.ShardViewRegistry{}, policy, nil)
	pl, err := b.Placement(t.Context(), 1)
	require.NoError(t, err)
	require.Len(t, pl.Nodes[10], 1)
	require.Len(t, pl.Nodes[11], 1)
	require.NotEqual(t, pl.Nodes[10], pl.Nodes[11])
	before := qviews.NewQueryViewAtCoordBuilder(10, syntheticDataView(cfg, qviews.DataVersion{StreamingVersion: 1}, c.GetCollection(1).DataView().Shard(id.VChannel)), id.VChannel).
		SetAssignments(map[int64]map[int64][]int64{pl.Nodes[10][0]: {1: {100}}}).Build()
	manager := &coordview.ShardViewManager{}
	for _, patch := range []*mockey.Mocker{
		mockey.Mock((*coordview.ShardViewRegistry).NodeShards).Return([]qviews.ShardID{id}).Build(),
		mockey.Mock((*coordview.ShardViewRegistry).CollectionShards).Return([]qviews.ShardID{id}).Build(),
		mockey.Mock((*coordview.ShardViewRegistry).Get).Return(manager).Build(),
		mockey.Mock((*coordview.ShardViewManager).PlacementBuilder).To(func(*coordview.ShardViewManager) *qviews.QueryViewAtCoordBuilder {
			return qviews.NewQueryViewAtCoordBuilderFromView(before)
		}).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
	_, err = b.MoveSegments(t.Context(), 1, pl.Nodes[10][0], pl.Nodes[11], []int64{100})
	require.ErrorIs(t, err, merr.ErrNodeNotFound, "same RG does not allow crossing replica ownership")
}

func TestManualPlaceEmptySnapshotPolicy(t *testing.T) {
	c := newTestCache(cfgFor(1, 10, nil, nil))
	c.MarkReady()
	b := NewDefaultBalancer(c, &coordview.ShardViewRegistry{}, NewDefaultBalancePolicy(), nil)
	ids := []qviews.ShardID{cacheShard(1, 10), cacheShard(2, 20)}
	manager := &coordview.ShardViewManager{}
	for _, patch := range []*mockey.Mocker{
		mockey.Mock((*coordview.ShardViewRegistry).ShardIDs).Return(ids).Build(),
		mockey.Mock((*coordview.ShardViewRegistry).Get).To(func(_ *coordview.ShardViewRegistry, id qviews.ShardID) *coordview.ShardViewManager {
			if id == ids[0] {
				return nil
			}
			return manager
		}).Build(),
		mockey.Mock((*coordview.ShardViewManager).PlacementBuilder).Return(nil).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
	results, err := b.ManualPlace(t.Context(), PlacementScope{}, func(builders map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder) error {
		require.Empty(t, builders, "removed and unavailable managers do not prevent callback execution")
		return nil
	})
	require.NoError(t, err)
	require.Empty(t, results)
	rejected := merr.WrapErrSegmentNotFound(100)
	_, err = b.ManualPlace(t.Context(), PlacementScope{}, func(builders map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder) error {
		require.Empty(t, builders)
		return rejected
	})
	require.Equal(t, rejected, err)
}
