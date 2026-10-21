package balancer

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

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
		101: {SegmentID: 101, RowNum: 10, ManifestVersion: 8, TransformStartAfterTimetick: 55},
		102: {SegmentID: 102, RowNum: 20, ManifestVersion: 9, TransformStartAfterTimetick: 56},
	}, shardDataView(id.VChannel, 100, 101, 102))
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
	for _, patch := range []*mockey.Mocker{
		mockey.Mock((*coordview.ShardViewRegistry).Get).Return(manager).Build(),
		mockey.Mock((*coordview.ShardViewManager).AddPreparing).To(func(_ *coordview.ShardViewManager, _ context.Context, builder *qviews.QueryViewAtCoordBuilder) error {
			builder.SetQueryVersion(2)
			accepted = append(accepted, builder.Build())
			return nil
		}).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
	for _, tc := range []struct {
		name                   string
		source                 int64
		destinations, segments []int64
		err                    error
	}{
		{"unknown source", 99, []int64{2}, []int64{101}, merr.ErrNodeNotFound},
		{"unknown destination", 1, []int64{99}, []int64{101}, merr.ErrNodeNotFound},
		{"one missing segment rejects whole request", 1, []int64{2}, []int64{101, 999}, merr.ErrSegmentNotFound},
		{"all missing segments", 1, []int64{2}, []int64{998, 999}, merr.ErrSegmentNotFound},
		{"source only", 1, []int64{1}, []int64{101}, merr.ErrParameterInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := b.MoveSegments(t.Context(), cfg, tc.source, tc.destinations, tc.segments)
			require.ErrorIs(t, err, tc.err)
			require.Empty(t, accepted)
		})
	}
	moves, err := b.MoveSegments(t.Context(), cfg, 1, []int64{2}, []int64{101})
	require.NoError(t, err)
	require.Len(t, accepted, 1)
	require.Equal(t, map[int64]int64{101: 2}, moves[id].Destinations)
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
	_, err = b.MoveSegments(t.Context(), cfg.Clone(), 1, []int64{2}, []int64{101})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Empty(t, accepted)
	moves, err = b.MoveSegments(t.Context(), cfg, 1, nil, nil)
	require.NoError(t, err)
	require.Equal(t, map[int64]int64{101: 2, 102: 2}, moves[id].Destinations)
	accepted = nil
	preparing := *stats
	preparing.PreparingVersion = &qviews.QueryViewVersion{DataVersion: dv, QueryVersion: 2}
	c.PublishShard(id, &preparing)
	_, err = b.MoveSegments(t.Context(), cfg, 1, []int64{2}, []int64{101})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Empty(t, accepted)
	c.PublishNode(2, &NodeInfo{NodeID: 2, Alive: true, Stopping: true, ResourceGroup: "rg1"})
	_, err = b.MoveSegments(t.Context(), cfg, 1, []int64{2}, []int64{101})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
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
	b := NewDefaultBalancer(c, nil, policy, nil)
	pl, err := b.Placement(t.Context(), 1)
	require.NoError(t, err)
	require.Len(t, pl.Nodes[10], 1)
	require.Len(t, pl.Nodes[11], 1)
	require.NotEqual(t, pl.Nodes[10], pl.Nodes[11])
	_, err = b.MoveSegments(t.Context(), cfg, pl.Nodes[10][0], pl.Nodes[11], nil)
	require.ErrorIs(t, err, merr.ErrNodeNotFound, "same RG does not allow crossing replica ownership")
}
