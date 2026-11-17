package balancer

import (
	"maps"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestMemoryPlanRollbackAndLazyMaterialization(t *testing.T) {
	for _, mandatory := range []bool{false, true} {
		t.Run(map[bool]string{false: "optional", true: "mandatory"}[mandatory], func(t *testing.T) {
			c := balancercache.New(nil)
			c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
			ids := []qviews.ShardID{cacheShard(1, 10), cacheShard(2, 20), cacheShard(3, 30)}
			for i, id := range ids {
				coll := int64(i + 1)
				c.PublishLoadConfig(coll, cfgFor(coll, id.ReplicaID, []int64{1}, nil), 1)
				data := cacheData(coll, id.VChannel, 100)
				if mandatory {
					data.DataVersion.StreamingVersion = 2
				}
				c.PublishDataView(coll, data)
				c.PublishShard(id, cacheStats(coll*1000, 1, 100, coordview.SegmentStateUp, true))
			}
			var calls int
			patch := mockey.Mock(allocate).To(func(p *planningContext, id qviews.ShardID, base map[int64]int64, optional bool) *allocationResult {
				require.Equal(t, !mandatory, optional)
				// Each previous failure/no-op must restore its 100-row contribution.
				require.Equal(t, int64(200), base[1])
				calls++
				if mandatory || id == ids[0] {
					return nil
				}
				return &allocationResult{assignments: maps.Clone(currentSegmentNodes(p, id)), rowsByNode: map[int64]int64{1: 100}, more: true}
			}).Build()
			defer patch.UnPatch()
			materialize := mockey.Mock((*allocationResult).materialize).To(func(*allocationResult, *planningContext, qviews.ShardID) *qviews.QueryViewAtCoordBuilder {
				t.Fatal("rejected candidates must not materialize")
				return nil
			}).Build()
			defer materialize.UnPatch()
			plan := NewDefaultBalancePolicy().Plan(c, ids)
			require.Equal(t, 3, calls)
			require.Empty(t, plan.Prepares)
			if mandatory {
				require.Len(t, plan.Retries, 3)
			} else {
				require.Equal(t, ids[:1], plan.Retries)
				require.Equal(t, ids[1:], plan.Continues)
			}
			require.Equal(t, int64(300), c.GetNode(1).TargetSize())
		})
	}
}

func TestMemoryNativeBuilderMatchesProto(t *testing.T) {
	c := balancercache.New(nil)
	id := cacheShard(1, 10)
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	c.PublishLoadConfig(1, cfgFor(1, 10, []int64{1}, nil), 7)
	data := cacheData(1, id.VChannel, 10, 20, 30)
	data.SchemaVersion = 9
	shard := data.Shards[0]
	shard.GrowingTransformStartAfterTimetick = 90
	shard.CompactedTransformStartAfterTimetick = 60
	shard.Partitions[0].PartitionStatsVersion = 42
	for i, s := range shard.Partitions[0].Segments {
		s.ManifestVersion = int64(i)
		s.TransformStartAfterTimetick = uint64(i + 60)
	}
	c.PublishDataView(1, data)
	p := newPlanningContext(c)
	p.targets[10] = []int64{1}
	p.fanouts = make(map[qviews.ShardID]int)
	result := allocate(p, id, map[int64]int64{1: 0}, false)
	require.NotNil(t, result)
	// Cache publications between allocation and admission cannot replace the
	// metadata pinned by this planning context.
	c.PublishDataView(1, cacheData(1, id.VChannel, 999))
	builder := result.materialize(p, id)
	expectedData := syntheticDataView(p.ConfigForShard(id), data.DataVersion, shard)
	expectedData.SchemaVersion = data.SchemaVersion
	expected := qviews.NewQueryViewAtCoordBuilder(10, expectedData, id.VChannel).SetLoadInfoVersion(7).SetAssignments(builder.Assignments()).SetPreparationPriority(p.preparationPriority(id)).Build()
	require.True(t, proto.Equal(expected, builder.Build()))
	// Metadata maps never alias the protobuf emitted by Build.
	built := builder.Build()
	built.QueryNode[0].Partitions[0].SegmentManifestVersions[0] = 999
	built.StreamingNode.PartitionStats[0].Version = 999
	require.True(t, proto.Equal(expected, builder.Build()))
}

func TestMemoryReusableResourcesWithoutData(t *testing.T) {
	c := balancercache.New(nil)
	id := cacheShard(1, 10)
	segment := &SegmentDataView{SegmentID: 1000, PartitionID: 1}
	require.Empty(t, reusableResources(newPlanningContext(c), id, segment, []int64{1}))
	c.PublishLoadConfig(1, cfgFor(1, 10, []int64{1}, nil), 1)
	require.Empty(t, reusableResources(newPlanningContext(c), id, segment, []int64{1}))
}
