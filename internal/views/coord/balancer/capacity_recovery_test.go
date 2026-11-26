package balancer

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestCapacityAllocationDoesNotResolveCollectionPerSegment(t *testing.T) {
	for _, mode := range []string{"initial", "retained", "failed"} {
		t.Run(mode, func(t *testing.T) {
			baseline := 0
			for _, nodes := range []int{2, 8} {
				for _, segments := range []int{1, 1000} {
					c := newTestCache(cfgFor(1, 10, []int64{1}, nil))
					id := cacheShard(1, 10)
					var targets []int64
					for n := int64(1); n <= int64(nodes); n++ {
						publishTestNodes(c, &NodeInfo{NodeID: n, Alive: true, ResourceGroup: "rg1"})
						targets = append(targets, n)
					}
					rows := make([]int64, segments)
					var placements []testSegmentPlacement
					for i := range rows {
						rows[i] = 1
						placements = append(placements, placement(1000+int64(i), 1, 1, coordview.SegmentStateUp))
					}
					c.PublishDataView(1, cacheData(1, id.VChannel, rows...))
					stats := &coordview.ShardStats{}
					if mode != "initial" {
						stats = upStats(qviews.DataVersion{StreamingVersion: 1}, placements...)
					}
					if mode == "failed" {
						key := coordview.FailedResourceKey{PartitionID: 1, SegmentID: 1000, DataVersion: qviews.DataVersion{StreamingVersion: 1}, LoadInfoVersion: 1}
						stats.FailedResources = map[int64]map[coordview.FailedResourceKey]coordview.ResourceFailure{1: {key: {Version: qviews.QueryViewVersion{DataVersion: key.DataVersion, QueryVersion: 1}}}}
					}
					c.PublishShard(id, stats)
					p := newPlanningContext(c)
					p.targets[10] = targets
					p.fanouts = make(map[qviews.ShardID]int)
					var original func(qviews.ShardID) (int64, bool)
					calls := 0
					patch := mockey.Mock(parseShardCollection).Origin(&original).To(func(id qviews.ShardID) (int64, bool) {
						calls++
						return original(id)
					}).Build()
					result := allocate(p, id, nil, false)
					patch.UnPatch()
					require.NotNil(t, result)
					require.Len(t, result.assignments, segments)
					if baseline == 0 {
						baseline = calls
					}
					require.Positive(t, baseline)
					require.Equal(t, baseline, calls, "lookup count must not grow with %d segments and %d nodes", segments, nodes)
				}
			}
		})
	}
}

func TestCapacityFailurePlacement(t *testing.T) {
	for _, oldUp := range []bool{false, true} {
		for _, alternative := range []bool{false, true} {
			t.Run(fmtCapacityCase(oldUp, alternative), func(t *testing.T) {
				id := qviews.ShardID{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_1v0"}
				c := newTestCache(cfgFor(1, 10, []int64{1}, nil))
				dv := qviews.DataVersion{StreamingVersion: 2}
				publishTestData(c, 1, dv, map[int64]*SegmentDataView{101: {SegmentID: 101, PartitionID: 1, LogicalSize: 10}}, shardDataView(id.VChannel, 1, 101))
				publishTestNodes(c, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
				if alternative {
					publishTestNodes(c, &NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"})
				}
				// Wrong-RG nodes are never alternatives, even with zero load.
				publishTestNodes(c, &NodeInfo{NodeID: 3, Alive: true, ResourceGroup: "other"})
				stats := &coordview.ShardStats{}
				if oldUp {
					stats = upStats(qviews.DataVersion{StreamingVersion: 1}, placement(101, 1, 10, coordview.SegmentStateUp))
				}
				key := coordview.FailedResourceKey{PartitionID: 1, SegmentID: 101, DataVersion: dv, LoadInfoVersion: c.GetCollection(1).ConfigVersion()}
				stats.FailedResources = map[int64]map[coordview.FailedResourceKey]coordview.ResourceFailure{1: {key: {Version: qviews.QueryViewVersion{DataVersion: dv, QueryVersion: 1}}}}
				c.PublishShard(id, stats)
				plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{id})
				require.Contains(t, plan.Prepares, id)
				require.Empty(t, plan.Releases)
				want := int64(1)
				if alternative {
					want = 2
				}
				require.Equal(t, want, assignmentsFromBuilder(plan.Prepares[id])[101])
				// A failed different load identity must not poison the next one.
				key.LoadInfoVersion++
				stats.FailedResources = map[int64]map[coordview.FailedResourceKey]coordview.ResourceFailure{1: {key: {Version: qviews.QueryViewVersion{DataVersion: dv, QueryVersion: 1}}}}
				c.PublishShard(id, stats)
				plan = NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{id})
				require.Equal(t, int64(1), assignmentsFromBuilder(plan.Prepares[id])[101])
			})
		}
	}
}

func fmtCapacityCase(up, alternative bool) string {
	name := "initial"
	if up {
		name = "handoff"
	}
	if alternative {
		return name + "/alternative"
	}
	return name + "/no-alternative"
}

func TestCapacityFailuresTryEveryEligibleNode(t *testing.T) {
	id := qviews.ShardID{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	c := newTestCache(cfgFor(1, 10, []int64{1}, nil))
	dv := qviews.DataVersion{StreamingVersion: 1}
	publishTestData(c, 1, dv, map[int64]*SegmentDataView{101: {SegmentID: 101, PartitionID: 1, LogicalSize: 10}}, shardDataView(id.VChannel, 1, 101))
	publishTestNodes(c, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"}, &NodeInfo{NodeID: 3, Alive: true, ResourceGroup: "rg1"})
	key := coordview.FailedResourceKey{PartitionID: 1, SegmentID: 101, DataVersion: dv, LoadInfoVersion: c.GetCollection(1).ConfigVersion()}
	stats := &coordview.ShardStats{FailedResources: make(map[int64]map[coordview.FailedResourceKey]coordview.ResourceFailure)}
	policy := NewDefaultBalancePolicy()
	for attempt, expected := range []int64{1, 2, 3, 1, 2, 3} {
		c.PublishShard(id, stats)
		plan := policy.Plan(c, []qviews.ShardID{id})
		require.Contains(t, plan.Prepares, id)
		require.Equal(t, expected, assignmentsFromBuilder(plan.Prepares[id])[101])
		// Publish a fresh snapshot, just as the manager does after a rejection.
		copy := &coordview.ShardStats{FailedResources: make(map[int64]map[coordview.FailedResourceKey]coordview.ResourceFailure)}
		for node, failed := range stats.FailedResources {
			copy.FailedResources[node] = failed
		}
		copy.FailedResources[expected] = map[coordview.FailedResourceKey]coordview.ResourceFailure{key: {Version: qviews.QueryViewVersion{DataVersion: dv, QueryVersion: int64(attempt + 1)}}}
		stats = copy
	}
}
