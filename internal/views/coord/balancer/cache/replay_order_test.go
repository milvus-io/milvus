package cache

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr/loadmgrtest"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// replaySources is the recovered state of the four sources that
// NewFromSources replays. Each replay visits one source in a caller-chosen
// order, as the map-backed production sources do.
type replaySources struct {
	nodes    map[int64]*api.NodeInfo
	configs  map[int64]*loadmgr.LoadConfig
	versions map[int64]uint64
	data     map[int64]*api.CollectionDataView
	shards   map[qviews.ShardID]*coordview.ShardStats
}

// newReplaySources builds resource groups with live, stopping, unpublished and
// cross-group nodes; loaded, released and DataView-only collections; legacy
// shard identities; known and unknown rows; and Preparing placements that are
// selected or fall back to Up.
func newReplaySources(seed int64) *replaySources {
	r := rand.New(rand.NewSource(seed))
	s := &replaySources{
		nodes:    make(map[int64]*api.NodeInfo),
		configs:  make(map[int64]*loadmgr.LoadConfig),
		versions: make(map[int64]uint64),
		data:     make(map[int64]*api.CollectionDataView),
		shards:   make(map[qviews.ShardID]*coordview.ShardStats),
	}
	groups := map[string][]int64{"rg1": {1, 2, 3}, "rg2": {4, 5}, "rg3": {6}}
	for group, nodes := range groups {
		for _, node := range nodes {
			s.nodes[node] = &api.NodeInfo{NodeID: node, Alive: true, Stopping: node == 3, ResourceGroup: group}
		}
	}
	// Nodes 7 and 8 hold placements but are never published.
	candidates := func(group string) []int64 { return append(slices.Clone(groups[group]), 7, 8, 6) }
	pick := func(nodes []int64) int64 { return nodes[r.Intn(len(nodes))] }
	states := []coordview.SegmentState{coordview.SegmentStateUp, coordview.SegmentStateUp, coordview.SegmentStateReady, coordview.SegmentStatePreparing, coordview.SegmentStateUnrecoverable}

	for collection := int64(1); collection <= 48; collection++ {
		replicaGroups := []string{"rg1"}
		if collection%2 == 0 {
			replicaGroups = []string{"rg2"}
		}
		if collection%3 == 0 {
			replicaGroups = []string{"rg1", "rg2"}
		}
		replicas := make([]*loadmgr.ReplicaAssignment, 0, len(replicaGroups))
		for i, group := range replicaGroups {
			replicas = append(replicas, &loadmgr.ReplicaAssignment{ReplicaID: collection*100 + int64(i), ResourceGroup: group})
		}
		if collection%6 != 0 { // Every sixth collection is released but keeps residual shards.
			s.configs[collection] = &loadmgr.LoadConfig{CollectionID: collection, PartitionIDs: []int64{1}, Replicas: replicas}
			s.versions[collection] = uint64(r.Int63())
		}

		channels := []string{fmt.Sprintf("by-dev-rootcoord-dml_%d_%dv0", collection%4, collection), fmt.Sprintf("by-dev-rootcoord-dml_%d_%dv1", (collection+1)%4, collection)}
		rows := make(map[int64]int64)
		shardSegments := make(map[string][]int64)
		data := &api.CollectionDataView{CollectionID: collection, DataVersion: qviews.DataVersion{StreamingVersion: 3, CompactVersion: 1}}
		for i, channel := range channels {
			shard := &api.ShardDataView{VChannel: channel, Partitions: []*api.PartitionDataView{{PartitionID: 1}}}
			for k := int64(0); k < 3; k++ {
				segment := collection*1000 + int64(i)*10 + k
				rows[segment] = r.Int63n(1000)
				shardSegments[channel] = append(shardSegments[channel], segment)
				shard.Partitions[0].Segments = append(shard.Partitions[0].Segments, &api.SegmentDataView{SegmentID: segment, PartitionID: 1, RowNum: rows[segment], LogicalSize: rows[segment]})
			}
			data.Shards = append(data.Shards, shard)
		}
		if collection%7 != 0 { // Every seventh collection has no DataView yet.
			s.data[collection] = api.PrepareCollectionDataView(data)
		}

		// A replica absent from the config still has a resident shard.
		shardReplicas := append(slices.Clone(replicas), &loadmgr.ReplicaAssignment{ReplicaID: collection*100 + 9, ResourceGroup: "rg1"})
		for r2, replica := range shardReplicas {
			for i, channel := range channels {
				id := qviews.ShardID{ReplicaID: replica.ReplicaID, VChannel: channel}
				if collection%11 == 0 && i == 1 && s.configs[collection] != nil && r2 < len(replicas) {
					// Legacy identity: the collection comes from the configured replica.
					// Without that binding, acceptance depends on the order within
					// the registry replay, whatever the source order is.
					id.VChannel = fmt.Sprintf("legacy-%d-%d", collection, replica.ReplicaID)
				}
				s.shards[id] = newReplayShardStats(r, shardSegments[channel], rows, candidates(replica.ResourceGroup), pick, states)
			}
		}
	}
	for collection := int64(100); collection < 110; collection++ { // DataView only.
		s.data[collection] = cacheData(collection, fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collection), 5)
	}
	return s
}

func newReplayShardStats(r *rand.Rand, segments []int64, rows map[int64]int64, nodes []int64, pick func([]int64) int64, states []coordview.SegmentState) *coordview.ShardStats {
	upVersion := qviews.QueryViewVersion{DataVersion: qviews.DataVersion{StreamingVersion: 3, CompactVersion: 1}, QueryVersion: 1}
	stats := &coordview.ShardStats{
		Segments:  make(map[int64]*coordview.SegmentStats),
		Resources: make(map[int64]map[coordview.ResourceKey]struct{}),
		UpVersion: &upVersion,
	}
	upSizes := make(map[int64]int64)
	up := &coordview.ViewPlacement{Assignments: make(map[int64]int64), PartitionSizes: map[int64]map[int64]int64{1: upSizes}}
	resident := make(map[int64]struct{})
	for _, segment := range segments {
		known := r.Intn(10) < 7
		segmentStats := &coordview.SegmentStats{SegmentID: segment, PartitionID: 1, LostStats: !known, Nodes: make(map[int64]coordview.SegmentState)}
		if known {
			segmentStats.RowNum = rows[segment]
			segmentStats.LogicalSize = rows[segment]
		}
		for n := 0; n <= r.Intn(2); n++ {
			node := pick(nodes)
			segmentStats.Nodes[node] = states[r.Intn(len(states))]
			resident[node] = struct{}{}
			if r.Intn(2) == 0 {
				if stats.Resources[node] == nil {
					stats.Resources[node] = make(map[coordview.ResourceKey]struct{})
				}
				stats.Resources[node][coordview.ResourceKey{PartitionID: 1, SegmentID: segment, DataVersion: upVersion.DataVersion, LoadInfoVersion: 7}] = struct{}{}
			}
		}
		stats.Segments[segment] = segmentStats
		node := pick(nodes)
		up.Assignments[segment] = node
		if known {
			upSizes[node] += rows[segment]
		} else {
			up.LostStats = append(up.LostStats, segment)
		}
	}
	stats.UpPlacement = up
	for node := range upSizes {
		stats.UpNodes = append(stats.UpNodes, node)
	}
	slices.Sort(stats.UpNodes)
	if r.Intn(2) == 0 {
		preparingVersion := upVersion
		preparingVersion.QueryVersion++
		stats.PreparingVersion = &preparingVersion
		preparingSizes := make(map[int64]int64)
		preparing := &coordview.ViewPlacement{Assignments: make(map[int64]int64), PartitionSizes: map[int64]map[int64]int64{1: preparingSizes}}
		used := make(map[int64]struct{})
		for _, segment := range segments {
			node := pick(nodes)
			preparing.Assignments[segment] = node
			preparingSizes[node] += rows[segment]
			used[node] = struct{}{}
		}
		for node := range used {
			stats.PreparingNodes = append(stats.PreparingNodes, node)
		}
		slices.Sort(stats.PreparingNodes)
		stats.PreparingPlacement = preparing
	}
	for node := range resident {
		stats.ResidentNodes = append(stats.ResidentNodes, node)
	}
	slices.Sort(stats.ResidentNodes)
	return stats
}

// shuffled returns the source keys in a random order.
func shuffled[K comparable, V any](r *rand.Rand, m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	r.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	return keys
}

func (s *replaySources) replay(c *Cache, r *rand.Rand, order []string) {
	for _, source := range order {
		switch source {
		case "nodes":
			for _, id := range shuffled(r, s.nodes) {
				c.PublishNode(id, s.nodes[id])
			}
		case "configs":
			for _, id := range shuffled(r, s.configs) {
				c.PublishLoadConfig(id, s.configs[id], s.versions[id])
			}
		case "data":
			for _, id := range shuffled(r, s.data) {
				c.PublishDataView(id, s.data[id])
			}
		case "shards":
			for _, id := range shuffled(r, s.shards) {
				c.PublishShard(id, s.shards[id])
			}
		}
	}
}

func walkIndex[T any](index immutableIndex[T], fn func([]byte, T)) {
	if index.tree != nil {
		index.tree.Root().Walk(func(key []byte, value interface{}) bool {
			fn(key, value.(T))
			return false
		})
	}
}

func sortedKeys[K int64 | string, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// replayDigest renders every published object, derived index and
// writer-owned slot field. Inputs are shared between compared caches, so
// pointer identities are comparable too.
func replayDigest(c *Cache) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var b strings.Builder
	fmt.Fprintf(&b, "replicas %v\n", c.replicas)
	for _, id := range sortedKeys(c.collections) {
		slot := c.collections[id]
		e := slot.value.Load()
		fmt.Fprintf(&b, "collection %d config=%p version=%d data=%p rows=%v uses=%v replicaUses=%v unknown=%v\n",
			id, e.config, e.configVersion, e.data, slot.rows, slot.uses, slot.replicaUses, slot.unknown)
		walkIndex(e.shards, func(key []byte, s *ShardEntry) {
			fmt.Fprintf(&b, " shard %x %+v stats=%p rows=%v target=%p targetRows=%v up=%v preparing=%v resident=%v\n",
				key, s.id, s.stats, s.rows, s.target, s.targetRows, s.upNodes, s.preparingNodes, s.residentNodes)
		})
		walkIndex(e.placements, func(key []byte, n *PlacementNode) {
			fmt.Fprintf(&b, " placement %x id=%d", key, n.id)
			walkIndex(n.replicas, func(key []byte, f ReplicaFootprint) { fmt.Fprintf(&b, " %x:%+v", key, f) })
			walkIndex(n.resources, func(key []byte, ref resourceReference) { fmt.Fprintf(&b, " %x:%+v", key, ref) })
			b.WriteString("\n")
		})
		walkIndex(e.suspended, func(key []byte, v bool) { fmt.Fprintf(&b, " suspended %x=%v\n", key, v) })
		walkIndex(e.targetLoads, func(key []byte, v targetLoad) { fmt.Fprintf(&b, " load %x=%d\n", key, v.rows) })
	}
	for _, id := range sortedKeys(c.nodes) {
		n := c.nodes[id].value.Load()
		fmt.Fprintf(&b, "node %d %+v\n", id, n.info)
		walkIndex(n.contributions, func(key []byte, v nodeContribution) { fmt.Fprintf(&b, " contribution %x %+v\n", key, v) })
		walkIndex(n.targets, func(key []byte, v targetContribution) { fmt.Fprintf(&b, " target %x=%d\n", key, v.rows) })
	}
	for _, name := range sortedKeys(c.groups) {
		g := c.groups[name]
		fmt.Fprintf(&b, "group %q\n", name)
		walkIndex(g.nodes, func(key []byte, v int64) { fmt.Fprintf(&b, " node %d\n", v) })
		walkIndex(g.collections, func(key []byte, v int64) { fmt.Fprintf(&b, " collection %d\n", v) })
		walkIndex(g.demand, func(key []byte, v demandBucket) { fmt.Fprintf(&b, " demand %x %+v\n", key, v) })
	}
	return b.String()
}

// The replay order is a cost choice only: replaying nodes before the other
// sources publishes exactly the state that replaying them last did.
func TestReplayOrderPublishesIdenticalState(t *testing.T) {
	legacy := []string{"configs", "data", "shards", "nodes"}
	nodesFirst := []string{"nodes", "configs", "data", "shards"}
	var preparingSelected, upFallback, targetRows int64
	for seed := int64(1); seed <= 20; seed++ {
		sources := newReplaySources(seed)
		reference := New(nil)
		sources.replay(reference, rand.New(rand.NewSource(seed)), legacy)
		want := replayDigest(reference)
		for shuffle := int64(0); shuffle < 4; shuffle++ {
			for _, order := range [][]string{legacy, nodesFirst} {
				c := New(nil)
				sources.replay(c, rand.New(rand.NewSource(seed*100+shuffle)), order)
				require.Equal(t, want, replayDigest(c), "seed %d shuffle %d order %v", seed, shuffle, order)
			}
		}

		reference.RangeCollectionIDs(func(id int64) bool {
			reference.GetCollection(id).RangeShards(func(s *ShardEntry) bool {
				switch {
				case s.Target() != nil && s.Target() == s.Stats().PreparingPlacement:
					preparingSelected++
				case s.Target() != nil && s.Stats().PreparingPlacement != nil:
					upFallback++
				}
				return true
			})
			return true
		})
		reference.RangeNodeIDs(func(id int64) bool {
			targetRows += reference.GetNode(id).TargetSize()
			return true
		})
	}
	// The data must exercise target selection, not only empty targets.
	require.Positive(t, preparingSelected)
	require.Positive(t, upFallback)
	require.Positive(t, targetRows)
}

type orderedNodePublisher struct {
	order *[]string
	nodes map[int64]*api.NodeInfo
}

func (p orderedNodePublisher) RegisterNodeListener(listener NodeListener) func() {
	*p.order = append(*p.order, "nodes")
	for id, info := range p.nodes {
		listener(id, info)
	}
	return func() {}
}

type orderedDataPublisher struct {
	order *[]string
	data  map[int64]*api.CollectionDataView
}

func (p orderedDataPublisher) RegisterDataViewListener(listener api.DataViewListener) func() {
	*p.order = append(*p.order, "data")
	for id, data := range p.data {
		listener(id, data)
	}
	return func() {}
}

type emptyQueryViewCatalog struct{}

func (emptyQueryViewCatalog) ListQueryViews(context.Context) ([]*viewpb.QueryViewOfShard, error) {
	return nil, nil
}

func (emptyQueryViewCatalog) SaveQueryViews(context.Context, []*viewpb.QueryViewOfShard) error {
	return nil
}

type idleSyncer struct{}

func (idleSyncer) SyncViews(context.Context, syncer.SyncGroup) error { return nil }
func (idleSyncer) Close() error                                      { return nil }

func TestNewFromSourcesReplaysNodesFirst(t *testing.T) {
	ctx := context.Background()
	configs, err := loadmgr.RecoverLoadConfigStore(ctx, loadmgrtest.Dependencies(t))
	require.NoError(t, err)
	require.NoError(t, configs.Put(ctx, &loadmgr.LoadConfig{
		CollectionID: 1,
		PartitionIDs: []int64{1},
		Replicas:     []*loadmgr.ReplicaAssignment{{ReplicaID: 10, ResourceGroup: "rg1"}},
	}))
	registry, err := coordview.RecoverShardViewRegistry(ctx, emptyQueryViewCatalog{}, idleSyncer{})
	require.NoError(t, err)
	t.Cleanup(registry.Close)

	var order []string
	nodes := orderedNodePublisher{order: &order, nodes: map[int64]*api.NodeInfo{1: {NodeID: 1, Alive: true, ResourceGroup: "rg1"}}}
	data := orderedDataPublisher{order: &order, data: map[int64]*api.CollectionDataView{1: cacheData(1, cacheShard(1, 10).VChannel, 5)}}
	c := NewFromSources(nil, configs, data, registry, nodes)
	t.Cleanup(c.Close)
	require.Equal(t, []string{"nodes", "data"}, order)
	require.True(t, c.Ready())
	require.True(t, c.GetNode(1).Info().Alive)
	require.NotNil(t, c.GetCollection(1).LoadConfig())
	require.NotNil(t, c.GetCollection(1).DataView())
}
