package balancer

import (
	"context"
	"slices"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Management exposes the same replica ownership and lifecycle used by automatic
// scheduling. Manual migration is a one-time placement change, not a node pin.
type Management interface {
	Placement(context.Context, int64) (*CollectionPlacement, error)
	MoveSegments(context.Context, *loadmgr.LoadConfig, int64, []int64, []int64) (map[qviews.ShardID]*SegmentMove, error)
}

type CollectionPlacement struct {
	Config        *loadmgr.LoadConfig
	ConfigVersion uint64
	Nodes         map[int64][]int64
	Shards        map[qviews.ShardID]*coordview.ShardStats
	LayoutReady   bool
}

type SegmentMove struct {
	Version      qviews.QueryViewVersion
	Destinations map[int64]int64
}

func (b *DefaultBalancer) Placement(ctx context.Context, collectionID int64) (*CollectionPlacement, error) {
	if err := b.cache.WaitForReady(ctx); err != nil {
		return nil, err
	}
	b.reconcileMu.Lock()
	defer b.reconcileMu.Unlock()
	return b.placementLocked(collectionID)
}

func (b *DefaultBalancer) placementLocked(collectionID int64) (*CollectionPlacement, error) {
	c := b.cache.GetCollection(collectionID)
	result := &CollectionPlacement{Nodes: make(map[int64][]int64), Shards: make(map[qviews.ShardID]*coordview.ShardStats), LayoutReady: true}
	if c == nil || c.LoadConfig() == nil {
		return result, nil
	}
	result.Config, result.ConfigVersion = c.LoadConfig(), c.ConfigVersion()
	policy, ok := b.policy.(*DefaultBalancePolicy)
	if !ok {
		return nil, merr.Wrap(merr.ErrServiceUnimplemented, "balance policy does not expose replica placement")
	}
	policy.mu.Lock()
	defer policy.mu.Unlock()
	reader := newPlanningContext(b.cache)
	for _, replica := range result.Config.Replicas {
		layout := policy.layouts.domains[collectionID][replica.ResourceGroup]
		if layout == nil {
			result.LayoutReady = false
			continue
		}
		nodes := reader.CandidateNodes(replica.ResourceGroup)
		slices.Sort(nodes)
		var replicas []int64
		for _, r := range result.Config.Replicas {
			if r.ResourceGroup == replica.ResourceGroup {
				replicas = append(replicas, r.ReplicaID)
			}
		}
		slices.Sort(replicas)
		if !slices.Equal(nodes, layout.nodes) || !slices.Equal(replicas, layout.replicas) {
			result.LayoutReady = false
			continue
		}
		result.Nodes[replica.ReplicaID] = slices.Clone(layout.targets[replica.ReplicaID])
	}
	c.RangeShards(func(s *cache.ShardEntry) bool { result.Shards[s.ID()] = s.Stats(); return true })
	return result, nil
}

// MoveSegments validates the complete request before accepting any views. The
// caller serializes load/release with this acceptance, then waits without locks.
func (b *DefaultBalancer) MoveSegments(ctx context.Context, cfg *loadmgr.LoadConfig, source int64, destinations, segments []int64) (map[qviews.ShardID]*SegmentMove, error) {
	if err := b.cache.WaitForReady(ctx); err != nil {
		return nil, err
	}
	b.reconcileMu.Lock()
	defer b.reconcileMu.Unlock()
	pl, err := b.placementLocked(cfg.CollectionID)
	if err != nil {
		return nil, err
	}
	if pl.Config != cfg {
		return nil, merr.WrapErrServiceUnavailableMsg("load configuration changed during manual balance")
	}
	if !pl.LayoutReady {
		return nil, merr.WrapErrServiceUnavailableMsg("replica placement is changing")
	}
	var replica *loadmgr.ReplicaAssignment
	for _, r := range cfg.Replicas {
		if slices.Contains(pl.Nodes[r.ReplicaID], source) {
			replica = r
			break
		}
	}
	if replica == nil {
		return nil, merr.WrapErrNodeNotFound(source, "source node not found in any replica")
	}
	if len(destinations) == 0 {
		destinations = pl.Nodes[replica.ReplicaID]
	}
	eligible := make([]int64, 0, len(destinations))
	for _, node := range destinations {
		if !slices.Contains(pl.Nodes[replica.ReplicaID], node) {
			return nil, merr.WrapErrNodeNotFound(node, "destination node not found in the same replica")
		}
		if node != source {
			eligible = append(eligible, node)
		}
	}
	slices.Sort(eligible)
	eligible = slices.Compact(eligible)
	if len(eligible) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("no destination query node other than source")
	}
	data := b.cache.GetCollection(cfg.CollectionID).DataView()
	if data == nil {
		return nil, merr.WrapErrServiceUnavailableMsg("DataView is not ready for manual balance")
	}
	sources := make(map[int64]qviews.ShardID)
	for _, shard := range data.Shards {
		id := qviews.ShardID{ReplicaID: replica.ReplicaID, VChannel: shard.VChannel}
		stat := pl.Shards[id]
		if stat == nil || stat.UpVersion == nil || stat.UpLoadInfoVersion != pl.ConfigVersion {
			return nil, merr.WrapErrCollectionNotFullyLoaded(cfg.CollectionID)
		}
		if stat.PreparingVersion != nil || stat.UpVersion.DataVersion != data.DataVersion {
			return nil, merr.WrapErrServiceUnavailableMsg("QueryView handoff is in progress; retry manual balance")
		}
		if stat.UpPlacement != nil {
			for segment, node := range stat.UpPlacement.Assignments {
				if node == source {
					sources[segment] = id
				}
			}
		}
	}
	if len(segments) == 0 {
		for segment := range sources {
			segments = append(segments, segment)
		}
	}
	segments = slices.Clone(segments)
	slices.Sort(segments)
	segments = slices.Compact(segments)
	for _, segment := range segments {
		if _, ok := sources[segment]; !ok {
			return nil, merr.WrapErrSegmentNotFound(segment, "segment not found in source node")
		}
	}
	rows := make(map[int64]int64, len(eligible))
	for _, node := range eligible {
		entry := b.cache.GetNode(node)
		if entry == nil || !entry.Info().Alive || entry.Info().Stopping {
			return nil, merr.WrapErrServiceUnavailableMsg("destination node %d became unavailable", node)
		}
		rows[node] = entry.Info().TargetLogicalSize
	}
	moves := make(map[qviews.ShardID]*SegmentMove)
	for _, segment := range segments {
		id := sources[segment]
		move := moves[id]
		if move == nil {
			move = &SegmentMove{Destinations: make(map[int64]int64)}
			moves[id] = move
		}
		destination := eligible[0]
		for _, node := range eligible {
			if rows[node] < rows[destination] {
				destination = node
			}
		}
		move.Destinations[segment] = destination
		info, ok := data.Segment(segment)
		if !ok {
			return nil, merr.WrapErrServiceUnavailableMsg("segment %d left the DataView during manual balance", segment)
		}
		rows[destination] += info.LogicalSize
	}
	builders := make(map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder, len(moves))
	for id, move := range moves {
		stat := pl.Shards[id]
		assignments := make(map[int64]map[int64][]int64)
		for segment, node := range stat.UpPlacement.Assignments {
			if destination, ok := move.Destinations[segment]; ok {
				node = destination
			}
			info, ok := data.Segment(segment)
			if !ok {
				return nil, merr.WrapErrServiceUnavailableMsg("segment %d left the DataView during manual balance", segment)
			}
			if assignments[node] == nil {
				assignments[node] = make(map[int64][]int64)
			}
			assignments[node][info.PartitionID] = append(assignments[node][info.PartitionID], segment)
		}
		builders[id] = qviews.NewQueryViewAtCoordBuilder(id.ReplicaID, syntheticDataView(cfg, data.DataVersion, data.Shard(id.VChannel)), id.VChannel).
			SetAssignments(assignments).SetLoadInfoVersion(pl.ConfigVersion).SetSchemaVersion(stat.UpSchemaVersion)
	}
	for id, builder := range builders {
		// A concurrent data/schema change is handled by normal reconciliation;
		// never manufacture a stale successful migration confirmation.
		manager := b.viewRegistry.Get(id)
		if manager == nil {
			return nil, merr.WrapErrServiceUnavailableMsg("QueryView was released during manual balance")
		}
		if err := manager.AddPreparing(ctx, builder); err != nil {
			return nil, err
		}
		moves[id].Version = qviews.FromProtoQueryViewVersion(builder.Build().GetMeta().GetVersion())
	}
	return moves, nil
}
