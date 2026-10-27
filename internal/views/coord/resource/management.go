package resource

import (
	"context"
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadstatus"
	"github.com/milvus-io/milvus/internal/views/coord/readiness"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (r *Resources) placementManagement() (balancer.Management, error) {
	manager, ok := r.Balancer.(balancer.Management)
	if !ok {
		return nil, merr.Wrap(merr.ErrServiceUnimplemented, "QueryView balancer does not support management operations")
	}
	return manager, nil
}

// Replicas reports desired replica identities and actual serving shard coverage.
// A configured but unprepared shard is never advertised as serving.
func (r *Resources) Replicas(ctx context.Context, collectionID int64, withShardNodes bool) ([]*milvuspb.ReplicaInfo, error) {
	if err := r.checkActive(ctx); err != nil {
		return nil, err
	}
	manager, err := r.placementManagement()
	if err != nil {
		return nil, err
	}
	placement, err := manager.Placement(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	if placement.Config == nil {
		return nil, nil
	}
	channels, err := r.VChannels(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	result := make([]*milvuspb.ReplicaInfo, 0, len(placement.Config.Replicas))
	for _, replica := range placement.Config.Replicas {
		info := &milvuspb.ReplicaInfo{
			ReplicaID: replica.ReplicaID, CollectionID: collectionID,
			PartitionIds: slices.Clone(placement.Config.PartitionIDs), ResourceGroupName: replica.ResourceGroup,
			NodeIds: slices.Clone(placement.Nodes[replica.ReplicaID]), NumOutboundNode: make(map[string]int32),
		}
		outbound := make(map[int64]bool)
		for _, channel := range channels {
			stat := placement.Shards[qviews.ShardID{ReplicaID: replica.ReplicaID, VChannel: channel}]
			if stat == nil || stat.UpVersion == nil {
				continue
			}
			shard := &milvuspb.ShardReplica{DmChannelName: channel}
			if withShardNodes {
				shard.NodeIds = slices.Clone(stat.UpNodes)
			}
			info.ShardReplicas = append(info.ShardReplicas, shard)
			for _, node := range stat.UpNodes {
				info.NodeIds = append(info.NodeIds, node)
				if outbound[node] {
					continue
				}
				outbound[node] = true
				if entry := r.BalanceCache.GetNode(node); entry != nil && entry.Info().ResourceGroup != replica.ResourceGroup {
					info.NumOutboundNode[entry.Info().ResourceGroup]++
				}
			}
		}
		slices.Sort(info.NodeIds)
		info.NodeIds = slices.Compact(info.NodeIds)
		result = append(result, info)
	}
	current, version := r.LoadConfigs.GetConfigWithVersion(collectionID)
	if current != placement.Config || version != placement.ConfigVersion {
		return nil, merr.WrapErrServiceUnavailableMsg("load configuration changed while reading replicas")
	}
	return result, nil
}

// Balance preserves the synchronous LoadBalance contract. Configuration locks
// cover only acceptance, never the wait for node preparation/handoff.
func (r *Resources) Balance(ctx context.Context, collectionID, source int64, destinations, segments []int64) error {
	if err := r.checkActive(ctx); err != nil {
		return err
	}
	manager, err := r.placementManagement()
	if err != nil {
		return err
	}
	channels, err := r.VChannels(ctx, collectionID)
	if err != nil {
		return err
	}
	progress := loadstatus.Get(r.LoadConfigs, r.ShardViews, collectionID, channels)
	if !progress.Ready() {
		return merr.WrapErrCollectionNotFullyLoaded(collectionID)
	}
	subscription := r.Readiness.Subscribe(collectionID)
	defer subscription.Close()
	var moves map[qviews.ShardID]*balancer.SegmentMove
	err = r.LoadConfigs.WithCurrentConfig(progress.Config, func() error {
		var err error
		moves, err = manager.MoveSegments(ctx, progress.Config, source, destinations, segments)
		return err
	})
	if err != nil {
		return err
	}
	return r.waitMoves(ctx, progress.Config, subscription, moves)
}

func (r *Resources) waitMoves(ctx context.Context, cfg *loadmgr.LoadConfig, subscription *readiness.Subscription, moves map[qviews.ShardID]*balancer.SegmentMove) error {
	for {
		if err := r.checkActive(ctx); err != nil {
			return err
		}
		changed, released := subscription.Observe()
		if released || r.LoadConfigs.GetConfig(cfg.CollectionID) != cfg {
			return merr.WrapErrServiceUnavailableMsg("collection was released or reconfigured during manual balance")
		}
		complete := true
		for id, move := range moves {
			manager := r.ShardViews.Get(id)
			if manager == nil {
				return merr.WrapErrServiceUnavailableMsg("shard was released during manual balance")
			}
			stat := manager.Stats()
			if stat.UpVersion == nil || !stat.UpVersion.GTE(move.Version) {
				if stat.PreparingVersion == nil {
					return merr.WrapErrServiceUnavailableMsg("manual balance preparation failed or was superseded")
				}
				complete = false
				continue
			}
			for segment, node := range move.Destinations {
				if stat.UpPlacement == nil || stat.UpPlacement.Assignments[segment] != node {
					return merr.WrapErrServiceUnavailableMsg("manual balance placement was superseded for segment %d", segment)
				}
			}
		}
		if _, released := subscription.Observe(); released || r.LoadConfigs.GetConfig(cfg.CollectionID) != cfg {
			return merr.WrapErrServiceUnavailableMsg("collection was released or reconfigured during manual balance")
		}
		if complete {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-r.Readiness.Done():
			return merr.WrapErrServiceUnavailableMsg("QueryView coordinator stopped during manual balance")
		case <-changed:
		}
	}
}
