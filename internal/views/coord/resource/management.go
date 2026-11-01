package resource

import (
	"context"
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
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
