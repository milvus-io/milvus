package querycoordv2

import (
	"context"
	"slices"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

// qviewsReplicaSnapshot adapts QV-owned facts to the existing management DTO.
// No ReplicaManager state is read or updated. Nodes are retained View references,
// including handoff cleanup, rather than a second node-placement authority.
func (s *Server) qviewsReplicaSnapshot(collectionID int64) []*meta.Replica {
	cfg := s.qviewsRuntime.loadConfigStore.GetConfig(collectionID)
	if cfg == nil {
		return nil
	}
	nodes := make(map[int64][]int64)
	for shard, stats := range s.qviewsRuntime.shardViewRegistry.SnapshotForCollection(collectionID).StatsMap() {
		nodes[shard.ReplicaID] = append(nodes[shard.ReplicaID], stats.ResidentNodes...)
	}
	result := make([]*meta.Replica, 0, len(cfg.Replicas))
	for _, r := range cfg.Replicas {
		members := nodes[r.ReplicaID]
		slices.Sort(members)
		result = append(result, meta.NewReplicaWithPriority(&querypb.Replica{
			ID: r.ReplicaID, CollectionID: collectionID, ResourceGroup: r.ResourceGroup,
			Nodes: slices.Compact(members),
		}, r.Priority))
	}
	return result
}

// qviewsReplicaErrors checks the complete expected channel set at the current
// load version. A config-only replica or an old Up view cannot certify readiness.
func (s *Server) qviewsReplicaErrors(ctx context.Context, collectionID int64) map[int64]error {
	runtime := s.qviewsRuntime
	cfg, version := runtime.loadConfigStore.GetConfigWithVersion(collectionID)
	result := make(map[int64]error)
	if cfg == nil {
		return result
	}
	channels, err := runtime.resources.VChannels(ctx, collectionID)
	stats := runtime.shardViewRegistry.SnapshotForCollection(collectionID).StatsMap()
	for _, replica := range cfg.Replicas {
		if err != nil {
			result[replica.ReplicaID] = err
			continue
		}
		if len(channels) == 0 {
			result[replica.ReplicaID] = merr.WrapErrServiceNotReadyMsg("collection %d has no channel metadata", collectionID)
			continue
		}
		for _, channel := range channels {
			stat := stats[qviews.ShardID{VChannel: channel, ReplicaID: replica.ReplicaID}]
			if stat == nil || stat.UpVersion == nil || stat.UpLoadInfoVersion != version {
				result[replica.ReplicaID] = merr.WrapErrServiceNotReadyMsg("replica %d has no current Up view for channel %s", replica.ReplicaID, channel)
				break
			}
		}
	}
	current, currentVersion := runtime.loadConfigStore.GetConfigWithVersion(collectionID)
	if current != cfg || currentVersion != version {
		for _, replica := range cfg.Replicas {
			result[replica.ReplicaID] = merr.WrapErrServiceNotReadyMsg("collection %d load config changed while reading replicas", collectionID)
		}
	}
	return result
}

func (s *Server) qviewsReplicasJSON(ctx context.Context) (string, error) {
	result := make([]*metricsinfo.Replica, 0)
	for id, cfg := range s.qviewsRuntime.loadConfigStore.Snapshot().ConfigsMap() {
		for _, r := range s.qviewsReplicaSnapshot(id) {
			result = append(result, &metricsinfo.Replica{
				ID: r.GetID(), CollectionID: id, DatabaseID: cfg.DbID,
				ResourceGroup: r.GetResourceGroup(), RWNodes: r.GetNodes(),
			})
		}
	}
	value, err := json.Marshal(result)
	if err != nil {
		return "", merr.Wrap(err, "encode QueryView replica metrics")
	}
	return string(value), nil
}

// qviewsResourceGroupStats describes desired replicas and nodes still retained
// across RG moves. Deduplicate a node across all channels of the same replica.
func (s *Server) qviewsResourceGroupStats(group string) (loaded, outgoing, incoming map[int64]int32) {
	loaded, outgoing, incoming = make(map[int64]int32), make(map[int64]int32), make(map[int64]int32)
	for id, cfg := range s.qviewsRuntime.loadConfigStore.Snapshot().ConfigsMap() {
		desired := make(map[int64]string, len(cfg.Replicas))
		for _, r := range cfg.Replicas {
			desired[r.ReplicaID] = r.ResourceGroup
			if r.ResourceGroup == group {
				loaded[id]++
			}
		}
		seen := make(map[[2]int64]bool)
		for shard, stats := range s.qviewsRuntime.shardViewRegistry.SnapshotForCollection(id).StatsMap() {
			target, ok := desired[shard.ReplicaID]
			if !ok {
				continue
			}
			for _, node := range stats.ResidentNodes {
				key := [2]int64{shard.ReplicaID, node}
				if seen[key] {
					continue
				}
				seen[key] = true
				actual := s.meta.GetResourceGroupByNodeID(node)
				if target == group && actual != group {
					outgoing[id]++
				}
				if target != group && actual == group {
					incoming[id]++
				}
			}
		}
	}
	return
}
