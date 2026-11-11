package querycoordv2

import (
	"context"
	"maps"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// updateLoadConfig applies placement under the same resource guards as Load.
// Cluster reconciliation resolves defaults and user intent under the collection
// lock, so a concurrent release cannot be undone by a stale watcher snapshot.
func (s *Server) updateLoadConfig(ctx context.Context, ids []int64, replicas int32, groups []string) error {
	var result error
	for _, id := range ids {
		if err := s.updateCollectionLoadConfig(ctx, id, replicas, groups, false); err != nil {
			result = merr.Combine(result, err)
		}
	}
	return result
}

func (s *Server) updateCollectionLoadConfig(ctx context.Context, id int64, replicas int32, groups []string, clusterDefault bool) error {
	// Most periodic checks are no-ops. Avoid metadata RPCs and a broadcast lock
	// for those collections; a possible change is rechecked after serialization.
	if clusterDefault {
		if _, _, changed := clusterDefaultPlacementChange(s.qviewsRuntime.loadConfigStore.GetConfig(id)); !changed {
			return nil
		}
	}
	broadcaster, err := s.startBroadcastWithCollectionIDLock(ctx, id)
	if err != nil {
		return err
	}
	defer broadcaster.Close()
	current := s.qviewsRuntime.loadConfigStore.GetConfig(id)
	if current == nil {
		return nil
	}
	if clusterDefault {
		var changed bool
		replicas, groups, changed = clusterDefaultPlacementChange(current)
		if !changed {
			return nil
		}
	}
	// Only borrow the resource guard: configuration reconciliation does not
	// create another WAL operation or wait for QueryView resource preparation.
	if err := s.qviewsRuntime.loadManager.UpdateReplicaConfig(ctx, id, replicas, groups); err != nil {
		return err
	}
	meta.GlobalFailedLoadCache.Remove(id)
	return nil
}

// clusterDefaultPlacementChange uses only immutable local load metadata. Always
// repeat it under the collection lock before publishing a changed placement.
func clusterDefaultPlacementChange(current *loadmgr.LoadConfig) (int32, []string, bool) {
	if current == nil {
		return 0, nil, false
	}
	cfg := &paramtable.Get().QueryCoordCfg
	if current.UserSpecifiedReplicaMode && !cfg.ClusterLevelLoadForceOverrideUserReplicaMode.GetAsBool() {
		return 0, nil, false
	}
	replicas := cfg.ClusterLevelLoadReplicaNumber.GetAsInt32()
	groups := cfg.ClusterLevelLoadResourceGroups.GetAsStrings()
	// Independently refreshed settings can temporarily be incomplete.
	if replicas <= 0 || len(groups) == 0 || (len(groups) != 1 && len(groups) != int(replicas)) {
		return 0, nil, false
	}
	expected, _ := utils.ReplicaNumberByResourceGroup(groups, replicas)
	actual := make(map[string]int)
	for _, replica := range current.Replicas {
		actual[replica.ResourceGroup]++
	}
	return replicas, groups, !maps.Equal(actual, expected)
}
