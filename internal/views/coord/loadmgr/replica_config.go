package loadmgr

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ReplicaConfigResources supplies ID allocation and resource-group existence;
// configuration construction and application remain owned by the load manager.
type ReplicaConfigResources interface {
	AllocateReplicaID(context.Context) (int64, error)
	ContainResourceGroup(context.Context, string) bool
}

// UpdateReplicaConfig reconciles replica intent under the caller's collection
// or database resource guard. It never reacquires that guard or broadcasts.
// A watcher must reread its cluster settings under the guard before calling.
func (m *CollectionLoadManager) UpdateReplicaConfig(ctx context.Context, collectionID int64, replicas int32, groups []string) error {
	return m.updateReplicaConfig(ctx, collectionID, replicas, groups, false)
}

// UpdateClusterReplicaConfig enrolls only new or moved assignments in the destination group's next admission.
func (m *CollectionLoadManager) UpdateClusterReplicaConfig(ctx context.Context, collectionID int64, replicas int32, groups []string) error {
	return m.updateReplicaConfig(ctx, collectionID, replicas, groups, true)
}

func (m *CollectionLoadManager) updateReplicaConfig(ctx context.Context, collectionID int64, replicas int32, groups []string, cluster bool) error {
	current := m.store.GetConfig(collectionID)
	if current == nil {
		return nil
	}
	if replicas == 0 {
		replicas = int32(len(current.Replicas))
	}
	if replicas < 0 {
		return merr.WrapErrParameterInvalidMsg("replica number must be positive")
	}
	actual := make(map[string]int)
	for _, replica := range current.Replicas {
		actual[replica.ResourceGroup]++
	}
	if len(groups) == 0 {
		if int(replicas) == len(current.Replicas) {
			return nil
		}
		groups = slices.Sorted(maps.Keys(actual))
	}
	expected, err := ReplicaNumberByResourceGroup(groups, replicas)
	if err != nil {
		return err
	}
	if maps.Equal(actual, expected) {
		return nil
	}
	for group := range expected {
		if !m.replicas.ContainResourceGroup(ctx, group) {
			return merr.WrapErrResourceGroupNotFound(group)
		}
	}
	next := current.Clone()
	slices.SortFunc(next.Replicas, func(a, b *ReplicaAssignment) int { return cmp.Compare(a.ReplicaID, b.ReplicaID) })
	priority := next.Replicas[0].Priority
	kept := make([]*ReplicaAssignment, 0, replicas)
	spare := make([]*ReplicaAssignment, 0)
	assigned := make(map[string]int)
	for _, replica := range next.Replicas {
		if assigned[replica.ResourceGroup] < expected[replica.ResourceGroup] {
			kept = append(kept, replica)
			assigned[replica.ResourceGroup]++
		} else {
			spare = append(spare, replica)
		}
	}
	retained := len(kept)
	for _, group := range slices.Sorted(maps.Keys(expected)) {
		for i := assigned[group]; i < expected[group]; i++ {
			var replica *ReplicaAssignment
			if len(spare) > 0 {
				replica, spare = spare[0], spare[1:]
			} else {
				id, err := m.replicas.AllocateReplicaID(ctx)
				if err != nil {
					return err
				}
				replica = &ReplicaAssignment{ReplicaID: id}
			}
			replica.ResourceGroup, replica.Priority = group, priority
			kept = append(kept, replica)
		}
	}
	next.Replicas = kept
	var newPlacements []*ReplicaAssignment
	if cluster {
		newPlacements = kept[retained:]
	}
	if err := m.store.put(ctx, next, newPlacements); err != nil {
		return err
	}
	m.notifyCollection(collectionID)
	return nil
}

// ReplicaNumberByResourceGroup resolves the count associated with each group.
func ReplicaNumberByResourceGroup(resourceGroups []string, replicaNumber int32) (map[string]int, error) {
	if len(resourceGroups) != 0 && len(resourceGroups) != 1 && len(resourceGroups) != int(replicaNumber) {
		return nil, merr.WrapErrParameterInvalidMsg("replica=[%d] resource group=[%s], resource group num can only be 0, 1 or same as replica number", replicaNumber, strings.Join(resourceGroups, ","))
	}
	replicaNumInRG := make(map[string]int)
	if len(resourceGroups) == 0 {
		// All replicas should be spawned in default resource group.
		replicaNumInRG[common.DefaultResourceGroupName] = int(replicaNumber)
	} else if len(resourceGroups) == 1 {
		// All replicas should be spawned in the given resource group.
		replicaNumInRG[resourceGroups[0]] = int(replicaNumber)
	} else {
		// replicas should be spawned in different resource groups one by one.
		for _, rgName := range resourceGroups {
			replicaNumInRG[rgName] += 1
		}
	}
	return replicaNumInRG, nil
}
