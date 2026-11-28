package loadmgr

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
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
	current := m.store.GetConfig(collectionID)
	if current == nil {
		return nil
	}
	next, err := m.prepareReplicaConfig(ctx, current, replicas, groups)
	if err != nil || next == nil {
		return err
	}
	return m.apply(ctx, next)
}

func (m *CollectionLoadManager) prepareReplicaConfig(ctx context.Context, current *LoadConfig, replicas int32, groups []string) (*LoadConfig, error) {
	if replicas == 0 {
		replicas = int32(len(current.Replicas))
	}
	if replicas < 0 {
		return nil, merr.WrapErrParameterInvalidMsg("replica number must be positive")
	}
	actual := make(map[string]int)
	for _, replica := range current.Replicas {
		actual[replica.ResourceGroup]++
	}
	if len(groups) == 0 {
		if int(replicas) == len(current.Replicas) {
			return nil, nil
		}
		groups = slices.Sorted(maps.Keys(actual))
	}
	expected, err := ReplicaNumberByResourceGroup(groups, replicas)
	if err != nil {
		return nil, err
	}
	if maps.Equal(actual, expected) {
		return nil, nil
	}
	for group := range expected {
		if !m.replicas.ContainResourceGroup(ctx, group) {
			return nil, merr.WrapErrResourceGroupNotFound(group)
		}
	}
	sorted := current.SortedReplicas()
	updated, err := ReconcileReplicas(sorted, expected, sorted[0].Priority, func() (int64, error) {
		return m.replicas.AllocateReplicaID(ctx)
	})
	if err != nil {
		return nil, err
	}
	next := *current
	next.Replicas = updated
	return &next, nil
}

// SortedReplicas returns the immutable assignments in stable ID order.
func (c *LoadConfig) SortedReplicas() []*ReplicaAssignment {
	if c == nil {
		return nil
	}
	replicas := slices.Clone(c.Replicas)
	slices.SortFunc(replicas, compareReplicaID)
	return replicas
}

func compareReplicaID(a, b *ReplicaAssignment) int { return cmp.Compare(a.ReplicaID, b.ReplicaID) }

// ReconcileReplicas retains the lowest IDs in each resource group and assigns
// fresh IDs to deficits. The caller supplies current assignments in ID order.
func ReconcileReplicas(current []*ReplicaAssignment, expected map[string]int, priority commonpb.LoadPriority, allocate func() (int64, error)) ([]*ReplicaAssignment, error) {
	assigned := make(map[string]int, len(expected))
	var replicas []*ReplicaAssignment
	for _, r := range current {
		if assigned[r.ResourceGroup] < expected[r.ResourceGroup] {
			replicas = append(replicas, r)
			assigned[r.ResourceGroup]++
		}
	}
	for _, group := range slices.Sorted(maps.Keys(expected)) {
		for i := assigned[group]; i < expected[group]; i++ {
			id, err := allocate()
			if err != nil {
				return nil, err
			}
			replicas = append(replicas, &ReplicaAssignment{ReplicaID: id, ResourceGroup: group, Priority: priority})
		}
	}
	slices.SortFunc(replicas, compareReplicaID)
	return replicas, nil
}

func ReplicasToMessage(replicas []*ReplicaAssignment) []*messagespb.LoadReplicaConfig {
	out := make([]*messagespb.LoadReplicaConfig, 0, len(replicas))
	for _, r := range replicas {
		out = append(out, &messagespb.LoadReplicaConfig{ReplicaId: r.ReplicaID, ResourceGroupName: r.ResourceGroup, Priority: r.Priority})
	}
	return out
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
