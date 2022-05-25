package querycoord

import "sort"

type balancer interface {
	addNode(nodeID int64) ([]*balancePlan, error)
	removeNode(nodeID int64) []*balancePlan
	rebalance() []*balancePlan
}

type balancePlan struct {
	nodeID        int64
	sourceReplica int64
	targetReplica int64
}

type replicaBalancer struct {
	meta    Meta
	cluster Cluster
}

func newReplicaBalancer(meta Meta, cluster Cluster) *replicaBalancer {
	return &replicaBalancer{meta, cluster}
}

func (b *replicaBalancer) addNode(nodeID int64) ([]*balancePlan, error) {
	// allocate this node to all collections replicas
	var ret []*balancePlan
	collections := b.meta.showCollections()
	for _, c := range collections {
		replicas, err := b.meta.getReplicasByCollectionID(c.GetCollectionID())
		if err != nil {
			return nil, err
		}
		if len(replicas) == 0 {
			continue
		}

		replicaAvailableMemory := make(map[UniqueID]uint64, len(replicas))
		for _, replica := range replicas {
			replicaAvailableMemory[replica.ReplicaID] = getReplicaAvailableMemory(b.cluster, replica)
		}
		sort.Slice(replicas, func(i, j int) bool {
			replicai := replicas[i].ReplicaID
			replicaj := replicas[j].ReplicaID

			return replicaAvailableMemory[replicai] < replicaAvailableMemory[replicaj]
		})

		ret = append(ret, &balancePlan{
			nodeID:        nodeID,
			sourceReplica: invalidReplicaID,
			targetReplica: replicas[0].GetReplicaID(),
		})
	}
	return ret, nil
}

func (b *replicaBalancer) removeNode(nodeID int64) []*balancePlan {
	// for this version, querynode does not support move from a replica to another
	return nil
}

func (b *replicaBalancer) rebalance() []*balancePlan {
	return nil
}
