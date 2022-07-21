package querycoord

import (
	"sort"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

type Balancer interface {
	AddNode(nodeID int64) ([]*balancePlan, error)
	RemoveNode(nodeID int64) []*balancePlan
	Rebalance() []*balancePlan
}

// Plan for adding/removing node from replica,
// adds node into targetReplica,
// removes node from sourceReplica.
// Set the replica ID to invalidReplicaID to avoid adding/removing into/from replica
type balancePlan struct {
	nodes         []UniqueID
	sourceReplica UniqueID
	targetReplica UniqueID
}

// NewAddBalancePlan creates plan for adding nodes into dest replica
func NewAddBalancePlan(dest UniqueID, nodes ...UniqueID) *balancePlan {
	return NewMoveBalancePlan(invalidReplicaID, dest, nodes...)
}

// NewRemoveBalancePlan creates plan for removing nodes from src replica
func NewRemoveBalancePlan(src UniqueID, nodes ...UniqueID) *balancePlan {
	return NewMoveBalancePlan(src, invalidReplicaID, nodes...)
}

// NewMoveBalancePlan creates plan for moving nodes from src replica into dest replicas
func NewMoveBalancePlan(src, dest UniqueID, nodes ...UniqueID) *balancePlan {
	return &balancePlan{
		nodes:         nodes,
		sourceReplica: src,
		targetReplica: dest,
	}
}

type replicaBalancer struct {
	meta    Meta
	cluster Cluster
}

func newReplicaBalancer(meta Meta, cluster Cluster) *replicaBalancer {
	return &replicaBalancer{meta, cluster}
}

func (b *replicaBalancer) AddNode(nodeID int64) ([]*balancePlan, error) {
	// allocate this node to all collections replicas
	var ret []*balancePlan
	collections := b.meta.showCollections()
	for _, c := range collections {
		replicas, err := b.meta.getReplicasByCollectionID(c.GetCollectionID())
		if err != nil {
			log.Error("find collection ID with no replica info", zap.Int64("collectionID", c.GetCollectionID()))
			return nil, err
		}
		if len(replicas) == 0 {
			continue
		}

		foundNode := false
		for _, replica := range replicas {
			for _, replicaNode := range replica.NodeIds {
				if replicaNode == nodeID {
					foundNode = true
					break
				}
			}

			if foundNode {
				break
			}
		}

		// This node is serving this collection
		if foundNode {
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

		ret = append(ret,
			NewAddBalancePlan(replicas[0].GetReplicaID(), nodeID))
	}
	return ret, nil
}

func (b *replicaBalancer) RemoveNode(nodeID int64) []*balancePlan {
	// for this version, querynode does not support move from a replica to another
	return nil
}

func (b *replicaBalancer) Rebalance() []*balancePlan {
	return nil
}
