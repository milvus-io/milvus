// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

var (
	ErrGetNodesFromRG       = errors.New("failed to get node from rg")
	ErrNoReplicaFound       = errors.New("no replica found during assign nodes")
	ErrReplicasInconsistent = errors.New("all replicas should belong to same collection during assign nodes")
	ErrUseWrongNumRG        = errors.New("resource group num can only be 0, 1 or same as replica number")
)

func GetReplicaNodesInfo(replicaMgr *meta.ReplicaManager, nodeMgr *session.NodeManager, replicaID int64) []*session.NodeInfo {
	replica := replicaMgr.Get(replicaID)
	if replica == nil {
		return nil
	}

	nodes := make([]*session.NodeInfo, 0, len(replica.GetNodes()))
	for _, node := range replica.GetNodes() {
		nodes = append(nodes, nodeMgr.Get(node))
	}
	return nodes
}

func GetPartitions(collectionMgr *meta.CollectionManager, collectionID int64) ([]int64, error) {
	collection := collectionMgr.GetCollection(collectionID)
	if collection != nil {
		partitions := collectionMgr.GetPartitionsByCollection(collectionID)
		if partitions != nil {
			return lo.Map(partitions, func(partition *meta.Partition, i int) int64 {
				return partition.PartitionID
			}), nil
		}
	}

	// todo(yah01): replace this error with a defined error
	return nil, fmt.Errorf("collection/partition not loaded")
}

// GroupNodesByReplica groups nodes by replica,
// returns ReplicaID -> NodeIDs
func GroupNodesByReplica(replicaMgr *meta.ReplicaManager, collectionID int64, nodes []int64) map[int64][]int64 {
	ret := make(map[int64][]int64)
	replicas := replicaMgr.GetByCollection(collectionID)
	for _, replica := range replicas {
		for _, node := range nodes {
			if replica.Contains(node) {
				ret[replica.ID] = append(ret[replica.ID], node)
			}
		}
	}
	return ret
}

// GroupPartitionsByCollection groups partitions by collection,
// returns CollectionID -> Partitions
func GroupPartitionsByCollection(partitions []*meta.Partition) map[int64][]*meta.Partition {
	ret := make(map[int64][]*meta.Partition, 0)
	for _, partition := range partitions {
		collection := partition.GetCollectionID()
		ret[collection] = append(ret[collection], partition)
	}
	return ret
}

// GroupSegmentsByReplica groups segments by replica,
// returns ReplicaID -> Segments
func GroupSegmentsByReplica(replicaMgr *meta.ReplicaManager, collectionID int64, segments []*meta.Segment) map[int64][]*meta.Segment {
	ret := make(map[int64][]*meta.Segment)
	replicas := replicaMgr.GetByCollection(collectionID)
	for _, replica := range replicas {
		for _, segment := range segments {
			if replica.Contains(segment.Node) {
				ret[replica.ID] = append(ret[replica.ID], segment)
			}
		}
	}
	return ret
}

// AssignNodesToReplicas assigns nodes to the given replicas,
// all given replicas must be the same collection,
// the given replicas have to be not in ReplicaManager
func AssignNodesToReplicas(m *meta.Meta, rgName string, replicas ...*meta.Replica) error {
	replicaIDs := lo.Map(replicas, func(r *meta.Replica, _ int) int64 { return r.GetID() })
	log := log.With(zap.Int64("collectionID", replicas[0].GetCollectionID()),
		zap.Int64s("replicas", replicaIDs),
		zap.String("rgName", rgName),
	)
	if len(replicaIDs) == 0 {
		return nil
	}

	nodeGroup, err := m.ResourceManager.GetNodes(rgName)
	if err != nil {
		log.Error("failed to get nodes", zap.Error(err))
		return err
	}

	if len(nodeGroup) < len(replicaIDs) {
		log.Error(meta.ErrNodeNotEnough.Error())
		return meta.ErrNodeNotEnough
	}

	rand.Shuffle(len(nodeGroup), func(i, j int) {
		nodeGroup[i], nodeGroup[j] = nodeGroup[j], nodeGroup[i]
	})

	log.Info("assign nodes to replicas",
		zap.Int64s("nodes", nodeGroup),
	)
	for i, node := range nodeGroup {
		replicas[i%len(replicas)].AddNode(node)
	}

	return nil
}

// add nodes to all collections in rgName
// for each collection, add node to replica with least number of nodes
func AddNodesToCollectionsInRG(m *meta.Meta, rgName string, nodes ...int64) {
	for _, node := range nodes {
		for _, collection := range m.CollectionManager.GetAll() {
			replica := m.ReplicaManager.GetByCollectionAndNode(collection, node)
			if replica == nil {
				replicas := m.ReplicaManager.GetByCollectionAndRG(collection, rgName)
				AddNodesToReplicas(m, replicas, node)
			}
		}
	}
}

func AddNodesToReplicas(m *meta.Meta, replicas []*meta.Replica, node int64) {
	if len(replicas) == 0 {
		return
	}
	sort.Slice(replicas, func(i, j int) bool {
		return replicas[i].Len() < replicas[j].Len()
	})
	replica := replicas[0]
	// TODO(yah01): this may fail, need a component to check whether a node is assigned
	err := m.ReplicaManager.AddNode(replica.GetID(), node)
	if err != nil {
		log.Warn("failed to assign node to replicas",
			zap.Int64("collectionID", replica.GetCollectionID()),
			zap.Int64("replicaID", replica.GetID()),
			zap.Int64("nodeId", node),
			zap.Error(err),
		)
		return
	}
	log.Info("assign node to replica",
		zap.Int64("collectionID", replica.GetCollectionID()),
		zap.Int64("replicaID", replica.GetID()),
		zap.Int64("nodeID", node),
	)
}

// SpawnReplicas spawns replicas for given collection, assign nodes to them, and save them
func SpawnAllReplicasInRG(m *meta.Meta, collection int64, replicaNumber int32, rgName string) ([]*meta.Replica, error) {
	replicas, err := m.ReplicaManager.Spawn(collection, replicaNumber, rgName)
	if err != nil {
		return nil, err
	}
	err = AssignNodesToReplicas(m, rgName, replicas...)
	if err != nil {
		return nil, err
	}
	return replicas, m.ReplicaManager.Put(replicas...)
}

func checkResourceGroup(collectionID int64, replicaNumber int32, resourceGroups []string) error {
	if len(resourceGroups) != 0 && len(resourceGroups) != 1 && len(resourceGroups) != int(replicaNumber) {
		return ErrUseWrongNumRG
	}

	return nil
}

func SpawnReplicasWithRG(m *meta.Meta, collection int64, resourceGroups []string, replicaNumber int32) ([]*meta.Replica, error) {
	if err := checkResourceGroup(collection, replicaNumber, resourceGroups); err != nil {
		return nil, err
	}

	if len(resourceGroups) == 0 {
		return SpawnAllReplicasInRG(m, collection, replicaNumber, meta.DefaultResourceGroupName)
	}

	if len(resourceGroups) == 1 {
		return SpawnAllReplicasInRG(m, collection, replicaNumber, resourceGroups[0])
	}

	replicaSet := make([]*meta.Replica, 0)
	for _, rgName := range resourceGroups {
		if !m.ResourceManager.ContainResourceGroup(rgName) {
			return nil, merr.WrapErrResourceGroupNotFound(rgName)
		}

		replicas, err := m.ReplicaManager.Spawn(collection, 1, rgName)
		if err != nil {
			return nil, err
		}

		err = AssignNodesToReplicas(m, rgName, replicas...)
		if err != nil {
			return nil, err
		}
		replicaSet = append(replicaSet, replicas...)
	}

	return replicaSet, m.ReplicaManager.Put(replicaSet...)
}
