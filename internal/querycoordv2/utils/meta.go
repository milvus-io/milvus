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
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var (
	ErrGetNodesFromRG       = errors.New("failed to get node from rg")
	ErrNoReplicaFound       = errors.New("no replica found during assign nodes")
	ErrReplicasInconsistent = errors.New("all replicas should belong to same collection during assign nodes")
	ErrUseWrongNumRG        = errors.New("resource group num can only be 0, 1 or same as replica number")
)

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

	return nil, merr.WrapErrCollectionNotLoaded(collectionID)
}

// GroupNodesByReplica groups nodes by replica,
// returns ReplicaID -> NodeIDs
func GroupNodesByReplica(replicaMgr *meta.ReplicaManager, collectionID int64, nodes []int64) map[int64][]int64 {
	ret := make(map[int64][]int64)
	replicas := replicaMgr.GetByCollection(collectionID)
	for _, replica := range replicas {
		for _, node := range nodes {
			if replica.Contains(node) {
				ret[replica.GetID()] = append(ret[replica.GetID()], node)
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
				ret[replica.GetID()] = append(ret[replica.GetID()], segment)
			}
		}
	}
	return ret
}

// RecoverReplicaOfCollection recovers all replica of collection with latest resource group.
func RecoverReplicaOfCollection(m *meta.Meta, collectionID typeutil.UniqueID) {
	logger := log.With(zap.Int64("collectionID", collectionID))
	rgNames := m.ReplicaManager.GetResourceGroupByCollection(collectionID)
	if rgNames.Len() == 0 {
		logger.Error("no resource group found for collection", zap.Int64("collectionID", collectionID))
		return
	}
	rgs, err := m.ResourceManager.GetNodesOfMultiRG(rgNames.Collect())
	if err != nil {
		logger.Error("unreachable code as expected, fail to get resource group for replica", zap.Error(err))
		return
	}

	if err := m.ReplicaManager.RecoverNodesInCollection(collectionID, rgs); err != nil {
		logger.Warn("fail to set available nodes in replica", zap.Error(err))
	}
}

// RecoverAllCollectionrecovers all replica of all collection in resource group.
func RecoverAllCollection(m *meta.Meta) {
	for _, collection := range m.CollectionManager.GetAll() {
		RecoverReplicaOfCollection(m, collection)
	}
}

func checkResourceGroup(m *meta.Meta, resourceGroups []string, replicaNumber int32) (map[string]int, error) {
	if len(resourceGroups) != 0 && len(resourceGroups) != 1 && len(resourceGroups) != int(replicaNumber) {
		return nil, ErrUseWrongNumRG
	}

	replicaNumInRG := make(map[string]int)
	if len(resourceGroups) == 0 {
		// All replicas should be spawned in default resource group.
		replicaNumInRG[meta.DefaultResourceGroupName] = int(replicaNumber)
	} else if len(resourceGroups) == 1 {
		// All replicas should be spawned in the given resource group.
		replicaNumInRG[resourceGroups[0]] = int(replicaNumber)
	} else {
		// replicas should be spawned in different resource groups one by one.
		for _, rgName := range resourceGroups {
			replicaNumInRG[rgName] += 1
		}
	}

	// TODO: !!!Warning, ResourceManager and ReplicaManager doesn't protected with each other in concurrent operation.
	// 1. replica1 got rg1's node snapshot but doesn't spawn finished.
	// 2. rg1 is removed.
	// 3. replica1 spawn finished, but cannot find related resource group.
	for rgName, num := range replicaNumInRG {
		if !m.ContainResourceGroup(rgName) {
			return nil, ErrGetNodesFromRG
		}
		nodes, err := m.ResourceManager.GetNodes(rgName)
		if err != nil {
			return nil, err
		}
		if num > len(nodes) {
			log.Warn("node not enough", zap.Error(meta.ErrNodeNotEnough), zap.Int("replicaNum", num), zap.Int("nodeNum", len(nodes)), zap.String("rgName", rgName))
			return nil, meta.ErrNodeNotEnough
		}
	}
	return replicaNumInRG, nil
}

// SpawnReplicasWithRG spawns replicas in rgs one by one for given collection.
func SpawnReplicasWithRG(m *meta.Meta, collection int64, resourceGroups []string, replicaNumber int32, channels []string) ([]*meta.Replica, error) {
	replicaNumInRG, err := checkResourceGroup(m, resourceGroups, replicaNumber)
	if err != nil {
		return nil, err
	}

	// Spawn it in replica manager.
	replicas, err := m.ReplicaManager.Spawn(collection, replicaNumInRG, channels)
	if err != nil {
		return nil, err
	}
	// Active recover it.
	RecoverReplicaOfCollection(m, collection)
	return replicas, nil
}
