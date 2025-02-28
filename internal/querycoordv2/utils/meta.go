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
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func GetPartitions(ctx context.Context, targetMgr meta.TargetManagerInterface, collectionID int64) ([]int64, error) {
	// fetch next target first, sync next target contains the wanted partition list
	// if not found, current will be used instead for dist adjustment requests
	return targetMgr.GetPartitions(ctx, collectionID, meta.NextTargetFirst)
}

// GroupNodesByReplica groups nodes by replica,
// returns ReplicaID -> NodeIDs
func GroupNodesByReplica(ctx context.Context, replicaMgr *meta.ReplicaManager, collectionID int64, nodes []int64) map[int64][]int64 {
	ret := make(map[int64][]int64)
	replicas := replicaMgr.GetByCollection(ctx, collectionID)
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
func GroupSegmentsByReplica(ctx context.Context, replicaMgr *meta.ReplicaManager, collectionID int64, segments []*meta.Segment) map[int64][]*meta.Segment {
	ret := make(map[int64][]*meta.Segment)
	replicas := replicaMgr.GetByCollection(ctx, collectionID)
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
func RecoverReplicaOfCollection(ctx context.Context, m *meta.Meta, collectionID typeutil.UniqueID) {
	logger := log.With(zap.Int64("collectionID", collectionID))
	rgNames := m.ReplicaManager.GetResourceGroupByCollection(ctx, collectionID)
	if rgNames.Len() == 0 {
		logger.Error("no resource group found for collection")
		return
	}
	rgs, err := m.ResourceManager.GetNodesOfMultiRG(ctx, rgNames.Collect())
	if err != nil {
		logger.Error("unreachable code as expected, fail to get resource group for replica", zap.Error(err))
		return
	}

	if err := m.ReplicaManager.RecoverNodesInCollection(ctx, collectionID, rgs); err != nil {
		logger.Warn("fail to set available nodes in replica", zap.Error(err))
	}
}

// RecoverAllCollectionrecovers all replica of all collection in resource group.
func RecoverAllCollection(m *meta.Meta) {
	for _, collection := range m.CollectionManager.GetAll(context.TODO()) {
		RecoverReplicaOfCollection(context.TODO(), m, collection)
	}
}

func AssignReplica(ctx context.Context, m *meta.Meta, resourceGroups []string, replicaNumber int32, checkNodeNum bool) (map[string]int, error) {
	if len(resourceGroups) != 0 && len(resourceGroups) != 1 && len(resourceGroups) != int(replicaNumber) {
		return nil, errors.Errorf(
			"replica=[%d] resource group=[%s], resource group num can only be 0, 1 or same as replica number", replicaNumber, strings.Join(resourceGroups, ","))
	}

	if streamingutil.IsStreamingServiceEnabled() {
		streamingNodeCount := snmanager.StaticStreamingNodeManager.GetStreamingQueryNodeIDs().Len()
		if replicaNumber > int32(streamingNodeCount) {
			return nil, merr.WrapErrStreamingNodeNotEnough(streamingNodeCount, int(replicaNumber), fmt.Sprintf("when load %d replica count", replicaNumber))
		}
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
		if !m.ContainResourceGroup(ctx, rgName) {
			return nil, merr.WrapErrResourceGroupNotFound(rgName)
		}
		nodes, err := m.ResourceManager.GetNodes(ctx, rgName)
		if err != nil {
			return nil, err
		}

		if num > len(nodes) {
			log.Warn("failed to check resource group", zap.Error(err))
			if checkNodeNum {
				err := merr.WrapErrResourceGroupNodeNotEnough(rgName, len(nodes), num)
				return nil, err
			}
		}
	}
	return replicaNumInRG, nil
}

// SpawnReplicasWithRG spawns replicas in rgs one by one for given collection.
func SpawnReplicasWithRG(ctx context.Context, m *meta.Meta, collection int64, resourceGroups []string, replicaNumber int32, channels []string) ([]*meta.Replica, error) {
	replicaNumInRG, err := AssignReplica(ctx, m, resourceGroups, replicaNumber, true)
	if err != nil {
		return nil, err
	}
	// Spawn it in replica manager.
	replicas, err := m.ReplicaManager.Spawn(ctx, collection, replicaNumInRG, channels)
	if err != nil {
		return nil, err
	}
	// Active recover it.
	RecoverReplicaOfCollection(ctx, m, collection)
	if streamingutil.IsStreamingServiceEnabled() {
		m.RecoverSQNodesInCollection(ctx, collection, snmanager.StaticStreamingNodeManager.GetStreamingQueryNodeIDs())
	}
	return replicas, nil
}

func ReassignReplicaToRG(
	ctx context.Context,
	m *meta.Meta,
	collectionID int64,
	newReplicaNumber int32,
	newResourceGroups []string,
) (map[string]int, map[string][]*meta.Replica, []int64, error) {
	// assign all replicas to newResourceGroups, got each rg's replica number
	newAssignment, err := AssignReplica(ctx, m, newResourceGroups, newReplicaNumber, false)
	if err != nil {
		return nil, nil, nil, err
	}

	replicas := m.ReplicaManager.GetByCollection(context.TODO(), collectionID)
	replicasInRG := lo.GroupBy(replicas, func(replica *meta.Replica) string {
		return replica.GetResourceGroup()
	})

	// if rg doesn't exist in newResourceGroups, add all replicas to candidateToRelease
	candidateToRelease := make([]*meta.Replica, 0)
	outRg, _ := lo.Difference(lo.Keys(replicasInRG), newResourceGroups)
	if len(outRg) > 0 {
		for _, rgName := range outRg {
			candidateToRelease = append(candidateToRelease, replicasInRG[rgName]...)
		}
	}

	// if rg has more replicas than newAssignment's replica number, add the rest replicas to candidateToMove
	// also set the lacked replica number as rg's replicaToSpawn value
	replicaToSpawn := make(map[string]int, len(newAssignment))
	for rgName, count := range newAssignment {
		if len(replicasInRG[rgName]) > count {
			candidateToRelease = append(candidateToRelease, replicasInRG[rgName][count:]...)
		} else {
			lack := count - len(replicasInRG[rgName])
			if lack > 0 {
				replicaToSpawn[rgName] = lack
			}
		}
	}

	candidateIdx := 0
	// if newReplicaNumber is small than current replica num, pick replica from candidate and add it to replicasToRelease
	replicasToRelease := make([]int64, 0)
	replicaReleaseCounter := len(replicas) - int(newReplicaNumber)
	for replicaReleaseCounter > 0 {
		replicasToRelease = append(replicasToRelease, candidateToRelease[candidateIdx].GetID())
		replicaReleaseCounter -= 1
		candidateIdx += 1
	}

	// if candidateToMove is not empty, pick replica from candidate add add it to replicaToTransfer
	// which means if rg has less replicas than expected, we transfer some existed replica to it.
	replicaToTransfer := make(map[string][]*meta.Replica)
	if candidateIdx < len(candidateToRelease) {
		for rg := range replicaToSpawn {
			for replicaToSpawn[rg] > 0 && candidateIdx < len(candidateToRelease) {
				if replicaToTransfer[rg] == nil {
					replicaToTransfer[rg] = make([]*meta.Replica, 0)
				}
				replicaToTransfer[rg] = append(replicaToTransfer[rg], candidateToRelease[candidateIdx])
				candidateIdx += 1
				replicaToSpawn[rg] -= 1
			}

			if replicaToSpawn[rg] == 0 {
				delete(replicaToSpawn, rg)
			}
		}
	}

	return replicaToSpawn, replicaToTransfer, replicasToRelease, nil
}
