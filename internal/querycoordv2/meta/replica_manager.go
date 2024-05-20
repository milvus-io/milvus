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

package meta

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ReplicaManager struct {
	rwmutex sync.RWMutex

	idAllocator        func() (int64, error)
	replicas           map[typeutil.UniqueID]*Replica
	collIDToReplicaIDs map[typeutil.UniqueID]typeutil.UniqueSet
	catalog            metastore.QueryCoordCatalog
}

func NewReplicaManager(idAllocator func() (int64, error), catalog metastore.QueryCoordCatalog) *ReplicaManager {
	return &ReplicaManager{
		idAllocator:        idAllocator,
		replicas:           make(map[int64]*Replica),
		collIDToReplicaIDs: make(map[int64]typeutil.UniqueSet),
		catalog:            catalog,
	}
}

// Recover recovers the replicas for given collections from meta store
func (m *ReplicaManager) Recover(collections []int64) error {
	replicas, err := m.catalog.GetReplicas()
	if err != nil {
		return fmt.Errorf("failed to recover replicas, err=%w", err)
	}

	collectionSet := typeutil.NewUniqueSet(collections...)
	for _, replica := range replicas {
		if len(replica.GetResourceGroup()) == 0 {
			replica.ResourceGroup = DefaultResourceGroupName
		}

		if collectionSet.Contain(replica.GetCollectionID()) {
			m.putReplicaInMemory(newReplica(replica))
			log.Info("recover replica",
				zap.Int64("collectionID", replica.GetCollectionID()),
				zap.Int64("replicaID", replica.GetID()),
				zap.Int64s("nodes", replica.GetNodes()),
			)
		} else {
			err := m.catalog.ReleaseReplica(replica.GetCollectionID(), replica.GetID())
			if err != nil {
				return err
			}
			log.Info("clear stale replica",
				zap.Int64("collectionID", replica.GetCollectionID()),
				zap.Int64("replicaID", replica.GetID()),
				zap.Int64s("nodes", replica.GetNodes()),
			)
		}
	}
	return nil
}

// Get returns the replica by id.
// Replica should be read-only, do not modify it.
func (m *ReplicaManager) Get(id typeutil.UniqueID) *Replica {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.replicas[id]
}

// Spawn spawns N replicas at resource group for given collection in ReplicaManager.
func (m *ReplicaManager) Spawn(collection int64, replicaNumInRG map[string]int, channels []string) ([]*Replica, error) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()
	if m.collIDToReplicaIDs[collection] != nil {
		return nil, fmt.Errorf("replicas of collection %d is already spawned", collection)
	}

	balancePolicy := paramtable.Get().QueryCoordCfg.Balancer.GetValue()
	enableChannelExclusiveMode := balancePolicy == ChannelLevelScoreBalancerName

	replicas := make([]*Replica, 0)
	for rgName, replicaNum := range replicaNumInRG {
		for ; replicaNum > 0; replicaNum-- {
			id, err := m.idAllocator()
			if err != nil {
				return nil, err
			}

			channelExclusiveNodeInfo := make(map[string]*querypb.ChannelNodeInfo)
			if enableChannelExclusiveMode {
				for _, channel := range channels {
					channelExclusiveNodeInfo[channel] = &querypb.ChannelNodeInfo{}
				}
			}
			replicas = append(replicas, newReplica(&querypb.Replica{
				ID:               id,
				CollectionID:     collection,
				ResourceGroup:    rgName,
				ChannelNodeInfos: channelExclusiveNodeInfo,
			}))
		}
	}
	if err := m.put(replicas...); err != nil {
		return nil, err
	}
	return replicas, nil
}

// Deprecated: Warning, break the consistency of ReplicaManager,
// never use it in non-test code, use Spawn instead.
func (m *ReplicaManager) Put(replicas ...*Replica) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.put(replicas...)
}

func (m *ReplicaManager) put(replicas ...*Replica) error {
	if len(replicas) == 0 {
		return nil
	}
	// Persist replicas into KV.
	replicaPBs := make([]*querypb.Replica, 0, len(replicas))
	for _, replica := range replicas {
		replicaPBs = append(replicaPBs, replica.replicaPB)
	}
	if err := m.catalog.SaveReplica(replicaPBs...); err != nil {
		return err
	}

	m.putReplicaInMemory(replicas...)
	return nil
}

// putReplicaInMemory puts replicas into in-memory map and collIDToReplicaIDs.
func (m *ReplicaManager) putReplicaInMemory(replicas ...*Replica) {
	for _, replica := range replicas {
		// update in-memory replicas.
		m.replicas[replica.GetID()] = replica

		// update collIDToReplicaIDs.
		if m.collIDToReplicaIDs[replica.GetCollectionID()] == nil {
			m.collIDToReplicaIDs[replica.GetCollectionID()] = typeutil.NewUniqueSet()
		}
		m.collIDToReplicaIDs[replica.GetCollectionID()].Insert(replica.GetID())
	}
}

// TransferReplica transfers N replicas from srcRGName to dstRGName.
func (m *ReplicaManager) TransferReplica(collectionID typeutil.UniqueID, srcRGName string, dstRGName string, replicaNum int) error {
	if srcRGName == dstRGName {
		return merr.WrapErrParameterInvalidMsg("source resource group and target resource group should not be the same, resource group: %s", srcRGName)
	}
	if replicaNum <= 0 {
		return merr.WrapErrParameterInvalid("NumReplica > 0", fmt.Sprintf("invalid NumReplica %d", replicaNum))
	}

	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	// Check if replica can be transfer.
	srcReplicas, err := m.getSrcReplicasAndCheckIfTransferable(collectionID, srcRGName, replicaNum)
	if err != nil {
		return err
	}

	// Transfer N replicas from srcRGName to dstRGName.
	// Node Change will be executed by replica_observer in background.
	replicas := make([]*Replica, 0, replicaNum)
	for i := 0; i < replicaNum; i++ {
		mutableReplica := srcReplicas[i].CopyForWrite()
		mutableReplica.SetResourceGroup(dstRGName)
		replicas = append(replicas, mutableReplica.IntoReplica())
	}
	return m.put(replicas...)
}

// getSrcReplicasAndCheckIfTransferable checks if the collection can be transfer from srcRGName to dstRGName.
func (m *ReplicaManager) getSrcReplicasAndCheckIfTransferable(collectionID typeutil.UniqueID, srcRGName string, replicaNum int) ([]*Replica, error) {
	// Check if collection is loaded.
	if m.collIDToReplicaIDs[collectionID] == nil {
		return nil, merr.WrapErrParameterInvalid(
			"Collection not loaded",
			fmt.Sprintf("collectionID %d", collectionID),
		)
	}

	// Check if replica in srcRGName is enough.
	srcReplicas := m.getByCollectionAndRG(collectionID, srcRGName)
	if len(srcReplicas) < replicaNum {
		err := merr.WrapErrParameterInvalid(
			"NumReplica not greater than the number of replica in source resource group", fmt.Sprintf("only found [%d] replicas of collection [%d] in source resource group [%s], but %d require",
				len(srcReplicas),
				collectionID,
				srcRGName,
				replicaNum))
		return nil, err
	}
	return srcReplicas, nil
}

// RemoveCollection removes replicas of given collection,
// returns error if failed to remove replica from KV
func (m *ReplicaManager) RemoveCollection(collectionID typeutil.UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	err := m.catalog.ReleaseReplicas(collectionID)
	if err != nil {
		return err
	}
	// Remove all replica of collection and remove collection from collIDToReplicaIDs.
	for replicaID := range m.collIDToReplicaIDs[collectionID] {
		delete(m.replicas, replicaID)
	}
	delete(m.collIDToReplicaIDs, collectionID)
	return nil
}

func (m *ReplicaManager) GetByCollection(collectionID typeutil.UniqueID) []*Replica {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	replicas := make([]*Replica, 0)
	if m.collIDToReplicaIDs[collectionID] != nil {
		for replicaID := range m.collIDToReplicaIDs[collectionID] {
			replicas = append(replicas, m.replicas[replicaID])
		}
	}
	return replicas
}

func (m *ReplicaManager) GetByCollectionAndNode(collectionID, nodeID typeutil.UniqueID) *Replica {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	if m.collIDToReplicaIDs[collectionID] != nil {
		for replicaID := range m.collIDToReplicaIDs[collectionID] {
			replica := m.replicas[replicaID]
			if replica.Contains(nodeID) {
				return replica
			}
		}
	}

	return nil
}

func (m *ReplicaManager) GetByNode(nodeID typeutil.UniqueID) []*Replica {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	replicas := make([]*Replica, 0)
	for _, replica := range m.replicas {
		if replica.Contains(nodeID) {
			replicas = append(replicas, replica)
		}
	}

	return replicas
}

func (m *ReplicaManager) getByCollectionAndRG(collectionID int64, rgName string) []*Replica {
	replicaIDs, ok := m.collIDToReplicaIDs[collectionID]
	if !ok {
		return make([]*Replica, 0)
	}

	ret := make([]*Replica, 0)
	replicaIDs.Range(func(replicaID typeutil.UniqueID) bool {
		if m.replicas[replicaID].GetResourceGroup() == rgName {
			ret = append(ret, m.replicas[replicaID])
		}
		return true
	})
	return ret
}

func (m *ReplicaManager) GetByResourceGroup(rgName string) []*Replica {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	ret := make([]*Replica, 0)
	for _, replica := range m.replicas {
		if replica.GetResourceGroup() == rgName {
			ret = append(ret, replica)
		}
	}

	return ret
}

// RecoverNodesInCollection recovers all nodes in collection with latest resource group.
// Promise a node will be only assigned to one replica in same collection at same time.
// 1. Move the rw nodes to ro nodes if they are not in related resource group.
// 2. Add new incoming nodes into the replica if they are not in-used by other replicas of same collection.
// 3. replicas in same resource group will shared the nodes in resource group fairly.
func (m *ReplicaManager) RecoverNodesInCollection(collectionID typeutil.UniqueID, rgs map[string]typeutil.UniqueSet) error {
	if err := m.validateResourceGroups(rgs); err != nil {
		return err
	}

	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	// create a helper to do the recover.
	helper, err := m.getCollectionAssignmentHelper(collectionID, rgs)
	if err != nil {
		return err
	}

	modifiedReplicas := make([]*Replica, 0)
	// recover node by resource group.
	helper.RangeOverResourceGroup(func(replicaHelper *replicasInSameRGAssignmentHelper) {
		replicaHelper.RangeOverReplicas(func(assignment *replicaAssignmentInfo) {
			roNodes := assignment.GetNewRONodes()
			recoverableNodes, incomingNodeCount := assignment.GetRecoverNodesAndIncomingNodeCount()
			// There may be not enough incoming nodes for current replica,
			// Even we filtering the nodes that are used by other replica of same collection in other resource group,
			// current replica's expected node may be still used by other replica of same collection in same resource group.
			incomingNode := replicaHelper.AllocateIncomingNodes(incomingNodeCount)
			if len(roNodes) == 0 && len(recoverableNodes) == 0 && len(incomingNode) == 0 {
				// nothing to do.
				return
			}
			mutableReplica := m.replicas[assignment.GetReplicaID()].CopyForWrite()
			mutableReplica.AddRONode(roNodes...)          // rw -> ro
			mutableReplica.AddRWNode(recoverableNodes...) // ro -> rw
			mutableReplica.AddRWNode(incomingNode...)     // unused -> rw
			log.Info(
				"new replica recovery found",
				zap.Int64("replicaID", assignment.GetReplicaID()),
				zap.Int64s("newRONodes", roNodes),
				zap.Int64s("roToRWNodes", recoverableNodes),
				zap.Int64s("newIncomingNodes", incomingNode))
			modifiedReplicas = append(modifiedReplicas, mutableReplica.IntoReplica())
		})
	})
	return m.put(modifiedReplicas...)
}

// validateResourceGroups checks if the resource groups are valid.
func (m *ReplicaManager) validateResourceGroups(rgs map[string]typeutil.UniqueSet) error {
	// make sure that node in resource group is mutual exclusive.
	node := typeutil.NewUniqueSet()
	for _, rg := range rgs {
		for id := range rg {
			if node.Contain(id) {
				return errors.New("node in resource group is not mutual exclusive")
			}
			node.Insert(id)
		}
	}
	return nil
}

// getCollectionAssignmentHelper checks if the collection is recoverable and group replicas by resource group.
func (m *ReplicaManager) getCollectionAssignmentHelper(collectionID typeutil.UniqueID, rgs map[string]typeutil.UniqueSet) (*collectionAssignmentHelper, error) {
	// check if the collection is exist.
	replicaIDs, ok := m.collIDToReplicaIDs[collectionID]
	if !ok {
		return nil, errors.Errorf("collection %d not loaded", collectionID)
	}

	rgToReplicas := make(map[string][]*Replica)
	for replicaID := range replicaIDs {
		replica := m.replicas[replicaID]
		rgName := replica.GetResourceGroup()
		if _, ok := rgs[rgName]; !ok {
			return nil, errors.Errorf("lost resource group info, collectionID: %d, replicaID: %d, resourceGroup: %s", collectionID, replicaID, rgName)
		}
		if _, ok := rgToReplicas[rgName]; !ok {
			rgToReplicas[rgName] = make([]*Replica, 0)
		}
		rgToReplicas[rgName] = append(rgToReplicas[rgName], replica)
	}
	return newCollectionAssignmentHelper(collectionID, rgToReplicas, rgs), nil
}

// RemoveNode removes the node from all replicas of given collection.
func (m *ReplicaManager) RemoveNode(replicaID typeutil.UniqueID, nodes ...typeutil.UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	replica, ok := m.replicas[replicaID]
	if !ok {
		return merr.WrapErrReplicaNotFound(replicaID)
	}

	mutableReplica := replica.CopyForWrite()
	mutableReplica.RemoveNode(nodes...) // ro -> unused
	return m.put(mutableReplica.IntoReplica())
}

func (m *ReplicaManager) GetResourceGroupByCollection(collection typeutil.UniqueID) typeutil.Set[string] {
	replicas := m.GetByCollection(collection)
	ret := typeutil.NewSet(lo.Map(replicas, func(r *Replica, _ int) string { return r.GetResourceGroup() })...)
	return ret
}
