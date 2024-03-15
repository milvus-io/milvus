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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
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
func (m *ReplicaManager) Spawn(collection int64, replicaNumInRG map[string]int) ([]*Replica, error) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()
	if m.collIDToReplicaIDs[collection] != nil {
		return nil, fmt.Errorf("replicas of collection %d is already spawned", collection)
	}

	replicas := make([]*Replica, 0)
	for rgName, replicaNum := range replicaNumInRG {
		for ; replicaNum > 0; replicaNum-- {
			id, err := m.idAllocator()
			if err != nil {
				return nil, err
			}
			replicas = append(replicas, newReplica(&querypb.Replica{
				ID:            id,
				CollectionID:  collection,
				ResourceGroup: rgName,
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
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	// Check if replica can be transfer.
	if err := m.isTransferable(collectionID, srcRGName, dstRGName, replicaNum); err != nil {
		return err
	}

	// Transfer N replicas from srcRGName to dstRGName.
	// Node Change will be executed by replica_observer in background.
	srcReplicas := m.getByCollectionAndRG(collectionID, srcRGName)
	replicas := make([]*Replica, 0, replicaNum)
	for i := 0; i < replicaNum; i++ {
		mutableReplica := srcReplicas[i].copyForWrite()
		mutableReplica.SetResourceGroup(dstRGName)
		replicas = append(replicas, mutableReplica.IntoReplica())
	}
	return m.put(replicas...)
}

// isTransferable checks if the collection can be transfer from srcRGName to dstRGName.
func (m *ReplicaManager) isTransferable(collectionID typeutil.UniqueID, srcRGName string, dstRGName string, replicaNum int) error {
	// Check if collection is loaded.
	if m.collIDToReplicaIDs[collectionID] == nil {
		return merr.WrapErrParameterInvalid(
			"Collection not loaded",
			fmt.Sprintf("collectionID %d", collectionID),
		)
	}

	// Check if replica in srcRGName is enough.
	srcReplicas := m.getByCollectionAndRG(collectionID, srcRGName)
	if len(srcReplicas) < replicaNum {
		err := merr.WrapErrParameterInvalid("NumReplica not greater than the number of replica in source resource group", fmt.Sprintf("only found [%d] replicas in source resource group[%s]",
			replicaNum, srcRGName))
		return err
	}

	// Check if collection is loaded in dstRGName.
	dstReplicas := m.getByCollectionAndRG(collectionID, dstRGName)
	if len(dstReplicas) > 0 {
		err := merr.WrapErrParameterInvalid("no same collection in target resource group", fmt.Sprintf("found [%d] replicas of same collection in target resource group[%s], dynamically increase replica num is unsupported",
			replicaNum, dstRGName))
		return err
	}

	// TODO: After resource group enhancement, we can remove these constraint.
	if (srcRGName == DefaultResourceGroupName || dstRGName == DefaultResourceGroupName) && m.collIDToReplicaIDs[collectionID].Len() != replicaNum {
		err := merr.WrapErrParameterInvalid("tranfer all replicas from/to default resource group",
			fmt.Sprintf("try to transfer %d replicas from/to but %d replicas exist", replicaNum, m.collIDToReplicaIDs[collectionID].Len()))
		return err
	}

	return nil
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

	replicas := make([]*Replica, 0, 3)
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

	for _, replica := range m.replicas {
		if replica.GetCollectionID() == collectionID && replica.Contains(nodeID) {
			return replica
		}
	}

	return nil
}

func (m *ReplicaManager) getByCollectionAndRG(collectionID int64, rgName string) []*Replica {
	ret := make([]*Replica, 0)
	for _, replica := range m.replicas {
		if replica.GetCollectionID() == collectionID && replica.GetResourceGroup() == rgName {
			ret = append(ret, replica)
		}
	}

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

// SetAvailableNodesInSameCollectionAndRG sets the available nodes for all replica in same collection and rg.
// 1. Add new incoming nodes into the replica if they are not in-used by other replicas of same collection.
// 2. Move the nodes to outbound if they are not in allAvailableNodes.
// 3. replicas in same resource group will share the incoming nodes.
func (m *ReplicaManager) SetAvailableNodesInSameCollectionAndRG(collectionID typeutil.UniqueID, rgName string, allAvailableNodes typeutil.UniqueSet) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	// replicaIDs in same collection and resource group.
	replicaIDs := m.findAllReplicaInSameCollectionAndRG(collectionID, rgName)
	if len(replicaIDs) == 0 {
		return nil
	}

	// Found all new outbound nodes, and set it into outbound node.
	if err := m.setNewOutboundNodesForReplicas(replicaIDs, allAvailableNodes); err != nil {
		return err
	}
	// Found all new incoming node, and assign it into one of the replica.
	return m.setNewIncomingNodesForReplicas(collectionID, replicaIDs, allAvailableNodes)
}

// findAllReplicaInSameCollectionAndRG finds all replicas in same collection and resource group.
func (m *ReplicaManager) findAllReplicaInSameCollectionAndRG(collectionID typeutil.UniqueID, rgName string) typeutil.UniqueSet {
	ret := typeutil.NewUniqueSet()
	for replicaID := range m.collIDToReplicaIDs[collectionID] {
		if m.replicas[replicaID].GetResourceGroup() == rgName {
			ret.Insert(replicaID)
		}
	}
	return ret
}

// setNewOutboundNodesForReplicas sets the new outbound nodes for the replica.
func (m *ReplicaManager) setNewOutboundNodesForReplicas(replicaIDs typeutil.UniqueSet, allAvailableNodes typeutil.UniqueSet) error {
	replicas := make([]*Replica, 0, len(replicaIDs))
	// Found all outbound nodes.
	for replicaID := range replicaIDs {
		// replica is always exists by protection of mutex.
		replica := m.replicas[replicaID]

		newOutboundNodes := typeutil.NewUniqueSet(replica.GetNodes()...)
		// filter out all available nodes, then get the outbound nodes.
		for node := range allAvailableNodes {
			newOutboundNodes.Remove(node)
		}

		if newOutboundNodes.Len() == 0 {
			continue
		}

		mutableReplica := replica.copyForWrite()
		mutableReplica.MoveNodeToOutbound(newOutboundNodes.Collect()...)
		replicas = append(replicas, mutableReplica.IntoReplica())
	}
	return m.put(replicas...)
}

// setNewIncomingNodesForReplicas sets the new incoming nodes for the replicas.
func (m *ReplicaManager) setNewIncomingNodesForReplicas(collectionID typeutil.UniqueID, replicaIDs typeutil.UniqueSet, allAvailableNodes typeutil.UniqueSet) error {
	newIncomingNodes := m.findAllIncomingNodes(replicaIDs, allAvailableNodes)

	// Check if the new incoming nodes are in-used by other replicas of same collection.
	for node := range newIncomingNodes {
		if m.isNodeIsUsedByOtherReplicaInSameCollection(collectionID, replicaIDs, node) {
			// If the node is in-used (available or outbound) by other replica of same collection,
			// we cannot load any channel or segment on it with current query node implementation.
			// do not set it as available node util other replica release on that query node.
			newIncomingNodes.Remove(node)
		}
	}

	// nothing to do.
	if newIncomingNodes.Len() == 0 {
		return nil
	}
	replicas := m.assignIncomingNodeIntoReplicas(replicaIDs, newIncomingNodes)
	return m.put(replicas...)
}

// assignIncomingNodeIntoReplicas assigns the new incoming nodes into the replicas.
func (m *ReplicaManager) assignIncomingNodeIntoReplicas(replicaIDs typeutil.UniqueSet, incomingNodes typeutil.UniqueSet) []*Replica {
	// Clone all mutableReplicas and modify it.
	mutableReplicas := make(map[typeutil.UniqueID]*mutableReplica, len(replicaIDs))
	assigned := typeutil.NewUniqueSet()
	for replicaID := range replicaIDs {
		// replica is always exists by protection of mutex.
		replica := m.replicas[replicaID].copyForWrite()
		mutableReplicas[replicaID] = replica
	}

	selector := func(nodeID typeutil.UniqueID) *mutableReplica {
		// If nodeID is in outbound of replica, it must be assigned to it.
		// e.g. node1 in rg1, replica1 and replica2 in rg1, node1 is used by replica1 now.
		// node1 moved from rg1, node1 is a outbound node of replica1, but do not removed.
		// than node1 move back to rg1, it must be assign to replica1, but not replica2.
		//
		// otherwise, select replica with minimum nodes.
		// TODO: It is not a final-balanced strategy, should be optimized in future.
		var minimumNodesReplica *mutableReplica
		for _, replica := range mutableReplicas {
			if replica.ContainOutboundNode(nodeID) {
				return replica
			}
			if minimumNodesReplica == nil || minimumNodesReplica.AvailableNodesCount() > replica.AvailableNodesCount() {
				minimumNodesReplica = replica
			}
		}
		return minimumNodesReplica
	}

	// Assign the new incoming nodes into the replicas.
	for node := range incomingNodes {
		mutableReplica := selector(node)
		mutableReplica.AddNode(node)
		assigned.Insert(mutableReplica.GetID())
	}

	// Filtering all updates.
	ret := make([]*Replica, 0, len(replicaIDs))
	for replicaID := range assigned {
		ret = append(ret, mutableReplicas[replicaID].IntoReplica())
	}
	return ret
}

// findAllIncomingNodes finds all new incoming nodes for all replicas in same collection and resource group.
func (m *ReplicaManager) findAllIncomingNodes(replicaIDs typeutil.UniqueSet, allAvailableNodes typeutil.UniqueSet) typeutil.UniqueSet {
	// Found all new incoming nodes doesn't in-used by all replicas.
	newIncomingNodes := typeutil.NewUniqueSet(allAvailableNodes.Collect()...)
	for replicaID := range replicaIDs {
		// replica is always exists by protection of mutex.
		replica := m.replicas[replicaID]
		replica.RangeOverAvailableNodes(func(node int64) bool {
			// If the node is in-used by replica, remove it.
			newIncomingNodes.Remove(node)
			return true
		})
	}
	return newIncomingNodes
}

// isNodeIsUsedByOtherReplicaInSameCollection checks if the node is in-used by other replica of same collection.
func (m *ReplicaManager) isNodeIsUsedByOtherReplicaInSameCollection(collectionID typeutil.UniqueID, replicaIDs typeutil.UniqueSet, nodeID typeutil.UniqueID) bool {
	for replicaID := range m.collIDToReplicaIDs[collectionID] {
		if replicaIDs.Contain(replicaID) {
			continue
		}
		r := m.replicas[replicaID]
		if r.InUseNode(nodeID) {
			return true
		}
	}
	return false
}

// RemoveNode removes the node from all replicas of given collection.
func (m *ReplicaManager) RemoveNode(replicaID typeutil.UniqueID, nodes ...typeutil.UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	replica, ok := m.replicas[replicaID]
	if !ok {
		return merr.WrapErrReplicaNotFound(replicaID)
	}

	mutableReplica := replica.copyForWrite()
	mutableReplica.RemoveNode(nodes...)
	return m.put(mutableReplica.IntoReplica())
}

func (m *ReplicaManager) GetResourceGroupByCollection(collection typeutil.UniqueID) typeutil.Set[string] {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	ret := typeutil.NewSet[string]()
	for _, r := range m.replicas {
		if r.GetCollectionID() == collection {
			ret.Insert(r.GetResourceGroup())
		}
	}

	return ret
}
