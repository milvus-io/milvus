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
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type ReplicaManager struct {
	rwmutex sync.RWMutex

	idAllocator   func() (int64, error)
	replicas      map[typeutil.UniqueID]*Replica
	coll2Replicas map[typeutil.UniqueID]*collectionReplicas // typeutil.UniqueSet
	catalog       metastore.QueryCoordCatalog
}

// collectionReplicas maintains collection secondary index mapping
type collectionReplicas struct {
	id2replicas map[typeutil.UniqueID]*Replica
	replicas    []*Replica
}

func (crs *collectionReplicas) removeReplicas(replicaIDs ...int64) (empty bool) {
	for _, replicaID := range replicaIDs {
		delete(crs.id2replicas, replicaID)
	}
	crs.replicas = lo.Values(crs.id2replicas)
	return len(crs.replicas) == 0
}

func (crs *collectionReplicas) putReplica(replica *Replica) {
	crs.id2replicas[replica.GetID()] = replica
	crs.replicas = lo.Values(crs.id2replicas)
}

func newCollectionReplicas() *collectionReplicas {
	return &collectionReplicas{
		id2replicas: make(map[typeutil.UniqueID]*Replica),
	}
}

func NewReplicaManager(idAllocator func() (int64, error), catalog metastore.QueryCoordCatalog) *ReplicaManager {
	return &ReplicaManager{
		idAllocator:   idAllocator,
		replicas:      make(map[int64]*Replica),
		coll2Replicas: make(map[int64]*collectionReplicas),
		catalog:       catalog,
	}
}

// Recover recovers the replicas for given collections from meta store
func (m *ReplicaManager) Recover(ctx context.Context, collections []int64) error {
	replicas, err := m.catalog.GetReplicas(ctx)
	if err != nil {
		return fmt.Errorf("failed to recover replicas, err=%w", err)
	}

	collectionSet := typeutil.NewUniqueSet(collections...)
	for _, replica := range replicas {
		if len(replica.GetResourceGroup()) == 0 {
			replica.ResourceGroup = DefaultResourceGroupName
		}

		if collectionSet.Contain(replica.GetCollectionID()) {
			rep := NewReplicaWithPriority(replica, commonpb.LoadPriority_HIGH)
			m.putReplicaInMemory(rep)
			log.Info("recover replica",
				zap.Int64("collectionID", replica.GetCollectionID()),
				zap.Int64("replicaID", replica.GetID()),
				zap.Int64s("nodes", replica.GetNodes()),
			)
		} else {
			err := m.catalog.ReleaseReplica(ctx, replica.GetCollectionID(), replica.GetID())
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
func (m *ReplicaManager) Get(ctx context.Context, id typeutil.UniqueID) *Replica {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.replicas[id]
}

// Spawn spawns N replicas at resource group for given collection in ReplicaManager.
func (m *ReplicaManager) Spawn(ctx context.Context, collection int64,
	replicaNumInRG map[string]int, channels []string, loadPriority commonpb.LoadPriority,
) ([]*Replica, error) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

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
			replicas = append(replicas, NewReplicaWithPriority(&querypb.Replica{
				ID:               id,
				CollectionID:     collection,
				ResourceGroup:    rgName,
				ChannelNodeInfos: channelExclusiveNodeInfo,
			}, loadPriority))
		}
	}
	if err := m.put(ctx, replicas...); err != nil {
		return nil, err
	}
	return replicas, nil
}

// Deprecated: Warning, break the consistency of ReplicaManager,
// never use it in non-test code, use Spawn instead.
func (m *ReplicaManager) Put(ctx context.Context, replicas ...*Replica) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	return m.put(ctx, replicas...)
}

func (m *ReplicaManager) put(ctx context.Context, replicas ...*Replica) error {
	if len(replicas) == 0 {
		return nil
	}
	// Persist replicas into KV.
	replicaPBs := make([]*querypb.Replica, 0, len(replicas))
	for _, replica := range replicas {
		replicaPBs = append(replicaPBs, replica.replicaPB)
	}
	if err := m.catalog.SaveReplica(ctx, replicaPBs...); err != nil {
		return err
	}

	m.putReplicaInMemory(replicas...)
	return nil
}

// putReplicaInMemory puts replicas into in-memory map and collIDToReplicaIDs.
func (m *ReplicaManager) putReplicaInMemory(replicas ...*Replica) {
	for _, replica := range replicas {
		if oldReplica, ok := m.replicas[replica.GetID()]; ok {
			metrics.QueryCoordResourceGroupReplicaTotal.WithLabelValues(oldReplica.GetResourceGroup()).Dec()
			metrics.QueryCoordReplicaRONodeTotal.Add(-float64(oldReplica.RONodesCount()))
		}
		// update in-memory replicas.
		m.replicas[replica.GetID()] = replica
		metrics.QueryCoordResourceGroupReplicaTotal.WithLabelValues(replica.GetResourceGroup()).Inc()
		metrics.QueryCoordReplicaRONodeTotal.Add(float64(replica.RONodesCount()))

		// update collIDToReplicaIDs.
		if m.coll2Replicas[replica.GetCollectionID()] == nil {
			m.coll2Replicas[replica.GetCollectionID()] = newCollectionReplicas()
		}
		m.coll2Replicas[replica.GetCollectionID()].putReplica(replica)
	}
}

// TransferReplica transfers N replicas from srcRGName to dstRGName.
func (m *ReplicaManager) TransferReplica(ctx context.Context, collectionID typeutil.UniqueID, srcRGName string, dstRGName string, replicaNum int) error {
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
	return m.put(ctx, replicas...)
}

func (m *ReplicaManager) MoveReplica(ctx context.Context, dstRGName string, toMove []*Replica) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()
	replicas := make([]*Replica, 0, len(toMove))
	replicaIDs := make([]int64, 0)
	for _, replica := range toMove {
		mutableReplica := replica.CopyForWrite()
		mutableReplica.SetResourceGroup(dstRGName)
		replicas = append(replicas, mutableReplica.IntoReplica())
		replicaIDs = append(replicaIDs, replica.GetID())
	}
	log.Info("move replicas to resource group", zap.String("dstRGName", dstRGName), zap.Int64s("replicas", replicaIDs))
	return m.put(ctx, replicas...)
}

// getSrcReplicasAndCheckIfTransferable checks if the collection can be transfer from srcRGName to dstRGName.
func (m *ReplicaManager) getSrcReplicasAndCheckIfTransferable(collectionID typeutil.UniqueID, srcRGName string, replicaNum int) ([]*Replica, error) {
	// Check if collection is loaded.
	if m.coll2Replicas[collectionID] == nil {
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
func (m *ReplicaManager) RemoveCollection(ctx context.Context, collectionID typeutil.UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	err := m.catalog.ReleaseReplicas(ctx, collectionID)
	if err != nil {
		return err
	}

	if collReplicas, ok := m.coll2Replicas[collectionID]; ok {
		// Remove all replica of collection and remove collection from collIDToReplicaIDs.
		for _, replica := range collReplicas.replicas {
			metrics.QueryCoordResourceGroupReplicaTotal.WithLabelValues(replica.GetResourceGroup()).Dec()
			metrics.QueryCoordReplicaRONodeTotal.Add(-float64(replica.RONodesCount()))
			delete(m.replicas, replica.GetID())
		}
		delete(m.coll2Replicas, collectionID)
	}
	return nil
}

func (m *ReplicaManager) RemoveReplicas(ctx context.Context, collectionID typeutil.UniqueID, replicas ...typeutil.UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	log.Info("release replicas", zap.Int64("collectionID", collectionID), zap.Int64s("replicas", replicas))

	return m.removeReplicas(ctx, collectionID, replicas...)
}

func (m *ReplicaManager) removeReplicas(ctx context.Context, collectionID typeutil.UniqueID, replicas ...typeutil.UniqueID) error {
	err := m.catalog.ReleaseReplica(ctx, collectionID, replicas...)
	if err != nil {
		return err
	}

	for _, replicaID := range replicas {
		if replica, ok := m.replicas[replicaID]; ok {
			metrics.QueryCoordResourceGroupReplicaTotal.WithLabelValues(replica.GetResourceGroup()).Dec()
			metrics.QueryCoordReplicaRONodeTotal.Add(float64(-replica.RONodesCount()))
			delete(m.replicas, replicaID)
		}
	}

	if m.coll2Replicas[collectionID].removeReplicas(replicas...) {
		delete(m.coll2Replicas, collectionID)
	}

	return nil
}

func (m *ReplicaManager) GetByCollection(ctx context.Context, collectionID typeutil.UniqueID) []*Replica {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()
	return m.getByCollection(collectionID)
}

func (m *ReplicaManager) getByCollection(collectionID typeutil.UniqueID) []*Replica {
	collReplicas, ok := m.coll2Replicas[collectionID]
	if !ok {
		return nil
	}

	return collReplicas.replicas
}

func (m *ReplicaManager) GetByCollectionAndNode(ctx context.Context, collectionID, nodeID typeutil.UniqueID) *Replica {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	if m.coll2Replicas[collectionID] != nil {
		for _, replica := range m.coll2Replicas[collectionID].replicas {
			if replica.Contains(nodeID) {
				return replica
			}
		}
	}

	return nil
}

func (m *ReplicaManager) GetByNode(ctx context.Context, nodeID typeutil.UniqueID) []*Replica {
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
	collReplicas, ok := m.coll2Replicas[collectionID]
	if !ok {
		return nil
	}

	return lo.Filter(collReplicas.replicas, func(replica *Replica, _ int) bool {
		return replica.GetResourceGroup() == rgName
	})
}

func (m *ReplicaManager) GetByResourceGroup(ctx context.Context, rgName string) []*Replica {
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
func (m *ReplicaManager) RecoverNodesInCollection(ctx context.Context, collectionID typeutil.UniqueID, rgs map[string]typeutil.UniqueSet) error {
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
	return m.put(ctx, modifiedReplicas...)
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
	collReplicas, ok := m.coll2Replicas[collectionID]
	if !ok {
		return nil, errors.Errorf("collection %d not loaded", collectionID)
	}

	rgToReplicas := make(map[string][]*Replica)
	for _, replica := range collReplicas.replicas {
		rgName := replica.GetResourceGroup()
		if _, ok := rgs[rgName]; !ok {
			return nil, errors.Errorf("lost resource group info, collectionID: %d, replicaID: %d, resourceGroup: %s", collectionID, replica.GetID(), rgName)
		}
		if _, ok := rgToReplicas[rgName]; !ok {
			rgToReplicas[rgName] = make([]*Replica, 0)
		}
		rgToReplicas[rgName] = append(rgToReplicas[rgName], replica)
	}
	return newCollectionAssignmentHelper(collectionID, rgToReplicas, rgs), nil
}

// RemoveNode removes the node from all replicas of given collection.
func (m *ReplicaManager) RemoveNode(ctx context.Context, replicaID typeutil.UniqueID, nodes ...typeutil.UniqueID) error {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	replica, ok := m.replicas[replicaID]
	if !ok {
		return merr.WrapErrReplicaNotFound(replicaID)
	}

	mutableReplica := replica.CopyForWrite()
	mutableReplica.RemoveNode(nodes...) // ro -> unused
	return m.put(ctx, mutableReplica.IntoReplica())
}

func (m *ReplicaManager) GetResourceGroupByCollection(ctx context.Context, collection typeutil.UniqueID) typeutil.Set[string] {
	replicas := m.GetByCollection(ctx, collection)
	ret := typeutil.NewSet(lo.Map(replicas, func(r *Replica, _ int) string { return r.GetResourceGroup() })...)
	return ret
}

// GetReplicasJSON returns a JSON representation of all replicas managed by the ReplicaManager.
// It locks the ReplicaManager for reading, converts the replicas to their protobuf representation,
// marshals them into a JSON string, and returns the result.
// If an error occurs during marshaling, it logs a warning and returns an empty string.
func (m *ReplicaManager) GetReplicasJSON(ctx context.Context, meta *Meta) string {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	replicas := lo.MapToSlice(m.replicas, func(i typeutil.UniqueID, r *Replica) *metricsinfo.Replica {
		channelTowRWNodes := make(map[string][]int64)
		for k, v := range r.replicaPB.GetChannelNodeInfos() {
			channelTowRWNodes[k] = v.GetRwNodes()
		}

		collectionInfo := meta.GetCollection(ctx, r.GetCollectionID())
		dbID := util.InvalidDBID
		if collectionInfo == nil {
			log.Ctx(ctx).Warn("failed to get collection info", zap.Int64("collectionID", r.GetCollectionID()))
		} else {
			dbID = collectionInfo.GetDbID()
		}

		return &metricsinfo.Replica{
			ID:               r.GetID(),
			CollectionID:     r.GetCollectionID(),
			DatabaseID:       dbID,
			RWNodes:          r.GetNodes(),
			ResourceGroup:    r.GetResourceGroup(),
			RONodes:          r.GetRONodes(),
			ChannelToRWNodes: channelTowRWNodes,
		}
	})
	ret, err := json.Marshal(replicas)
	if err != nil {
		log.Warn("failed to marshal replicas", zap.Error(err))
		return ""
	}
	return string(ret)
}
