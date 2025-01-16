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
	"strconv"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var ErrNodeNotEnough = errors.New("nodes not enough")

type ResourceManager struct {
	incomingNode typeutil.UniqueSet // incomingNode is a temporary set for incoming hangup node,
	// after node is assigned to resource group, it will be removed from this set.
	groups    map[string]*ResourceGroup // primary index from resource group name to resource group
	nodeIDMap map[int64]string          // secondary index from node id to resource group

	catalog metastore.QueryCoordCatalog
	nodeMgr *session.NodeManager // TODO: ResourceManager is watch node status with service discovery, so it can handle node up and down as fast as possible.
	// All function can get latest online node without checking with node manager.
	// so node manager is a redundant type here.

	rwmutex           sync.RWMutex
	rgChangedNotifier *syncutil.VersionedNotifier // used to notify that resource group has been changed.
	// resource_observer will listen this notifier to do a resource group recovery.
	nodeChangedNotifier *syncutil.VersionedNotifier // used to notify that node distribution in resource group has been changed.
	// replica_observer will listen this notifier to do a replica recovery.
}

// NewResourceManager is used to create a ResourceManager instance.
func NewResourceManager(catalog metastore.QueryCoordCatalog, nodeMgr *session.NodeManager) *ResourceManager {
	groups := make(map[string]*ResourceGroup)
	// Always create a default resource group to keep compatibility.
	groups[DefaultResourceGroupName] = NewResourceGroup(DefaultResourceGroupName, newResourceGroupConfig(0, defaultResourceGroupCapacity), nodeMgr)
	return &ResourceManager{
		incomingNode: typeutil.NewUniqueSet(),
		groups:       groups,
		nodeIDMap:    make(map[int64]string),
		catalog:      catalog,
		nodeMgr:      nodeMgr,

		rwmutex:             sync.RWMutex{},
		rgChangedNotifier:   syncutil.NewVersionedNotifier(),
		nodeChangedNotifier: syncutil.NewVersionedNotifier(),
	}
}

// Recover recover resource group from meta, other interface of ResourceManager can be only called after recover is done.
func (rm *ResourceManager) Recover(ctx context.Context) error {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()

	rgs, err := rm.catalog.GetResourceGroups(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to recover resource group from store")
	}

	// Resource group meta upgrade to latest version.
	upgrades := make([]*querypb.ResourceGroup, 0)
	for _, meta := range rgs {
		needUpgrade := meta.Config == nil

		rg := NewResourceGroupFromMeta(meta, rm.nodeMgr)
		rm.setupInMemResourceGroup(rg)
		for _, node := range rg.GetNodes() {
			if _, ok := rm.nodeIDMap[node]; ok {
				// unreachable code, should never happen.
				panic(fmt.Sprintf("dirty meta, node has been assign to multi resource group, %s, %s", rm.nodeIDMap[node], rg.GetName()))
			}
			rm.nodeIDMap[node] = rg.GetName()
		}
		log.Info("Recover resource group",
			zap.String("rgName", rg.GetName()),
			zap.Int64s("nodes", rm.groups[rg.GetName()].GetNodes()),
			zap.Any("config", rg.GetConfig()),
		)
		if needUpgrade {
			upgrades = append(upgrades, rg.GetMeta())
		}
	}
	if len(upgrades) > 0 {
		log.Info("upgrade resource group meta into latest", zap.Int("num", len(upgrades)))
		return rm.catalog.SaveResourceGroup(ctx, upgrades...)
	}
	return nil
}

// AddResourceGroup create a new ResourceGroup.
// Do no changed with node, all node will be reassign to new resource group by auto recover.
func (rm *ResourceManager) AddResourceGroup(ctx context.Context, rgName string, cfg *rgpb.ResourceGroupConfig) error {
	if len(rgName) == 0 {
		return merr.WrapErrParameterMissing("resource group name couldn't be empty")
	}
	if cfg == nil {
		// Use default config if not set, compatible with old client.
		cfg = newResourceGroupConfig(0, 0)
	}

	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()
	if rm.groups[rgName] != nil {
		// Idempotent promise.
		// If resource group already exist, check if configuration is the same,
		if proto.Equal(rm.groups[rgName].GetConfig(), cfg) {
			return nil
		}
		return merr.WrapErrResourceGroupAlreadyExist(rgName)
	}

	maxResourceGroup := paramtable.Get().QuotaConfig.MaxResourceGroupNumOfQueryNode.GetAsInt()
	if len(rm.groups) >= maxResourceGroup {
		return merr.WrapErrResourceGroupReachLimit(rgName, maxResourceGroup)
	}

	if err := rm.validateResourceGroupConfig(rgName, cfg); err != nil {
		return err
	}

	rg := NewResourceGroup(rgName, cfg, rm.nodeMgr)
	if err := rm.catalog.SaveResourceGroup(ctx, rg.GetMeta()); err != nil {
		log.Warn("failed to add resource group",
			zap.String("rgName", rgName),
			zap.Any("config", cfg),
			zap.Error(err),
		)
		return merr.WrapErrResourceGroupServiceAvailable()
	}

	rm.setupInMemResourceGroup(rg)
	log.Info("add resource group",
		zap.String("rgName", rgName),
		zap.Any("config", cfg),
	)

	// notify that resource group config has been changed.
	rm.rgChangedNotifier.NotifyAll()
	return nil
}

// UpdateResourceGroups update resource group configuration.
// Only change the configuration, no change with node. all node will be reassign by auto recover.
func (rm *ResourceManager) UpdateResourceGroups(ctx context.Context, rgs map[string]*rgpb.ResourceGroupConfig) error {
	if len(rgs) == 0 {
		return nil
	}

	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()
	return rm.updateResourceGroups(ctx, rgs)
}

// updateResourceGroups update resource group configuration.
func (rm *ResourceManager) updateResourceGroups(ctx context.Context, rgs map[string]*rgpb.ResourceGroupConfig) error {
	modifiedRG := make([]*ResourceGroup, 0, len(rgs))
	updates := make([]*querypb.ResourceGroup, 0, len(rgs))
	for rgName, cfg := range rgs {
		if _, ok := rm.groups[rgName]; !ok {
			return merr.WrapErrResourceGroupNotFound(rgName)
		}
		if err := rm.validateResourceGroupConfig(rgName, cfg); err != nil {
			return err
		}
		// Update with copy on write.
		mrg := rm.groups[rgName].CopyForWrite()
		mrg.UpdateConfig(cfg)
		rg := mrg.ToResourceGroup()

		updates = append(updates, rg.GetMeta())
		modifiedRG = append(modifiedRG, rg)
	}

	if err := rm.catalog.SaveResourceGroup(ctx, updates...); err != nil {
		for rgName, cfg := range rgs {
			log.Warn("failed to update resource group",
				zap.String("rgName", rgName),
				zap.Any("config", cfg),
				zap.Error(err),
			)
		}
		return merr.WrapErrResourceGroupServiceAvailable()
	}

	// Commit updates to memory.
	for _, rg := range modifiedRG {
		log.Info("update resource group",
			zap.String("rgName", rg.GetName()),
			zap.Any("config", rg.GetConfig()),
		)
		rm.setupInMemResourceGroup(rg)
	}

	// notify that resource group config has been changed.
	rm.rgChangedNotifier.NotifyAll()
	return nil
}

// go:deprecated TransferNode transfer node from source resource group to target resource group.
// Deprecated, use Declarative API `UpdateResourceGroups` instead.
func (rm *ResourceManager) TransferNode(ctx context.Context, sourceRGName string, targetRGName string, nodeNum int) error {
	if sourceRGName == targetRGName {
		return merr.WrapErrParameterInvalidMsg("source resource group and target resource group should not be the same, resource group: %s", sourceRGName)
	}
	if nodeNum <= 0 {
		return merr.WrapErrParameterInvalid("NumNode > 0", fmt.Sprintf("invalid NumNode %d", nodeNum))
	}

	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()

	if rm.groups[sourceRGName] == nil {
		return merr.WrapErrResourceGroupNotFound(sourceRGName)
	}
	if rm.groups[targetRGName] == nil {
		return merr.WrapErrResourceGroupNotFound(targetRGName)
	}

	sourceRG := rm.groups[sourceRGName]
	targetRG := rm.groups[targetRGName]

	// Check if source resource group has enough node to transfer.
	if len(sourceRG.GetNodes()) < nodeNum {
		return merr.WrapErrResourceGroupNodeNotEnough(sourceRGName, len(sourceRG.GetNodes()), nodeNum)
	}

	// Compatible with old version.
	sourceCfg := sourceRG.GetConfigCloned()
	targetCfg := targetRG.GetConfigCloned()
	sourceCfg.Requests.NodeNum -= int32(nodeNum)
	if sourceCfg.Requests.NodeNum < 0 {
		sourceCfg.Requests.NodeNum = 0
	}
	// Special case for compatibility with old version.
	if sourceRGName != DefaultResourceGroupName {
		sourceCfg.Limits.NodeNum -= int32(nodeNum)
		if sourceCfg.Limits.NodeNum < 0 {
			sourceCfg.Limits.NodeNum = 0
		}
	}

	targetCfg.Requests.NodeNum += int32(nodeNum)
	if targetCfg.Requests.NodeNum > targetCfg.Limits.NodeNum {
		targetCfg.Limits.NodeNum = targetCfg.Requests.NodeNum
	}
	return rm.updateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		sourceRGName: sourceCfg,
		targetRGName: targetCfg,
	})
}

// RemoveResourceGroup remove resource group.
func (rm *ResourceManager) RemoveResourceGroup(ctx context.Context, rgName string) error {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()

	if rm.groups[rgName] == nil {
		// Idempotent promise: delete a non-exist rg should be ok
		return nil
	}

	// validateResourceGroupIsDeletable will check if rg is deletable.
	if err := rm.validateResourceGroupIsDeletable(rgName); err != nil {
		return err
	}

	// Nodes may be still assign to these group,
	// recover the resource group from redundant status before remove it.
	if rm.groups[rgName].NodeNum() > 0 {
		if err := rm.recoverRedundantNodeRG(ctx, rgName); err != nil {
			log.Info("failed to recover redundant node resource group before remove it",
				zap.String("rgName", rgName),
				zap.Error(err),
			)
			return err
		}
	}

	// Remove it from meta storage.
	if err := rm.catalog.RemoveResourceGroup(ctx, rgName); err != nil {
		log.Info("failed to remove resource group",
			zap.String("rgName", rgName),
			zap.Error(err),
		)
		return merr.WrapErrResourceGroupServiceAvailable()
	}

	// After recovering, all node assigned to these rg has been removed.
	// no secondary index need to be removed.
	delete(rm.groups, rgName)
	metrics.QueryCoordResourceGroupInfo.DeletePartialMatch(prometheus.Labels{
		metrics.ResourceGroupLabelName: rgName,
	})
	metrics.QueryCoordResourceGroupReplicaTotal.DeletePartialMatch(prometheus.Labels{
		metrics.ResourceGroupLabelName: rgName,
	})

	log.Info("remove resource group",
		zap.String("rgName", rgName),
	)
	// notify that resource group has been changed.
	rm.rgChangedNotifier.NotifyAll()
	return nil
}

// GetNodesOfMultiRG return nodes of multi rg, it can be used to get a consistent view of nodes of multi rg.
func (rm *ResourceManager) GetNodesOfMultiRG(ctx context.Context, rgName []string) (map[string]typeutil.UniqueSet, error) {
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()

	ret := make(map[string]typeutil.UniqueSet)
	for _, name := range rgName {
		if rm.groups[name] == nil {
			return nil, merr.WrapErrResourceGroupNotFound(name)
		}
		ret[name] = typeutil.NewUniqueSet(rm.groups[name].GetNodes()...)
	}
	return ret, nil
}

// GetNodes return nodes of given resource group.
func (rm *ResourceManager) GetNodes(ctx context.Context, rgName string) ([]int64, error) {
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()
	if rm.groups[rgName] == nil {
		return nil, merr.WrapErrResourceGroupNotFound(rgName)
	}
	return rm.groups[rgName].GetNodes(), nil
}

// GetResourceGroupByNodeID return whether resource group's node match required node count
func (rm *ResourceManager) VerifyNodeCount(ctx context.Context, requiredNodeCount map[string]int) error {
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()
	for rgName, nodeCount := range requiredNodeCount {
		if rm.groups[rgName] == nil {
			return merr.WrapErrResourceGroupNotFound(rgName)
		}
		if rm.groups[rgName].NodeNum() != nodeCount {
			return ErrNodeNotEnough
		}
	}

	return nil
}

// GetOutgoingNodeNumByReplica return outgoing node num on each rg from this replica.
func (rm *ResourceManager) GetOutgoingNodeNumByReplica(ctx context.Context, replica *Replica) map[string]int32 {
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()

	if rm.groups[replica.GetResourceGroup()] == nil {
		return nil
	}
	rg := rm.groups[replica.GetResourceGroup()]

	ret := make(map[string]int32)
	replica.RangeOverRONodes(func(node int64) bool {
		// if rgOfNode is not equal to rg of replica, outgoing node found.
		if rgOfNode := rm.getResourceGroupByNodeID(node); rgOfNode != nil && rgOfNode.GetName() != rg.GetName() {
			ret[rgOfNode.GetName()]++
		}
		return true
	})
	return ret
}

// getResourceGroupByNodeID get resource group by node id.
func (rm *ResourceManager) getResourceGroupByNodeID(nodeID int64) *ResourceGroup {
	if rgName, ok := rm.nodeIDMap[nodeID]; ok {
		return rm.groups[rgName]
	}
	return nil
}

// ContainsNode return whether given node is in given resource group.
func (rm *ResourceManager) ContainsNode(ctx context.Context, rgName string, node int64) bool {
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()
	if rm.groups[rgName] == nil {
		return false
	}
	return rm.groups[rgName].ContainNode(node)
}

// ContainResourceGroup return whether given resource group is exist.
func (rm *ResourceManager) ContainResourceGroup(ctx context.Context, rgName string) bool {
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()
	return rm.groups[rgName] != nil
}

// GetResourceGroup return resource group snapshot by name.
func (rm *ResourceManager) GetResourceGroup(ctx context.Context, rgName string) *ResourceGroup {
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()

	if rm.groups[rgName] == nil {
		return nil
	}
	return rm.groups[rgName].Snapshot()
}

// ListResourceGroups return all resource groups names.
func (rm *ResourceManager) ListResourceGroups(ctx context.Context) []string {
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()

	return lo.Keys(rm.groups)
}

// MeetRequirement return whether resource group meet requirement.
// Return error with reason if not meet requirement.
func (rm *ResourceManager) MeetRequirement(ctx context.Context, rgName string) error {
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()
	if rm.groups[rgName] == nil {
		return nil
	}
	return rm.groups[rgName].MeetRequirement()
}

// CheckIncomingNodeNum return incoming node num.
func (rm *ResourceManager) CheckIncomingNodeNum(ctx context.Context) int {
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()
	return rm.incomingNode.Len()
}

// HandleNodeUp handle node when new node is incoming.
func (rm *ResourceManager) HandleNodeUp(ctx context.Context, node int64) {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()

	rm.incomingNode.Insert(node)
	// Trigger assign incoming node right away.
	// error can be ignored here, because `AssignPendingIncomingNode`` will retry assign node.
	rgName, err := rm.assignIncomingNodeWithNodeCheck(ctx, node)
	log.Info("HandleNodeUp: add node to resource group",
		zap.String("rgName", rgName),
		zap.Int64("node", node),
		zap.Error(err),
	)
}

// HandleNodeDown handle the node when node is leave.
func (rm *ResourceManager) HandleNodeDown(ctx context.Context, node int64) {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()

	rm.incomingNode.Remove(node)

	// for stopping query node becomes offline, node change won't be triggered,
	// cause when it becomes stopping, it already remove from resource manager
	// then `unassignNode` will do nothing
	rgName, err := rm.unassignNode(ctx, node)

	// trigger node changes, expected to remove ro node from replica immediately
	rm.nodeChangedNotifier.NotifyAll()
	log.Info("HandleNodeDown: remove node from resource group",
		zap.String("rgName", rgName),
		zap.Int64("node", node),
		zap.Error(err),
	)
}

func (rm *ResourceManager) HandleNodeStopping(ctx context.Context, node int64) {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()

	rm.incomingNode.Remove(node)
	rgName, err := rm.unassignNode(ctx, node)
	log.Info("HandleNodeStopping: remove node from resource group",
		zap.String("rgName", rgName),
		zap.Int64("node", node),
		zap.Error(err),
	)
}

// ListenResourceGroupChanged return a listener for resource group changed.
func (rm *ResourceManager) ListenResourceGroupChanged(ctx context.Context) *syncutil.VersionedListener {
	return rm.rgChangedNotifier.Listen(syncutil.VersionedListenAtEarliest)
}

// ListenNodeChanged return a listener for node changed.
func (rm *ResourceManager) ListenNodeChanged(ctx context.Context) *syncutil.VersionedListener {
	return rm.nodeChangedNotifier.Listen(syncutil.VersionedListenAtEarliest)
}

// AssignPendingIncomingNode assign incoming node to resource group.
func (rm *ResourceManager) AssignPendingIncomingNode(ctx context.Context) {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()

	for node := range rm.incomingNode {
		rgName, err := rm.assignIncomingNodeWithNodeCheck(ctx, node)
		log.Info("Pending HandleNodeUp: add node to resource group",
			zap.String("rgName", rgName),
			zap.Int64("node", node),
			zap.Error(err),
		)
	}
}

// AutoRecoverResourceGroup auto recover rg, return recover used node num
func (rm *ResourceManager) AutoRecoverResourceGroup(ctx context.Context, rgName string) error {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()

	rg := rm.groups[rgName]
	if rg == nil {
		return nil
	}

	if rg.MissingNumOfNodes() > 0 {
		return rm.recoverMissingNodeRG(ctx, rgName)
	}

	// DefaultResourceGroup is the backup resource group of redundant recovery,
	// So after all other resource group is reach the `limits`, rest redundant node will be transfer to DefaultResourceGroup.
	if rg.RedundantNumOfNodes() > 0 {
		return rm.recoverRedundantNodeRG(ctx, rgName)
	}
	return nil
}

// recoverMissingNodeRG recover resource group by transfer node from other resource group.
func (rm *ResourceManager) recoverMissingNodeRG(ctx context.Context, rgName string) error {
	for rm.groups[rgName].MissingNumOfNodes() > 0 {
		targetRG := rm.groups[rgName]
		node, sourceRG := rm.selectNodeForMissingRecover(targetRG)
		if sourceRG == nil {
			log.Warn("fail to select source resource group", zap.String("rgName", targetRG.GetName()))
			return ErrNodeNotEnough
		}

		err := rm.transferNode(ctx, targetRG.GetName(), node)
		if err != nil {
			log.Warn("failed to recover missing node by transfer node from other resource group",
				zap.String("sourceRG", sourceRG.GetName()),
				zap.String("targetRG", targetRG.GetName()),
				zap.Int64("nodeID", node),
				zap.Error(err))
			return err
		}
		log.Info("recover missing node by transfer node from other resource group",
			zap.String("sourceRG", sourceRG.GetName()),
			zap.String("targetRG", targetRG.GetName()),
			zap.Int64("nodeID", node),
		)
	}
	return nil
}

// selectNodeForMissingRecover selects a node for missing recovery.
// It takes a target ResourceGroup and returns the selected node's ID and the source ResourceGroup with highest priority.
func (rm *ResourceManager) selectNodeForMissingRecover(targetRG *ResourceGroup) (int64, *ResourceGroup) {
	computeRGPriority := func(rg *ResourceGroup) int {
		// If the ResourceGroup has redundant nodes,  boost it's priority its priority 1000,000.
		if rg.RedundantNumOfNodes() > 0 {
			return rg.RedundantNumOfNodes() * 1000000
		}
		// If the target ResourceGroup has a 'from' relationship with the current ResourceGroup,
		// boost it's priority its priority 100,000.
		if targetRG.HasFrom(rg.GetName()) {
			return rg.OversizedNumOfNodes() * 100000
		}
		return rg.OversizedNumOfNodes()
	}

	maxPriority := 0
	var sourceRG *ResourceGroup
	candidateNode := int64(-1)

	for _, rg := range rm.groups {
		if rg.GetName() == targetRG.GetName() {
			continue
		}
		if rg.OversizedNumOfNodes() <= 0 {
			continue
		}

		priority := computeRGPriority(rg)
		if priority > maxPriority {
			// Select a node from the current resource group that is preferred to be removed and assigned to the target resource group.
			node := rg.SelectNodeForRG(targetRG)
			// If no such node is found, skip the current resource group.
			if node == -1 {
				continue
			}

			sourceRG = rg
			candidateNode = node
			maxPriority = priority
		}
	}

	return candidateNode, sourceRG
}

// recoverRedundantNodeRG recover resource group by transfer node to other resource group.
func (rm *ResourceManager) recoverRedundantNodeRG(ctx context.Context, rgName string) error {
	for rm.groups[rgName].RedundantNumOfNodes() > 0 {
		sourceRG := rm.groups[rgName]
		node, targetRG := rm.selectNodeForRedundantRecover(sourceRG)
		if node == -1 {
			log.Info("failed to select redundant recover target resource group, please check resource group configuration if as expected.",
				zap.String("rgName", sourceRG.GetName()))
			return errors.New("all resource group reach limits")
		}

		if err := rm.transferNode(ctx, targetRG.GetName(), node); err != nil {
			log.Warn("failed to recover redundant node by transfer node to other resource group",
				zap.String("sourceRG", sourceRG.GetName()),
				zap.String("targetRG", targetRG.GetName()),
				zap.Int64("nodeID", node),
				zap.Error(err))
			return err
		}
		log.Info("recover redundant node by transfer node to other resource group",
			zap.String("sourceRG", sourceRG.GetName()),
			zap.String("targetRG", targetRG.GetName()),
			zap.Int64("nodeID", node),
		)
	}
	return nil
}

// selectNodeForRedundantRecover selects a node for redundant recovery.
// It takes a source ResourceGroup and returns the selected node's ID and the target ResourceGroup with highest priority.
func (rm *ResourceManager) selectNodeForRedundantRecover(sourceRG *ResourceGroup) (int64, *ResourceGroup) {
	// computeRGPriority calculates the priority of a ResourceGroup based on certain conditions.
	computeRGPriority := func(rg *ResourceGroup) int {
		// If the ResourceGroup is missing nodes, boost it's priority by 1,000,000.
		if rg.MissingNumOfNodes() > 0 {
			return rg.MissingNumOfNodes() * 1000000
		}
		// If the source ResourceGroup has a 'to' relationship with the current ResourceGroup,
		// boost it's priority by 1,000,00.
		if sourceRG.HasTo(rg.GetName()) {
			return rg.ReachLimitNumOfNodes() * 100000
		}
		return rg.ReachLimitNumOfNodes()
	}

	maxPriority := 0
	var targetRG *ResourceGroup
	candidateNode := int64(-1)
	for _, rg := range rm.groups {
		if rg.GetName() == sourceRG.GetName() {
			continue
		}

		if rg.ReachLimitNumOfNodes() <= 0 {
			continue
		}

		// Calculate the priority of the current resource group.
		priority := computeRGPriority(rg)
		if priority > maxPriority {
			// select a node from it that is preferred to be removed and assigned to the target resource group.
			node := sourceRG.SelectNodeForRG(rg)
			// If no such node is found, skip the current resource group.
			if node == -1 {
				continue
			}
			candidateNode = node
			targetRG = rg
			maxPriority = priority
		}
	}

	// Finally, always transfer the node to the default resource group if no other target resource group is found.
	if targetRG == nil && sourceRG.GetName() != DefaultResourceGroupName {
		targetRG = rm.groups[DefaultResourceGroupName]
		if sourceRG != nil {
			candidateNode = sourceRG.SelectNodeForRG(targetRG)
		}
	}
	return candidateNode, targetRG
}

// assignIncomingNodeWithNodeCheck assign node to resource group with node status check.
func (rm *ResourceManager) assignIncomingNodeWithNodeCheck(ctx context.Context, node int64) (string, error) {
	// node is on stopping or stopped, remove it from incoming node set.
	if rm.nodeMgr.Get(node) == nil {
		rm.incomingNode.Remove(node)
		return "", errors.New("node is not online")
	}
	if ok, _ := rm.nodeMgr.IsStoppingNode(node); ok {
		rm.incomingNode.Remove(node)
		return "", errors.New("node has been stopped")
	}

	rgName, err := rm.assignIncomingNode(ctx, node)
	if err != nil {
		return "", err
	}
	// node assignment is finished, remove the node from incoming node set.
	rm.incomingNode.Remove(node)
	return rgName, nil
}

// assignIncomingNode assign node to resource group.
func (rm *ResourceManager) assignIncomingNode(ctx context.Context, node int64) (string, error) {
	// If node already assign to rg.
	rg := rm.getResourceGroupByNodeID(node)
	if rg != nil {
		log.Info("HandleNodeUp: node already assign to resource group",
			zap.String("rgName", rg.GetName()),
			zap.Int64("node", node),
		)
		return rg.GetName(), nil
	}

	// select a resource group to assign incoming node.
	rg = rm.mustSelectAssignIncomingNodeTargetRG(node)
	if err := rm.transferNode(ctx, rg.GetName(), node); err != nil {
		return "", errors.Wrap(err, "at finally assign to default resource group")
	}
	return rg.GetName(), nil
}

// mustSelectAssignIncomingNodeTargetRG select resource group for assign incoming node.
func (rm *ResourceManager) mustSelectAssignIncomingNodeTargetRG(nodeID int64) *ResourceGroup {
	// First, Assign it to rg with the most missing nodes at high priority.
	if rg := rm.findMaxRGWithGivenFilter(
		func(rg *ResourceGroup) bool {
			return rg.MissingNumOfNodes() > 0 && rg.AcceptNode(nodeID)
		},
		func(rg *ResourceGroup) int {
			return rg.MissingNumOfNodes()
		},
	); rg != nil {
		return rg
	}

	// Second, assign it to rg do not reach limit.
	if rg := rm.findMaxRGWithGivenFilter(
		func(rg *ResourceGroup) bool {
			return rg.ReachLimitNumOfNodes() > 0 && rg.AcceptNode(nodeID)
		},
		func(rg *ResourceGroup) int {
			return rg.ReachLimitNumOfNodes()
		},
	); rg != nil {
		return rg
	}

	// Finally, add node to default rg.
	return rm.groups[DefaultResourceGroupName]
}

// findMaxRGWithGivenFilter find resource group with given filter and return the max one.
// not efficient, but it's ok for low nodes and low resource group.
func (rm *ResourceManager) findMaxRGWithGivenFilter(filter func(rg *ResourceGroup) bool, attr func(rg *ResourceGroup) int) *ResourceGroup {
	var maxRG *ResourceGroup
	for _, rg := range rm.groups {
		if filter == nil || filter(rg) {
			if maxRG == nil || attr(rg) > attr(maxRG) {
				maxRG = rg
			}
		}
	}
	return maxRG
}

// transferNode transfer given node to given resource group.
// if given node is assigned in given resource group, do nothing.
// if given node is assigned to other resource group, it will be unassigned first.
func (rm *ResourceManager) transferNode(ctx context.Context, rgName string, node int64) error {
	if rm.groups[rgName] == nil {
		return merr.WrapErrResourceGroupNotFound(rgName)
	}

	updates := make([]*querypb.ResourceGroup, 0, 2)
	modifiedRG := make([]*ResourceGroup, 0, 2)
	originalRG := "_"
	// Check if node is already assign to rg.
	if rg := rm.getResourceGroupByNodeID(node); rg != nil {
		if rg.GetName() == rgName {
			// node is already assign to rg.
			log.Info("node already assign to resource group",
				zap.String("rgName", rgName),
				zap.Int64("node", node),
			)
			return nil
		}
		// Apply update.
		mrg := rg.CopyForWrite()
		mrg.UnassignNode(node)
		rg := mrg.ToResourceGroup()

		updates = append(updates, rg.GetMeta())
		modifiedRG = append(modifiedRG, rg)
		originalRG = rg.GetName()
	}

	// assign the node to rg.
	mrg := rm.groups[rgName].CopyForWrite()
	mrg.AssignNode(node)
	rg := mrg.ToResourceGroup()
	updates = append(updates, rg.GetMeta())
	modifiedRG = append(modifiedRG, rg)

	// Commit updates to meta storage.
	if err := rm.catalog.SaveResourceGroup(ctx, updates...); err != nil {
		log.Warn("failed to transfer node to resource group",
			zap.String("rgName", rgName),
			zap.String("originalRG", originalRG),
			zap.Int64("node", node),
			zap.Error(err),
		)
		return merr.WrapErrResourceGroupServiceAvailable()
	}

	// Commit updates to memory.
	for _, rg := range modifiedRG {
		rm.setupInMemResourceGroup(rg)
	}
	rm.nodeIDMap[node] = rgName
	log.Info("transfer node to resource group",
		zap.String("rgName", rgName),
		zap.String("originalRG", originalRG),
		zap.Int64("node", node),
	)

	// notify that node distribution has been changed.
	rm.nodeChangedNotifier.NotifyAll()
	return nil
}

// unassignNode remove a node from resource group where it belongs to.
func (rm *ResourceManager) unassignNode(ctx context.Context, node int64) (string, error) {
	if rg := rm.getResourceGroupByNodeID(node); rg != nil {
		mrg := rg.CopyForWrite()
		mrg.UnassignNode(node)
		rg := mrg.ToResourceGroup()

		if err := rm.catalog.SaveResourceGroup(ctx, rg.GetMeta()); err != nil {
			log.Fatal("unassign node from resource group",
				zap.String("rgName", rg.GetName()),
				zap.Int64("node", node),
				zap.Error(err),
			)
		}

		// Commit updates to memory.
		rm.setupInMemResourceGroup(rg)
		delete(rm.nodeIDMap, node)
		log.Info("unassign node to resource group",
			zap.String("rgName", rg.GetName()),
			zap.Int64("node", node),
		)

		// notify that node distribution has been changed.
		rm.nodeChangedNotifier.NotifyAll()
		return rg.GetName(), nil
	}

	return "", errors.Errorf("node %d not found in any resource group", node)
}

// validateResourceGroupConfig validate resource group config.
// validateResourceGroupConfig must be called after lock, because it will check with other resource group.
func (rm *ResourceManager) validateResourceGroupConfig(rgName string, cfg *rgpb.ResourceGroupConfig) error {
	if cfg.GetLimits() == nil || cfg.GetRequests() == nil {
		return merr.WrapErrResourceGroupIllegalConfig(rgName, cfg, "requests or limits is required")
	}
	if cfg.GetRequests().GetNodeNum() < 0 || cfg.GetLimits().GetNodeNum() < 0 {
		return merr.WrapErrResourceGroupIllegalConfig(rgName, cfg, "node num in `requests` or `limits` should not less than 0")
	}
	if cfg.GetLimits().GetNodeNum() < cfg.GetRequests().GetNodeNum() {
		return merr.WrapErrResourceGroupIllegalConfig(rgName, cfg, "limits node num should not less than requests node num")
	}

	for _, transferCfg := range cfg.GetTransferFrom() {
		if transferCfg.GetResourceGroup() == rgName {
			return merr.WrapErrResourceGroupIllegalConfig(rgName, cfg, fmt.Sprintf("resource group in `TransferFrom` %s should not be itself", rgName))
		}
		if rm.groups[transferCfg.GetResourceGroup()] == nil {
			return merr.WrapErrResourceGroupIllegalConfig(rgName, cfg, fmt.Sprintf("resource group in `TransferFrom` %s not exist", transferCfg.GetResourceGroup()))
		}
	}
	for _, transferCfg := range cfg.GetTransferTo() {
		if transferCfg.GetResourceGroup() == rgName {
			return merr.WrapErrResourceGroupIllegalConfig(rgName, cfg, fmt.Sprintf("resource group in `TransferTo` %s should not be itself", rgName))
		}
		if rm.groups[transferCfg.GetResourceGroup()] == nil {
			return merr.WrapErrResourceGroupIllegalConfig(rgName, cfg, fmt.Sprintf("resource group in `TransferTo` %s not exist", transferCfg.GetResourceGroup()))
		}
	}
	return nil
}

// validateResourceGroupIsDeletable validate a resource group is deletable.
func (rm *ResourceManager) validateResourceGroupIsDeletable(rgName string) error {
	// default rg is not deletable.
	if rgName == DefaultResourceGroupName {
		return merr.WrapErrParameterInvalid("not default resource group", rgName, "default resource group is not deletable")
	}

	// If rg is not empty, it's not deletable.
	if rm.groups[rgName].GetConfig().GetLimits().GetNodeNum() != 0 {
		return merr.WrapErrParameterInvalid("not empty resource group", rgName, "resource group's limits node num is not 0")
	}

	// If rg is used by other rg, it's not deletable.
	for _, rg := range rm.groups {
		for _, transferCfg := range rg.GetConfig().GetTransferFrom() {
			if transferCfg.GetResourceGroup() == rgName {
				return merr.WrapErrParameterInvalid("not `TransferFrom` of resource group", rgName, fmt.Sprintf("resource group %s is used by %s's `TransferFrom`, remove that configuration first", rgName, rg.name))
			}
		}
		for _, transferCfg := range rg.GetConfig().GetTransferTo() {
			if transferCfg.GetResourceGroup() == rgName {
				return merr.WrapErrParameterInvalid("not `TransferTo` of resource group", rgName, fmt.Sprintf("resource group %s is used by %s's `TransferTo`, remove that configuration first", rgName, rg.name))
			}
		}
	}
	return nil
}

// setupInMemResourceGroup setup resource group in memory.
func (rm *ResourceManager) setupInMemResourceGroup(r *ResourceGroup) {
	// clear old metrics.
	if oldR, ok := rm.groups[r.GetName()]; ok {
		for _, nodeID := range oldR.GetNodes() {
			metrics.QueryCoordResourceGroupInfo.DeletePartialMatch(prometheus.Labels{
				metrics.ResourceGroupLabelName: r.GetName(),
				metrics.NodeIDLabelName:        strconv.FormatInt(nodeID, 10),
			})
		}
	}
	// add new metrics.
	for _, nodeID := range r.GetNodes() {
		metrics.QueryCoordResourceGroupInfo.WithLabelValues(
			r.GetName(),
			strconv.FormatInt(nodeID, 10),
		).Set(1)
	}
	rm.groups[r.GetName()] = r
}

func (rm *ResourceManager) GetResourceGroupsJSON(ctx context.Context) string {
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()

	rgs := lo.MapToSlice(rm.groups, func(i string, r *ResourceGroup) *metricsinfo.ResourceGroup {
		return &metricsinfo.ResourceGroup{
			Name:  r.GetName(),
			Nodes: r.GetNodes(),
			Cfg:   r.GetConfig(),
		}
	})
	ret, err := json.Marshal(rgs)
	if err != nil {
		log.Error("failed to marshal resource groups", zap.Error(err))
		return ""
	}

	return string(ret)
}
