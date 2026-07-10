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
	"sort"
	"strconv"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/rgpb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// errNodeNotEnough is an INTERNAL sentinel: observed only inside the
// resource manager / resource observer recovery loop, never serialized
// across any gRPC boundary. See docs/dev/error_sentinel_convention.md.
var errNodeNotEnough = errors.New("nodes not enough")

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
		return merr.Wrap(err, "failed to recover resource group from store")
	}

	// Resource group meta upgrade to latest version.
	upgrades := make([]*querypb.ResourceGroup, 0)
	nodeToRG := make(map[int64]string) // local map for duplicate node detection during recovery
	for _, meta := range rgs {
		needUpgrade := meta.Config == nil

		rg := NewResourceGroupFromMeta(meta, rm.nodeMgr)
		// Check for duplicate node assignments before committing to memory.
		for _, node := range rg.GetNodes() {
			if existingRG, ok := nodeToRG[node]; ok {
				// unreachable code, should never happen.
				panic(fmt.Sprintf("dirty meta, node has been assign to multi resource group, %s, %s", existingRG, rg.GetName()))
			}
			nodeToRG[node] = rg.GetName()
		}
		rm.setupInMemResourceGroup(rg)
		mlog.Info(context.TODO(), "Recover resource group",
			mlog.String("rgName", rg.GetName()),
			mlog.Int64s("nodes", rm.groups[rg.GetName()].GetNodes()),
			mlog.Any("config", rg.GetConfig()),
		)
		if needUpgrade {
			upgrades = append(upgrades, rg.GetMeta())
		}
	}
	if len(upgrades) > 0 {
		mlog.Info(context.TODO(), "upgrade resource group meta into latest", mlog.Int("num", len(upgrades)))
		return rm.catalog.SaveResourceGroup(ctx, upgrades...)
	}
	return nil
}

// Deprecated: only for compatibility with unittest.
// AddResourceGroup adds a resource group. Returns ignored=true if the
// resource group already exists with the same config (idempotent no-op).
func (rm *ResourceManager) AddResourceGroup(ctx context.Context, rgName string, cfg *rgpb.ResourceGroupConfig) (ignored bool, err error) {
	if ignored, err := rm.CheckIfResourceGroupAddable(ctx, rgName, cfg); err != nil || ignored {
		return ignored, err
	}
	return false, rm.AlterResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{rgName: cfg})
}

// CheckIfResourceGroupAddable check if a resource group can be added.
// Returns ignored=true if the resource group already exists with the
// same config (idempotent no-op for callers to translate to success).
func (rm *ResourceManager) CheckIfResourceGroupAddable(ctx context.Context, rgName string, cfg *rgpb.ResourceGroupConfig) (ignored bool, err error) {
	if len(rgName) == 0 {
		return false, merr.WrapErrParameterMissing("resource group name couldn't be empty")
	}

	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()
	if rm.groups[rgName] != nil {
		// Idempotent promise.
		// If resource group already exist, check if configuration is the same,
		if proto.Equal(rm.groups[rgName].GetConfig(), cfg) {
			return true, nil
		}
		return false, merr.WrapErrResourceGroupAlreadyExist(rgName)
	}

	maxResourceGroup := paramtable.Get().QuotaConfig.MaxResourceGroupNumOfQueryNode.GetAsInt()
	if len(rm.groups) >= maxResourceGroup {
		return false, merr.WrapErrResourceGroupReachLimit(rgName, maxResourceGroup)
	}

	if err := rm.validateResourceGroupConfig(rgName, cfg); err != nil {
		return false, err
	}
	return false, nil
}

// AlterResourceGroups alter resource group configuration.
// Only change the configuration, no change with node. all node will be reassign by auto recover.
func (rm *ResourceManager) AlterResourceGroups(ctx context.Context, rgs map[string]*rgpb.ResourceGroupConfig) error {
	if len(rgs) == 0 {
		return nil
	}

	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()
	return rm.updateResourceGroups(ctx, rgs)
}

// CheckIfResourceGroupsUpdatable check if resource groups can be updated.
func (rm *ResourceManager) CheckIfResourceGroupsUpdatable(ctx context.Context, rgs map[string]*rgpb.ResourceGroupConfig) error {
	if len(rgs) == 0 {
		return nil
	}
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()

	for rgName, cfg := range rgs {
		if _, ok := rm.groups[rgName]; !ok {
			return merr.WrapErrResourceGroupNotFound(rgName)
		}
		if err := rm.validateResourceGroupConfig(rgName, cfg); err != nil {
			return err
		}
	}
	return nil
}

// updateResourceGroups update resource group configuration.
func (rm *ResourceManager) updateResourceGroups(ctx context.Context, rgs map[string]*rgpb.ResourceGroupConfig) error {
	modifiedRG := make([]*ResourceGroup, 0, len(rgs))
	updates := make([]*querypb.ResourceGroup, 0, len(rgs))
	for rgName, cfg := range rgs {
		// redundant check for safety, it will always be checked by CheckIfResourceGroupsUpdatable and CheckIfResourceGroupAddable.
		if err := rm.validateResourceGroupConfig(rgName, cfg); err != nil {
			return err
		}
		if _, ok := rm.groups[rgName]; !ok {
			// create new resource group
			newRG := NewResourceGroup(rgName, cfg, rm.nodeMgr)
			modifiedRG = append(modifiedRG, newRG)
			updates = append(updates, newRG.GetMeta())
			continue
		}
		// Update with copy on write.
		mrg := rm.groups[rgName].CopyForWrite()
		mrg.UpdateConfig(cfg)
		rg := mrg.ToResourceGroup()

		updates = append(updates, rg.GetMeta())
		modifiedRG = append(modifiedRG, rg)
	}

	// Detect node transfer intent: if rgA is being zeroed (old request=N,limit=N → new 0,0)
	// and rgB's new config matches rgA's old (new request=N,limit=N), directly move rgA's
	// nodes to rgB. This preserves node assignment stability during RG transitions.
	rm.transferNodesOnRGSwap(modifiedRG)

	// Rebuild updates slice since node lists may have changed.
	updates = updates[:0]
	for _, rg := range modifiedRG {
		updates = append(updates, rg.GetMeta())
	}

	if err := rm.catalog.SaveResourceGroup(ctx, updates...); err != nil {
		for rgName, cfg := range rgs {
			mlog.Warn(context.TODO(), "failed to update resource group",
				mlog.String("rgName", rgName),
				mlog.Any("config", cfg),
				mlog.Err(err),
			)
		}
		return merr.WrapErrResourceGroupServiceUnAvailable()
	}

	// Commit updates to memory.
	for _, rg := range modifiedRG {
		mlog.Info(context.TODO(), "update resource group",
			mlog.String("rgName", rg.GetName()),
			mlog.Any("config", rg.GetConfig()),
		)
		rm.setupInMemResourceGroup(rg)
	}

	// notify that resource group config has been changed.
	rm.rgChangedNotifier.NotifyAll()
	return nil
}

// transferNodesOnRGSwap detects "RG rename" intent in a batch config update and
// directly transfers nodes between paired RGs to preserve node assignment stability.
//
// During replica scale-up/down, the external control plane changes RG names
// (e.g., __default_resource_group → rg_for_replica_1). Without this optimization,
// nodes would first be pushed to __recycle_resource_group by the async resource_observer,
// then pulled into rg_for_replica_1 — with non-deterministic node selection at each hop,
// breaking the original node-to-replica mapping and potentially causing replica unavailability.
//
// Detection: for each RG being zeroed (old config request=N,limit=N → new 0,0),
// find the first unmatched RG in the same batch whose new config matches (request=N,limit=N).
// RGs are sorted by name so the lexicographically smallest recipient wins.
func (rm *ResourceManager) transferNodesOnRGSwap(modifiedRGs []*ResourceGroup) {
	// Sort by name for deterministic matching order.
	sort.Slice(modifiedRGs, func(i, j int) bool {
		return modifiedRGs[i].GetName() < modifiedRGs[j].GetName()
	})

	matched := make(map[int]bool)
	for i, rg := range modifiedRGs {
		// Find a zeroed RG: new config (0,0) but old config had (N,N) with nodes.
		newReq := rg.GetConfig().GetRequests().GetNodeNum()
		newLim := rg.GetConfig().GetLimits().GetNodeNum()
		if newReq != 0 || newLim != 0 {
			continue
		}
		oldRG := rm.groups[rg.GetName()]
		if oldRG == nil {
			continue
		}
		oldReq := oldRG.GetConfig().GetRequests().GetNodeNum()
		oldLim := oldRG.GetConfig().GetLimits().GetNodeNum()
		nodes := oldRG.GetNodes()
		if oldReq <= 0 || oldLim <= 0 || len(nodes) == 0 {
			continue
		}

		// Find the first unmatched recipient whose new (request, limit) == old (request, limit).
		for j, candidate := range modifiedRGs {
			if j == i || matched[j] || candidate.NodeNum() > 0 {
				continue
			}
			cReq := candidate.GetConfig().GetRequests().GetNodeNum()
			cLim := candidate.GetConfig().GetLimits().GetNodeNum()
			if cReq != oldReq || cLim != oldLim {
				continue
			}

			// Swap nodes from donor to recipient.
			donorMut := rg.CopyForWrite()
			recipientMut := candidate.CopyForWrite()
			for _, node := range nodes {
				donorMut.UnassignNode(node)
				recipientMut.AssignNode(node)
			}
			modifiedRGs[i] = donorMut.ToResourceGroup()
			modifiedRGs[j] = recipientMut.ToResourceGroup()
			matched[i] = true
			matched[j] = true

			mlog.Info(context.TODO(), "direct node transfer on RG swap",
				mlog.String("from", rg.GetName()),
				mlog.String("to", candidate.GetName()),
				mlog.Int64s("nodes", nodes),
			)
			break
		}
	}
}

// Deprecated: only for compatibility with unittest.
func (rm *ResourceManager) TransferNode(ctx context.Context, sourceRGName string, targetRGName string, nodeNum int) error {
	rgs, err := rm.CheckIfTransferNode(ctx, sourceRGName, targetRGName, nodeNum)
	if err != nil {
		return err
	}
	return rm.AlterResourceGroups(ctx, rgs)
}

// Deprecated: use declarative API `UpdateResourceGroups` instead.
func (rm *ResourceManager) CheckIfTransferNode(ctx context.Context, sourceRGName string, targetRGName string, nodeNum int) (map[string]*rgpb.ResourceGroupConfig, error) {
	if sourceRGName == targetRGName {
		return nil, merr.WrapErrParameterInvalidMsg("source resource group and target resource group should not be the same, resource group: %s", sourceRGName)
	}
	if nodeNum <= 0 {
		return nil, merr.WrapErrParameterInvalid("NumNode > 0", fmt.Sprintf("invalid NumNode %d", nodeNum))
	}

	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()

	if rm.groups[sourceRGName] == nil {
		return nil, merr.WrapErrResourceGroupNotFound(sourceRGName)
	}
	if rm.groups[targetRGName] == nil {
		return nil, merr.WrapErrResourceGroupNotFound(targetRGName)
	}

	sourceRG := rm.groups[sourceRGName]
	targetRG := rm.groups[targetRGName]

	// Check if source resource group has enough node to transfer.
	if len(sourceRG.GetNodes()) < nodeNum {
		return nil, merr.WrapErrResourceGroupNodeNotEnough(sourceRGName, len(sourceRG.GetNodes()), nodeNum)
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
	return map[string]*rgpb.ResourceGroupConfig{
		sourceRGName: sourceCfg,
		targetRGName: targetCfg,
	}, nil
}

// Deprecated: only for compatibility with unittest.
func (rm *ResourceManager) RemoveResourceGroup(ctx context.Context, rgName string) error {
	ignored, err := rm.CheckIfResourceGroupDropable(ctx, rgName)
	if err != nil || ignored {
		return err
	}
	return rm.DropResourceGroup(ctx, rgName)
}

// CheckIfResourceGroupDropable check if resource group can be dropped.
// Returns ignored=true if the resource group doesn't exist (idempotent
// no-op for callers to translate to success).
func (rm *ResourceManager) CheckIfResourceGroupDropable(ctx context.Context, rgName string) (ignored bool, err error) {
	if rm.groups[rgName] == nil {
		// Idempotent promise: delete a non-exist rg should be ok
		return true, nil
	}

	// validateResourceGroupIsDeletable will check if rg is deletable.
	if err := rm.validateResourceGroupIsDeletable(rgName); err != nil {
		return false, err
	}

	// Nodes may be still assign to these group,
	// recover the resource group from redundant status before remove it.
	if rm.groups[rgName].NodeNum() > 0 {
		if err := rm.recoverRedundantNodeRG(ctx, rgName); err != nil {
			mlog.Info(context.TODO(), "failed to recover redundant node resource group before remove it",
				mlog.String("rgName", rgName),
				mlog.Err(err),
			)
			return false, err
		}
	}
	return false, nil
}

// DropResourceGroup drop resource group.
func (rm *ResourceManager) DropResourceGroup(ctx context.Context, rgName string) error {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()
	if _, ok := rm.groups[rgName]; !ok {
		// Idempotent promise: delete a non-exist rg should be ok
		return nil
	}

	// Remove it from meta storage.
	if err := rm.catalog.RemoveResourceGroup(ctx, rgName); err != nil {
		mlog.Info(context.TODO(), "failed to remove resource group",
			mlog.String("rgName", rgName),
			mlog.Err(err),
		)
		return merr.WrapErrResourceGroupServiceUnAvailable()
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

	mlog.Info(context.TODO(), "remove resource group",
		mlog.String("rgName", rgName),
	)
	// notify that resource group has been changed.
	rm.rgChangedNotifier.NotifyAll()
	return nil
}

// GetResourceGroups return snapshots of multi resource groups, it can be used to get a consistent view of multi rg.
func (rm *ResourceManager) GetResourceGroups(ctx context.Context, rgNames []string) (map[string]*ResourceGroup, error) {
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()

	ret := make(map[string]*ResourceGroup, len(rgNames))
	for _, name := range rgNames {
		if rm.groups[name] == nil {
			return nil, merr.WrapErrResourceGroupNotFound(name)
		}
		ret[name] = rm.groups[name].Snapshot()
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
			return errNodeNotEnough
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

// IsNodeSuspended checks whether a node is suspended.
// If a node is not in any resource group, return true.
func (rm *ResourceManager) IsNodeSuspended(nodeID int64) bool {
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()
	return rm.getResourceGroupByNodeID(nodeID) == nil
}

// GetNodesSuspended returns a map indicating whether each node is suspended.
// A node is considered suspended if it is not associated with any resource group.
func (rm *ResourceManager) GetNodesSuspended(nodeIDs []int64) map[int64]bool {
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()

	// Initialize a map to store the results.
	result := make(map[int64]bool, len(nodeIDs))

	// Iterate through the list of node IDs to check their status.
	for _, nodeID := range nodeIDs {
		// Check if the node is associated with a resource group.
		isSuspended := rm.getResourceGroupByNodeID(nodeID) == nil

		// Store the result in the map.
		result[nodeID] = isSuspended
	}
	return result
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

	rm.handleNodeUp(ctx, node)
}

func (rm *ResourceManager) handleNodeUp(ctx context.Context, node int64) {
	nodeInfo := rm.nodeMgr.Get(node)
	if nodeInfo == nil || nodeInfo.IsEmbeddedQueryNodeInStreamingNode() {
		return
	}
	if nodeInfo.IsStoppingState() {
		mlog.Warn(context.TODO(), "node is stopping, skip handle node up in resource manager", mlog.Int64("node", node))
		return
	}
	rm.incomingNode.Insert(node)
	// Trigger assign incoming node right away.
	// error can be ignored here, because `AssignPendingIncomingNode`` will retry assign node.
	rgName, err := rm.assignIncomingNodeWithNodeCheck(ctx, node)
	mlog.Info(context.TODO(), "HandleNodeUp: add node to resource group",
		mlog.String("rgName", rgName),
		mlog.Int64("node", node),
		mlog.Err(err),
	)
}

// HandleNodeDown handle the node when node is leave.
func (rm *ResourceManager) HandleNodeDown(ctx context.Context, node int64) {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()
	rm.handleNodeDown(ctx, node)
}

func (rm *ResourceManager) handleNodeDown(ctx context.Context, node int64) {
	rm.incomingNode.Remove(node)

	// for stopping query node becomes offline, node change won't be triggered,
	// cause when it becomes stopping, it already remove from resource manager
	// then `stageUnassignNode` will do nothing
	staging := newRGStaging()
	rgName, err := rm.stageUnassignNode(ctx, staging, node)
	if err == nil {
		err = rm.commitStaging(ctx, staging)
	}
	if err != nil {
		// commitStaging notifies node changes on successful commit; keep the
		// notification on the no-op/failure path as well, expected to remove
		// ro node from replica immediately.
		rm.nodeChangedNotifier.NotifyAll()
	}
	mlog.Info(context.TODO(), "HandleNodeDown: remove node from resource group",
		mlog.String("rgName", rgName),
		mlog.Int64("node", node),
		mlog.Err(err),
	)
}

func (rm *ResourceManager) HandleNodeStopping(ctx context.Context, node int64) {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()
	rm.handleNodeStopping(ctx, node)
}

func (rm *ResourceManager) handleNodeStopping(ctx context.Context, node int64) {
	rm.incomingNode.Remove(node)
	staging := newRGStaging()
	rgName, err := rm.stageUnassignNode(ctx, staging, node)
	if err == nil {
		err = rm.commitStaging(ctx, staging)
	}
	mlog.Info(context.TODO(), "HandleNodeStopping: remove node from resource group",
		mlog.String("rgName", rgName),
		mlog.Int64("node", node),
		mlog.Err(err),
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
		mlog.Info(context.TODO(), "Pending HandleNodeUp: add node to resource group",
			mlog.String("rgName", rgName),
			mlog.Int64("node", node),
			mlog.Err(err),
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
// All transfers are staged in memory first and persisted by a single batch commit.
func (rm *ResourceManager) recoverMissingNodeRG(ctx context.Context, rgName string) error {
	staging := newRGStaging()
	for staging.get(rm, rgName).MissingNumOfNodes() > 0 {
		targetRG := staging.get(rm, rgName)
		node, sourceRG := rm.selectNodeForMissingRecover(staging, targetRG)
		if sourceRG == nil {
			mlog.Warn(context.TODO(), "fail to select source resource group", mlog.String("rgName", targetRG.GetName()))
			// commit the staged transfers before returning, partial recovery is still progress.
			if err := rm.commitStaging(ctx, staging); err != nil {
				return err
			}
			return errNodeNotEnough
		}

		err := rm.stageTransferNode(ctx, staging, targetRG.GetName(), node)
		if err != nil {
			mlog.Warn(context.TODO(), "failed to recover missing node by transfer node from other resource group",
				mlog.String("sourceRG", sourceRG.GetName()),
				mlog.String("targetRG", targetRG.GetName()),
				mlog.Int64("nodeID", node),
				mlog.Err(err))
			// commit the staged transfers before returning, partial recovery is still progress.
			if cerr := rm.commitStaging(ctx, staging); cerr != nil {
				return cerr
			}
			return err
		}
		mlog.Info(context.TODO(), "recover missing node by transfer node from other resource group",
			mlog.String("sourceRG", sourceRG.GetName()),
			mlog.String("targetRG", targetRG.GetName()),
			mlog.Int64("nodeID", node),
		)
	}
	return rm.commitStaging(ctx, staging)
}

// selectNodeForMissingRecover selects a node for missing recovery under the staged view.
// It takes a target ResourceGroup and returns the selected node's ID and the source ResourceGroup with highest priority.
func (rm *ResourceManager) selectNodeForMissingRecover(staging *rgStaging, targetRG *ResourceGroup) (int64, *ResourceGroup) {
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

	rm.rangeGroupsWithStaging(staging, func(rg *ResourceGroup) bool {
		if rg.GetName() == targetRG.GetName() {
			return true
		}
		if rg.OversizedNumOfNodes() <= 0 {
			return true
		}

		priority := computeRGPriority(rg)
		if priority > maxPriority {
			// Select a node from the current resource group that is preferred to be removed and assigned to the target resource group.
			node := rg.SelectNodeForRG(targetRG)
			// If no such node is found, skip the current resource group.
			if node == -1 {
				return true
			}

			sourceRG = rg
			candidateNode = node
			maxPriority = priority
		}
		return true
	})

	return candidateNode, sourceRG
}

// recoverRedundantNodeRG recover resource group by transfer node to other resource group.
// All transfers are staged in memory first and persisted by a single batch commit.
func (rm *ResourceManager) recoverRedundantNodeRG(ctx context.Context, rgName string) error {
	staging := newRGStaging()
	for staging.get(rm, rgName).RedundantNumOfNodes() > 0 {
		sourceRG := staging.get(rm, rgName)
		node, targetRG := rm.selectNodeForRedundantRecover(staging, sourceRG)
		if node == -1 {
			mlog.Info(context.TODO(), "failed to select redundant recover target resource group, please check resource group configuration if as expected.",
				mlog.String("rgName", sourceRG.GetName()))
			// commit the staged transfers before returning, partial recovery is still progress.
			if err := rm.commitStaging(ctx, staging); err != nil {
				return err
			}
			return merr.WrapErrServiceInternalMsg("all resource group reach limits")
		}

		if err := rm.stageTransferNode(ctx, staging, targetRG.GetName(), node); err != nil {
			mlog.Warn(context.TODO(), "failed to recover redundant node by transfer node to other resource group",
				mlog.String("sourceRG", sourceRG.GetName()),
				mlog.String("targetRG", targetRG.GetName()),
				mlog.Int64("nodeID", node),
				mlog.Err(err))
			// commit the staged transfers before returning, partial recovery is still progress.
			if cerr := rm.commitStaging(ctx, staging); cerr != nil {
				return cerr
			}
			return err
		}
		mlog.Info(context.TODO(), "recover redundant node by transfer node to other resource group",
			mlog.String("sourceRG", sourceRG.GetName()),
			mlog.String("targetRG", targetRG.GetName()),
			mlog.Int64("nodeID", node),
		)
	}
	return rm.commitStaging(ctx, staging)
}

// selectNodeForRedundantRecover selects a node for redundant recovery under the staged view.
// It takes a source ResourceGroup and returns the selected node's ID and the target ResourceGroup with highest priority.
func (rm *ResourceManager) selectNodeForRedundantRecover(staging *rgStaging, sourceRG *ResourceGroup) (int64, *ResourceGroup) {
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
	rm.rangeGroupsWithStaging(staging, func(rg *ResourceGroup) bool {
		if rg.GetName() == sourceRG.GetName() {
			return true
		}

		if rg.ReachLimitNumOfNodes() <= 0 {
			return true
		}

		// Calculate the priority of the current resource group.
		priority := computeRGPriority(rg)
		if priority > maxPriority {
			// select a node from it that is preferred to be removed and assigned to the target resource group.
			node := sourceRG.SelectNodeForRG(rg)
			// If no such node is found, skip the current resource group.
			if node == -1 {
				return true
			}
			candidateNode = node
			targetRG = rg
			maxPriority = priority
		}
		return true
	})

	// Finally, always transfer the node to the default resource group if no other target resource group is found.
	if targetRG == nil && sourceRG.GetName() != DefaultResourceGroupName {
		targetRG = staging.get(rm, DefaultResourceGroupName)
		if sourceRG != nil {
			candidateNode = sourceRG.SelectNodeForRG(targetRG)
		}
	}
	return candidateNode, targetRG
}

// assignIncomingNodeWithNodeCheck assign node to resource group with node status check.
func (rm *ResourceManager) assignIncomingNodeWithNodeCheck(ctx context.Context, node int64) (string, error) {
	// node is on stopping or stopped, remove it from incoming node set.
	nodeInfo := rm.nodeMgr.Get(node)
	if nodeInfo == nil {
		rm.incomingNode.Remove(node)
		return "", merr.WrapErrServiceInternalMsg("node is not online")
	}

	if nodeInfo.IsStoppingState() {
		rm.incomingNode.Remove(node)
		return "", merr.WrapErrServiceInternalMsg("node has been stopped")
	}

	// stage all modification (rg creation and node transfer) triggered by the
	// incoming node and persist them by a single batch commit.
	staging := newRGStaging()
	rgName, err := rm.assignIncomingNode(ctx, staging, nodeInfo)
	if err != nil {
		return "", err
	}
	if err := rm.commitStaging(ctx, staging); err != nil {
		return "", err
	}
	// node assignment is finished, remove the node from incoming node set.
	rm.incomingNode.Remove(node)
	return rgName, nil
}

// assignIncomingNode assign node to resource group.
// All modification is staged into the given staging overlay, the caller is
// responsible for committing it.
func (rm *ResourceManager) assignIncomingNode(ctx context.Context, staging *rgStaging, nodeInfo *session.NodeInfo) (string, error) {
	node := nodeInfo.ID()

	// If node already assign to rg.
	rg := rm.getResourceGroupByNodeIDWithStaging(staging, node)
	if rg != nil {
		mlog.Info(context.TODO(), "HandleNodeUp: node already assign to resource group",
			mlog.String("rgName", rg.GetName()),
			mlog.Int64("node", node),
		)
		return rg.GetName(), nil
	}

	if err := rm.stageCreateResourceGroupIfNotExists(ctx, staging, nodeInfo); err != nil {
		return "", err
	}

	// select a resource group to assign incoming node.
	rg = rm.mustSelectAssignIncomingNodeTargetRG(staging, nodeInfo)
	if err := rm.stageTransferNode(ctx, staging, rg.GetName(), node); err != nil {
		return "", merr.Wrap(err, "at finally assign to default resource group")
	}
	return rg.GetName(), nil
}

// stageCreateResourceGroupIfNotExists stage the creation of the resource group
// declared in the node session if it does not exist yet.
// It performs the same config validation as updateResourceGroups before staging;
// rgChangedNotifier is fired by commitStaging once the creation is persisted.
func (rm *ResourceManager) stageCreateResourceGroupIfNotExists(ctx context.Context, staging *rgStaging, nodeInfo *session.NodeInfo) error {
	rgName := nodeInfo.ResourceGroupName()
	nodeID := nodeInfo.ID()
	if rgName == "" {
		return nil
	}
	if staging.get(rm, rgName) != nil {
		return nil
	}
	cfg := &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{
			NodeNum: 0,
		},
		Limits: &rgpb.ResourceGroupLimit{
			NodeNum: defaultResourceGroupCapacity,
		},
	}
	if err := rm.validateResourceGroupConfig(rgName, cfg); err != nil {
		mlog.Warn(context.TODO(), "failed to create resource group from session of new incoming node", mlog.String("rgName", rgName), mlog.Int64("nodeID", nodeID), mlog.Err(err))
		return err
	}
	staging.put(NewResourceGroup(rgName, cfg, rm.nodeMgr))
	staging.rgCreated = true
	mlog.Info(context.TODO(), "stage create resource group from session of new incoming node", mlog.String("rgName", rgName), mlog.Int64("nodeID", nodeID))
	return nil
}

// mustSelectAssignIncomingNodeTargetRG select resource group for assign incoming node under the staged view.
func (rm *ResourceManager) mustSelectAssignIncomingNodeTargetRG(staging *rgStaging, nodeInfo *session.NodeInfo) *ResourceGroup {
	if nodeInfo.ResourceGroupName() != "" {
		// rg will be staged if not exists by stageCreateResourceGroupIfNotExists
		return staging.get(rm, nodeInfo.ResourceGroupName())
	}

	nodeID := nodeInfo.ID()
	// First, Assign it to rg with the most missing nodes at high priority.
	if rg := rm.findMaxRGWithGivenFilter(
		staging,
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
		staging,
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
	return staging.get(rm, DefaultResourceGroupName)
}

// findMaxRGWithGivenFilter find resource group with given filter under the staged view and return the max one.
// not efficient, but it's ok for low nodes and low resource group.
func (rm *ResourceManager) findMaxRGWithGivenFilter(staging *rgStaging, filter func(rg *ResourceGroup) bool, attr func(rg *ResourceGroup) int) *ResourceGroup {
	var maxRG *ResourceGroup
	rm.rangeGroupsWithStaging(staging, func(rg *ResourceGroup) bool {
		if filter == nil || filter(rg) {
			if maxRG == nil || attr(rg) > attr(maxRG) {
				maxRG = rg
			}
		}
		return true
	})
	return maxRG
}

// rgStaging is an uncommitted overlay over ResourceManager.groups.
// stageTransferNode/stageUnassignNode only modify this overlay, so a chain of
// node movements can be persisted by a single batch SaveResourceGroup call in
// commitStaging. Before commit, neither catalog nor in-memory state is touched.
type rgStaging struct {
	groups    map[string]*ResourceGroup // staged view of modified resource groups.
	dirty     []string                  // staged resource group names in first-staged order.
	rgCreated bool                      // a new resource group is staged, rgChangedNotifier fires on commit.
}

func newRGStaging() *rgStaging {
	return &rgStaging{groups: make(map[string]*ResourceGroup)}
}

// get returns the staged version of the resource group if present, otherwise the committed one.
func (s *rgStaging) get(rm *ResourceManager, rgName string) *ResourceGroup {
	if rg, ok := s.groups[rgName]; ok {
		return rg
	}
	return rm.groups[rgName]
}

// put stages a modified resource group into the overlay.
func (s *rgStaging) put(rg *ResourceGroup) {
	if _, ok := s.groups[rg.GetName()]; !ok {
		s.dirty = append(s.dirty, rg.GetName())
	}
	s.groups[rg.GetName()] = rg
}

// rangeGroupsWithStaging iterates over the union view of committed and staged resource groups.
func (rm *ResourceManager) rangeGroupsWithStaging(staging *rgStaging, f func(rg *ResourceGroup) bool) {
	for rgName, rg := range rm.groups {
		if staged, ok := staging.groups[rgName]; ok {
			rg = staged
		}
		if !f(rg) {
			return
		}
	}
	// staged-only (newly created) resource groups.
	for rgName, rg := range staging.groups {
		if _, ok := rm.groups[rgName]; ok {
			continue
		}
		if !f(rg) {
			return
		}
	}
}

// getResourceGroupByNodeIDWithStaging get resource group by node id under the staged view.
func (rm *ResourceManager) getResourceGroupByNodeIDWithStaging(staging *rgStaging, node int64) *ResourceGroup {
	// staged resource groups win: the node may have been moved by a staged-but-uncommitted change.
	for _, rgName := range staging.dirty {
		if staging.groups[rgName].ContainNode(node) {
			return staging.groups[rgName]
		}
	}
	if rgName, ok := rm.nodeIDMap[node]; ok {
		if _, staged := staging.groups[rgName]; staged {
			// the resource group is staged but no longer contains the node,
			// so the node has been unassigned in staging.
			return nil
		}
		return rm.groups[rgName]
	}
	return nil
}

// stageTransferNode stage a transfer of the given node to the given resource group.
// if given node is assigned in given resource group (under the staged view), do nothing.
// if given node is assigned to other resource group, it will be unassigned first.
func (rm *ResourceManager) stageTransferNode(ctx context.Context, staging *rgStaging, rgName string, node int64) error {
	if staging.get(rm, rgName) == nil {
		return merr.WrapErrResourceGroupNotFound(rgName)
	}

	originalRG := "_"
	// Check if node is already assign to rg.
	if rg := rm.getResourceGroupByNodeIDWithStaging(staging, node); rg != nil {
		if rg.GetName() == rgName {
			// node is already assign to rg.
			mlog.Info(context.TODO(), "node already assign to resource group",
				mlog.String("rgName", rgName),
				mlog.Int64("node", node),
			)
			return nil
		}
		// Stage the unassignment from the original resource group.
		mrg := rg.CopyForWrite()
		mrg.UnassignNode(node)
		rg := mrg.ToResourceGroup()
		staging.put(rg)
		originalRG = rg.GetName()
	}

	// Stage the assignment to the target resource group.
	mrg := staging.get(rm, rgName).CopyForWrite()
	mrg.AssignNode(node)
	staging.put(mrg.ToResourceGroup())

	mlog.Info(context.TODO(), "stage node transfer to resource group",
		mlog.String("rgName", rgName),
		mlog.String("originalRG", originalRG),
		mlog.Int64("node", node),
	)
	return nil
}

// stageUnassignNode stage the removal of a node from the resource group it
// belongs to (under the staged view).
func (rm *ResourceManager) stageUnassignNode(ctx context.Context, staging *rgStaging, node int64) (string, error) {
	if rg := rm.getResourceGroupByNodeIDWithStaging(staging, node); rg != nil {
		mrg := rg.CopyForWrite()
		mrg.UnassignNode(node)
		rg := mrg.ToResourceGroup()
		staging.put(rg)

		mlog.Info(context.TODO(), "stage unassign node from resource group",
			mlog.String("rgName", rg.GetName()),
			mlog.Int64("node", node),
		)
		return rg.GetName(), nil
	}

	return "", merr.WrapErrNodeNotFound(node, "not found in any resource group")
}

// commitStaging persist all staged resource groups by a single batch SaveResourceGroup
// call, then commit them into memory in staged order and notify listeners once.
// On failure neither catalog nor memory is modified, so the in-memory state never
// diverges from the persisted one and the whole batch can be retried by the caller.
func (rm *ResourceManager) commitStaging(ctx context.Context, staging *rgStaging) error {
	if len(staging.dirty) == 0 {
		return nil
	}

	updates := make([]*querypb.ResourceGroup, 0, len(staging.dirty))
	for _, rgName := range staging.dirty {
		updates = append(updates, staging.groups[rgName].GetMeta())
	}
	if err := rm.catalog.SaveResourceGroup(ctx, updates...); err != nil {
		mlog.Warn(context.TODO(), "failed to save staged resource groups",
			mlog.Strings("rgNames", staging.dirty),
			mlog.Err(err),
		)
		return merr.WrapErrResourceGroupServiceUnAvailable()
	}

	// Commit updates to memory.
	for _, rgName := range staging.dirty {
		rm.setupInMemResourceGroup(staging.groups[rgName])
	}
	mlog.Info(context.TODO(), "commit staged resource groups",
		mlog.Strings("rgNames", staging.dirty),
	)

	if staging.rgCreated {
		// notify that resource group has been changed.
		rm.rgChangedNotifier.NotifyAll()
	}
	// notify that node distribution has been changed.
	rm.nodeChangedNotifier.NotifyAll()

	// reset the overlay so that a reused staging never commits twice.
	staging.groups = make(map[string]*ResourceGroup)
	staging.dirty = nil
	staging.rgCreated = false
	return nil
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

	for _, nodeInfo := range rm.nodeMgr.GetAll() {
		if nodeInfo.ResourceGroupName() == rgName {
			return merr.WrapErrParameterInvalid("not empty resource group", fmt.Sprintf("node %d is still in the resource group", nodeInfo.ID()))
		}
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
	// clear old metrics and nodeIDMap entries.
	// Use GetAllNodes (bypasses label filter) to ensure all physical nodes are cleaned up,
	// even when the RG's label filter has changed.
	if oldR, ok := rm.groups[r.GetName()]; ok {
		for _, nodeID := range oldR.GetAllNodes() {
			metrics.QueryCoordResourceGroupInfo.DeletePartialMatch(prometheus.Labels{
				metrics.ResourceGroupLabelName: r.GetName(),
				metrics.NodeIDLabelName:        strconv.FormatInt(nodeID, 10),
			})
			delete(rm.nodeIDMap, nodeID)
		}
	}
	// add new metrics and nodeIDMap entries.
	for _, nodeID := range r.GetAllNodes() {
		metrics.QueryCoordResourceGroupInfo.WithLabelValues(
			r.GetName(),
			strconv.FormatInt(nodeID, 10),
		).Set(1)
		rm.nodeIDMap[nodeID] = r.GetName()
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
		mlog.Error(context.TODO(), "failed to marshal resource groups", mlog.Err(err))
		return ""
	}

	return string(ret)
}

func (rm *ResourceManager) CheckNodesInResourceGroup(ctx context.Context) {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()

	// clean stopping/offline nodes
	assignedNodes := typeutil.NewUniqueSet()
	for _, rg := range rm.groups {
		for _, node := range rg.GetNodes() {
			assignedNodes.Insert(node)
			info := rm.nodeMgr.Get(node)
			if info == nil {
				rm.handleNodeDown(ctx, node)
			} else if info.GetState() == session.NodeStateStopping {
				mlog.Warn(context.TODO(), "node is stopping", mlog.Int64("node", node))
				rm.handleNodeStopping(ctx, node)
			} else if info.IsEmbeddedQueryNodeInStreamingNode() {
				mlog.Warn(context.TODO(), "unreachable code, but just for dirty meta clean up", mlog.Int64("node", node))
				rm.handleNodeStopping(ctx, node)
			}
		}
	}

	// add new nodes
	for _, node := range rm.nodeMgr.GetAll() {
		if !assignedNodes.Contain(node.ID()) {
			rm.handleNodeUp(context.Background(), node.ID())
		}
	}
}
