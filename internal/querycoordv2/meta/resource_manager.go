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
	"errors"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

var (
	ErrNodeNotExistInRG            = errors.New("node doesn't exist in resource group")
	ErrNodeAlreadyAssign           = errors.New("node already assign to other resource group")
	ErrRGIsFull                    = errors.New("resource group is full")
	ErrRGNotExist                  = errors.New("resource group doesn't exist")
	ErrRGAlreadyExist              = errors.New("resource group already exist")
	ErrRGAssignNodeFailed          = errors.New("failed to assign node to resource group")
	ErrRGUnAssignNodeFailed        = errors.New("failed to unassign node from resource group")
	ErrSaveResourceGroupToStore    = errors.New("failed to save resource group to store")
	ErrRecoverResourceGroupToStore = errors.New("failed to recover resource group to store")
	ErrNodeNotAssignToRG           = errors.New("node hasn't been assign to any resource group")
	ErrRGNameIsEmpty               = errors.New("resource group name couldn't be empty")
	ErrDeleteDefaultRG             = errors.New("delete default rg is not permitted")
	ErrNodeNotExist                = errors.New("node does not exist")
	ErrNodeStopped                 = errors.New("node has been stoped")
)

var DefaultResourceGroupName = "__default_resource_group"

type ResourceGroup struct {
	nodes    UniqueSet
	capacity int
}

func NewResourceGroup(capacity int, nodes ...int64) *ResourceGroup {
	rg := &ResourceGroup{
		nodes:    typeutil.NewUniqueSet(),
		capacity: capacity,
	}

	for _, node := range nodes {
		rg.assignNode(node)
	}

	return rg
}

// assign node to resource group
func (rg *ResourceGroup) assignNode(id int64) error {
	if rg.containsNode(id) {
		return ErrNodeAlreadyAssign
	}

	rg.nodes.Insert(id)
	rg.capacity++

	return nil
}

// unassign node from resource group
func (rg *ResourceGroup) unassignNode(id int64) error {
	if !rg.containsNode(id) {
		// remove non exist node should be tolerable
		return nil
	}

	rg.nodes.Remove(id)
	rg.capacity--

	return nil
}

func (rg *ResourceGroup) handleNodeUp(id int64) error {
	if rg.isFull() {
		return ErrRGIsFull
	}

	if rg.containsNode(id) {
		return ErrNodeAlreadyAssign
	}

	rg.nodes.Insert(id)
	return nil
}

func (rg *ResourceGroup) handleNodeDown(id int64) error {
	if !rg.containsNode(id) {
		// remove non exist node should be tolerable
		return nil
	}

	rg.nodes.Remove(id)
	return nil
}

func (rg *ResourceGroup) isFull() bool {
	return rg.nodes.Len() == rg.capacity
}

func (rg *ResourceGroup) containsNode(id int64) bool {
	return rg.nodes.Contain(id)
}

func (rg *ResourceGroup) getNodes() []int64 {
	return rg.nodes.Collect()
}

func (rg *ResourceGroup) clear() {
	for k := range rg.nodes {
		delete(rg.nodes, k)
	}
	rg.capacity = 0
}

func (rg *ResourceGroup) getCapacity() int {
	return rg.capacity
}

type ResourceManager struct {
	groups  map[string]*ResourceGroup
	store   Store
	nodeMgr *session.NodeManager

	rwmutex sync.RWMutex
}

func NewResourceManager(store Store, nodeMgr *session.NodeManager) *ResourceManager {
	groupMap := make(map[string]*ResourceGroup)
	groupMap[DefaultResourceGroupName] = NewResourceGroup(1024)
	return &ResourceManager{
		groups:  groupMap,
		store:   store,
		nodeMgr: nodeMgr,
	}
}

func (rm *ResourceManager) AddResourceGroup(rgName string, nodes ...int64) error {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()
	if len(rgName) == 0 {
		return ErrRGNameIsEmpty
	}

	if rm.groups[rgName] != nil {
		return ErrRGAlreadyExist
	}

	rm.groups[rgName] = NewResourceGroup(len(nodes), nodes...)

	log.Info("add resource group",
		zap.String("rgName", rgName),
		zap.Int64s("nodes", nodes),
	)
	return nil
}

func (rm *ResourceManager) RemoveResourceGroup(rgName string) error {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()
	if rgName == DefaultResourceGroupName {
		return ErrDeleteDefaultRG
	}

	if rm.groups[rgName] == nil {
		// delete a non-exist rg should be tolerable
		return nil
	}

	delete(rm.groups, rgName)

	log.Info("remove resource group",
		zap.String("rgName", rgName),
	)
	return nil
}

func (rm *ResourceManager) AssignNode(rgName string, node int64) error {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()
	return rm.assignNode(rgName, node)
}

func (rm *ResourceManager) assignNode(rgName string, node int64) error {
	if rm.groups[rgName] == nil {
		return ErrRGNotExist
	}

	if rm.nodeMgr.Get(node) == nil {
		return ErrNodeNotExist
	}

	if ok, _ := rm.nodeMgr.IsStoppingNode(node); ok {
		return ErrNodeStopped
	}

	rm.checkRGNodeStatus(rgName)
	if rm.checkNodeAssigned(node) {
		return ErrNodeAlreadyAssign
	}

	err := rm.groups[rgName].assignNode(node)
	if err != nil {
		return err
	}

	err = rm.updateResourceGroupInStore(rgName)
	if err != nil {
		// roll back, acutually unreachable logic path
		rm.groups[rgName].unassignNode(node)
		return ErrRGAssignNodeFailed
	}

	log.Info("add node to resource group",
		zap.String("rgName", rgName),
		zap.Int64("node", node),
	)

	return nil
}

func (rm *ResourceManager) updateResourceGroupInStore(rgName string) error {
	err := rm.store.SaveResourceGroup(rgName, &querypb.ResourceGroup{
		Name:     rgName,
		Capacity: int32(rm.groups[rgName].getCapacity()),
		Nodes:    rm.groups[rgName].getNodes(),
	})

	if err != nil {
		return ErrSaveResourceGroupToStore
	}

	return nil
}

func (rm *ResourceManager) checkNodeAssigned(node int64) bool {
	for _, group := range rm.groups {
		if group.containsNode(node) {
			return true
		}
	}

	return false
}

func (rm *ResourceManager) UnassignNode(rgName string, node int64) error {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()

	return rm.unassignNode(rgName, node)
}

func (rm *ResourceManager) unassignNode(rgName string, node int64) error {
	if rm.groups[rgName] == nil {
		return ErrRGNotExist
	}

	if rm.nodeMgr.Get(node) == nil {
		// remove non exist node should be tolerable
		return nil
	}

	rm.checkRGNodeStatus(rgName)
	err := rm.groups[rgName].unassignNode(node)
	if err != nil {
		return err
	}

	err = rm.updateResourceGroupInStore(rgName)
	if err != nil {
		// roll back
		rm.groups[rgName].assignNode(node)
		return ErrRGUnAssignNodeFailed
	}

	log.Info("remove  node from resource group",
		zap.String("rgName", rgName),
		zap.Int64("node", node),
	)

	return nil
}

func (rm *ResourceManager) GetNodes(rgName string) ([]int64, error) {
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()
	if rm.groups[rgName] == nil {
		return nil, ErrRGNotExist
	}

	rm.checkRGNodeStatus(rgName)

	return rm.groups[rgName].getNodes(), nil
}

func (rm *ResourceManager) ContainsNode(rgName string, node int64) bool {
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()
	if rm.groups[rgName] == nil {
		return false
	}

	rm.checkRGNodeStatus(rgName)
	return rm.groups[rgName].containsNode(node)
}

func (rm *ResourceManager) GetResourceGroups(rgName string) (*querypb.ResourceGroup, error) {
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()

	if rm.groups[rgName] == nil {
		return nil, ErrRGNotExist
	}

	rm.checkRGNodeStatus(rgName)
	return &querypb.ResourceGroup{
		Name:     rgName,
		Nodes:    rm.groups[rgName].getNodes(),
		Capacity: int32(rm.groups[rgName].getCapacity()),
	}, nil
}

func (rm *ResourceManager) ListResourceGroups() []*querypb.ResourceGroup {
	rm.rwmutex.RLock()
	defer rm.rwmutex.RUnlock()

	ret := make([]*querypb.ResourceGroup, len(rm.groups))
	for rgName, rg := range rm.groups {
		rm.checkRGNodeStatus(rgName)
		ret = append(ret, &querypb.ResourceGroup{
			Name:     rgName,
			Nodes:    rg.getNodes(),
			Capacity: int32(rg.getCapacity()),
		})
	}

	return ret
}

func (rm *ResourceManager) findResourceGroupByNode(node int64) (string, error) {
	for name, group := range rm.groups {
		if group.containsNode(node) {
			return name, nil
		}
	}

	return "", ErrNodeNotAssignToRG
}

func (rm *ResourceManager) HandleNodeUp(node int64) (string, error) {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()

	if rm.nodeMgr.Get(node) == nil {
		return "", ErrNodeNotExist
	}

	if ok, _ := rm.nodeMgr.IsStoppingNode(node); ok {
		return "", ErrNodeStopped
	}

	// if node already assign to rg
	rgName, err := rm.findResourceGroupByNode(node)
	if err == nil {
		log.Info("HandleNodeUp: node already assign to resource group",
			zap.String("rgName", rgName),
			zap.Int64("node", node),
		)
		return rgName, nil
	}

	// find rg which lack of node
	for name, group := range rm.groups {
		// if rg lack nodes, assign new node to it
		if ok := group.isFull(); !ok {
			log.Info("HandleNodeUp: add node to resource group",
				zap.String("rgName", name),
				zap.Int64("node", node),
			)
			return name, rm.groups[name].handleNodeUp(node)
		}
	}

	// if all group is full, then assign node to group with the least nodes
	rm.assignNode(DefaultResourceGroupName, node)
	log.Info("HandleNodeUp: assign node to default resource group",
		zap.String("rgName", DefaultResourceGroupName),
		zap.Int64("node", node),
	)
	return DefaultResourceGroupName, nil
}

func (rm *ResourceManager) HandleNodeDown(node int64) (string, error) {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()

	if rm.nodeMgr.Get(node) == nil {
		return "", ErrNodeNotExist
	}

	rgName, err := rm.findResourceGroupByNode(node)
	if err == nil {
		log.Info("HandleNodeDown: remove node from resource group",
			zap.String("rgName", DefaultResourceGroupName),
			zap.Int64("node", node),
		)
		return rgName, rm.groups[rgName].handleNodeDown(node)
	}

	return "", ErrNodeNotAssignToRG
}

func (rm *ResourceManager) TransferNode(from string, to string, node int64) error {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()

	if rm.groups[from] == nil || rm.groups[to] == nil {
		return ErrRGNotExist
	}

	if rm.nodeMgr.Get(node) == nil {
		return ErrNodeNotExist
	}

	rm.checkRGNodeStatus(from)
	err := rm.unassignNode(from, node)
	if err != nil {
		// interrupt transfer
		return err
	}

	rm.checkRGNodeStatus(to)
	err = rm.assignNode(to, node)
	if err != nil {
		// roll back
		rm.assignNode(from, node)
	}

	return nil
}

func (rm *ResourceManager) Recover() error {
	rm.rwmutex.Lock()
	defer rm.rwmutex.Unlock()
	rgs, err := rm.store.GetResourceGroups()
	if err != nil {
		return ErrRecoverResourceGroupToStore
	}

	for _, rg := range rgs {
		rm.groups[rg.GetName()] = NewResourceGroup(int(rg.GetCapacity()), rg.GetNodes()...)
		rm.checkRGNodeStatus(rg.GetName())
	}

	return nil
}

// every operation which involves nodes access, should check nodes status first
func (rm *ResourceManager) checkRGNodeStatus(rgName string) {
	for _, node := range rm.groups[rgName].getNodes() {
		if rm.nodeMgr.Get(node) == nil {
			log.Info("found node down, remove it",
				zap.String("rgName", rgName),
				zap.Int64("nodeID", node),
			)

			rm.groups[rgName].handleNodeDown(node)
		}
	}
}
