// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package indexcoord

import (
	"container/heap"
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"
	grpcindexnodeclient "github.com/milvus-io/milvus/internal/distributed/indexnode/client"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/types"
)

// An Item is something we manage in a priority queue.
type PQItem struct {
	value types.IndexNode // The value of the item; arbitrary.
	key   UniqueID
	addr  string

	priority int // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.

type PriorityQueue struct {
	items []*PQItem
	lock  sync.RWMutex

	kv *etcdkv.EtcdKV
}

func NewNodeClients(kv *etcdkv.EtcdKV) *PriorityQueue {
	pq := &PriorityQueue{
		kv:   kv,
		lock: sync.RWMutex{},
	}
	pq.reloadFromETCD()

	return pq
}

func (pq *PriorityQueue) Reset() {
	*pq = PriorityQueue{}
}

func (pq *PriorityQueue) String() string {
	return proto.CompactTextString(pq)
}

func (pq *PriorityQueue) ProtoMessage() {}

func (pq *PriorityQueue) Len() int {
	return len(pq.items)
}

func (pq *PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq.items[i].priority < pq.items[j].priority
}

func (pq *PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	n := (*pq).Len()
	item := x.(*PQItem)
	item.index = n
	pq.items = append(pq.items, item)
}

// Pop do not call this directly.
func (pq *PriorityQueue) Pop() interface{} {
	old := pq.items
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	pq.items = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) CheckAddressExist(addr string) bool {
	pq.lock.RLock()
	defer pq.lock.RUnlock()

	for _, item := range pq.items {
		if item.addr == addr {
			return true
		}
	}
	return false
}

func (pq *PriorityQueue) getItemByKey(key UniqueID) interface{} {
	var ret interface{} = nil
	for _, item := range pq.items {
		if item.key == key {
			ret = item
			break
		}
	}
	return ret
}

// IncPriority update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) IncPriority(key UniqueID, priority int) {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	item := pq.getItemByKey(key)
	if item != nil {
		item.(*PQItem).priority += priority
		heap.Fix(pq, item.(*PQItem).index)
	}
}

// UpdatePriority update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) UpdatePriority(key UniqueID, priority int) {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	item := pq.getItemByKey(key)
	if item != nil {
		item.(*PQItem).priority = priority
		heap.Fix(pq, item.(*PQItem).index)
	}
}

func (pq *PriorityQueue) Remove(key UniqueID) {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	item := pq.getItemByKey(key)
	if item != nil {
		heap.Remove(pq, item.(*PQItem).index)
	}
}

func (pq *PriorityQueue) Peek() interface{} {
	pq.lock.RLock()
	defer pq.lock.RUnlock()
	if pq.Len() == 0 {
		return nil
	}
	return pq.items[0]
	//item := pq.items[0]
	//return item.value
}

// PeekClient picks an IndexNode with the lowest load.
func (pq *PriorityQueue) PeekClient() (UniqueID, types.IndexNode) {
	item := pq.Peek()
	if item == nil {
		return UniqueID(-1), nil
	}
	return item.(*PQItem).key, item.(*PQItem).value
}

func (pq *PriorityQueue) PeekAllClients() []types.IndexNode {
	pq.lock.RLock()
	defer pq.lock.RUnlock()

	var ret []types.IndexNode
	for _, item := range pq.items {
		ret = append(ret, item.value)
	}

	return ret
}

func (pq *PriorityQueue) reloadFromETCD() {
	key := "IndexNodes"
	value, err := pq.kv.Load(key)
	if err != nil {
		log.Debug("IndexCoord PriorityQueue", zap.Any("Load IndexNode err", err))
	}
	err = proto.UnmarshalText(value, pq)
	if err != nil {
		log.Debug("IndexCoord PriorityQueue", zap.Any("UnmarshalText err", err))
	}
}

func (pq *PriorityQueue) saveNodeClients() {
	log.Debug("IndexCoord saveNodeClients", zap.Any("PriorityQueue", pq))
	value := proto.MarshalTextString(pq)

	key := "IndexNodes"
	if err := pq.kv.Save(key, value); err != nil {
		log.Debug("IndexCoord PriorityQueue", zap.Any("IndexNodes save ETCD err", err))
	}
}

func (pq *PriorityQueue) removeNode(nodeID UniqueID) {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	log.Debug("IndexCoord", zap.Any("Remove node with ID", nodeID))
	pq.Remove(nodeID)
	pq.saveNodeClients()
}

func (pq *PriorityQueue) addNode(nodeID UniqueID, address string) error {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	log.Debug("IndexCoord addNode", zap.Any("nodeID", nodeID), zap.Any("node address", address))

	if pq.CheckAddressExist(address) {
		log.Debug("IndexCoord", zap.Any("Node client already exist with ID:", nodeID))
		return nil
	}

	nodeClient, err := grpcindexnodeclient.NewClient(context.TODO(), address)
	if err != nil {
		return err
	}
	err = nodeClient.Init()
	if err != nil {
		return err
	}
	item := &PQItem{
		value:    nodeClient,
		key:      nodeID,
		addr:     address,
		priority: 0,
	}
	pq.Push(item)
	pq.saveNodeClients()
	return nil
}
