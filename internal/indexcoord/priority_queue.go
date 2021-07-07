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
	"context"
	"strconv"
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
}

func (pq *PQItem) Reset() {
	*pq = PQItem{}
}

func (pq *PQItem) String() string {
	return proto.CompactTextString(pq)
}

func (pq *PQItem) ProtoMessage() {}

// A PriorityQueue implements heap.Interface and holds Items.

type PriorityQueue struct {
	items map[UniqueID]*PQItem
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

func (pq *PriorityQueue) checkAddressExist(addr string) bool {
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

func (pq *PriorityQueue) Len() int {
	pq.lock.RLock()
	defer pq.lock.RUnlock()
	return len(pq.items)
}

// IncPriority update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) IncPriority(key UniqueID, priority int) {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	item := pq.getItemByKey(key)
	if item != nil {
		item.(*PQItem).priority += priority
	}
	pq.items[key] = item.(*PQItem)
	pq.saveNodeClients(key, item.(*PQItem))
}

// PeekClient picks an IndexNode with the lowest load.
func (pq *PriorityQueue) PeekClient() (UniqueID, types.IndexNode) {
	pq.lock.RLock()
	defer pq.lock.RUnlock()

	if len(pq.items) == 0 {
		return UniqueID(-1), nil
	}
	it := &PQItem{}
	for k := range pq.items {
		it = pq.items[k]
		break
	}
	for _, item := range pq.items {
		if item.priority < it.priority {
			it = item
		}
	}
	return it.key, it.value
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
	pq.lock.RLock()
	defer pq.lock.RUnlock()

	pq.items = make(map[UniqueID]*PQItem)
	key := "IndexNodes"
	_, values, err := pq.kv.LoadWithPrefix(key)
	if err != nil {
		log.Debug("IndexCoord PriorityQueue", zap.Any("Load IndexNode err", err))
	}
	for _, value := range values {
		item := PQItem{}
		err = proto.UnmarshalText(value, &item)
		if err != nil {
			log.Debug("IndexCoord PriorityQueue", zap.Any("UnmarshalText err", err))
		}
		pq.items[item.key] = &item
	}
}

func (pq *PriorityQueue) saveNodeClients(serverID UniqueID, item *PQItem) {
	log.Debug("IndexCoord saveNodeClients", zap.Any("PriorityQueue", pq.items))
	value := proto.MarshalTextString(item)

	key := "IndexNodes/" + strconv.FormatInt(serverID, 10)
	if err := pq.kv.Save(key, value); err != nil {
		log.Debug("IndexCoord PriorityQueue", zap.Any("IndexNodes save ETCD err", err))
	}
}

func (pq *PriorityQueue) removeNode(nodeID UniqueID) error {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	log.Debug("IndexCoord", zap.Any("Remove node with ID", nodeID))
	delete(pq.items, nodeID)
	key := "IndexNodes/" + strconv.FormatInt(nodeID, 10)
	return pq.kv.Remove(key)
}

func (pq *PriorityQueue) addNode(nodeID UniqueID, address string) error {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	log.Debug("IndexCoord addNode", zap.Any("nodeID", nodeID), zap.Any("node address", address))
	if pq.checkAddressExist(address) {
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
	pq.items[nodeID] = item
	pq.saveNodeClients(nodeID, item)
	return nil
}
