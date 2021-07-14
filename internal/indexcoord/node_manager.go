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
	"sync"

	grpcindexnodeclient "github.com/milvus-io/milvus/internal/distributed/indexnode/client"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/types"
	"go.uber.org/zap"
)

type NodeManager struct {
	nodeClients map[UniqueID]types.IndexNode
	pq          *PriorityQueue

	lock sync.RWMutex
}

func NewNodeManager() *NodeManager {
	return &NodeManager{
		nodeClients: make(map[UniqueID]types.IndexNode),
		pq:          &PriorityQueue{},
		lock:        sync.RWMutex{},
	}
}

func (nm *NodeManager) RemoveNode(nodeID UniqueID) {
	nm.lock.Lock()
	defer nm.lock.Unlock()

	log.Debug("IndexCoord", zap.Any("Remove node with ID", nodeID))
	delete(nm.nodeClients, nodeID)
	nm.pq.Remove(nodeID)
}

func (nm *NodeManager) AddNode(nodeID UniqueID, address string) error {
	nm.lock.Lock()
	defer nm.lock.Unlock()

	log.Debug("IndexCoord addNode", zap.Any("nodeID", nodeID), zap.Any("node address", address))
	if nm.pq.CheckExist(nodeID) {
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
		key:      nodeID,
		priority: 0,
	}
	nm.nodeClients[nodeID] = nodeClient
	nm.pq.Push(item)
	return nil
}

func (nm *NodeManager) PeekClient() (UniqueID, types.IndexNode) {
	nm.lock.Lock()
	defer nm.lock.Unlock()

	log.Debug("IndexCoord NodeManager PeekClient")

	nodeID := nm.pq.Peek()
	client, ok := nm.nodeClients[nodeID]
	if !ok {
		log.Error("IndexCoord NodeManager PeekClient", zap.Any("There is no IndexNode client corresponding to NodeID", nodeID))
		return nodeID, nil
	}
	return nodeID, client
}
