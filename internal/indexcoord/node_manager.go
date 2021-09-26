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

	"github.com/milvus-io/milvus/internal/proto/milvuspb"

	grpcindexnodeclient "github.com/milvus-io/milvus/internal/distributed/indexnode/client"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/types"
	"go.uber.org/zap"
)

// NodeManager is used by IndexCoord to manage the client of IndexNode.
type NodeManager struct {
	nodeClients map[UniqueID]types.IndexNode
	pq          *PriorityQueue

	lock sync.RWMutex
}

// NewNodeManager is used to create a new NodeManager.
func NewNodeManager() *NodeManager {
	return &NodeManager{
		nodeClients: make(map[UniqueID]types.IndexNode),
		pq:          &PriorityQueue{},
		lock:        sync.RWMutex{},
	}
}

func (nm *NodeManager) setClient(nodeID UniqueID, client types.IndexNode) {
	nm.lock.Lock()
	defer nm.lock.Unlock()

	log.Debug("IndexCoord NodeManager setClient", zap.Int64("nodeID", nodeID))
	defer log.Debug("IndexNode NodeManager setclient success", zap.Any("nodeID", nodeID))
	item := &PQItem{
		key:      nodeID,
		priority: 0,
	}
	nm.nodeClients[nodeID] = client
	nm.pq.Push(item)
}

// RemoveNode removes the unused client of IndexNode.
func (nm *NodeManager) RemoveNode(nodeID UniqueID) {
	nm.lock.Lock()
	defer nm.lock.Unlock()

	log.Debug("IndexCoord", zap.Any("Remove node with ID", nodeID))
	delete(nm.nodeClients, nodeID)
	nm.pq.Remove(nodeID)
}

// AddNode adds the client of IndexNode.
func (nm *NodeManager) AddNode(nodeID UniqueID, address string) error {
	log.Debug("IndexCoord addNode", zap.Any("nodeID", nodeID), zap.Any("node address", address))
	if nm.pq.CheckExist(nodeID) {
		log.Warn("IndexCoord", zap.Any("Node client already exist with ID:", nodeID))
		return nil
	}

	nodeClient, err := grpcindexnodeclient.NewClient(context.TODO(), address)
	if err != nil {
		log.Error("IndexCoord NodeManager", zap.Any("Add node err", err))
		return err
	}
	err = nodeClient.Init()
	if err != nil {
		log.Error("IndexCoord NodeManager", zap.Any("Add node err", err))
		return err
	}
	nm.setClient(nodeID, nodeClient)
	return nil
}

// PeekClient peeks the client with the least load.
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

type indexNodeGetMetricsResponse struct {
	resp *milvuspb.GetMetricsResponse
	err  error
}

func (nm *NodeManager) getMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) []indexNodeGetMetricsResponse {
	nm.lock.RLock()
	defer nm.lock.RUnlock()

	ret := make([]indexNodeGetMetricsResponse, 0, len(nm.nodeClients))
	for _, node := range nm.nodeClients {
		resp, err := node.GetMetrics(ctx, req)
		ret = append(ret, indexNodeGetMetricsResponse{
			resp: resp,
			err:  err,
		})
	}

	return ret
}
