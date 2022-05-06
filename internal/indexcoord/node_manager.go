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

package indexcoord

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/metrics"

	"go.uber.org/zap"

	grpcindexnodeclient "github.com/milvus-io/milvus/internal/distributed/indexnode/client"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
)

// NodeManager is used by IndexCoord to manage the client of IndexNode.
type NodeManager struct {
	nodeClients map[UniqueID]types.IndexNode
	pq          *PriorityQueue
	lock        sync.RWMutex
	ctx         context.Context
}

// NewNodeManager is used to create a new NodeManager.
func NewNodeManager(ctx context.Context) *NodeManager {
	return &NodeManager{
		nodeClients: make(map[UniqueID]types.IndexNode),
		pq: &PriorityQueue{
			policy: PeekClientV1,
		},
		lock: sync.RWMutex{},
		ctx:  ctx,
	}
}

// setClient sets IndexNode client to node manager.
func (nm *NodeManager) setClient(nodeID UniqueID, client types.IndexNode) error {
	log.Debug("IndexCoord NodeManager setClient", zap.Int64("nodeID", nodeID))
	defer log.Debug("IndexNode NodeManager setclient success", zap.Any("nodeID", nodeID))
	item := &PQItem{
		key:      nodeID,
		priority: 0,
		weight:   0,
		totalMem: 0,
	}
	nm.lock.Lock()
	nm.nodeClients[nodeID] = client
	nm.lock.Unlock()
	nm.pq.Push(item)
	return nil
}

// RemoveNode removes the unused client of IndexNode.
func (nm *NodeManager) RemoveNode(nodeID UniqueID) {
	log.Debug("IndexCoord", zap.Any("Remove node with ID", nodeID))
	nm.lock.Lock()
	delete(nm.nodeClients, nodeID)
	nm.lock.Unlock()
	nm.pq.Remove(nodeID)
	metrics.IndexCoordIndexNodeNum.WithLabelValues().Dec()
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
	metrics.IndexCoordIndexNodeNum.WithLabelValues().Inc()
	return nm.setClient(nodeID, nodeClient)
}

// PeekClient peeks the client with the least load.
func (nm *NodeManager) PeekClient(meta Meta) (UniqueID, types.IndexNode) {
	log.Debug("IndexCoord NodeManager PeekClient")

	dataSize, err := estimateIndexSizeByReq(meta.indexMeta.Req)
	if err != nil {
		log.Warn(err.Error())
		return UniqueID(-1), nil
	}
	log.Debug("IndexCoord peek IndexNode client from pq", zap.Uint64("data size", dataSize))
	nodeID := nm.pq.Peek(dataSize*indexSizeFactor, meta.indexMeta.Req.IndexParams, meta.indexMeta.Req.TypeParams)
	if nodeID == -1 {
		log.Error("there is no indexnode online")
		return nodeID, nil
	}
	if nodeID == 0 {
		log.Error("No IndexNode available", zap.Uint64("data size", dataSize),
			zap.Uint64("IndexNode must have memory size", dataSize*indexSizeFactor))
		return nodeID, nil
	}
	nm.lock.Lock()
	defer nm.lock.Unlock()
	client, ok := nm.nodeClients[nodeID]
	if !ok {
		log.Error("IndexCoord NodeManager PeekClient", zap.Int64("There is no IndexNode client corresponding to NodeID", nodeID))
		return nodeID, nil
	}
	log.Debug("IndexCoord NodeManager PeekClient ", zap.Int64("node", nodeID), zap.Uint64("data size", dataSize))
	return nodeID, client
}

// ListNode lists all IndexNodes in node manager.
func (nm *NodeManager) ListNode() []UniqueID {
	//nm.lock.Lock()
	//defer nm.lock.Unlock()
	var clientIDs []UniqueID
	nm.lock.RLock()
	for id := range nm.nodeClients {
		clientIDs = append(clientIDs, id)
	}

	nm.lock.RUnlock()
	var wg sync.WaitGroup

	for _, id := range clientIDs {
		memory := nm.pq.GetMemory(id)
		if memory == 0 {
			log.Debug("IndexCoord get IndexNode metrics info", zap.Int64("nodeID", id))
			req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
			if err != nil {
				log.Error("create metrics request failed", zap.Error(err))
				continue
			}

			nm.lock.RLock()
			client, ok := nm.nodeClients[id]
			if !ok {
				nm.lock.RUnlock()
				log.Debug("NodeManager ListNode find client not exist")
				continue
			}
			nm.lock.RUnlock()

			wg.Add(1)
			go func(group *sync.WaitGroup, id UniqueID) {
				defer group.Done()
				ctx, cancel := context.WithTimeout(nm.ctx, time.Second*5)
				defer cancel()
				metrics, err := client.GetMetrics(ctx, req)
				if err != nil {
					log.Error("get IndexNode metrics failed", zap.Error(err))
					return
				}
				infos := &metricsinfo.IndexNodeInfos{}
				err = metricsinfo.UnmarshalComponentInfos(metrics.Response, infos)
				if err != nil {
					log.Error("get IndexNode metrics info failed", zap.Error(err))
					return
				}
				log.Debug("IndexCoord get IndexNode's metrics success", zap.Int64("nodeID", id),
					zap.Int("CPUCoreCount", infos.HardwareInfos.CPUCoreCount), zap.Float64("CPUCoreUsage", infos.HardwareInfos.CPUCoreUsage),
					zap.Uint64("Memory", infos.HardwareInfos.Memory), zap.Uint64("MemoryUsage", infos.HardwareInfos.MemoryUsage))
				nm.pq.SetMemory(id, infos.HardwareInfos.Memory)
			}(&wg, id)
		}
	}
	wg.Wait()
	return clientIDs
}

// indexNodeGetMetricsResponse record the metrics information of IndexNode.
type indexNodeGetMetricsResponse struct {
	resp *milvuspb.GetMetricsResponse
	err  error
}

// getMetrics get metrics information of all IndexNode.
func (nm *NodeManager) getMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) []indexNodeGetMetricsResponse {
	var clients []types.IndexNode
	nm.lock.RLock()
	for _, node := range nm.nodeClients {
		clients = append(clients, node)
	}
	nm.lock.RUnlock()

	ret := make([]indexNodeGetMetricsResponse, 0, len(nm.nodeClients))
	for _, node := range clients {
		resp, err := node.GetMetrics(ctx, req)
		ret = append(ret, indexNodeGetMetricsResponse{
			resp: resp,
			err:  err,
		})
	}
	return ret
}
