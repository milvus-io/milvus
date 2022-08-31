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

	"go.uber.org/zap"

	grpcindexnodeclient "github.com/milvus-io/milvus/internal/distributed/indexnode/client"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
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
func (nm *NodeManager) setClient(nodeID UniqueID, client types.IndexNode) {
	log.Debug("IndexCoord NodeManager setClient", zap.Int64("nodeID", nodeID))
	item := &PQItem{
		key:      nodeID,
		priority: 0,
		weight:   0,
		totalMem: 0,
	}
	nm.lock.Lock()
	nm.nodeClients[nodeID] = client
	log.Debug("IndexNode NodeManager setClient success", zap.Int64("nodeID", nodeID), zap.Int("IndexNode num", len(nm.nodeClients)))
	nm.lock.Unlock()
	nm.pq.Push(item)
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
	nm.setClient(nodeID, nodeClient)
	return nil
}

// PeekClient peeks the client with the least load.
func (nm *NodeManager) PeekClient(meta *model.SegmentIndex) (UniqueID, types.IndexNode) {
	log.Info("IndexCoord peek client")
	allClients := nm.GetAllClients()
	if len(allClients) == 0 {
		log.Error("there is no IndexNode online")
		return -1, nil
	}

	// Note: In order to quickly end other goroutines, an error is returned when the client is successfully selected
	ctx, cancel := context.WithCancel(nm.ctx)
	var (
		peekNodeID = UniqueID(0)
		nodeMutex  = sync.Mutex{}
		wg         = sync.WaitGroup{}
	)

	for nodeID, client := range allClients {
		nodeID := nodeID
		client := client
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.GetJobStats(ctx, &indexpb.GetJobStatsRequest{})
			if err != nil {
				log.Warn("get IndexNode slots failed", zap.Int64("nodeID", nodeID), zap.Error(err))
				return
			}
			if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
				log.Warn("get IndexNode slots failed", zap.Int64("nodeID", nodeID),
					zap.String("reason", resp.Status.Reason))
				return
			}
			if resp.TaskSlots > 0 {
				nodeMutex.Lock()
				defer nodeMutex.Unlock()
				log.Info("peek client success", zap.Int64("nodeID", nodeID))
				if peekNodeID == 0 {
					peekNodeID = nodeID
				}
				cancel()
				// Note: In order to quickly end other goroutines, an error is returned when the client is successfully selected
				return
			}
		}()
	}
	wg.Wait()
	cancel()
	if peekNodeID != 0 {
		log.Info("IndexCoord peek client success", zap.Int64("nodeID", peekNodeID))
		return peekNodeID, allClients[peekNodeID]
	}

	log.Warn("IndexCoord peek client fail")
	return 0, nil
}

func (nm *NodeManager) GetAllClients() map[UniqueID]types.IndexNode {
	nm.lock.RLock()
	defer nm.lock.RUnlock()

	allClients := make(map[UniqueID]types.IndexNode, len(nm.nodeClients))
	for nodeID, client := range nm.nodeClients {
		allClients[nodeID] = client
	}

	return allClients
}

func (nm *NodeManager) GetClientByID(nodeID UniqueID) (types.IndexNode, bool) {
	nm.lock.RLock()
	defer nm.lock.RUnlock()

	client, ok := nm.nodeClients[nodeID]
	return client, ok
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
