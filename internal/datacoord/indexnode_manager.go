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

package datacoord

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type WorkerManager interface {
	AddNode(nodeID UniqueID, address string) error
	RemoveNode(nodeID UniqueID)
	StoppingNode(nodeID UniqueID)
	PickClient() (UniqueID, types.IndexNodeClient)
	ClientSupportDisk() bool
	GetAllClients() map[UniqueID]types.IndexNodeClient
	GetClientByID(nodeID UniqueID) (types.IndexNodeClient, bool)
}

// IndexNodeManager is used to manage the client of IndexNode.
type IndexNodeManager struct {
	nodeClients      map[UniqueID]types.IndexNodeClient
	stoppingNodes    map[UniqueID]struct{}
	lock             lock.RWMutex
	ctx              context.Context
	indexNodeCreator indexNodeCreatorFunc
}

// NewNodeManager is used to create a new IndexNodeManager.
func NewNodeManager(ctx context.Context, indexNodeCreator indexNodeCreatorFunc) *IndexNodeManager {
	return &IndexNodeManager{
		nodeClients:      make(map[UniqueID]types.IndexNodeClient),
		stoppingNodes:    make(map[UniqueID]struct{}),
		lock:             lock.RWMutex{},
		ctx:              ctx,
		indexNodeCreator: indexNodeCreator,
	}
}

// setClient sets IndexNode client to node manager.
func (nm *IndexNodeManager) setClient(nodeID UniqueID, client types.IndexNodeClient) {
	log.Debug("set IndexNode client", zap.Int64("nodeID", nodeID))
	nm.lock.Lock()
	defer nm.lock.Unlock()
	nm.nodeClients[nodeID] = client
	metrics.IndexNodeNum.WithLabelValues().Set(float64(len(nm.nodeClients)))
	log.Debug("IndexNode IndexNodeManager setClient success", zap.Int64("nodeID", nodeID), zap.Int("IndexNode num", len(nm.nodeClients)))
}

// RemoveNode removes the unused client of IndexNode.
func (nm *IndexNodeManager) RemoveNode(nodeID UniqueID) {
	log.Debug("remove IndexNode", zap.Int64("nodeID", nodeID))
	nm.lock.Lock()
	defer nm.lock.Unlock()
	delete(nm.nodeClients, nodeID)
	delete(nm.stoppingNodes, nodeID)
	metrics.IndexNodeNum.WithLabelValues().Set(float64(len(nm.nodeClients)))
}

func (nm *IndexNodeManager) StoppingNode(nodeID UniqueID) {
	log.Debug("IndexCoord", zap.Int64("Stopping node with ID", nodeID))
	nm.lock.Lock()
	defer nm.lock.Unlock()
	nm.stoppingNodes[nodeID] = struct{}{}
}

// AddNode adds the client of IndexNode.
func (nm *IndexNodeManager) AddNode(nodeID UniqueID, address string) error {
	log.Debug("add IndexNode", zap.Int64("nodeID", nodeID), zap.String("node address", address))
	var (
		nodeClient types.IndexNodeClient
		err        error
	)

	nodeClient, err = nm.indexNodeCreator(context.TODO(), address, nodeID)
	if err != nil {
		log.Error("create IndexNode client fail", zap.Error(err))
		return err
	}

	nm.setClient(nodeID, nodeClient)
	return nil
}

func (nm *IndexNodeManager) PickClient() (UniqueID, types.IndexNodeClient) {
	nm.lock.Lock()
	defer nm.lock.Unlock()

	// Note: In order to quickly end other goroutines, an error is returned when the client is successfully selected
	ctx, cancel := context.WithCancel(nm.ctx)
	var (
		pickNodeID = UniqueID(0)
		nodeMutex  = lock.Mutex{}
		wg         = sync.WaitGroup{}
	)

	for nodeID, client := range nm.nodeClients {
		if _, ok := nm.stoppingNodes[nodeID]; !ok {
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
				if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
					log.Warn("get IndexNode slots failed", zap.Int64("nodeID", nodeID),
						zap.String("reason", resp.GetStatus().GetReason()))
					return
				}
				if resp.GetTaskSlots() > 0 {
					nodeMutex.Lock()
					defer nodeMutex.Unlock()
					if pickNodeID == 0 {
						pickNodeID = nodeID
					}
					cancel()
					// Note: In order to quickly end other goroutines, an error is returned when the client is successfully selected
					return
				}
			}()
		}
	}
	wg.Wait()
	cancel()
	if pickNodeID != 0 {
		log.Info("pick indexNode success", zap.Int64("nodeID", pickNodeID))
		return pickNodeID, nm.nodeClients[pickNodeID]
	}

	return 0, nil
}

func (nm *IndexNodeManager) ClientSupportDisk() bool {
	log.Debug("check if client support disk index")
	allClients := nm.GetAllClients()
	if len(allClients) == 0 {
		log.Warn("there is no IndexNode online")
		return false
	}

	// Note: In order to quickly end other goroutines, an error is returned when the client is successfully selected
	ctx, cancel := context.WithCancel(nm.ctx)
	var (
		enableDisk = false
		nodeMutex  = lock.Mutex{}
		wg         = sync.WaitGroup{}
	)

	for nodeID, client := range allClients {
		nodeID := nodeID
		client := client
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.GetJobStats(ctx, &indexpb.GetJobStatsRequest{})
			if err := merr.CheckRPCCall(resp, err); err != nil {
				log.Warn("get IndexNode slots failed", zap.Int64("nodeID", nodeID), zap.Error(err))
				return
			}
			log.Debug("get job stats success", zap.Int64("nodeID", nodeID), zap.Bool("enable disk", resp.GetEnableDisk()))
			if resp.GetEnableDisk() {
				nodeMutex.Lock()
				defer nodeMutex.Unlock()
				cancel()
				if !enableDisk {
					enableDisk = true
				}
				return
			}
		}()
	}
	wg.Wait()
	cancel()
	if enableDisk {
		log.Info("IndexNode support disk index")
		return true
	}

	log.Error("all IndexNodes do not support disk indexes")
	return false
}

func (nm *IndexNodeManager) GetAllClients() map[UniqueID]types.IndexNodeClient {
	nm.lock.RLock()
	defer nm.lock.RUnlock()

	allClients := make(map[UniqueID]types.IndexNodeClient, len(nm.nodeClients))
	for nodeID, client := range nm.nodeClients {
		if _, ok := nm.stoppingNodes[nodeID]; !ok {
			allClients[nodeID] = client
		}
	}

	return allClients
}

func (nm *IndexNodeManager) GetClientByID(nodeID UniqueID) (types.IndexNodeClient, bool) {
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
func (nm *IndexNodeManager) getMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) []indexNodeGetMetricsResponse {
	var clients []types.IndexNodeClient
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
