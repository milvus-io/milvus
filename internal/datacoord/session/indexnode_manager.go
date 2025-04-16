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

package session

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	datanodeclient "github.com/milvus-io/milvus/internal/distributed/datanode/client"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func defaultIndexNodeCreatorFunc(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
	return datanodeclient.NewClient(ctx, addr, nodeID, paramtable.Get().DataCoordCfg.WithCredential.GetAsBool())
}

type WorkerManager interface {
	AddNode(nodeID typeutil.UniqueID, address string) error
	RemoveNode(nodeID typeutil.UniqueID)
	StoppingNode(nodeID typeutil.UniqueID)
	PickClient() (typeutil.UniqueID, types.DataNodeClient)
	QuerySlots() map[typeutil.UniqueID]*WorkerSlots
	GetAllClients() map[typeutil.UniqueID]types.DataNodeClient
	GetClientByID(nodeID typeutil.UniqueID) (types.DataNodeClient, bool)
}

type WorkerSlots struct {
	NodeID         int64
	TotalSlots     int64
	AvailableSlots int64
}

// IndexNodeManager is used to manage the client of IndexNode.
type IndexNodeManager struct {
	nodeClients      map[typeutil.UniqueID]types.DataNodeClient
	stoppingNodes    map[typeutil.UniqueID]struct{}
	lock             lock.RWMutex
	ctx              context.Context
	indexNodeCreator DataNodeCreatorFunc
}

// NewNodeManager is used to create a new IndexNodeManager.
func NewNodeManager(ctx context.Context, dataNodeCreator DataNodeCreatorFunc) *IndexNodeManager {
	return &IndexNodeManager{
		nodeClients:      make(map[typeutil.UniqueID]types.DataNodeClient),
		stoppingNodes:    make(map[typeutil.UniqueID]struct{}),
		lock:             lock.RWMutex{},
		ctx:              ctx,
		indexNodeCreator: dataNodeCreator,
	}
}

// SetClient sets IndexNode client to node manager.
func (nm *IndexNodeManager) SetClient(nodeID typeutil.UniqueID, client types.DataNodeClient) {
	log := log.Ctx(nm.ctx)
	log.Debug("set IndexNode client", zap.Int64("nodeID", nodeID))
	nm.lock.Lock()
	defer nm.lock.Unlock()
	nm.nodeClients[nodeID] = client
	metrics.IndexNodeNum.WithLabelValues().Set(float64(len(nm.nodeClients)))
	log.Debug("IndexNode IndexNodeManager setClient success", zap.Int64("nodeID", nodeID), zap.Int("IndexNode num", len(nm.nodeClients)))
}

// RemoveNode removes the unused client of IndexNode.
func (nm *IndexNodeManager) RemoveNode(nodeID typeutil.UniqueID) {
	log.Ctx(nm.ctx).Debug("remove IndexNode", zap.Int64("nodeID", nodeID))
	nm.lock.Lock()
	defer nm.lock.Unlock()
	if in, ok := nm.nodeClients[nodeID]; ok {
		if err := in.Close(); err != nil {
			log.Warn("Failed to close client connection", zap.Error(err))
		}
		delete(nm.nodeClients, nodeID)
	}
	delete(nm.stoppingNodes, nodeID)
	metrics.IndexNodeNum.WithLabelValues().Set(float64(len(nm.nodeClients)))
}

func (nm *IndexNodeManager) StoppingNode(nodeID typeutil.UniqueID) {
	log.Ctx(nm.ctx).Debug("IndexCoord", zap.Int64("Stopping node with ID", nodeID))
	nm.lock.Lock()
	defer nm.lock.Unlock()
	nm.stoppingNodes[nodeID] = struct{}{}
}

// AddNode adds the client of IndexNode.
func (nm *IndexNodeManager) AddNode(nodeID typeutil.UniqueID, address string) error {
	log.Ctx(nm.ctx).Debug("add IndexNode", zap.Int64("nodeID", nodeID), zap.String("node address", address))
	var (
		nodeClient types.DataNodeClient
		err        error
	)

	nodeClient, err = nm.indexNodeCreator(context.TODO(), address, nodeID)
	if err != nil {
		log.Ctx(nm.ctx).Error("create IndexNode client fail", zap.Error(err))
		return err
	}

	nm.SetClient(nodeID, nodeClient)
	return nil
}

func (nm *IndexNodeManager) QuerySlots() map[typeutil.UniqueID]*WorkerSlots {
	nm.lock.Lock()
	defer nm.lock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), querySlotTimeout)
	defer cancel()

	nodeSlots := make(map[typeutil.UniqueID]*WorkerSlots)
	mu := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	for nodeID, client := range nm.nodeClients {
		if _, ok := nm.stoppingNodes[nodeID]; !ok {
			wg.Add(1)
			go func(nodeID int64) {
				defer wg.Done()
				resp, err := client.GetJobStats(ctx, &workerpb.GetJobStatsRequest{})
				if err != nil {
					log.Warn("get IndexNode slots failed", zap.Int64("nodeID", nodeID), zap.Error(err))
					return
				}
				if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
					log.Warn("get IndexNode slots failed", zap.Int64("nodeID", nodeID),
						zap.String("reason", resp.GetStatus().GetReason()))
					return
				}
				mu.Lock()
				defer mu.Unlock()
				nodeSlots[nodeID] = &WorkerSlots{
					NodeID:         nodeID,
					TotalSlots:     resp.GetTotalSlots(),
					AvailableSlots: resp.GetAvailableSlots(),
				}
			}(nodeID)
		}
	}
	wg.Wait()
	log.Ctx(context.TODO()).Debug("query slot done", zap.Any("nodeSlots", nodeSlots))
	return nodeSlots
}

func (nm *IndexNodeManager) PickClient() (typeutil.UniqueID, types.DataNodeClient) {
	nm.lock.Lock()
	defer nm.lock.Unlock()

	// Note: In order to quickly end other goroutines, an error is returned when the client is successfully selected
	ctx, cancel := context.WithCancel(nm.ctx)
	var (
		pickNodeID = typeutil.UniqueID(0)
		nodeMutex  = sync.Mutex{}
		wg         = sync.WaitGroup{}
	)

	log := log.Ctx(ctx)
	for nodeID, client := range nm.nodeClients {
		if _, ok := nm.stoppingNodes[nodeID]; !ok {
			nodeID := nodeID
			client := client
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := client.GetJobStats(ctx, &workerpb.GetJobStatsRequest{})
				if err != nil {
					log.Warn("get IndexNode slots failed", zap.Int64("nodeID", nodeID), zap.Error(err))
					return
				}
				if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
					log.Warn("get IndexNode slots failed", zap.Int64("nodeID", nodeID),
						zap.String("reason", resp.GetStatus().GetReason()))
					return
				}
				if resp.GetAvailableSlots() > 0 {
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

func (nm *IndexNodeManager) GetAllClients() map[typeutil.UniqueID]types.DataNodeClient {
	nm.lock.RLock()
	defer nm.lock.RUnlock()

	allClients := make(map[typeutil.UniqueID]types.DataNodeClient, len(nm.nodeClients))
	for nodeID, client := range nm.nodeClients {
		if _, ok := nm.stoppingNodes[nodeID]; !ok {
			allClients[nodeID] = client
		}
	}

	return allClients
}

func (nm *IndexNodeManager) GetClientByID(nodeID typeutil.UniqueID) (types.DataNodeClient, bool) {
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
	var clients []types.DataNodeClient
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
