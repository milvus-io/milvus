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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// NodeManager defines the interface for managing DataNode clients in the cluster
type NodeManager interface {
	// AddNode adds a new DataNode to the cluster with the given nodeID and address
	AddNode(nodeID int64, address string) error
	// RemoveNode removes a DataNode from the cluster by its nodeID
	RemoveNode(nodeID int64)
	// GetClient returns the DataNode client for the given nodeID
	GetClient(nodeID int64) (types.DataNodeClient, error)
	// GetClientIDs returns a list of all DataNode IDs in the cluster
	GetClientIDs() []int64
}

var _ NodeManager = (*nodeManager)(nil)

// nodeManager implements the NodeManager interface
type nodeManager struct {
	mu          lock.RWMutex
	nodeClients map[int64]types.DataNodeClient
	nodeCreator DataNodeCreatorFunc
}

// NewNodeManager creates a new instance of nodeManager
func NewNodeManager(nodeCreator DataNodeCreatorFunc) NodeManager {
	c := &nodeManager{
		nodeClients: make(map[int64]types.DataNodeClient),
		nodeCreator: nodeCreator,
	}
	return c
}

func (m *nodeManager) AddNode(nodeID int64, address string) error {
	log := log.Ctx(context.Background()).With(zap.Int64("nodeID", nodeID), zap.String("address", address))
	log.Info("adding node...")
	nodeClient, err := m.nodeCreator(context.Background(), address, nodeID)
	if err != nil {
		log.Error("create client fail", zap.Error(err))
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodeClients[nodeID] = nodeClient
	numNodes := len(m.nodeClients)
	metrics.IndexNodeNum.WithLabelValues().Set(float64(numNodes))
	metrics.DataCoordNumDataNodes.WithLabelValues().Set(float64(numNodes))
	log.Info("node added", zap.Int("numNodes", numNodes))
	return nil
}

func (m *nodeManager) RemoveNode(nodeID int64) {
	log := log.Ctx(context.Background()).With(zap.Int64("nodeID", nodeID))
	log.Info("removing node...")
	m.mu.Lock()
	defer m.mu.Unlock()
	if client, ok := m.nodeClients[nodeID]; ok {
		if err := client.Close(); err != nil {
			log.Warn("failed to close client", zap.Error(err))
		}
		delete(m.nodeClients, nodeID)
		numNodes := len(m.nodeClients)
		metrics.IndexNodeNum.WithLabelValues().Set(float64(numNodes))
		metrics.DataCoordNumDataNodes.WithLabelValues().Set(float64(numNodes))
		log.Info("node removed", zap.Int("numNodes", numNodes))
	}
}

func (m *nodeManager) GetClient(nodeID int64) (types.DataNodeClient, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	client, ok := m.nodeClients[nodeID]
	if !ok {
		return nil, merr.WrapErrNodeNotFound(nodeID)
	}
	return client, nil
}

func (m *nodeManager) GetClientIDs() []int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return lo.Keys(m.nodeClients)
}
