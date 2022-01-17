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

package querynode

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
)

// dataSyncService manages a lot of flow graphs
type dataSyncService struct {
	ctx context.Context

	mu                     sync.Mutex // guards FlowGraphs
	dmlChannel2FlowGraph   map[Channel]*queryNodeFlowGraph
	deltaChannel2FlowGraph map[Channel]*queryNodeFlowGraph

	streamingReplica  ReplicaInterface
	historicalReplica ReplicaInterface
	tSafeReplica      TSafeReplicaInterface
	msFactory         msgstream.Factory
}

// addFlowGraphsForDMLChannels add flowGraphs to dmlChannel2FlowGraph
func (dsService *dataSyncService) addFlowGraphsForDMLChannels(collectionID UniqueID, dmlChannels []string) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	for _, channel := range dmlChannels {
		if _, ok := dsService.dmlChannel2FlowGraph[channel]; ok {
			log.Warn("dml flow graph has been existed",
				zap.Any("collectionID", collectionID),
				zap.Any("channel", channel),
			)
			continue
		}
		newFlowGraph := newQueryNodeFlowGraph(dsService.ctx,
			collectionID,
			dsService.streamingReplica,
			dsService.tSafeReplica,
			channel,
			dsService.msFactory)
		dsService.dmlChannel2FlowGraph[channel] = newFlowGraph
		log.Debug("add DML flow graph",
			zap.Any("collectionID", collectionID),
			zap.Any("channel", channel))
	}
}

// addFlowGraphsForDeltaChannels add flowGraphs to deltaChannel2FlowGraph
func (dsService *dataSyncService) addFlowGraphsForDeltaChannels(collectionID UniqueID, deltaChannels []string) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	for _, channel := range deltaChannels {
		if _, ok := dsService.deltaChannel2FlowGraph[channel]; ok {
			log.Warn("delta flow graph has been existed",
				zap.Any("collectionID", collectionID),
				zap.Any("channel", channel),
			)
			continue
		}
		newFlowGraph := newQueryNodeDeltaFlowGraph(dsService.ctx,
			collectionID,
			dsService.historicalReplica,
			dsService.tSafeReplica,
			channel,
			dsService.msFactory)
		dsService.deltaChannel2FlowGraph[channel] = newFlowGraph
		log.Debug("add delta flow graph",
			zap.Any("collectionID", collectionID),
			zap.Any("channel", channel))
	}
}

// getFlowGraphByDMLChannel returns the DML flowGraph by channel
func (dsService *dataSyncService) getFlowGraphByDMLChannel(collectionID UniqueID, channel Channel) (*queryNodeFlowGraph, error) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.dmlChannel2FlowGraph[channel]; !ok {
		return nil, fmt.Errorf("DML flow graph doesn't existed, collectionID = %d", collectionID)
	}

	// TODO: return clone?
	return dsService.dmlChannel2FlowGraph[channel], nil
}

// getFlowGraphByDeltaChannel returns the delta flowGraph by channel
func (dsService *dataSyncService) getFlowGraphByDeltaChannel(collectionID UniqueID, channel Channel) (*queryNodeFlowGraph, error) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.deltaChannel2FlowGraph[channel]; !ok {
		return nil, fmt.Errorf("delta flow graph doesn't existed, collectionID = %d", collectionID)
	}

	// TODO: return clone?
	return dsService.deltaChannel2FlowGraph[channel], nil
}

// startFlowGraphByDMLChannel starts the DML flow graph by channel
func (dsService *dataSyncService) startFlowGraphByDMLChannel(collectionID UniqueID, channel Channel) error {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.dmlChannel2FlowGraph[channel]; !ok {
		return fmt.Errorf("DML flow graph doesn't existed, collectionID = %d", collectionID)
	}
	log.Debug("start DML flow graph",
		zap.Any("collectionID", collectionID),
		zap.Any("channel", channel),
	)
	dsService.dmlChannel2FlowGraph[channel].flowGraph.Start()
	return nil
}

// startFlowGraphForDeltaChannel would start the delta flow graph by channel
func (dsService *dataSyncService) startFlowGraphForDeltaChannel(collectionID UniqueID, channel Channel) error {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.deltaChannel2FlowGraph[channel]; !ok {
		return fmt.Errorf("delta flow graph doesn't existed, collectionID = %d", collectionID)
	}
	log.Debug("start delta flow graph",
		zap.Any("collectionID", collectionID),
		zap.Any("channel", channel),
	)
	dsService.deltaChannel2FlowGraph[channel].flowGraph.Start()
	return nil
}

// removeFlowGraphsByDMLChannels would remove the DML flow graphs by channels
func (dsService *dataSyncService) removeFlowGraphsByDMLChannels(channels []Channel) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	for _, channel := range channels {
		if _, ok := dsService.dmlChannel2FlowGraph[channel]; ok {
			// close flow graph
			dsService.dmlChannel2FlowGraph[channel].close()
		}
		delete(dsService.dmlChannel2FlowGraph, channel)
	}
}

// removeFlowGraphsByDeltaChannels would remove the delta flow graphs by channels
func (dsService *dataSyncService) removeFlowGraphsByDeltaChannels(channels []Channel) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	for _, channel := range channels {
		if _, ok := dsService.deltaChannel2FlowGraph[channel]; ok {
			// close flow graph
			dsService.deltaChannel2FlowGraph[channel].close()
		}
		delete(dsService.deltaChannel2FlowGraph, channel)
	}
}

// newDataSyncService returns a new dataSyncService
func newDataSyncService(ctx context.Context,
	streamingReplica ReplicaInterface,
	historicalReplica ReplicaInterface,
	tSafeReplica TSafeReplicaInterface,
	factory msgstream.Factory) *dataSyncService {

	return &dataSyncService{
		ctx:                    ctx,
		dmlChannel2FlowGraph:   make(map[Channel]*queryNodeFlowGraph),
		deltaChannel2FlowGraph: make(map[Channel]*queryNodeFlowGraph),
		streamingReplica:       streamingReplica,
		historicalReplica:      historicalReplica,
		tSafeReplica:           tSafeReplica,
		msFactory:              factory,
	}
}

// close would close and remove all flow graphs in dataSyncService
func (dsService *dataSyncService) close() {
	// close DML flow graphs
	for channel, nodeFG := range dsService.dmlChannel2FlowGraph {
		if nodeFG != nil {
			nodeFG.flowGraph.Close()
		}
		delete(dsService.dmlChannel2FlowGraph, channel)
	}
	// close delta flow graphs
	for channel, nodeFG := range dsService.deltaChannel2FlowGraph {
		if nodeFG != nil {
			nodeFG.flowGraph.Close()
		}
		delete(dsService.deltaChannel2FlowGraph, channel)
	}
}
