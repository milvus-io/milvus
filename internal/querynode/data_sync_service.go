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
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
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

// checkReplica used to check replica info before init flow graph, it's a private method of dataSyncService
func (dsService *dataSyncService) checkReplica(collectionID UniqueID) error {
	// check if the collection exists
	hisColl, err := dsService.historicalReplica.getCollectionByID(collectionID)
	if err != nil {
		return err
	}
	strColl, err := dsService.streamingReplica.getCollectionByID(collectionID)
	if err != nil {
		return err
	}
	if hisColl.getLoadType() != strColl.getLoadType() {
		return fmt.Errorf("inconsistent loadType of collection, collectionID = %d", collectionID)
	}
	for _, channel := range hisColl.getVChannels() {
		if _, err := dsService.tSafeReplica.getTSafe(channel); err != nil {
			return fmt.Errorf("getTSafe failed, err = %s", err)
		}
	}
	for _, channel := range hisColl.getVDeltaChannels() {
		if _, err := dsService.tSafeReplica.getTSafe(channel); err != nil {
			return fmt.Errorf("getTSafe failed, err = %s", err)
		}
	}
	return nil
}

// addFlowGraphsForDMLChannels add flowGraphs to dmlChannel2FlowGraph
func (dsService *dataSyncService) addFlowGraphsForDMLChannels(collectionID UniqueID, dmlChannels []string) (map[string]*queryNodeFlowGraph, error) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if err := dsService.checkReplica(collectionID); err != nil {
		return nil, err
	}

	results := make(map[string]*queryNodeFlowGraph)
	for _, channel := range dmlChannels {
		if _, ok := dsService.dmlChannel2FlowGraph[channel]; ok {
			log.Warn("dml flow graph has been existed",
				zap.Any("collectionID", collectionID),
				zap.Any("channel", channel),
			)
			continue
		}
		newFlowGraph, err := newQueryNodeFlowGraph(dsService.ctx,
			collectionID,
			dsService.streamingReplica,
			dsService.tSafeReplica,
			channel,
			dsService.msFactory)
		if err != nil {
			for _, fg := range results {
				fg.flowGraph.Close()
			}
			return nil, err
		}
		results[channel] = newFlowGraph
	}

	for channel, fg := range results {
		dsService.dmlChannel2FlowGraph[channel] = fg
		log.Info("add DML flow graph",
			zap.Any("collectionID", collectionID),
			zap.Any("channel", channel))
		metrics.QueryNodeNumFlowGraphs.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Inc()
	}

	return results, nil
}

// addFlowGraphsForDeltaChannels add flowGraphs to deltaChannel2FlowGraph
func (dsService *dataSyncService) addFlowGraphsForDeltaChannels(collectionID UniqueID, deltaChannels []string) (map[string]*queryNodeFlowGraph, error) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if err := dsService.checkReplica(collectionID); err != nil {
		return nil, err
	}

	results := make(map[string]*queryNodeFlowGraph)
	for _, channel := range deltaChannels {
		if _, ok := dsService.deltaChannel2FlowGraph[channel]; ok {
			log.Warn("delta flow graph has been existed",
				zap.Any("collectionID", collectionID),
				zap.Any("channel", channel),
			)
			continue
		}
		newFlowGraph, err := newQueryNodeDeltaFlowGraph(dsService.ctx,
			collectionID,
			dsService.historicalReplica,
			dsService.tSafeReplica,
			channel,
			dsService.msFactory)
		if err != nil {
			for _, fg := range results {
				fg.flowGraph.Close()
			}
			return nil, err
		}
		results[channel] = newFlowGraph
	}

	for channel, fg := range results {
		dsService.deltaChannel2FlowGraph[channel] = fg
		log.Info("add delta flow graph",
			zap.Any("collectionID", collectionID),
			zap.Any("channel", channel))
		metrics.QueryNodeNumFlowGraphs.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Inc()
	}

	return results, nil
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
	log.Info("start DML flow graph",
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
	log.Info("start delta flow graph",
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
			metrics.QueryNodeNumFlowGraphs.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Dec()
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
			metrics.QueryNodeNumFlowGraphs.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Dec()
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
