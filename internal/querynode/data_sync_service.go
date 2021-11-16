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

package querynode

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
)

// loadType is load collection or load partition
type loadType = int32

const (
	loadTypeCollection loadType = 0
	loadTypePartition  loadType = 1
)

// dataSyncService manages a lot of flow graphs for collections and partitions
type dataSyncService struct {
	ctx context.Context

	mu                        sync.Mutex                                   // guards FlowGraphs
	collectionFlowGraphs      map[UniqueID]map[Channel]*queryNodeFlowGraph // map[collectionID]flowGraphs
	collectionDeltaFlowGraphs map[UniqueID]map[Channel]*queryNodeFlowGraph
	partitionFlowGraphs       map[UniqueID]map[Channel]*queryNodeFlowGraph // map[partitionID]flowGraphs

	streamingReplica  ReplicaInterface
	historicalReplica ReplicaInterface
	tSafeReplica      TSafeReplicaInterface
	msFactory         msgstream.Factory
}

// collection flow graph
// addCollectionFlowGraph add a collection flowGraph to collectionFlowGraphs
func (dsService *dataSyncService) addCollectionFlowGraph(collectionID UniqueID, vChannels []string) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.collectionFlowGraphs[collectionID]; !ok {
		dsService.collectionFlowGraphs[collectionID] = make(map[Channel]*queryNodeFlowGraph)
	}
	for _, vChannel := range vChannels {
		// collection flow graph doesn't need partition id
		partitionID := UniqueID(0)
		newFlowGraph := newQueryNodeFlowGraph(dsService.ctx,
			loadTypeCollection,
			collectionID,
			partitionID,
			dsService.streamingReplica,
			dsService.tSafeReplica,
			vChannel,
			dsService.msFactory)
		dsService.collectionFlowGraphs[collectionID][vChannel] = newFlowGraph
		log.Debug("add collection flow graph",
			zap.Any("collectionID", collectionID),
			zap.Any("channel", vChannel))
	}
}

// collection flow graph
// addCollectionFlowGraphDelta add a collection flowGraph to collectionFlowGraphs
func (dsService *dataSyncService) addCollectionDeltaFlowGraph(collectionID UniqueID, vChannels []string) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.collectionDeltaFlowGraphs[collectionID]; !ok {
		dsService.collectionDeltaFlowGraphs[collectionID] = make(map[Channel]*queryNodeFlowGraph)
	}
	for _, vChannel := range vChannels {
		// collection flow graph doesn't need partition id
		partitionID := UniqueID(0)
		newFlowGraph := newQueryNodeDeltaFlowGraph(dsService.ctx,
			loadTypeCollection,
			collectionID,
			partitionID,
			dsService.historicalReplica,
			dsService.tSafeReplica,
			vChannel,
			dsService.msFactory)
		dsService.collectionDeltaFlowGraphs[collectionID][vChannel] = newFlowGraph
		log.Debug("add collection flow graph",
			zap.Any("collectionID", collectionID),
			zap.Any("channel", vChannel))
	}
}

// getCollectionFlowGraphs returns the collection flowGraph by collectionID
func (dsService *dataSyncService) getCollectionFlowGraphs(collectionID UniqueID, vChannels []string) (map[Channel]*queryNodeFlowGraph, error) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.collectionFlowGraphs[collectionID]; !ok {
		return nil, errors.New("collection flow graph doesn't existed, collectionID = " + fmt.Sprintln(collectionID))
	}

	tmpFGs := make(map[Channel]*queryNodeFlowGraph)
	for _, channel := range vChannels {
		if _, ok := dsService.collectionFlowGraphs[collectionID][channel]; ok {
			tmpFGs[channel] = dsService.collectionFlowGraphs[collectionID][channel]
		}
	}

	return tmpFGs, nil
}

// getCollectionDeltaFlowGraphs returns the collection delta flowGraph by collectionID
func (dsService *dataSyncService) getCollectionDeltaFlowGraphs(collectionID UniqueID, vChannels []string) (map[Channel]*queryNodeFlowGraph, error) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.collectionDeltaFlowGraphs[collectionID]; !ok {
		return nil, errors.New("collection flow graph doesn't existed, collectionID = " + fmt.Sprintln(collectionID))
	}

	tmpFGs := make(map[Channel]*queryNodeFlowGraph)
	for _, channel := range vChannels {
		if _, ok := dsService.collectionDeltaFlowGraphs[collectionID][channel]; ok {
			tmpFGs[channel] = dsService.collectionDeltaFlowGraphs[collectionID][channel]
		}
	}

	return tmpFGs, nil
}

// startCollectionFlowGraph starts the collection flow graph by collectionID
func (dsService *dataSyncService) startCollectionFlowGraph(collectionID UniqueID, vChannels []string) error {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.collectionFlowGraphs[collectionID]; !ok {
		return errors.New("collection flow graph doesn't existed, collectionID = " + fmt.Sprintln(collectionID))
	}
	for _, channel := range vChannels {
		if _, ok := dsService.collectionFlowGraphs[collectionID][channel]; ok {
			// start flow graph
			log.Debug("start collection flow graph", zap.Any("channel", channel))
			dsService.collectionFlowGraphs[collectionID][channel].flowGraph.Start()
		}
	}
	return nil
}

// startCollectionDeltaFlowGraph would start the collection delta flow graph by collectionID
func (dsService *dataSyncService) startCollectionDeltaFlowGraph(collectionID UniqueID, vChannels []string) error {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.collectionDeltaFlowGraphs[collectionID]; !ok {
		return errors.New("collection flow graph doesn't existed, collectionID = " + fmt.Sprintln(collectionID))
	}
	for _, channel := range vChannels {
		if _, ok := dsService.collectionDeltaFlowGraphs[collectionID][channel]; ok {
			// start flow graph
			log.Debug("start collection flow graph", zap.Any("channel", channel))
			dsService.collectionDeltaFlowGraphs[collectionID][channel].flowGraph.Start()
		}
	}
	return nil
}

// removeCollectionFlowGraph would remove the collection flow graph by collectionID
func (dsService *dataSyncService) removeCollectionFlowGraph(collectionID UniqueID) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.collectionFlowGraphs[collectionID]; ok {
		for _, nodeFG := range dsService.collectionFlowGraphs[collectionID] {
			// close flow graph
			nodeFG.close()
		}
		dsService.collectionFlowGraphs[collectionID] = nil
	}
	delete(dsService.collectionFlowGraphs, collectionID)
}

// removeCollectionDeltaFlowGraph would remove the collection delta flow graph by collectionID
func (dsService *dataSyncService) removeCollectionDeltaFlowGraph(collectionID UniqueID) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.collectionDeltaFlowGraphs[collectionID]; ok {
		for _, nodeFG := range dsService.collectionDeltaFlowGraphs[collectionID] {
			// close flow graph
			nodeFG.close()
		}
		dsService.collectionDeltaFlowGraphs[collectionID] = nil
	}
	delete(dsService.collectionDeltaFlowGraphs, collectionID)
}

// partition flow graph
// addPartitionFlowGraph adds a partition flow graph to dataSyncService
func (dsService *dataSyncService) addPartitionFlowGraph(collectionID UniqueID, partitionID UniqueID, vChannels []string) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.partitionFlowGraphs[partitionID]; !ok {
		dsService.partitionFlowGraphs[partitionID] = make(map[Channel]*queryNodeFlowGraph)
	}
	for _, vChannel := range vChannels {
		newFlowGraph := newQueryNodeFlowGraph(dsService.ctx,
			loadTypePartition,
			collectionID,
			partitionID,
			dsService.streamingReplica,
			dsService.tSafeReplica,
			vChannel,
			dsService.msFactory)
		dsService.partitionFlowGraphs[partitionID][vChannel] = newFlowGraph
	}
}

// getPartitionFlowGraphs returns the partition flow graph by partitionID
func (dsService *dataSyncService) getPartitionFlowGraphs(partitionID UniqueID, vChannels []string) (map[Channel]*queryNodeFlowGraph, error) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.partitionFlowGraphs[partitionID]; !ok {
		return nil, errors.New("partition flow graph doesn't existed, partitionID = " + fmt.Sprintln(partitionID))
	}

	tmpFGs := make(map[Channel]*queryNodeFlowGraph)
	for _, channel := range vChannels {
		if _, ok := dsService.partitionFlowGraphs[partitionID][channel]; ok {
			tmpFGs[channel] = dsService.partitionFlowGraphs[partitionID][channel]
		}
	}

	return tmpFGs, nil
}

func (dsService *dataSyncService) startPartitionFlowGraph(partitionID UniqueID, vChannels []string) error {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.partitionFlowGraphs[partitionID]; !ok {
		return errors.New("partition flow graph doesn't existed, partitionID = " + fmt.Sprintln(partitionID))
	}
	for _, channel := range vChannels {
		if _, ok := dsService.partitionFlowGraphs[partitionID][channel]; ok {
			// start flow graph
			log.Debug("start partition flow graph", zap.Any("channel", channel))
			dsService.partitionFlowGraphs[partitionID][channel].flowGraph.Start()
		}
	}
	return nil
}

func (dsService *dataSyncService) removePartitionFlowGraph(partitionID UniqueID) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.partitionFlowGraphs[partitionID]; ok {
		for channel, nodeFG := range dsService.partitionFlowGraphs[partitionID] {
			// close flow graph
			nodeFG.close()
			// remove tSafe record
			// no tSafe in tSafeReplica, don't return error
			err := dsService.tSafeReplica.removeRecord(channel, partitionID)
			if err != nil {
				log.Warn(err.Error())
			}
		}
		dsService.partitionFlowGraphs[partitionID] = nil
	}
	delete(dsService.partitionFlowGraphs, partitionID)
}

// newDataSyncService returns a new dataSyncService
func newDataSyncService(ctx context.Context,
	streamingReplica ReplicaInterface,
	historicalReplica ReplicaInterface,
	tSafeReplica TSafeReplicaInterface,
	factory msgstream.Factory) *dataSyncService {

	return &dataSyncService{
		ctx:                       ctx,
		collectionFlowGraphs:      make(map[UniqueID]map[Channel]*queryNodeFlowGraph),
		collectionDeltaFlowGraphs: map[int64]map[string]*queryNodeFlowGraph{},
		partitionFlowGraphs:       make(map[UniqueID]map[Channel]*queryNodeFlowGraph),
		streamingReplica:          streamingReplica,
		historicalReplica:         historicalReplica,
		tSafeReplica:              tSafeReplica,
		msFactory:                 factory,
	}
}

func (dsService *dataSyncService) close() {
	// close collection flow graphs
	for _, nodeFGs := range dsService.collectionFlowGraphs {
		for _, nodeFG := range nodeFGs {
			if nodeFG != nil {
				nodeFG.flowGraph.Close()
			}
		}
	}
	// close partition flow graphs
	for _, nodeFGs := range dsService.partitionFlowGraphs {
		for _, nodeFG := range nodeFGs {
			if nodeFG != nil {
				nodeFG.flowGraph.Close()
			}
		}
	}
	dsService.collectionFlowGraphs = make(map[UniqueID]map[Channel]*queryNodeFlowGraph)
	dsService.partitionFlowGraphs = make(map[UniqueID]map[Channel]*queryNodeFlowGraph)
}
