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

type flowGraphType = int32

const (
	flowGraphTypeCollection = 0
	flowGraphTypePartition  = 1
)

type dataSyncService struct {
	ctx context.Context

	mu                   sync.Mutex                         // guards FlowGraphs
	collectionFlowGraphs map[UniqueID][]*queryNodeFlowGraph // map[collectionID]flowGraphs
	partitionFlowGraphs  map[UniqueID][]*queryNodeFlowGraph // map[partitionID]flowGraphs

	streamingReplica ReplicaInterface
	tSafeReplica     TSafeReplicaInterface
	msFactory        msgstream.Factory
}

// collection flow graph
func (dsService *dataSyncService) addCollectionFlowGraph(collectionID UniqueID, vChannels []string) error {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.collectionFlowGraphs[collectionID]; ok {
		return errors.New("collection flow graph has been existed, collectionID = " + fmt.Sprintln(collectionID))
	}
	dsService.collectionFlowGraphs[collectionID] = make([]*queryNodeFlowGraph, 0)
	for _, vChannel := range vChannels {
		// collection flow graph doesn't need partition id
		partitionID := UniqueID(0)
		newFlowGraph := newQueryNodeFlowGraph(dsService.ctx,
			flowGraphTypeCollection,
			collectionID,
			partitionID,
			dsService.streamingReplica,
			dsService.tSafeReplica,
			vChannel,
			dsService.msFactory)
		dsService.collectionFlowGraphs[collectionID] = append(dsService.collectionFlowGraphs[collectionID], newFlowGraph)
		log.Debug("add collection flow graph",
			zap.Any("collectionID", collectionID),
			zap.Any("channel", vChannel))
	}
	return nil
}

func (dsService *dataSyncService) getCollectionFlowGraphs(collectionID UniqueID) ([]*queryNodeFlowGraph, error) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.collectionFlowGraphs[collectionID]; !ok {
		return nil, errors.New("collection flow graph doesn't existed, collectionID = " + fmt.Sprintln(collectionID))
	}
	return dsService.collectionFlowGraphs[collectionID], nil
}

func (dsService *dataSyncService) startCollectionFlowGraph(collectionID UniqueID) error {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.collectionFlowGraphs[collectionID]; !ok {
		return errors.New("collection flow graph doesn't existed, collectionID = " + fmt.Sprintln(collectionID))
	}
	for _, fg := range dsService.collectionFlowGraphs[collectionID] {
		// start flow graph
		log.Debug("start flow graph", zap.Any("channel", fg.channel))
		go fg.flowGraph.Start()
	}
	return nil
}

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

// partition flow graph
func (dsService *dataSyncService) addPartitionFlowGraph(collectionID UniqueID, partitionID UniqueID, vChannels []string) error {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.partitionFlowGraphs[partitionID]; ok {
		return errors.New("partition flow graph has been existed, partitionID = " + fmt.Sprintln(partitionID))
	}
	dsService.partitionFlowGraphs[partitionID] = make([]*queryNodeFlowGraph, 0)
	for _, vChannel := range vChannels {
		newFlowGraph := newQueryNodeFlowGraph(dsService.ctx,
			flowGraphTypePartition,
			collectionID,
			partitionID,
			dsService.streamingReplica,
			dsService.tSafeReplica,
			vChannel,
			dsService.msFactory)
		dsService.partitionFlowGraphs[partitionID] = append(dsService.partitionFlowGraphs[partitionID], newFlowGraph)
	}
	return nil
}

func (dsService *dataSyncService) getPartitionFlowGraphs(partitionID UniqueID) ([]*queryNodeFlowGraph, error) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.partitionFlowGraphs[partitionID]; !ok {
		return nil, errors.New("partition flow graph doesn't existed, partitionID = " + fmt.Sprintln(partitionID))
	}
	return dsService.partitionFlowGraphs[partitionID], nil
}

func (dsService *dataSyncService) startPartitionFlowGraph(partitionID UniqueID) error {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.partitionFlowGraphs[partitionID]; !ok {
		return errors.New("partition flow graph doesn't existed, partitionID = " + fmt.Sprintln(partitionID))
	}
	for _, fg := range dsService.partitionFlowGraphs[partitionID] {
		// start flow graph
		go fg.flowGraph.Start()
	}
	return nil
}

func (dsService *dataSyncService) removePartitionFlowGraph(partitionID UniqueID) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.partitionFlowGraphs[partitionID]; ok {
		for _, nodeFG := range dsService.partitionFlowGraphs[partitionID] {
			// close flow graph
			nodeFG.close()
		}
		dsService.partitionFlowGraphs[partitionID] = nil
	}
	delete(dsService.partitionFlowGraphs, partitionID)
}

func newDataSyncService(ctx context.Context,
	streamingReplica ReplicaInterface,
	tSafeReplica TSafeReplicaInterface,
	factory msgstream.Factory) *dataSyncService {

	return &dataSyncService{
		ctx:                  ctx,
		collectionFlowGraphs: make(map[UniqueID][]*queryNodeFlowGraph),
		partitionFlowGraphs:  make(map[UniqueID][]*queryNodeFlowGraph),
		streamingReplica:     streamingReplica,
		tSafeReplica:         tSafeReplica,
		msFactory:            factory,
	}
}

func (dsService *dataSyncService) close() {
	for _, nodeFGs := range dsService.collectionFlowGraphs {
		for _, nodeFG := range nodeFGs {
			if nodeFG != nil {
				nodeFG.flowGraph.Close()
			}
		}
	}
	for _, nodeFGs := range dsService.partitionFlowGraphs {
		for _, nodeFG := range nodeFGs {
			if nodeFG != nil {
				nodeFG.flowGraph.Close()
			}
		}
	}
	dsService.collectionFlowGraphs = make(map[UniqueID][]*queryNodeFlowGraph)
	dsService.partitionFlowGraphs = make(map[UniqueID][]*queryNodeFlowGraph)
}
