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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

type dataSyncService struct {
	ctx    context.Context
	cancel context.CancelFunc

	collectionID UniqueID
	fg           *flowgraph.TimeTickedFlowGraph

	dmStream  msgstream.MsgStream
	msFactory msgstream.Factory

	replica      ReplicaInterface
	tSafeReplica TSafeReplicaInterface
}

func newDataSyncService(ctx context.Context,
	replica ReplicaInterface,
	tSafeReplica TSafeReplicaInterface,
	factory msgstream.Factory,
	collectionID UniqueID) *dataSyncService {

	ctx1, cancel := context.WithCancel(ctx)

	service := &dataSyncService{
		ctx:          ctx1,
		cancel:       cancel,
		collectionID: collectionID,
		fg:           nil,
		replica:      replica,
		tSafeReplica: tSafeReplica,
		msFactory:    factory,
	}

	service.initNodes()
	return service
}

func (dsService *dataSyncService) start() {
	dsService.fg.Start()
}

func (dsService *dataSyncService) close() {
	dsService.cancel()
	if dsService.fg != nil {
		dsService.fg.Close()
	}
	log.Debug("dataSyncService closed", zap.Int64("collectionID", dsService.collectionID))
}

func (dsService *dataSyncService) initNodes() {
	// TODO: add delete pipeline support

	dsService.fg = flowgraph.NewTimeTickedFlowGraph(dsService.ctx)

	var dmStreamNode node = dsService.newDmInputNode(dsService.ctx)

	var filterDmNode node = newFilteredDmNode(dsService.replica, dsService.collectionID)

	var insertNode node = newInsertNode(dsService.replica, dsService.collectionID)
	var serviceTimeNode node = newServiceTimeNode(dsService.ctx,
		dsService.replica,
		dsService.tSafeReplica,
		dsService.msFactory,
		dsService.collectionID)

	dsService.fg.AddNode(dmStreamNode)

	dsService.fg.AddNode(filterDmNode)

	dsService.fg.AddNode(insertNode)
	dsService.fg.AddNode(serviceTimeNode)

	// dmStreamNode
	var err = dsService.fg.SetEdges(dmStreamNode.Name(),
		[]string{},
		[]string{filterDmNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node:", zap.String("node name", dmStreamNode.Name()))
	}

	// filterDmNode
	err = dsService.fg.SetEdges(filterDmNode.Name(),
		[]string{dmStreamNode.Name()},
		[]string{insertNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node:", zap.String("node name", filterDmNode.Name()))
	}

	// insertNode
	err = dsService.fg.SetEdges(insertNode.Name(),
		[]string{filterDmNode.Name()},
		[]string{serviceTimeNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node:", zap.String("node name", insertNode.Name()))
	}

	// serviceTimeNode
	err = dsService.fg.SetEdges(serviceTimeNode.Name(),
		[]string{insertNode.Name()},
		[]string{},
	)
	if err != nil {
		log.Error("set edges failed in node:", zap.String("node name", serviceTimeNode.Name()))
	}
}

func (dsService *dataSyncService) seekSegment(position *internalpb.MsgPosition) error {
	err := dsService.dmStream.Seek([]*internalpb.MsgPosition{position})
	if err != nil {
		return err
	}
	return nil
}
