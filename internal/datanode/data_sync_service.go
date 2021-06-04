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

package datanode

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/flowgraph"

	"go.uber.org/zap"
)

type dataSyncService struct {
	ctx          context.Context
	fg           *flowgraph.TimeTickedFlowGraph
	flushChan    <-chan *flushMsg
	replica      Replica
	idAllocator  allocatorInterface
	msFactory    msgstream.Factory
	collectionID UniqueID
	dataService  types.DataService
}

func newDataSyncService(ctx context.Context,
	flushChan <-chan *flushMsg,
	replica Replica,
	alloc allocatorInterface,
	factory msgstream.Factory,
	vchan *datapb.VchannelInfo) *dataSyncService {

	service := &dataSyncService{
		ctx:          ctx,
		fg:           nil,
		flushChan:    flushChan,
		replica:      replica,
		idAllocator:  alloc,
		msFactory:    factory,
		collectionID: vchan.GetCollectionID(),
	}

	service.initNodes(vchan)
	return service
}

func (dsService *dataSyncService) start() {
	log.Debug("Data Sync Service Start Successfully")
	if dsService.fg != nil {
		dsService.fg.Start()
	} else {
		log.Debug("Data Sync Service flowgraph nil")
	}
}

func (dsService *dataSyncService) close() {
	if dsService.fg != nil {
		dsService.fg.Close()
	}
}

func (dsService *dataSyncService) initNodes(vchanPair *datapb.VchannelInfo) {
	// TODO: add delete pipeline support
	dsService.fg = flowgraph.NewTimeTickedFlowGraph(dsService.ctx)

	m := map[string]interface{}{
		"PulsarAddress":  Params.PulsarAddress,
		"ReceiveBufSize": 1024,
		"PulsarBufSize":  1024,
	}

	err := dsService.msFactory.SetParams(m)
	if err != nil {
		panic(err)
	}

	saveBinlog := func(fu *autoFlushUnit) error {
		id2path := []*datapb.ID2PathList{}
		checkPoints := []*datapb.CheckPoint{}
		for k, v := range fu.field2Path {
			id2path = append(id2path, &datapb.ID2PathList{ID: k, Paths: []string{v}})
		}
		for k, v := range fu.openSegCheckpoints {
			v := v
			checkPoints = append(checkPoints, &datapb.CheckPoint{
				SegmentID: k,
				NumOfRows: fu.numRows[k],
				Position:  &v,
			})
		}

		req := &datapb.SaveBinlogPathsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0, //TOD msg type
				MsgID:     0, //TODO,msg id
				Timestamp: 0, //TODO, time stamp
				SourceID:  Params.NodeID,
			},
			SegmentID:         fu.segID,
			CollectionID:      0, //TODO
			Field2BinlogPaths: id2path,
			CheckPoints:       checkPoints,
			Flushed:           fu.flushed,
		}
		rsp, err := dsService.dataService.SaveBinlogPaths(dsService.ctx, req)
		if err != nil {
			return fmt.Errorf("data service save bin log path failed, err = %w", err)
		}
		if rsp.ErrorCode != commonpb.ErrorCode_Success {
			return fmt.Errorf("data service save bin log path failed, reason = %s", rsp.Reason)
		}
		return nil
	}
	var dmStreamNode Node = newDmInputNode(dsService.ctx, dsService.msFactory, vchanPair.GetChannelName(), vchanPair.GetCheckPoints())
	var ddNode Node = newDDNode()
	var insertBufferNode Node = newInsertBufferNode(
		dsService.ctx,
		dsService.replica,
		dsService.msFactory,
		dsService.idAllocator,
		dsService.flushChan,
		saveBinlog,
	)

	dsService.fg.AddNode(dmStreamNode)
	dsService.fg.AddNode(ddNode)
	dsService.fg.AddNode(insertBufferNode)

	// ddStreamNode
	err = dsService.fg.SetEdges(dmStreamNode.Name(),
		[]string{},
		[]string{ddNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node", zap.String("name", dmStreamNode.Name()), zap.Error(err))
		panic("set edges faild in the node")
	}

	// ddNode
	err = dsService.fg.SetEdges(ddNode.Name(),
		[]string{dmStreamNode.Name()},
		[]string{insertBufferNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node", zap.String("name", ddNode.Name()), zap.Error(err))
		panic("set edges faild in the node")
	}

	// insertBufferNode
	err = dsService.fg.SetEdges(insertBufferNode.Name(),
		[]string{ddNode.Name()},
		[]string{},
	)
	if err != nil {
		log.Error("set edges failed in node", zap.String("name", insertBufferNode.Name()), zap.Error(err))
		panic("set edges faild in the node")
	}
}
