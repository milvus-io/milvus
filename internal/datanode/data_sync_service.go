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
	"errors"
	"fmt"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/flowgraph"

	"go.uber.org/zap"
)

// dataSyncService controls a flowgraph for a specific collection
type dataSyncService struct {
	ctx          context.Context
	cancelFn     context.CancelFunc
	fg           *flowgraph.TimeTickedFlowGraph
	flushChan    <-chan *flushMsg
	replica      Replica
	idAllocator  allocatorInterface
	msFactory    msgstream.Factory
	collectionID UniqueID
	dataCoord    types.DataCoord
	clearSignal  chan<- UniqueID

	saveBinlog func(fu *segmentFlushUnit) error
}

func newDataSyncService(ctx context.Context,
	flushChan <-chan *flushMsg,
	replica Replica,
	alloc allocatorInterface,
	factory msgstream.Factory,
	vchan *datapb.VchannelInfo,
	clearSignal chan<- UniqueID,
	dataCoord types.DataCoord,

) (*dataSyncService, error) {

	if replica == nil {
		return nil, errors.New("Nil input")
	}

	ctx1, cancel := context.WithCancel(ctx)

	service := &dataSyncService{
		ctx:          ctx1,
		cancelFn:     cancel,
		fg:           nil,
		flushChan:    flushChan,
		replica:      replica,
		idAllocator:  alloc,
		msFactory:    factory,
		collectionID: vchan.GetCollectionID(),
		dataCoord:    dataCoord,
		clearSignal:  clearSignal,
	}

	if err := service.initNodes(vchan); err != nil {
		return nil, err
	}
	return service, nil
}

// start starts the flowgraph in datasyncservice
func (dsService *dataSyncService) start() {
	if dsService.fg != nil {
		log.Debug("Data Sync Service starting flowgraph")
		dsService.fg.Start()
	} else {
		log.Debug("Data Sync Service flowgraph nil")
	}
}

func (dsService *dataSyncService) close() {
	if dsService.fg != nil {
		log.Debug("Data Sync Service closing flowgraph")
		dsService.fg.Close()
	}

	dsService.cancelFn()
}

// initNodes inits a TimetickedFlowGraph
func (dsService *dataSyncService) initNodes(vchanInfo *datapb.VchannelInfo) error {
	// TODO: add delete pipeline support
	dsService.fg = flowgraph.NewTimeTickedFlowGraph(dsService.ctx)

	m := map[string]interface{}{
		"PulsarAddress":  Params.PulsarAddress,
		"ReceiveBufSize": 1024,
		"PulsarBufSize":  1024,
	}

	err := dsService.msFactory.SetParams(m)
	if err != nil {
		return err
	}

	saveBinlog := func(fu *segmentFlushUnit) error {
		id2path := []*datapb.FieldBinlog{}
		checkPoints := []*datapb.CheckPoint{}
		for k, v := range fu.field2Path {
			id2path = append(id2path, &datapb.FieldBinlog{FieldID: k, Binlogs: []string{v}})
		}
		for k, v := range fu.checkPoint {
			v := v
			checkPoints = append(checkPoints, &datapb.CheckPoint{
				SegmentID: k,
				NumOfRows: v.numRows,
				Position:  &v.pos,
			})
		}
		log.Debug("SaveBinlogPath",
			zap.Int64("SegmentID", fu.segID),
			zap.Int64("CollectionID", fu.collID),
			zap.Int("Length of Field2BinlogPaths", len(id2path)),
		)

		req := &datapb.SaveBinlogPathsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0, //TODO msg type
				MsgID:     0, //TODO msg id
				Timestamp: 0, //TODO time stamp
				SourceID:  Params.NodeID,
			},
			SegmentID:         fu.segID,
			CollectionID:      fu.collID,
			Field2BinlogPaths: id2path,
			CheckPoints:       checkPoints,
			StartPositions:    fu.startPositions,
			Flushed:           fu.flushed,
		}
		rsp, err := dsService.dataCoord.SaveBinlogPaths(dsService.ctx, req)
		if err != nil {
			return fmt.Errorf(err.Error())
		}
		if rsp.ErrorCode != commonpb.ErrorCode_Success {
			return fmt.Errorf("data service save bin log path failed, reason = %s", rsp.Reason)
		}
		return nil
	}

	dsService.saveBinlog = saveBinlog

	var dmStreamNode Node = newDmInputNode(
		dsService.ctx,
		dsService.msFactory,
		vchanInfo.CollectionID,
		vchanInfo.GetChannelName(),
		vchanInfo.GetSeekPosition(),
	)
	var ddNode Node = newDDNode(dsService.clearSignal, dsService.collectionID, vchanInfo)
	var insertBufferNode Node
	insertBufferNode, err = newInsertBufferNode(
		dsService.ctx,
		dsService.replica,
		dsService.msFactory,
		dsService.idAllocator,
		dsService.flushChan,
		saveBinlog,
		vchanInfo.GetChannelName(),
	)
	if err != nil {
		return err
	}

	dn := newDeleteDNode(dsService.replica)

	var deleteNode Node = dn

	// recover segment checkpoints
	for _, us := range vchanInfo.GetUnflushedSegments() {
		if us.CollectionID != dsService.collectionID ||
			us.GetInsertChannel() != vchanInfo.ChannelName {
			log.Warn("Collection ID or ChannelName not compact",
				zap.Int64("Wanted ID", dsService.collectionID),
				zap.Int64("Actual ID", us.CollectionID),
				zap.String("Wanted Channel Name", vchanInfo.ChannelName),
				zap.String("Actual Channel Name", us.GetInsertChannel()),
			)
			continue
		}

		log.Info("Recover Segment NumOfRows form checkpoints",
			zap.String("InsertChannel", us.GetInsertChannel()),
			zap.Int64("SegmentID", us.GetID()),
			zap.Int64("NumOfRows", us.GetNumOfRows()),
		)

		dsService.replica.addNormalSegment(us.GetID(), us.CollectionID, us.PartitionID, us.GetInsertChannel(),
			us.GetNumOfRows(), &segmentCheckPoint{us.GetNumOfRows(), *us.GetDmlPosition()})
	}

	dsService.fg.AddNode(dmStreamNode)
	dsService.fg.AddNode(ddNode)
	dsService.fg.AddNode(insertBufferNode)
	dsService.fg.AddNode(deleteNode)

	// ddStreamNode
	err = dsService.fg.SetEdges(dmStreamNode.Name(),
		[]string{},
		[]string{ddNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node", zap.String("name", dmStreamNode.Name()), zap.Error(err))
		return err
	}

	// ddNode
	err = dsService.fg.SetEdges(ddNode.Name(),
		[]string{dmStreamNode.Name()},
		[]string{insertBufferNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node", zap.String("name", ddNode.Name()), zap.Error(err))
		return err
	}

	// insertBufferNode
	err = dsService.fg.SetEdges(insertBufferNode.Name(),
		[]string{ddNode.Name()},
		[]string{deleteNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node", zap.String("name", insertBufferNode.Name()), zap.Error(err))
		return err
	}

	//deleteNode
	err = dsService.fg.SetEdges(deleteNode.Name(),
		[]string{insertBufferNode.Name()},
		[]string{},
	)
	if err != nil {
		log.Error("set edges failed in node", zap.String("name", deleteNode.Name()), zap.Error(err))
		return err
	}
	return nil
}
