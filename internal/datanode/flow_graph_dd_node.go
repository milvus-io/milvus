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

package datanode

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"

	"github.com/golang/protobuf/proto"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// make sure ddNode implements flowgraph.Node
var _ flowgraph.Node = (*ddNode)(nil)

// ddNode filters messages from message streams.
//
// ddNode recives all the messages from message stream dml channels, including insert messages,
//
//	delete messages and ddl messages like CreateCollectionMsg and DropCollectionMsg.
//
// ddNode filters insert messages according to the `sealedSegment`.
// ddNode will filter out the insert message for those who belong to `sealedSegment`
//
// When receiving a `DropCollection` message, ddNode will send a signal to DataNode `BackgroundGC`
//
//	goroutinue, telling DataNode to release the resources of this particular flow graph.
//
// After the filtering process, ddNode passes all the valid insert messages and delete message
//
//	to the following flow graph node, which in DataNode is `insertBufferNode`
type ddNode struct {
	BaseNode

	ctx          context.Context
	collectionID UniqueID
	vChannelName string

	dropMode           atomic.Value
	compactionExecutor *compactionExecutor

	// for recovery
	growingSegInfo    map[UniqueID]*datapb.SegmentInfo // segmentID
	sealedSegInfo     map[UniqueID]*datapb.SegmentInfo // segmentID
	droppedSegmentIDs []int64
}

// Name returns node name, implementing flowgraph.Node
func (ddn *ddNode) Name() string {
	return fmt.Sprintf("ddNode-%d-%s", ddn.collectionID, ddn.vChannelName)
}

func (ddn *ddNode) IsValidInMsg(in []Msg) bool {
	if !ddn.BaseNode.IsValidInMsg(in) {
		return false
	}
	_, ok := in[0].(*MsgStreamMsg)
	if !ok {
		log.Warn("type assertion failed for MsgStreamMsg", zap.String("name", reflect.TypeOf(in[0]).Name()))
		return false
	}
	return true
}

// Operate handles input messages, implementing flowgrpah.Node
func (ddn *ddNode) Operate(in []Msg) []Msg {
	msMsg, ok := in[0].(*MsgStreamMsg)
	if !ok {
		log.Warn("type assertion failed for MsgStreamMsg", zap.String("name", reflect.TypeOf(in[0]).Name()))
		return []Msg{}
	}

	if msMsg.IsCloseMsg() {
		var fgMsg = flowGraphMsg{
			BaseMsg:        flowgraph.NewBaseMsg(true),
			insertMessages: make([]*msgstream.InsertMsg, 0),
			timeRange: TimeRange{
				timestampMin: msMsg.TimestampMin(),
				timestampMax: msMsg.TimestampMax(),
			},
			startPositions: msMsg.StartPositions(),
			endPositions:   msMsg.EndPositions(),
			dropCollection: false,
		}
		log.Warn("MsgStream closed", zap.Any("ddNode node", ddn.Name()), zap.Int64("collection", ddn.collectionID), zap.String("channel", ddn.vChannelName))
		return []Msg{&fgMsg}
	}

	if load := ddn.dropMode.Load(); load != nil && load.(bool) {
		log.Info("ddNode in dropMode",
			zap.String("vChannelName", ddn.vChannelName),
			zap.Int64("collectionID", ddn.collectionID))
		return []Msg{}
	}

	var spans []trace.Span
	for _, msg := range msMsg.TsMessages() {
		ctx, sp := startTracer(msg, "DDNode-Operate")
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}
	defer func() {
		for _, sp := range spans {
			sp.End()
		}
	}()

	var fgMsg = flowGraphMsg{
		insertMessages: make([]*msgstream.InsertMsg, 0),
		timeRange: TimeRange{
			timestampMin: msMsg.TimestampMin(),
			timestampMax: msMsg.TimestampMax(),
		},
		startPositions: make([]*msgpb.MsgPosition, 0),
		endPositions:   make([]*msgpb.MsgPosition, 0),
		dropCollection: false,
	}

	for _, msg := range msMsg.TsMessages() {
		switch msg.Type() {
		case commonpb.MsgType_DropCollection:
			if msg.(*msgstream.DropCollectionMsg).GetCollectionID() == ddn.collectionID {
				log.Info("Receiving DropCollection msg",
					zap.Any("collectionID", ddn.collectionID),
					zap.String("vChannelName", ddn.vChannelName))
				ddn.dropMode.Store(true)

				log.Info("Stop compaction of vChannel", zap.String("vChannelName", ddn.vChannelName))
				ddn.compactionExecutor.stopExecutingtaskByVChannelName(ddn.vChannelName)
				fgMsg.dropCollection = true

				pChan := funcutil.ToPhysicalChannel(ddn.vChannelName)
				metrics.CleanupDataNodeCollectionMetrics(paramtable.GetNodeID(), ddn.collectionID, pChan)
			}

		case commonpb.MsgType_DropPartition:
			dpMsg := msg.(*msgstream.DropPartitionMsg)
			if dpMsg.GetCollectionID() == ddn.collectionID {
				log.Info("drop partition msg received",
					zap.Int64("collectionID", dpMsg.GetCollectionID()),
					zap.Int64("partitionID", dpMsg.GetPartitionID()),
					zap.String("vChanneName", ddn.vChannelName))
				fgMsg.dropPartitions = append(fgMsg.dropPartitions, dpMsg.PartitionID)
			}

		case commonpb.MsgType_Insert:
			imsg := msg.(*msgstream.InsertMsg)
			if imsg.CollectionID != ddn.collectionID {
				log.Warn("filter invalid insert message, collection mis-match",
					zap.Int64("Get collID", imsg.CollectionID),
					zap.Int64("Expected collID", ddn.collectionID))
				continue
			}

			if ddn.tryToFilterSegmentInsertMessages(imsg) {
				log.Info("filter insert messages",
					zap.Int64("filter segmentID", imsg.GetSegmentID()),
					zap.Uint64("message timestamp", msg.EndTs()),
					zap.String("segment's vChannel", imsg.GetShardName()),
					zap.String("current vChannel", ddn.vChannelName))
				continue
			}

			rateCol.Add(metricsinfo.InsertConsumeThroughput, float64(proto.Size(&imsg.InsertRequest)))

			metrics.DataNodeConsumeBytesCount.
				WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.InsertLabel).
				Add(float64(proto.Size(&imsg.InsertRequest)))

			metrics.DataNodeConsumeMsgCount.
				WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.InsertLabel, fmt.Sprint(ddn.collectionID)).
				Inc()

			log.Debug("DDNode receive insert messages",
				zap.Int("numRows", len(imsg.GetRowIDs())),
				zap.String("vChannelName", ddn.vChannelName))
			fgMsg.insertMessages = append(fgMsg.insertMessages, imsg)

		case commonpb.MsgType_Delete:
			dmsg := msg.(*msgstream.DeleteMsg)
			log.Debug("DDNode receive delete messages",
				zap.Int64("numRows", dmsg.NumRows),
				zap.String("vChannelName", ddn.vChannelName))
			for i := int64(0); i < dmsg.NumRows; i++ {
				dmsg.HashValues = append(dmsg.HashValues, uint32(0))
			}
			if dmsg.CollectionID != ddn.collectionID {
				log.Warn("filter invalid DeleteMsg, collection mis-match",
					zap.Int64("Get collID", dmsg.CollectionID),
					zap.Int64("Expected collID", ddn.collectionID))
				continue
			}
			rateCol.Add(metricsinfo.DeleteConsumeThroughput, float64(proto.Size(&dmsg.DeleteRequest)))

			metrics.DataNodeConsumeBytesCount.
				WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.DeleteLabel).
				Add(float64(proto.Size(&dmsg.DeleteRequest)))

			metrics.DataNodeConsumeMsgCount.
				WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.DeleteLabel, fmt.Sprint(ddn.collectionID)).
				Inc()
			fgMsg.deleteMessages = append(fgMsg.deleteMessages, dmsg)
		}
	}

	fgMsg.startPositions = append(fgMsg.startPositions, msMsg.StartPositions()...)
	fgMsg.endPositions = append(fgMsg.endPositions, msMsg.EndPositions()...)

	return []Msg{&fgMsg}
}

func (ddn *ddNode) tryToFilterSegmentInsertMessages(msg *msgstream.InsertMsg) bool {
	if msg.GetShardName() != ddn.vChannelName {
		return true
	}

	// Filter all dropped segments
	if ddn.isDropped(msg.GetSegmentID()) {
		return true
	}

	// Filter all sealed segments until current Ts > Sealed segment cp
	for segID, segInfo := range ddn.sealedSegInfo {
		if msg.EndTs() > segInfo.GetDmlPosition().GetTimestamp() {
			delete(ddn.sealedSegInfo, segID)
		}
	}
	if _, ok := ddn.sealedSegInfo[msg.GetSegmentID()]; ok {
		return true
	}

	// Filter all growing segments until current Ts > growing segment dmlPosition
	if si, ok := ddn.growingSegInfo[msg.GetSegmentID()]; ok {
		if msg.EndTs() <= si.GetDmlPosition().GetTimestamp() {
			return true
		}

		delete(ddn.growingSegInfo, msg.GetSegmentID())
	}

	return false
}

func (ddn *ddNode) isDropped(segID UniqueID) bool {
	for _, droppedSegmentID := range ddn.droppedSegmentIDs {
		if droppedSegmentID == segID {
			return true
		}
	}
	return false
}

func (ddn *ddNode) Close() {}

func newDDNode(ctx context.Context, collID UniqueID, vChannelName string, droppedSegmentIDs []UniqueID,
	sealedSegments []*datapb.SegmentInfo, growingSegments []*datapb.SegmentInfo, compactor *compactionExecutor) (*ddNode, error) {

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(Params.DataNodeCfg.FlowGraphMaxQueueLength.GetAsInt32())
	baseNode.SetMaxParallelism(Params.DataNodeCfg.FlowGraphMaxParallelism.GetAsInt32())

	dd := &ddNode{
		ctx:                ctx,
		BaseNode:           baseNode,
		collectionID:       collID,
		sealedSegInfo:      make(map[UniqueID]*datapb.SegmentInfo, len(sealedSegments)),
		growingSegInfo:     make(map[UniqueID]*datapb.SegmentInfo, len(growingSegments)),
		droppedSegmentIDs:  droppedSegmentIDs,
		vChannelName:       vChannelName,
		compactionExecutor: compactor,
	}

	dd.dropMode.Store(false)

	for _, s := range sealedSegments {
		dd.sealedSegInfo[s.GetID()] = s
	}

	for _, s := range growingSegments {
		dd.growingSegInfo[s.GetID()] = s
	}
	log.Info("ddNode add sealed and growing segments",
		zap.Int64("collectionID", collID),
		zap.Int("No. sealed segments", len(sealedSegments)),
		zap.Int("No. growing segments", len(growingSegments)),
	)

	return dd, nil
}
