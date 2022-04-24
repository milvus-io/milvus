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
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
)

// make sure ddNode implements flowgraph.Node
var _ flowgraph.Node = (*ddNode)(nil)

// ddNode filters messages from message streams.
//
// ddNode recives all the messages from message stream dml channels, including insert messages,
//  delete messages and ddl messages like CreateCollectionMsg.
//
// ddNode filters insert messages according to the `flushedSegment` and `FilterThreshold`.
//  If the timestamp of the insert message is earlier than `FilterThreshold`, ddNode will
//  filter out the insert message for those who belong to `flushedSegment`
//
// When receiving a `DropCollection` message, ddNode will send a signal to DataNode `BackgroundGC`
//  goroutinue, telling DataNode to release the resources of this perticular flow graph.
//
// After the filtering process, ddNode passes all the valid insert messages and delete message
//  to the following flow graph node, which in DataNode is `insertBufferNode`
type ddNode struct {
	BaseNode

	collectionID UniqueID

	segID2SegInfo   sync.Map // segment ID to *SegmentInfo
	flushedSegments []*datapb.SegmentInfo
	droppedSegments []*datapb.SegmentInfo
	vchannelName    string

	deltaMsgStream     msgstream.MsgStream
	dropMode           atomic.Value
	compactionExecutor *compactionExecutor
}

// Name returns node name, implementing flowgraph.Node
func (ddn *ddNode) Name() string {
	return fmt.Sprintf("ddNode-%d-%s", ddn.collectionID, ddn.vchannelName)
}

// Operate handles input messages, implementing flowgrpah.Node
func (ddn *ddNode) Operate(in []Msg) []Msg {
	if len(in) != 1 {
		log.Warn("Invalid operate message input in ddNode", zap.Int("input length", len(in)))
		return []Msg{}
	}

	msMsg, ok := in[0].(*MsgStreamMsg)
	if !ok {
		log.Warn("Type assertion failed for MsgStreamMsg")
		return []Msg{}
	}

	var spans []opentracing.Span
	for _, msg := range msMsg.TsMessages() {
		sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}

	if load := ddn.dropMode.Load(); load != nil && load.(bool) {
		log.Debug("ddNode in dropMode",
			zap.String("vchannel name", ddn.vchannelName),
			zap.Int64("collection ID", ddn.collectionID))
		return []Msg{}
	}

	var fgMsg = flowGraphMsg{
		insertMessages: make([]*msgstream.InsertMsg, 0),
		timeRange: TimeRange{
			timestampMin: msMsg.TimestampMin(),
			timestampMax: msMsg.TimestampMax(),
		},
		startPositions: make([]*internalpb.MsgPosition, 0),
		endPositions:   make([]*internalpb.MsgPosition, 0),
		dropCollection: false,
	}

	var forwardMsgs []msgstream.TsMsg
	for _, msg := range msMsg.TsMessages() {
		switch msg.Type() {
		case commonpb.MsgType_DropCollection:
			if msg.(*msgstream.DropCollectionMsg).GetCollectionID() == ddn.collectionID {
				log.Info("Receiving DropCollection msg",
					zap.Any("collectionID", ddn.collectionID),
					zap.String("vChannelName", ddn.vchannelName))
				ddn.dropMode.Store(true)

				log.Info("Stop compaction of vChannel", zap.String("vChannelName", ddn.vchannelName))
				ddn.compactionExecutor.stopExecutingtaskByVChannelName(ddn.vchannelName)
				fgMsg.dropCollection = true
			}
		case commonpb.MsgType_Insert:
			imsg := msg.(*msgstream.InsertMsg)
			if imsg.CollectionID != ddn.collectionID {
				log.Warn("filter invalid InsertMsg, collection mis-match",
					zap.Int64("Get collID", imsg.CollectionID),
					zap.Int64("Expected collID", ddn.collectionID))
				continue
			}
			if msg.EndTs() < FilterThreshold {
				log.Info("Filtering Insert Messages",
					zap.Uint64("Message endts", msg.EndTs()),
					zap.Uint64("FilterThreshold", FilterThreshold),
				)
				if ddn.filterFlushedSegmentInsertMessages(imsg) {
					continue
				}
			}
			log.Debug("DDNode receive insert messages",
				zap.Int("numRows", len(imsg.GetRowIDs())),
				zap.String("vChannelName", ddn.vchannelName))
			fgMsg.insertMessages = append(fgMsg.insertMessages, imsg)
		case commonpb.MsgType_Delete:
			dmsg := msg.(*msgstream.DeleteMsg)
			log.Debug("DDNode receive delete messages",
				zap.Int64("num", dmsg.NumRows),
				zap.String("vChannelName", ddn.vchannelName))
			for i := int64(0); i < dmsg.NumRows; i++ {
				dmsg.HashValues = append(dmsg.HashValues, uint32(0))
			}
			forwardMsgs = append(forwardMsgs, dmsg)
			if dmsg.CollectionID != ddn.collectionID {
				log.Warn("filter invalid DeleteMsg, collection mis-match",
					zap.Int64("Get collID", dmsg.CollectionID),
					zap.Int64("Expected collID", ddn.collectionID))
				continue
			}
			fgMsg.deleteMessages = append(fgMsg.deleteMessages, dmsg)
		}
	}
	err := ddn.forwardDeleteMsg(forwardMsgs, msMsg.TimestampMin(), msMsg.TimestampMax())
	if err != nil {
		// TODO: proper deal with error
		log.Warn("DDNode forward delete msg failed",
			zap.String("vChannelName", ddn.vchannelName),
			zap.Error(err))
	}

	fgMsg.startPositions = append(fgMsg.startPositions, msMsg.StartPositions()...)
	fgMsg.endPositions = append(fgMsg.endPositions, msMsg.EndPositions()...)

	for _, sp := range spans {
		sp.Finish()
	}

	return []Msg{&fgMsg}
}

func (ddn *ddNode) filterFlushedSegmentInsertMessages(msg *msgstream.InsertMsg) bool {
	if ddn.isFlushed(msg.GetSegmentID()) || ddn.isDropped(msg.GetSegmentID()) {
		return true
	}

	if si, ok := ddn.segID2SegInfo.Load(msg.GetSegmentID()); ok {
		if msg.EndTs() <= si.(*datapb.SegmentInfo).GetDmlPosition().GetTimestamp() {
			return true
		}

		ddn.segID2SegInfo.Delete(msg.GetSegmentID())
	}
	return false
}

func (ddn *ddNode) isFlushed(segmentID UniqueID) bool {
	for _, s := range ddn.flushedSegments {
		if s.ID == segmentID {
			return true
		}
	}
	return false
}

func (ddn *ddNode) isDropped(segID UniqueID) bool {
	for _, droppedSegment := range ddn.droppedSegments {
		if droppedSegment.GetID() == segID {
			return true
		}
	}
	return false
}

func (ddn *ddNode) forwardDeleteMsg(msgs []msgstream.TsMsg, minTs Timestamp, maxTs Timestamp) error {
	if len(msgs) != 0 {
		var msgPack = msgstream.MsgPack{
			Msgs:    msgs,
			BeginTs: minTs,
			EndTs:   maxTs,
		}
		if err := ddn.deltaMsgStream.Produce(&msgPack); err != nil {
			return err
		}
	}
	if err := ddn.sendDeltaTimeTick(maxTs); err != nil {
		return err
	}
	return nil
}

func (ddn *ddNode) sendDeltaTimeTick(ts Timestamp) error {
	msgPack := msgstream.MsgPack{}
	baseMsg := msgstream.BaseMsg{
		BeginTimestamp: ts,
		EndTimestamp:   ts,
		HashValues:     []uint32{0},
	}
	timeTickResult := internalpb.TimeTickMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_TimeTick,
			MsgID:     0,
			Timestamp: ts,
			SourceID:  Params.DataNodeCfg.GetNodeID(),
		},
	}
	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg:     baseMsg,
		TimeTickMsg: timeTickResult,
	}
	msgPack.Msgs = append(msgPack.Msgs, timeTickMsg)

	if err := ddn.deltaMsgStream.Produce(&msgPack); err != nil {
		return err
	}
	return nil
}

func (ddn *ddNode) Close() {
	if ddn.deltaMsgStream != nil {
		ddn.deltaMsgStream.Close()
	}
}

func newDDNode(ctx context.Context, collID UniqueID, vchanInfo *datapb.VchannelInfo,
	msFactory msgstream.Factory, compactor *compactionExecutor) *ddNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(Params.DataNodeCfg.FlowGraphMaxQueueLength)
	baseNode.SetMaxParallelism(Params.DataNodeCfg.FlowGraphMaxParallelism)

	fs := make([]*datapb.SegmentInfo, 0, len(vchanInfo.GetFlushedSegments()))
	fs = append(fs, vchanInfo.GetFlushedSegments()...)
	log.Info("ddNode add flushed segment",
		zap.Int64("collectionID", vchanInfo.GetCollectionID()),
		zap.Int("No. Segment", len(vchanInfo.GetFlushedSegments())),
	)

	deltaStream, err := msFactory.NewMsgStream(ctx)
	if err != nil {
		log.Error(err.Error())
		return nil
	}
	pChannelName := funcutil.ToPhysicalChannel(vchanInfo.ChannelName)
	log.Info("ddNode add flushed segment",
		zap.String("channelName", vchanInfo.ChannelName),
		zap.String("pChannelName", pChannelName),
	)
	deltaChannelName, err := funcutil.ConvertChannelName(pChannelName, Params.CommonCfg.RootCoordDml, Params.CommonCfg.RootCoordDelta)
	if err != nil {
		log.Error(err.Error())
		return nil
	}

	deltaStream.SetRepackFunc(msgstream.DefaultRepackFunc)
	deltaStream.AsProducer([]string{deltaChannelName})
	metrics.DataNodeNumProducers.WithLabelValues(fmt.Sprint(Params.DataNodeCfg.GetNodeID())).Inc()
	log.Debug("datanode AsProducer", zap.String("DeltaChannelName", deltaChannelName))
	var deltaMsgStream msgstream.MsgStream = deltaStream
	deltaMsgStream.Start()

	dd := &ddNode{
		BaseNode:           baseNode,
		collectionID:       collID,
		flushedSegments:    fs,
		droppedSegments:    vchanInfo.GetDroppedSegments(),
		vchannelName:       vchanInfo.ChannelName,
		deltaMsgStream:     deltaMsgStream,
		compactionExecutor: compactor,
	}

	dd.dropMode.Store(false)

	for _, us := range vchanInfo.GetUnflushedSegments() {
		dd.segID2SegInfo.Store(us.GetID(), us)
	}

	log.Info("ddNode add unflushed segment",
		zap.Int64("collectionID", collID),
		zap.Int("No. Segment", len(vchanInfo.GetUnflushedSegments())),
	)

	return dd
}
