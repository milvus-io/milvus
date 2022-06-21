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

	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
)

// make sure ddNode implements flowgraph.Node
var _ flowgraph.Node = (*ddNode)(nil)

// ddNode filters messages from message streams.
//
// ddNode recives all the messages from message stream dml channels, including insert messages,
//  delete messages and ddl messages like CreateCollectionMsg and DropCollectionMsg.
//
// ddNode filters insert messages according to the `sealedSegment`.
// ddNode will filter out the insert message for those who belong to `sealedSegment`
//
// When receiving a `DropCollection` message, ddNode will send a signal to DataNode `BackgroundGC`
//  goroutinue, telling DataNode to release the resources of this particular flow graph.
//
// After the filtering process, ddNode passes all the valid insert messages and delete message
//  to the following flow graph node, which in DataNode is `insertBufferNode`
type ddNode struct {
	BaseNode

	ctx          context.Context
	collectionID UniqueID
	vChannelName string

	deltaMsgStream     msgstream.MsgStream
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

// Operate handles input messages, implementing flowgrpah.Node
func (ddn *ddNode) Operate(in []Msg) []Msg {
	if len(in) != 1 {
		log.Warn("Invalid operate message input in ddNode", zap.Int("input length", len(in)))
		return []Msg{}
	}

	msMsg, ok := in[0].(*MsgStreamMsg)
	if !ok {
		if in[0] == nil {
			log.Debug("type assertion failed for MsgStreamMsg because it's nil")
		} else {
			log.Warn("type assertion failed for MsgStreamMsg", zap.String("name", reflect.TypeOf(in[0]).Name()))
		}
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
			zap.String("vChannelName", ddn.vChannelName),
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
					zap.String("vChannelName", ddn.vChannelName))
				ddn.dropMode.Store(true)

				log.Info("Stop compaction of vChannel", zap.String("vChannelName", ddn.vChannelName))
				ddn.compactionExecutor.stopExecutingtaskByVChannelName(ddn.vChannelName)
				fgMsg.dropCollection = true
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
					zap.Int64("filter segment ID", imsg.GetSegmentID()),
					zap.Uint64("message timestamp", msg.EndTs()),
				)
				continue
			}

			log.Debug("DDNode receive insert messages",
				zap.Int("numRows", len(imsg.GetRowIDs())),
				zap.String("vChannelName", ddn.vChannelName))
			fgMsg.insertMessages = append(fgMsg.insertMessages, imsg)

		case commonpb.MsgType_Delete:
			dmsg := msg.(*msgstream.DeleteMsg)
			log.Debug("DDNode receive delete messages",
				zap.Int64("num", dmsg.NumRows),
				zap.String("vChannelName", ddn.vChannelName))
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
	err := retry.Do(ddn.ctx, func() error {
		return ddn.forwardDeleteMsg(forwardMsgs, msMsg.TimestampMin(), msMsg.TimestampMax())
	}, flowGraphRetryOpt)
	if err != nil {
		err = fmt.Errorf("DDNode forward delete msg failed, vChannel = %s, err = %s", ddn.vChannelName, err)
		log.Error(err.Error())
		if !common.IsIgnorableError(err) {
			panic(err)
		}
	}

	fgMsg.startPositions = append(fgMsg.startPositions, msMsg.StartPositions()...)
	fgMsg.endPositions = append(fgMsg.endPositions, msMsg.EndPositions()...)

	setVchannelForPositions(ddn.vChannelName, fgMsg.startPositions...)
	setVchannelForPositions(ddn.vChannelName, fgMsg.endPositions...)

	for _, sp := range spans {
		sp.Finish()
	}

	return []Msg{&fgMsg}
}

func setVchannelForPositions(vchan string, positions ...*internalpb.MsgPosition) {
	for _, p := range positions {
		p.ChannelName = vchan
	}
}

func (ddn *ddNode) tryToFilterSegmentInsertMessages(msg *msgstream.InsertMsg) bool {
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
	p, _ := tsoutil.ParseTS(ts)
	log.RatedDebug(10.0, "DDNode sent delta timeTick",
		zap.Any("collectionID", ddn.collectionID),
		zap.Any("ts", ts),
		zap.Any("ts_p", p),
		zap.Any("channel", ddn.vChannelName),
	)
	return nil
}

func (ddn *ddNode) Close() {
	if ddn.deltaMsgStream != nil {
		ddn.deltaMsgStream.Close()
	}
}

func newDDNode(ctx context.Context, collID UniqueID, vChannelName string, droppedSegmentIDs []UniqueID,
	sealedSegments []*datapb.SegmentInfo, growingSegments []*datapb.SegmentInfo,
	msFactory msgstream.Factory, compactor *compactionExecutor) (*ddNode, error) {

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(Params.DataNodeCfg.FlowGraphMaxQueueLength)
	baseNode.SetMaxParallelism(Params.DataNodeCfg.FlowGraphMaxParallelism)

	deltaStream, err := msFactory.NewMsgStream(ctx)
	if err != nil {
		return nil, err
	}
	pChannelName := funcutil.ToPhysicalChannel(vChannelName)
	log.Info("ddNode convert vChannel to pChannel",
		zap.String("vChannelName", vChannelName),
		zap.String("pChannelName", pChannelName),
	)

	deltaChannelName, err := funcutil.ConvertChannelName(pChannelName, Params.CommonCfg.RootCoordDml, Params.CommonCfg.RootCoordDelta)
	if err != nil {
		return nil, err
	}
	deltaStream.SetRepackFunc(msgstream.DefaultRepackFunc)
	deltaStream.AsProducer([]string{deltaChannelName})
	metrics.DataNodeNumProducers.WithLabelValues(fmt.Sprint(Params.DataNodeCfg.GetNodeID())).Inc()
	log.Debug("datanode AsProducer", zap.String("DeltaChannelName", deltaChannelName))
	var deltaMsgStream msgstream.MsgStream = deltaStream
	deltaMsgStream.Start()

	dd := &ddNode{
		ctx:                ctx,
		BaseNode:           baseNode,
		collectionID:       collID,
		sealedSegInfo:      make(map[UniqueID]*datapb.SegmentInfo, len(sealedSegments)),
		growingSegInfo:     make(map[UniqueID]*datapb.SegmentInfo, len(growingSegments)),
		droppedSegmentIDs:  droppedSegmentIDs,
		vChannelName:       vChannelName,
		deltaMsgStream:     deltaMsgStream,
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
