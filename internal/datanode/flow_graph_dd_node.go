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
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

type ddNode struct {
	BaseNode

	clearSignal  chan<- UniqueID
	collectionID UniqueID

	mu          sync.RWMutex
	seg2SegInfo map[UniqueID]*datapb.SegmentInfo // Segment ID to UnFlushed Segment
	vchanInfo   *datapb.VchannelInfo
}

func (ddn *ddNode) Name() string {
	return "ddNode"
}

func (ddn *ddNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {

	// log.Debug("DDNode Operating")

	if len(in) != 1 {
		log.Error("Invalid operate message input in ddNode", zap.Int("input length", len(in)))
		// TODO: add error handling
	}

	if len(in) == 0 {
		return []flowgraph.Msg{}
	}

	msMsg, ok := in[0].(*MsgStreamMsg)
	if !ok {
		log.Error("type assertion failed for MsgStreamMsg")
		// TODO: add error handling
	}

	if msMsg == nil {
		return []Msg{}
	}

	var iMsg = insertMsg{
		insertMessages: make([]*msgstream.InsertMsg, 0),
		timeRange: TimeRange{
			timestampMin: msMsg.TimestampMin(),
			timestampMax: msMsg.TimestampMax(),
		},
		startPositions: make([]*internalpb.MsgPosition, 0),
		endPositions:   make([]*internalpb.MsgPosition, 0),
	}

	for _, msg := range msMsg.TsMessages() {
		switch msg.Type() {
		case commonpb.MsgType_DropCollection:
			if msg.(*msgstream.DropCollectionMsg).GetCollectionID() == ddn.collectionID {
				log.Info("Destroying current flowgraph", zap.Any("collectionID", ddn.collectionID))
				ddn.clearSignal <- ddn.collectionID
			}
		case commonpb.MsgType_Insert:
			log.Debug("DDNode with insert messages")
			if msg.EndTs() < FilterThreshold {
				log.Info("Filtering Insert Messages",
					zap.Uint64("Message endts", msg.EndTs()),
					zap.Uint64("FilterThreshold", FilterThreshold),
				)
				if ddn.filterFlushedSegmentInsertMessages(msg.(*msgstream.InsertMsg)) {
					continue
				}
			}
			iMsg.insertMessages = append(iMsg.insertMessages, msg.(*msgstream.InsertMsg))
		}
	}

	iMsg.startPositions = append(iMsg.startPositions, msMsg.StartPositions()...)
	iMsg.endPositions = append(iMsg.endPositions, msMsg.EndPositions()...)

	var res Msg = &iMsg

	return []Msg{res}
}

func (ddn *ddNode) filterFlushedSegmentInsertMessages(msg *msgstream.InsertMsg) bool {
	if ddn.isFlushed(msg.GetSegmentID()) {
		return true
	}

	ddn.mu.Lock()
	if si, ok := ddn.seg2SegInfo[msg.GetSegmentID()]; ok {
		if msg.EndTs() <= si.GetDmlPosition().GetTimestamp() {
			return true
		}
		delete(ddn.seg2SegInfo, msg.GetSegmentID())
	}

	ddn.mu.Unlock()
	return false
}

func (ddn *ddNode) isFlushed(segmentID UniqueID) bool {
	ddn.mu.Lock()
	defer ddn.mu.Unlock()

	for _, id := range ddn.vchanInfo.GetFlushedSegments() {
		if id == segmentID {
			return true
		}
	}
	return false
}

func newDDNode(clearSignal chan<- UniqueID, collID UniqueID, vchanInfo *datapb.VchannelInfo) *ddNode {
	baseNode := BaseNode{}
	baseNode.SetMaxParallelism(Params.FlowGraphMaxQueueLength)

	si := make(map[UniqueID]*datapb.SegmentInfo)
	for _, us := range vchanInfo.GetUnflushedSegments() {
		si[us.GetID()] = us
	}

	return &ddNode{
		BaseNode:     baseNode,
		clearSignal:  clearSignal,
		collectionID: collID,
		seg2SegInfo:  si,
		vchanInfo:    vchanInfo,
	}
}
