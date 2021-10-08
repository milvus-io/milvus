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
	"encoding/binary"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

// DeleteNode is to process delete msg, flush delete info into storage.
type deleteNode struct {
	BaseNode

	channelName string
	replica     Replica

	flushCh <-chan *flushMsg
}

func (dn *deleteNode) Name() string {
	return "deleteNode"
}

func (dn *deleteNode) Close() {
	log.Info("Flowgraph Delete Node closing")
}

func (dn *deleteNode) Operate(in []Msg) []Msg {
	log.Debug("deleteNode Operating")

	if len(in) != 1 {
		log.Warn("Invalid operate message input in deleteNode", zap.Int("input length", len(in)))
		return []Msg{}
	}

	_, ok := in[0].(*MsgStreamMsg)
	if !ok {
		log.Warn("type assertion failed for MsgStreamMsg")
		return []Msg{}
	}

	select {
	case fmsg := <-dn.flushCh:
		currentSegID := fmsg.segmentID
		log.Debug("DeleteNode receives flush message",
			zap.Int64("segmentID", currentSegID),
			zap.Int64("collectionID", fmsg.collectionID),
		)
	default:
	}

	return []Msg{}
}

// filterSegmentByPK returns the bloom filter check result.
// If the key may exists in the segment, returns it in map.
// If the key not exists in the segment, the segment is filter out.
func (dn *deleteNode) filterSegmentByPK(partID UniqueID, pks []int64) map[int64][]int64 {
	result := make(map[int64][]int64)
	buf := make([]byte, 8)
	segments := dn.replica.filterSegments(dn.channelName, partID)
	for _, pk := range pks {
		for _, segment := range segments {
			binary.BigEndian.PutUint64(buf, uint64(pk))
			exist := segment.pkFilter.Test(buf)
			if exist {
				result[pk] = append(result[pk], segment.segmentID)
			}
		}
	}
	return result
}

func newDeleteNode(replica Replica, channelName string, flushCh <-chan *flushMsg) *deleteNode {
	baseNode := BaseNode{}
	baseNode.SetMaxParallelism(Params.FlowGraphMaxQueueLength)

	return &deleteNode{
		BaseNode: baseNode,

		channelName: channelName,
		replica:     replica,

		flushCh: flushCh,
	}
}
