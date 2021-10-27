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
	"encoding/binary"
	"math"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/opentracing/opentracing-go"
)

type (
	// DeleteData record deleted IDs and Timestamps
	DeleteData = storage.DeleteData
)

// DeleteNode is to process delete msg, flush delete info into storage.
type deleteNode struct {
	BaseNode
	channelName  string
	delBuf       sync.Map // map[segmentID]*DelDataBuf
	replica      Replica
	idAllocator  allocatorInterface
	flushManager flushManager
}

// DelDataBuf buffers insert data, monitoring buffer size and limit
// size and limit both indicate numOfRows
type DelDataBuf struct {
	delData  *DeleteData
	size     int64
	tsFrom   Timestamp
	tsTo     Timestamp
	fileSize int64
	filePath string
}

func (ddb *DelDataBuf) updateSize(size int64) {
	ddb.size += size
}

func (ddb *DelDataBuf) updateTimeRange(tr TimeRange) {
	if tr.timestampMin < ddb.tsFrom {
		ddb.tsFrom = tr.timestampMin
	}
	if tr.timestampMax > ddb.tsTo {
		ddb.tsTo = tr.timestampMax
	}
}

func newDelDataBuf() *DelDataBuf {
	return &DelDataBuf{
		delData: &DeleteData{
			Data: make(map[int64]int64),
		},
		size:   0,
		tsFrom: math.MaxUint64,
		tsTo:   0,
	}
}

func (dn *deleteNode) Name() string {
	return "deleteNode"
}

func (dn *deleteNode) Close() {
	log.Info("Flowgraph Delete Node closing")
}

func (dn *deleteNode) bufferDeleteMsg(msg *msgstream.DeleteMsg, tr TimeRange) error {
	log.Debug("bufferDeleteMsg", zap.Any("primary keys", msg.PrimaryKeys))

	segIDToPkMap := make(map[UniqueID][]int64)
	segIDToTsMap := make(map[UniqueID][]int64)

	m := dn.filterSegmentByPK(msg.PartitionID, msg.PrimaryKeys)
	for i, pk := range msg.PrimaryKeys {
		segIDs, ok := m[pk]
		if !ok {
			log.Warn("primary key not exist in all segments", zap.Int64("primary key", pk))
			continue
		}
		for _, segID := range segIDs {
			segIDToPkMap[segID] = append(segIDToPkMap[segID], pk)
			segIDToTsMap[segID] = append(segIDToTsMap[segID], int64(msg.Timestamps[i]))
		}
	}

	for segID, pks := range segIDToPkMap {
		rows := len(pks)
		tss, ok := segIDToTsMap[segID]
		if !ok || rows != len(tss) {
			log.Error("primary keys and timestamp's element num mis-match")
		}

		var delDataBuf *DelDataBuf
		value, ok := dn.delBuf.Load(segID)
		if ok {
			delDataBuf = value.(*DelDataBuf)
		} else {
			delDataBuf = newDelDataBuf()
		}
		delData := delDataBuf.delData

		for i := 0; i < rows; i++ {
			delData.Data[pks[i]] = tss[i]
			log.Debug("delete", zap.Int64("primary key", pks[i]), zap.Int64("ts", tss[i]))
		}

		// store
		delDataBuf.updateSize(int64(rows))
		delDataBuf.updateTimeRange(tr)
		dn.delBuf.Store(segID, delDataBuf)
	}

	return nil
}

func (dn *deleteNode) showDelBuf() {
	segments := dn.replica.filterSegments(dn.channelName, common.InvalidPartitionID)
	for _, seg := range segments {
		segID := seg.segmentID
		if v, ok := dn.delBuf.Load(segID); ok {
			delDataBuf, _ := v.(*DelDataBuf)
			log.Debug("del data buffer status", zap.Int64("segID", segID), zap.Int64("size", delDataBuf.size))
			for pk, ts := range delDataBuf.delData.Data {
				log.Debug("del data", zap.Int64("pk", pk), zap.Int64("ts", ts))
			}
		} else {
			log.Error("segment not exist", zap.Int64("segID", segID))
		}
	}
}

func (dn *deleteNode) Operate(in []Msg) []Msg {
	//log.Debug("deleteNode Operating")

	if len(in) != 1 {
		log.Error("Invalid operate message input in deleteNode", zap.Int("input length", len(in)))
		return nil
	}

	fgMsg, ok := in[0].(*flowGraphMsg)
	if !ok {
		log.Error("type assertion failed for flowGraphMsg")
		return nil
	}

	var spans []opentracing.Span
	for _, msg := range fgMsg.deleteMessages {
		sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}

	for _, msg := range fgMsg.deleteMessages {
		if err := dn.bufferDeleteMsg(msg, fgMsg.timeRange); err != nil {
			log.Error("buffer delete msg failed", zap.Error(err))
		}
	}

	// show all data in dn.delBuf
	if len(fgMsg.deleteMessages) != 0 {
		dn.showDelBuf()
	}

	// handle flush
	if len(fgMsg.segmentsToFlush) > 0 {
		log.Debug("DeleteNode receives flush message", zap.Int64s("segIDs", fgMsg.segmentsToFlush))
		for _, segmentToFlush := range fgMsg.segmentsToFlush {
			buf, ok := dn.delBuf.Load(segmentToFlush)
			if !ok {
				// send signal
				dn.flushManager.flushDelData(nil, segmentToFlush, fgMsg.endPositions[0])
			} else {
				err := dn.flushManager.flushDelData(buf.(*DelDataBuf), segmentToFlush, fgMsg.endPositions[0])
				if err != nil {
					log.Warn("Failed to flush delete data", zap.Error(err))
				} else {
					// clean up
					dn.delBuf.Delete(segmentToFlush)
				}
			}

		}
	}

	for _, sp := range spans {
		sp.Finish()
	}
	return nil
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

func newDeleteNode(ctx context.Context, fm flushManager, config *nodeConfig) (*deleteNode, error) {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(config.maxQueueLength)
	baseNode.SetMaxParallelism(config.maxParallelism)

	return &deleteNode{
		BaseNode: baseNode,
		delBuf:   sync.Map{},

		replica:      config.replica,
		idAllocator:  config.allocator,
		channelName:  config.vChannelName,
		flushManager: fm,
	}, nil
}
