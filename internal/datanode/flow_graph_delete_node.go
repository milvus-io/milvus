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

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// DeleteNode is to process delete msg, flush delete info into storage.
type deleteNode struct {
	BaseNode
	ctx              context.Context
	channelName      string
	delBufferManager *DeltaBufferManager // manager of delete msg
	channel          Channel
	flushManager     flushManager

	clearSignal chan<- string
}

func (dn *deleteNode) Name() string {
	return "deleteNode-" + dn.channelName
}

func (dn *deleteNode) Close() {
	log.Info("Flowgraph Delete Node closing")
}

func (dn *deleteNode) showDelBuf(segIDs []UniqueID, ts Timestamp) {
	for _, segID := range segIDs {
		if buffer, ok := dn.delBufferManager.Load(segID); ok {
			log.Debug("delta buffer status",
				zap.Int64("segmentID", segID),
				zap.Uint64("timestamp", ts),
				zap.Int64("entriesNum", buffer.GetEntriesNum()),
				zap.Int64("memorySize", buffer.GetMemorySize()),
				zap.String("vChannel", dn.channelName))
		}
	}
}

func (dn *deleteNode) IsValidInMsg(in []Msg) bool {
	if !dn.BaseNode.IsValidInMsg(in) {
		return false
	}
	_, ok := in[0].(*flowGraphMsg)
	if !ok {
		log.Warn("type assertion failed for flowGraphMsg", zap.String("name", reflect.TypeOf(in[0]).Name()))
		return false
	}
	return true
}

// Operate implementing flowgraph.Node, performs delete data process
func (dn *deleteNode) Operate(in []Msg) []Msg {
	fgMsg := in[0].(*flowGraphMsg)

	var spans []trace.Span
	for _, msg := range fgMsg.deleteMessages {
		ctx, sp := startTracer(msg, "Delete-Node")
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}

	// update compacted segment before operation
	dn.delBufferManager.UpdateCompactedSegments()

	// process delete messages
	segIDs := typeutil.NewUniqueSet()
	for i, msg := range fgMsg.deleteMessages {
		traceID := spans[i].SpanContext().TraceID().String()
		log.Debug("Buffer delete request in DataNode", zap.String("traceID", traceID))
		tmpSegIDs, err := dn.bufferDeleteMsg(msg, fgMsg.timeRange, fgMsg.startPositions[0], fgMsg.endPositions[0])
		if err != nil {
			// error occurs only when deleteMsg is misaligned, should not happen
			err = fmt.Errorf("buffer delete msg failed, err = %s", err)
			log.Error(err.Error())
			panic(err)
		}
		segIDs.Insert(tmpSegIDs...)
	}

	// display changed segment's status in dn.delBuf of a certain ts
	if len(fgMsg.deleteMessages) != 0 {
		dn.showDelBuf(segIDs.Collect(), fgMsg.timeRange.timestampMax)
	}

	// process flush messages
	if len(fgMsg.segmentsToSync) > 0 {
		log.Info("DeleteNode receives flush message",
			zap.Int64s("segIDs", fgMsg.segmentsToSync),
			zap.String("vChannelName", dn.channelName),
			zap.Time("posTime", tsoutil.PhysicalTime(fgMsg.endPositions[0].Timestamp)))
		for _, segmentToFlush := range fgMsg.segmentsToSync {
			buf, ok := dn.delBufferManager.Load(segmentToFlush)
			if !ok {
				// no related delta data to flush, send empty buf to complete flush life-cycle
				dn.flushManager.flushDelData(nil, segmentToFlush, fgMsg.endPositions[0])
			} else {
				// TODO, this has to be async, no need to block here
				err := retry.Do(dn.ctx, func() error {
					return dn.flushManager.flushDelData(buf, segmentToFlush, fgMsg.endPositions[0])
				}, getFlowGraphRetryOpt())
				if err != nil {
					err = fmt.Errorf("failed to flush delete data, err = %s", err)
					log.Error(err.Error())
					panic(err)
				}
				// remove delete buf
				dn.delBufferManager.Delete(segmentToFlush)
			}
		}
	}

	// process drop collection message, delete node shall notify flush manager all data are cleared and send signal to DataSyncService cleaner
	if fgMsg.dropCollection {
		dn.flushManager.notifyAllFlushed()
		log.Info("DeleteNode notifies BackgroundGC to release vchannel", zap.String("vChannelName", dn.channelName))
		dn.clearSignal <- dn.channelName
	}

	for _, sp := range spans {
		sp.End()
	}
	return in
}

func (dn *deleteNode) bufferDeleteMsg(msg *msgstream.DeleteMsg, tr TimeRange, startPos, endPos *msgpb.MsgPosition) ([]UniqueID, error) {
	log.Debug("bufferDeleteMsg", zap.Any("primary keys", msg.PrimaryKeys), zap.String("vChannelName", dn.channelName))

	primaryKeys := storage.ParseIDs2PrimaryKeys(msg.PrimaryKeys)
	segIDToPks, segIDToTss := dn.filterSegmentByPK(msg.PartitionID, primaryKeys, msg.Timestamps)

	segIDs := make([]UniqueID, 0, len(segIDToPks))
	for segID, pks := range segIDToPks {
		segIDs = append(segIDs, segID)

		tss, ok := segIDToTss[segID]
		if !ok || len(pks) != len(tss) {
			return nil, fmt.Errorf("primary keys and timestamp's element num mis-match, segmentID = %d", segID)
		}
		dn.delBufferManager.StoreNewDeletes(segID, pks, tss, tr, startPos, endPos)
	}

	return segIDs, nil
}

// filterSegmentByPK returns the bloom filter check result.
// If the key may exist in the segment, returns it in map.
// If the key not exist in the segment, the segment is filter out.
func (dn *deleteNode) filterSegmentByPK(partID UniqueID, pks []primaryKey, tss []Timestamp) (
	map[UniqueID][]primaryKey, map[UniqueID][]uint64) {
	segID2Pks := make(map[UniqueID][]primaryKey)
	segID2Tss := make(map[UniqueID][]uint64)
	segments := dn.channel.filterSegments(partID)
	for index, pk := range pks {
		for _, segment := range segments {
			segmentID := segment.segmentID
			if segment.isPKExist(pk) {
				segID2Pks[segmentID] = append(segID2Pks[segmentID], pk)
				segID2Tss[segmentID] = append(segID2Tss[segmentID], tss[index])
			}
		}
	}

	return segID2Pks, segID2Tss
}

func newDeleteNode(ctx context.Context, fm flushManager, manager *DeltaBufferManager, sig chan<- string, config *nodeConfig) (*deleteNode, error) {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(config.maxQueueLength)
	baseNode.SetMaxParallelism(config.maxParallelism)

	return &deleteNode{
		ctx:              ctx,
		BaseNode:         baseNode,
		delBufferManager: manager,
		channel:          config.channel,
		channelName:      config.vChannelName,
		flushManager:     fm,
		clearSignal:      sig,
	}, nil
}
