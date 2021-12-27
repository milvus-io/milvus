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

package querynode

import (
	"sync"

	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/trace"
)

// deleteNode is the one of nodes in delta flow graph
type deleteNode struct {
	baseNode
	replica ReplicaInterface // historical
}

// Name returns the name of deleteNode
func (dNode *deleteNode) Name() string {
	return "dNode"
}

// Operate handles input messages, do delete operations
func (dNode *deleteNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	if len(in) != 1 {
		log.Error("Invalid operate message input in deleteNode", zap.Int("input length", len(in)))
		// TODO: add error handling
	}

	dMsg, ok := in[0].(*deleteMsg)
	if !ok {
		log.Warn("type assertion failed for deleteMsg")
		// TODO: add error handling
	}

	delData := &deleteData{
		deleteIDs:        map[UniqueID][]int64{},
		deleteTimestamps: map[UniqueID][]Timestamp{},
		deleteOffset:     map[UniqueID]int64{},
	}

	if dMsg == nil {
		return []Msg{}
	}

	var spans []opentracing.Span
	for _, msg := range dMsg.deleteMessages {
		sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}
	var collectionID UniqueID
	// 1. filter segment by bloom filter
	for i, delMsg := range dMsg.deleteMessages {
		traceID, _, _ := trace.InfoFromSpan(spans[i])
		log.Info("fg_delete_node Process delete request in QueryNode", zap.String("traceID", traceID),
			zap.Int64("collectionID", delMsg.CollectionID))
		collectionID = delMsg.CollectionID
		if dNode.replica.getSegmentNum() != 0 {
			log.Debug("fg_delete_node delete in historical replica",
				zap.Any("collectionID", delMsg.CollectionID),
				zap.Any("collectionName", delMsg.CollectionName),
				zap.Any("pks", delMsg.PrimaryKeys),
				zap.Any("timestamp", delMsg.Timestamps),
				zap.Any("timestampBegin", delMsg.BeginTs()),
				zap.Any("timestampEnd", delMsg.EndTs()),
			)
			processDeleteMessages(dNode.replica, delMsg, delData)
		} else {
			log.Warn("fg_delete_node delete in historical replica",
				zap.Any("collectionID", delMsg.CollectionID),
				zap.Any("collectionName", delMsg.CollectionName),
				zap.Any("pks", delMsg.PrimaryKeys),
				zap.Any("timestamp", delMsg.Timestamps),
				zap.Any("timestampBegin", delMsg.BeginTs()),
				zap.Any("timestampEnd", delMsg.EndTs()),
				zap.Int("segmentNum", 0),
			)
		}
	}

	// 2. do preDelete
	for segmentID, pks := range delData.deleteIDs {
		segment, err := dNode.replica.getSegmentByID(segmentID)
		if err != nil {
			log.Warn("fg_delete_node getSegmentByID failed",
				zap.Int64("collectionID", collectionID),
				zap.Int64("segmentID", segmentID),
				zap.Error(err))
			continue
		}
		offset := segment.segmentPreDelete(len(pks))
		log.Debug("fg_delete_node segmentPreDelete",
			zap.Any("pks", pks),
			zap.Int64("collectionID", collectionID),
			zap.Int64("segmentID", segmentID),
			zap.Int64("offset", offset))
		delData.deleteOffset[segmentID] = offset
	}

	log.Debug("fg_delete_node after segmentPreDelete",
		zap.Int64("collectionID", collectionID),
		zap.Any("deleteOffset", delData.deleteOffset),
		zap.Any("deleteIDs", delData.deleteIDs),
		zap.Any("deleteTimestamps", delData.deleteTimestamps))

	// 3. do delete
	wg := sync.WaitGroup{}
	for segmentID := range delData.deleteOffset {
		wg.Add(1)
		go dNode.delete(delData, segmentID, collectionID, &wg)
	}
	wg.Wait()

	var res Msg = &serviceTimeMsg{
		timeRange: dMsg.timeRange,
	}
	for _, sp := range spans {
		sp.Finish()
	}

	return []Msg{res}
}

// delete will do delete operation at segment which id is segmentID
func (dNode *deleteNode) delete(deleteData *deleteData, segmentID UniqueID, collectionID UniqueID, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Debug("fg_delete_node delete",
		zap.Int64("collectionID", collectionID),
		zap.Int64("SegmentID", segmentID))
	targetSegment, err := dNode.replica.getSegmentByID(segmentID)
	if err != nil {
		log.Error("fg_delete_node delete failed,",
			zap.Int64("collectionID", collectionID),
			zap.Int64("SegmentID", segmentID),
			zap.Error(err),
		)
		return
	}

	log.Debug("fg_delete_node delete",
		zap.Int64("collectionID", collectionID),
		zap.Int64("SegmentID", segmentID),
		zap.String("segmentType", targetSegment.segmentType.String()))
	if targetSegment.segmentType != segmentTypeSealed && targetSegment.segmentType != segmentTypeIndexing {
		log.Debug("fg_delete_node delete segmentType not match",
			zap.Int64("collectionID", collectionID),
			zap.Int64("SegmentID", segmentID),
			zap.String("segmentType", targetSegment.segmentType.String()))
		return
	}

	ids := deleteData.deleteIDs[segmentID]
	timestamps := deleteData.deleteTimestamps[segmentID]
	offset := deleteData.deleteOffset[segmentID]

	err = targetSegment.segmentDelete(offset, &ids, &timestamps)
	if err != nil {
		log.Debug("fg_delete_node delete failed",
			zap.Int64("collectionID", collectionID),
			zap.Int64("SegmentID", segmentID),
			zap.String("segmentType", targetSegment.segmentType.String()),
			zap.Error(err))
		return
	}
	log.Debug("fg_delete_node delete failed",
		zap.Int64("collectionID", collectionID),
		zap.Int64("SegmentID", segmentID),
		zap.String("segmentType", targetSegment.segmentType.String()),
		zap.Any("offset", offset),
		zap.Any("ids", ids),
		zap.Any("timestamps", timestamps))
}

// newDeleteNode returns a new deleteNode
func newDeleteNode(historicalReplica ReplicaInterface) *deleteNode {
	maxQueueLength := Params.QueryNodeCfg.FlowGraphMaxQueueLength
	maxParallelism := Params.QueryNodeCfg.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &deleteNode{
		baseNode: baseNode,
		replica:  historicalReplica,
	}
}
