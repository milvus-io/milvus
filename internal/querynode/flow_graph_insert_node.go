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

package querynode

import (
	"sync"

	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/trace"
)

type insertNode struct {
	baseNode
	replica ReplicaInterface
}

type InsertData struct {
	insertIDs        map[UniqueID][]int64
	insertTimestamps map[UniqueID][]Timestamp
	insertRecords    map[UniqueID][]*commonpb.Blob
	insertOffset     map[UniqueID]int64
}

func (iNode *insertNode) Name() string {
	return "iNode"
}

func (iNode *insertNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	//log.Debug("Do insertNode operation")

	if len(in) != 1 {
		log.Error("Invalid operate message input in insertNode", zap.Int("input length", len(in)))
		// TODO: add error handling
	}

	iMsg, ok := in[0].(*insertMsg)
	if !ok {
		log.Warn("type assertion failed for insertMsg")
		// TODO: add error handling
	}

	insertData := InsertData{
		insertIDs:        make(map[UniqueID][]int64),
		insertTimestamps: make(map[UniqueID][]Timestamp),
		insertRecords:    make(map[UniqueID][]*commonpb.Blob),
		insertOffset:     make(map[UniqueID]int64),
	}

	if iMsg == nil {
		return []Msg{}
	}

	var spans []opentracing.Span
	for _, msg := range iMsg.insertMessages {
		sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}

	// 1. hash insertMessages to insertData
	for _, task := range iMsg.insertMessages {
		// check if partition exists, if not, create partition
		if hasPartition := iNode.replica.hasPartition(task.PartitionID); !hasPartition {
			err := iNode.replica.addPartition(task.CollectionID, task.PartitionID)
			if err != nil {
				log.Warn(err.Error())
				continue
			}
		}

		// check if segment exists, if not, create this segment
		if !iNode.replica.hasSegment(task.SegmentID) {
			err := iNode.replica.addSegment(task.SegmentID, task.PartitionID, task.CollectionID, task.ChannelID, segmentTypeGrowing, true)
			if err != nil {
				log.Warn(err.Error())
				continue
			}
		}

		insertData.insertIDs[task.SegmentID] = append(insertData.insertIDs[task.SegmentID], task.RowIDs...)
		insertData.insertTimestamps[task.SegmentID] = append(insertData.insertTimestamps[task.SegmentID], task.Timestamps...)
		insertData.insertRecords[task.SegmentID] = append(insertData.insertRecords[task.SegmentID], task.RowData...)
	}

	// 2. do preInsert
	for segmentID := range insertData.insertRecords {
		var targetSegment, err = iNode.replica.getSegmentByID(segmentID)
		if err != nil {
			log.Warn(err.Error())
		}

		var numOfRecords = len(insertData.insertRecords[segmentID])
		if targetSegment != nil {
			offset, err := targetSegment.segmentPreInsert(numOfRecords)
			if err != nil {
				log.Warn(err.Error())
			}
			insertData.insertOffset[segmentID] = offset
			log.Debug("insertNode operator", zap.Int("insert size", numOfRecords), zap.Int64("insert offset", offset), zap.Int64("segment id", segmentID))
		}
	}

	// 3. do insert
	wg := sync.WaitGroup{}
	for segmentID := range insertData.insertRecords {
		wg.Add(1)
		go iNode.insert(&insertData, segmentID, &wg)
	}
	wg.Wait()

	var res Msg = &serviceTimeMsg{
		timeRange: iMsg.timeRange,
	}
	for _, sp := range spans {
		sp.Finish()
	}

	return []Msg{res}
}

func (iNode *insertNode) insert(insertData *InsertData, segmentID UniqueID, wg *sync.WaitGroup) {
	log.Debug("QueryNode::iNode::insert", zap.Any("SegmentID", segmentID))
	var targetSegment, err = iNode.replica.getSegmentByID(segmentID)
	if err != nil {
		log.Warn("cannot find segment:", zap.Int64("segmentID", segmentID))
		// TODO: add error handling
		wg.Done()
		return
	}

	if targetSegment.segmentType != segmentTypeGrowing {
		wg.Done()
		return
	}

	ids := insertData.insertIDs[segmentID]
	timestamps := insertData.insertTimestamps[segmentID]
	records := insertData.insertRecords[segmentID]
	offsets := insertData.insertOffset[segmentID]

	err = targetSegment.segmentInsert(offsets, &ids, &timestamps, &records)
	if err != nil {
		log.Debug("QueryNode: targetSegmentInsert failed", zap.Error(err))
		// TODO: add error handling
		wg.Done()
		return
	}

	log.Debug("Do insert done", zap.Int("len", len(insertData.insertIDs[segmentID])),
		zap.Int64("segmentID", segmentID))
	wg.Done()
}

func newInsertNode(replica ReplicaInterface) *insertNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &insertNode{
		baseNode: baseNode,
		replica:  replica,
	}
}
