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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/trace"
)

// insertNode is one of the nodes in query flow graph
type insertNode struct {
	baseNode
	streamingReplica ReplicaInterface
}

// insertData stores the valid insert data
type insertData struct {
	insertIDs        map[UniqueID][]int64
	insertTimestamps map[UniqueID][]Timestamp
	insertRecords    map[UniqueID][]*commonpb.Blob
	insertOffset     map[UniqueID]int64
	insertPKs        map[UniqueID][]int64
}

// deleteData stores the valid delete data
type deleteData struct {
	deleteIDs        map[UniqueID][]int64
	deleteTimestamps map[UniqueID][]Timestamp
	deleteOffset     map[UniqueID]int64
}

// Name returns the name of insertNode
func (iNode *insertNode) Name() string {
	return "iNode"
}

// Operate handles input messages, to execute insert operations
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

	iData := insertData{
		insertIDs:        make(map[UniqueID][]int64),
		insertTimestamps: make(map[UniqueID][]Timestamp),
		insertRecords:    make(map[UniqueID][]*commonpb.Blob),
		insertOffset:     make(map[UniqueID]int64),
		insertPKs:        make(map[UniqueID][]int64),
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
		// if loadType is loadCollection, check if partition exists, if not, create partition
		col, err := iNode.streamingReplica.getCollectionByID(task.CollectionID)
		if err != nil {
			log.Error(err.Error())
			continue
		}
		if col.getLoadType() == loadTypeCollection {
			err = iNode.streamingReplica.addPartition(task.CollectionID, task.PartitionID)
			if err != nil {
				log.Error(err.Error())
				continue
			}
		}

		// check if segment exists, if not, create this segment
		if !iNode.streamingReplica.hasSegment(task.SegmentID) {
			err := iNode.streamingReplica.addSegment(task.SegmentID, task.PartitionID, task.CollectionID, task.ShardName, segmentTypeGrowing, true)
			if err != nil {
				log.Warn(err.Error())
				continue
			}
		}

		iData.insertIDs[task.SegmentID] = append(iData.insertIDs[task.SegmentID], task.RowIDs...)
		iData.insertTimestamps[task.SegmentID] = append(iData.insertTimestamps[task.SegmentID], task.Timestamps...)
		iData.insertRecords[task.SegmentID] = append(iData.insertRecords[task.SegmentID], task.RowData...)
		pks, err := getPrimaryKeys(task, iNode.streamingReplica)
		if err != nil {
			log.Warn(err.Error())
			continue
		}
		iData.insertPKs[task.SegmentID] = append(iData.insertPKs[task.SegmentID], pks...)
	}

	// 2. do preInsert
	for segmentID := range iData.insertRecords {
		var targetSegment, err = iNode.streamingReplica.getSegmentByID(segmentID)
		if err != nil {
			log.Warn(err.Error())
			continue
		}

		var numOfRecords = len(iData.insertRecords[segmentID])
		if targetSegment != nil {
			offset, err := targetSegment.segmentPreInsert(numOfRecords)
			if err != nil {
				log.Warn(err.Error())
				continue
			}
			iData.insertOffset[segmentID] = offset
			log.Debug("fg_insert_node updateBloomFilter",
				zap.Int64("collectionID", targetSegment.collectionID),
				zap.Int64("segmentID", segmentID),
				zap.String("segmentType", targetSegment.segmentType.String()),
				zap.Any("offset", offset),
			)
			targetSegment.updateBloomFilter(iData.insertPKs[segmentID])
		}
	}

	// 3. do insert
	wg := sync.WaitGroup{}
	for segmentID := range iData.insertRecords {
		wg.Add(1)
		go iNode.insert(&iData, segmentID, &wg)
	}
	wg.Wait()

	delData := &deleteData{
		deleteIDs:        make(map[UniqueID][]int64),
		deleteTimestamps: make(map[UniqueID][]Timestamp),
		deleteOffset:     make(map[UniqueID]int64),
	}
	var delCollectionID UniqueID
	// 1. filter segment by bloom filter
	for _, delMsg := range iMsg.deleteMessages {
		if iNode.streamingReplica.getSegmentNum() != 0 {
			log.Debug("fg_insert_node delete in streaming replica",
				zap.Any("collectionID", delMsg.CollectionID),
				zap.Any("collectionName", delMsg.CollectionName),
				zap.Any("pks", delMsg.PrimaryKeys),
				zap.Any("timestamp", delMsg.Timestamps))
			processDeleteMessages(iNode.streamingReplica, delMsg, delData)
		} else {
			log.Warn("fg_insert_node delete in streaming replica getSementNum = 0",
				zap.Any("collectionID", delMsg.CollectionID),
				zap.Any("collectionName", delMsg.CollectionName),
				zap.Any("pks", delMsg.PrimaryKeys),
				zap.Any("timestamp", delMsg.Timestamps))
		}
		delCollectionID = delMsg.CollectionID
	}

	log.Debug("fg_insert_node after processDeleteMessages",
		zap.Int64("collectionID", delCollectionID),
		zap.Any("deleteIDs", delData.deleteIDs),
		zap.Any("deleteOffset", delData.deleteOffset),
		zap.Any("deleteTimestamps", delData.deleteTimestamps),
	)

	// 2. do preDelete
	for segmentID, pks := range delData.deleteIDs {
		segment, err := iNode.streamingReplica.getSegmentByID(segmentID)
		if err != nil {
			log.Warn("fg_insert_node getSegmentByID failed",
				zap.Int64("collectionID", delCollectionID),
				zap.Int64("segmentID", segmentID),
				zap.Error(err),
			)
			continue
		}
		log.Debug("fg_insert_node segmentPreDelete",
			zap.Int64("collectionID", delCollectionID),
			zap.Int64("segmentID", segmentID),
			zap.Any("pks", pks),
			zap.Any("len", len(pks)),
		)
		offset := segment.segmentPreDelete(len(pks))
		delData.deleteOffset[segmentID] = offset
	}

	log.Debug("fg_insert_node after segmentPreDelete",
		zap.Int64("collectionID", delCollectionID),
		zap.Any("deleteIDs", delData.deleteIDs),
		zap.Any("deleteOffset", delData.deleteOffset),
		zap.Any("deleteTimestamps", delData.deleteTimestamps),
	)

	// 3. do delete
	for segmentID := range delData.deleteOffset {
		wg.Add(1)
		go iNode.delete(delData, segmentID, delCollectionID, &wg)
	}
	wg.Wait()

	log.Debug("fg_insert_node done",
		zap.Int64("collectionID", delCollectionID),
		zap.Any("timeRange", iMsg.timeRange),
	)

	var res Msg = &serviceTimeMsg{
		timeRange: iMsg.timeRange,
	}
	for _, sp := range spans {
		sp.Finish()
	}

	return []Msg{res}
}

// processDeleteMessages would execute delete operations for growing segments
func processDeleteMessages(replica ReplicaInterface, msg *msgstream.DeleteMsg, delData *deleteData) {
	var partitionIDs []UniqueID
	var err error
	if msg.PartitionID != -1 {
		partitionIDs = []UniqueID{msg.PartitionID}
	} else {
		partitionIDs, err = replica.getPartitionIDs(msg.CollectionID)
		if err != nil {
			log.Warn("processDeleteMessages error occur", zap.Error(err))
			return
		}
	}
	resultSegmentIDs := make([]UniqueID, 0)
	for _, partitionID := range partitionIDs {
		segmentIDs, err := replica.getSegmentIDs(partitionID)
		if err != nil {
			log.Warn("processDeleteMessages getSegmentIDs",
				zap.Int64("collectionID", msg.CollectionID),
				zap.Error(err))
			continue
		}
		resultSegmentIDs = append(resultSegmentIDs, segmentIDs...)
	}
	for _, segmentID := range resultSegmentIDs {
		segment, err := replica.getSegmentByID(segmentID)
		if err != nil {
			log.Warn("processDeleteMessages getSegment",
				zap.Int64("collectionID", msg.CollectionID),
				zap.Error(err))
			continue
		}
		pks, err := filterSegmentsByPKs(msg.PrimaryKeys, segment)
		if err != nil {
			log.Warn("processDeleteMessages filterSegmentsByPKs",
				zap.Int64("collectionID", msg.CollectionID),
				zap.Error(err),
			)
			continue
		}
		if len(pks) > 0 {
			delData.deleteIDs[segmentID] = append(delData.deleteIDs[segmentID], pks...)
			// TODO(yukun) get offset of pks
			delData.deleteTimestamps[segmentID] = append(delData.deleteTimestamps[segmentID], msg.Timestamps[:len(pks)]...)
			log.Debug("processDeleteMessages collection pks",
				zap.Int("len", len(pks)),
				zap.Any("pks", pks),
				zap.Int64("collectionID", msg.CollectionID),
				zap.Int64("segmentID", segmentID),
			)
		}
	}
}

// filterSegmentsByPKs would filter segments by primary keys
func filterSegmentsByPKs(pks []int64, segment *Segment) ([]int64, error) {
	if pks == nil {
		return nil, fmt.Errorf("pks is nil when getSegmentsByPKs")
	}
	if segment == nil {
		return nil, fmt.Errorf("segments is nil when getSegmentsByPKs")
	}
	buf := make([]byte, 8)
	res := make([]int64, 0)
	for _, pk := range pks {
		common.Endian.PutUint64(buf, uint64(pk))
		exist := segment.pkFilter.Test(buf)
		if exist {
			res = append(res, pk)
		}
	}
	log.Debug("In filterSegmentsByPKs", zap.Any("pk len", len(res)), zap.Int64("segment", segment.segmentID),
		zap.String("segmentType", segment.segmentType.String()))
	return res, nil
}

// insert would execute insert operations for specific growing segment
func (iNode *insertNode) insert(iData *insertData, segmentID UniqueID, wg *sync.WaitGroup) {
	log.Debug("QueryNode::iNode::insert", zap.Int64("SegmentID", segmentID))
	var targetSegment, err = iNode.streamingReplica.getSegmentByID(segmentID)
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

	ids := iData.insertIDs[segmentID]
	timestamps := iData.insertTimestamps[segmentID]
	records := iData.insertRecords[segmentID]
	offsets := iData.insertOffset[segmentID]

	err = targetSegment.segmentInsert(offsets, &ids, &timestamps, &records)
	if err != nil {
		log.Debug("QueryNode: targetSegmentInsert failed", zap.Error(err),
			zap.Int64("collectionID", targetSegment.collectionID))
		// TODO: add error handling
		wg.Done()
		return
	}

	log.Debug("Do insert done", zap.Int("len", len(iData.insertIDs[segmentID])), zap.Int64("collectionID", targetSegment.collectionID), zap.Int64("segmentID", segmentID))
	wg.Done()
}

// delete would execute delete operations for specific growing segment
func (iNode *insertNode) delete(deleteData *deleteData, segmentID UniqueID, collectionID UniqueID, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Debug("fg_insert_node delete",
		zap.Int64("collectionID", collectionID),
		zap.Int64("SegmentID", segmentID),
	)
	targetSegment, err := iNode.streamingReplica.getSegmentByID(segmentID)
	if err != nil {
		log.Error("flow_graph_insert_node delete getSegmentByID failed",
			zap.Int64("collectionID", collectionID),
			zap.Int64("segmentID", segmentID),
			zap.Error(err))
		return
	}

	if targetSegment.segmentType != segmentTypeGrowing {
		log.Warn("fg_insert_node delete segmentType not match",
			zap.Int64("collectionID", collectionID),
			zap.Int64("segmentID", segmentID),
			zap.String("needType", segmentTypeGrowing.String()),
			zap.String("actualType", targetSegment.segmentType.String()),
			zap.Error(err))
		return
	}

	ids := deleteData.deleteIDs[segmentID]
	timestamps := deleteData.deleteTimestamps[segmentID]
	offset := deleteData.deleteOffset[segmentID]
	log.Debug("fg_insert_node delete segmentDelete",
		zap.Int64("collectionID", collectionID),
		zap.Int64("SegmentID", segmentID),
		zap.Any("ids", ids),
		zap.Any("offset", offset),
		zap.Any("timestamps", timestamps),
	)

	err = targetSegment.segmentDelete(offset, &ids, &timestamps)
	if err != nil {
		log.Warn("fg_insert_node targetSegment call segmentDelete failed",
			zap.Int64("collectionID", targetSegment.collectionID),
			zap.Int64("segmentID", targetSegment.segmentID),
			zap.String("segmentType", targetSegment.segmentType.String()),
			zap.Error(err))
		return
	}

	log.Debug("fg_insert_node do delete done", zap.Int("len", len(deleteData.deleteIDs[segmentID])),
		zap.Int64("segmentID", segmentID),
		zap.Any("ids", ids),
		zap.Int64("collectionID", targetSegment.collectionID))
}

// TODO: remove this function to proper file
// getPrimaryKeys would get primary keys by insert messages
func getPrimaryKeys(msg *msgstream.InsertMsg, streamingReplica ReplicaInterface) ([]int64, error) {
	if len(msg.RowIDs) != len(msg.Timestamps) || len(msg.RowIDs) != len(msg.RowData) {
		log.Warn("misaligned messages detected")
		return nil, errors.New("misaligned messages detected")
	}
	collectionID := msg.GetCollectionID()

	collection, err := streamingReplica.getCollectionByID(collectionID)
	if err != nil {
		log.Warn(err.Error())
		return nil, err
	}
	offset := 0
	for _, field := range collection.schema.Fields {
		if field.IsPrimaryKey {
			break
		}
		switch field.DataType {
		case schemapb.DataType_Bool:
			offset++
		case schemapb.DataType_Int8:
			offset++
		case schemapb.DataType_Int16:
			offset += 2
		case schemapb.DataType_Int32:
			offset += 4
		case schemapb.DataType_Int64:
			offset += 8
		case schemapb.DataType_Float:
			offset += 4
		case schemapb.DataType_Double:
			offset += 8
		case schemapb.DataType_FloatVector:
			for _, t := range field.TypeParams {
				if t.Key == "dim" {
					dim, err := strconv.Atoi(t.Value)
					if err != nil {
						log.Error("strconv wrong on get dim", zap.Error(err))
						break
					}
					offset += dim * 4
					break
				}
			}
		case schemapb.DataType_BinaryVector:
			for _, t := range field.TypeParams {
				if t.Key == "dim" {
					dim, err := strconv.Atoi(t.Value)
					if err != nil {
						log.Error("strconv wrong on get dim", zap.Error(err))
						return nil, err
					}
					offset += dim / 8
					break
				}
			}
		}
	}

	blobReaders := make([]io.Reader, len(msg.RowData))
	for i, blob := range msg.RowData {
		blobReaders[i] = bytes.NewReader(blob.GetValue()[offset : offset+8])
	}
	pks := make([]int64, len(blobReaders))

	for i, reader := range blobReaders {
		err := binary.Read(reader, common.Endian, &pks[i])
		if err != nil {
			log.Warn("binary read blob value failed", zap.Error(err))
			return nil, err
		}
	}

	return pks, nil
}

// newInsertNode returns a new insertNode
func newInsertNode(streamingReplica ReplicaInterface) *insertNode {
	maxQueueLength := Params.QueryNodeCfg.FlowGraphMaxQueueLength
	maxParallelism := Params.QueryNodeCfg.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &insertNode{
		baseNode:         baseNode,
		streamingReplica: streamingReplica,
	}
}
