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
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"sync"

	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// insertNode is one of the nodes in query flow graph
type insertNode struct {
	baseNode
	streamingReplica ReplicaInterface
}

// insertData stores the valid insert data
type insertData struct {
	insertIDs        map[UniqueID][]int64 // rowIDs
	insertTimestamps map[UniqueID][]Timestamp
	insertRecords    map[UniqueID][]*schemapb.FieldData
	insertOffset     map[UniqueID]int64
	insertPKs        map[UniqueID][]primaryKey // pks
}

// deleteData stores the valid delete data
type deleteData struct {
	deleteIDs        map[UniqueID][]primaryKey // pks
	deleteTimestamps map[UniqueID][]Timestamp
	deleteOffset     map[UniqueID]int64
}

// Name returns the name of insertNode
func (iNode *insertNode) Name() string {
	return "iNode"
}

// Operate handles input messages, to execute insert operations
func (iNode *insertNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	if len(in) != 1 {
		log.Warn("Invalid operate message input in insertNode", zap.Int("input length", len(in)))
		return []Msg{}
	}

	iMsg, ok := in[0].(*insertMsg)
	if !ok {
		if in[0] == nil {
			log.Debug("type assertion failed for insertMsg because it's nil")
		} else {
			log.Warn("type assertion failed for insertMsg", zap.String("name", reflect.TypeOf(in[0]).Name()))
		}
		return []Msg{}
	}

	iData := insertData{
		insertIDs:        make(map[UniqueID][]int64),
		insertTimestamps: make(map[UniqueID][]Timestamp),
		insertRecords:    make(map[UniqueID][]*schemapb.FieldData),
		insertOffset:     make(map[UniqueID]int64),
		insertPKs:        make(map[UniqueID][]primaryKey),
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
	// sort timestamps ensures that the data in iData.insertRecords is sorted in ascending order of timestamp
	// avoiding re-sorting in segCore, which will need data copying
	sort.Slice(iMsg.insertMessages, func(i, j int) bool {
		return iMsg.insertMessages[i].BeginTs() < iMsg.insertMessages[j].BeginTs()
	})
	for _, insertMsg := range iMsg.insertMessages {
		// if loadType is loadCollection, check if partition exists, if not, create partition
		col, err := iNode.streamingReplica.getCollectionByID(insertMsg.CollectionID)
		if err != nil {
			log.Warn("failed to get collection", zap.Error(err))
			continue
		}
		if col.getLoadType() == loadTypeCollection {
			err = iNode.streamingReplica.addPartition(insertMsg.CollectionID, insertMsg.PartitionID)
			if err != nil {
				log.Warn("failed to add partition", zap.Error(err))
				continue
			}
		}

		// check if segment exists, if not, create this segment
		if !iNode.streamingReplica.hasSegment(insertMsg.SegmentID) {
			err := iNode.streamingReplica.addSegment(insertMsg.SegmentID, insertMsg.PartitionID, insertMsg.CollectionID, insertMsg.ShardName, segmentTypeGrowing)
			if err != nil {
				log.Warn("failed to add segment", zap.Error(err))
				continue
			}
		}

		insertRecord, err := storage.TransferInsertMsgToInsertRecord(col.schema, insertMsg)
		if err != nil {
			log.Warn("failed to transfer msgStream.insertMsg to segcorepb.InsertRecord", zap.Error(err))
			return []Msg{}
		}

		iData.insertIDs[insertMsg.SegmentID] = append(iData.insertIDs[insertMsg.SegmentID], insertMsg.RowIDs...)
		iData.insertTimestamps[insertMsg.SegmentID] = append(iData.insertTimestamps[insertMsg.SegmentID], insertMsg.Timestamps...)
		if _, ok := iData.insertRecords[insertMsg.SegmentID]; !ok {
			iData.insertRecords[insertMsg.SegmentID] = insertRecord.FieldsData
		} else {
			typeutil.MergeFieldData(iData.insertRecords[insertMsg.SegmentID], insertRecord.FieldsData)
		}
		pks, err := getPrimaryKeys(insertMsg, iNode.streamingReplica)
		if err != nil {
			log.Warn("failed to get primary keys", zap.Error(err))
			continue
		}
		iData.insertPKs[insertMsg.SegmentID] = append(iData.insertPKs[insertMsg.SegmentID], pks...)
	}

	// 2. do preInsert
	for segmentID := range iData.insertRecords {
		var targetSegment, err = iNode.streamingReplica.getSegmentByID(segmentID)
		if err != nil {
			log.Warn(err.Error())
			continue
		}

		var numOfRecords = len(iData.insertIDs[segmentID])
		if targetSegment != nil {
			offset, err := targetSegment.segmentPreInsert(numOfRecords)
			if err != nil {
				log.Warn(err.Error())
				continue
			}
			iData.insertOffset[segmentID] = offset
			log.Debug("insertNode operator", zap.Int("insert size", numOfRecords), zap.Int64("insert offset", offset), zap.Int64("segment id", segmentID))
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
		deleteIDs:        make(map[UniqueID][]primaryKey),
		deleteTimestamps: make(map[UniqueID][]Timestamp),
		deleteOffset:     make(map[UniqueID]int64),
	}
	// 1. filter segment by bloom filter
	for _, delMsg := range iMsg.deleteMessages {
		if iNode.streamingReplica.getSegmentNum() != 0 {
			log.Debug("delete in streaming replica",
				zap.Any("collectionID", delMsg.CollectionID),
				zap.Any("collectionName", delMsg.CollectionName),
				zap.Int64("numPKs", delMsg.NumRows),
				zap.Any("timestamp", delMsg.Timestamps))
			processDeleteMessages(iNode.streamingReplica, delMsg, delData)
		}
	}

	// 2. do preDelete
	for segmentID, pks := range delData.deleteIDs {
		segment, err := iNode.streamingReplica.getSegmentByID(segmentID)
		if err != nil {
			log.Debug(err.Error())
			continue
		}
		offset := segment.segmentPreDelete(len(pks))
		delData.deleteOffset[segmentID] = offset
	}

	// 3. do delete
	for segmentID := range delData.deleteOffset {
		wg.Add(1)
		go iNode.delete(delData, segmentID, &wg)
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

// processDeleteMessages would execute delete operations for growing segments
func processDeleteMessages(replica ReplicaInterface, msg *msgstream.DeleteMsg, delData *deleteData) {
	var partitionIDs []UniqueID
	var err error
	if msg.PartitionID != -1 {
		partitionIDs = []UniqueID{msg.PartitionID}
	} else {
		partitionIDs, err = replica.getPartitionIDs(msg.CollectionID)
		if err != nil {
			log.Warn(err.Error())
			return
		}
	}
	resultSegmentIDs := make([]UniqueID, 0)
	for _, partitionID := range partitionIDs {
		segmentIDs, err := replica.getSegmentIDs(partitionID)
		if err != nil {
			log.Warn(err.Error())
			continue
		}
		resultSegmentIDs = append(resultSegmentIDs, segmentIDs...)
	}

	primaryKeys := storage.ParseIDs2PrimaryKeys(msg.PrimaryKeys)
	for _, segmentID := range resultSegmentIDs {
		segment, err := replica.getSegmentByID(segmentID)
		if err != nil {
			log.Warn(err.Error())
			continue
		}
		pks, tss, err := filterSegmentsByPKs(primaryKeys, msg.Timestamps, segment)
		if err != nil {
			log.Warn(err.Error())
			continue
		}
		if len(pks) > 0 {
			delData.deleteIDs[segmentID] = append(delData.deleteIDs[segmentID], pks...)
			delData.deleteTimestamps[segmentID] = append(delData.deleteTimestamps[segmentID], tss...)
		}
	}
}

// filterSegmentsByPKs would filter segments by primary keys
func filterSegmentsByPKs(pks []primaryKey, timestamps []Timestamp, segment *Segment) ([]primaryKey, []Timestamp, error) {
	if pks == nil {
		return nil, nil, fmt.Errorf("pks is nil when getSegmentsByPKs")
	}
	if segment == nil {
		return nil, nil, fmt.Errorf("segments is nil when getSegmentsByPKs")
	}

	retPks := make([]primaryKey, 0)
	retTss := make([]Timestamp, 0)
	buf := make([]byte, 8)
	for index, pk := range pks {
		exist := false
		switch pk.Type() {
		case schemapb.DataType_Int64:
			int64Pk := pk.(*int64PrimaryKey)
			common.Endian.PutUint64(buf, uint64(int64Pk.Value))
			exist = segment.pkFilter.Test(buf)
		case schemapb.DataType_VarChar:
			varCharPk := pk.(*varCharPrimaryKey)
			exist = segment.pkFilter.TestString(varCharPk.Value)
		default:
			return nil, nil, fmt.Errorf("invalid data type of delete primary keys")
		}
		if exist {
			retPks = append(retPks, pk)
			retTss = append(retTss, timestamps[index])
		}
	}
	return retPks, retTss, nil
}

// insert would execute insert operations for specific growing segment
func (iNode *insertNode) insert(iData *insertData, segmentID UniqueID, wg *sync.WaitGroup) {
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
	offsets := iData.insertOffset[segmentID]
	insertRecord := &segcorepb.InsertRecord{
		FieldsData: iData.insertRecords[segmentID],
		NumRows:    int64(len(ids)),
	}

	err = targetSegment.segmentInsert(offsets, ids, timestamps, insertRecord)
	if err != nil {
		log.Debug("QueryNode: targetSegmentInsert failed", zap.Error(err))
		// TODO: add error handling
		wg.Done()
		return
	}

	log.Debug("Do insert done", zap.Int("len", len(iData.insertIDs[segmentID])), zap.Int64("collectionID", targetSegment.collectionID), zap.Int64("segmentID", segmentID))
	wg.Done()
}

// delete would execute delete operations for specific growing segment
func (iNode *insertNode) delete(deleteData *deleteData, segmentID UniqueID, wg *sync.WaitGroup) {
	defer wg.Done()
	targetSegment, err := iNode.streamingReplica.getSegmentByID(segmentID)
	if err != nil {
		log.Warn(err.Error())
		return
	}

	if targetSegment.segmentType != segmentTypeGrowing {
		return
	}

	ids := deleteData.deleteIDs[segmentID]
	timestamps := deleteData.deleteTimestamps[segmentID]
	offset := deleteData.deleteOffset[segmentID]

	err = targetSegment.segmentDelete(offset, ids, timestamps)
	if err != nil {
		log.Warn("QueryNode: targetSegmentDelete failed", zap.Error(err))
		return
	}

	log.Debug("Do delete done", zap.Int("len", len(deleteData.deleteIDs[segmentID])), zap.Int64("segmentID", segmentID))
}

// TODO: remove this function to proper file
// getPrimaryKeys would get primary keys by insert messages
func getPrimaryKeys(msg *msgstream.InsertMsg, streamingReplica ReplicaInterface) ([]primaryKey, error) {
	if err := msg.CheckAligned(); err != nil {
		log.Warn("misaligned messages detected", zap.Error(err))
		return nil, err
	}
	collectionID := msg.GetCollectionID()

	collection, err := streamingReplica.getCollectionByID(collectionID)
	if err != nil {
		log.Warn(err.Error())
		return nil, err
	}

	return getPKs(msg, collection.schema)
}

func getPKs(msg *msgstream.InsertMsg, schema *schemapb.CollectionSchema) ([]primaryKey, error) {
	if msg.IsRowBased() {
		return getPKsFromRowBasedInsertMsg(msg, schema)
	}
	return getPKsFromColumnBasedInsertMsg(msg, schema)
}

func getPKsFromRowBasedInsertMsg(msg *msgstream.InsertMsg, schema *schemapb.CollectionSchema) ([]primaryKey, error) {
	offset := 0
	for _, field := range schema.Fields {
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
						log.Warn("strconv wrong on get dim", zap.Error(err))
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
						log.Warn("strconv wrong on get dim", zap.Error(err))
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
	pks := make([]primaryKey, len(blobReaders))

	for i, reader := range blobReaders {
		var int64PkValue int64
		err := binary.Read(reader, common.Endian, &int64PkValue)
		if err != nil {
			log.Warn("binary read blob value failed", zap.Error(err))
			return nil, err
		}
		pks[i] = newInt64PrimaryKey(int64PkValue)
	}

	return pks, nil
}

func getPKsFromColumnBasedInsertMsg(msg *msgstream.InsertMsg, schema *schemapb.CollectionSchema) ([]primaryKey, error) {
	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}

	primaryFieldData, err := typeutil.GetPrimaryFieldData(msg.GetFieldsData(), primaryFieldSchema)
	if err != nil {
		return nil, err
	}

	pks, err := storage.ParseFieldData2PrimaryKeys(primaryFieldData)
	if err != nil {
		return nil, err
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
