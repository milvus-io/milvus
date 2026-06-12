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

package pipeline

import (
	"context"
	"fmt"
	"sort"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function"
	base "github.com/milvus-io/milvus/internal/util/pipeline"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type insertNode struct {
	*BaseNode
	collectionID int64
	channel      string
	manager      *DataManager
	delegator    delegator.ShardDelegator

	functionRunners        map[int32][]function.FunctionRunner
	functionOutputFieldIDs map[int32][]int64
}

func (iNode *insertNode) addInsertData(insertDatas map[UniqueID]*delegator.InsertData, msg *InsertMsg, collection *Collection) {
	insertRecord, err := storage.TransferInsertMsgToInsertRecord(collection.Schema(), msg)
	if err != nil {
		err = merr.Wrap(err, "failed to get primary keys")
		log.Error(err.Error(), zap.Int64("collectionID", iNode.collectionID), zap.String("channel", iNode.channel))
		panic(err)
	}
	iData, ok := insertDatas[msg.SegmentID]
	if !ok {
		iData = &delegator.InsertData{
			PartitionID:  msg.PartitionID,
			InsertRecord: insertRecord,
			StartPosition: &msgpb.MsgPosition{
				Timestamp:   msg.BeginTs(),
				ChannelName: msg.GetShardName(),
			},
		}
		insertDatas[msg.SegmentID] = iData
	} else {
		err := typeutil.MergeFieldData(iData.InsertRecord.FieldsData, insertRecord.FieldsData)
		if err != nil {
			log.Error("failed to merge field data", zap.String("channel", iNode.channel), zap.Error(err))
			panic(err)
		}
		iData.InsertRecord.NumRows += insertRecord.NumRows
	}

	if err := iNode.appendBM25Stats(iData, msg, collection.Schema()); err != nil {
		mlog.Error(context.TODO(), "failed to append BM25 stats from insert message", mlog.String("channel", iNode.channel), mlog.Err(err))
		panic(err)
	}

	pks, err := segments.GetPrimaryKeys(msg, collection.Schema())
	if err != nil {
		log.Error("failed to get primary keys from insert message", zap.Error(err))
		panic(err)
	}

	iData.PrimaryKeys = append(iData.PrimaryKeys, pks...)
	iData.RowIDs = append(iData.RowIDs, msg.RowIDs...)
	iData.Timestamps = append(iData.Timestamps, msg.Timestamps...)
	log.Ctx(context.TODO()).Debug("pipeline fetch insert msg",
		zap.Int64("collectionID", iNode.collectionID),
		zap.Int64("segmentID", msg.SegmentID),
		zap.Int("insertRowNum", len(pks)),
		zap.Uint64("timestampMin", msg.BeginTimestamp),
		zap.Uint64("timestampMax", msg.EndTimestamp))
}

// fillEmbeddingData is only used to handle old insert messages that were not embedded before WAL append.
func (iNode *insertNode) fillEmbeddingData(schema *schemapb.CollectionSchema, msg *InsertMsg) error {
	if !function.HasEmbeddingFunctions(schema) {
		return nil
	}
	schemaVersion := schema.GetVersion()
	_, ok, err := function.TryMaterialize(iNode.collectionID, schemaVersion, msg.InsertRequest)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	runners, ok := iNode.functionRunners[schemaVersion]
	if !ok {
		runners, err = function.BuildEmbeddingRunners(schema)
		if err != nil {
			return err
		}
		iNode.functionRunners[schemaVersion] = runners
	}
	_, err = function.FillFunctionFields(runners, msg.InsertRequest)
	return err
}

func (iNode *insertNode) getEmbeddingOutputFieldIDs(schema *schemapb.CollectionSchema) ([]int64, error) {
	schemaVersion := schema.GetVersion()
	if outputFieldIDs, ok := iNode.functionOutputFieldIDs[schemaVersion]; ok {
		return outputFieldIDs, nil
	}

	if !function.HasEmbeddingFunctions(schema) {
		iNode.functionOutputFieldIDs[schemaVersion] = nil
		return nil, nil
	}
	outputFieldIDs, err := function.EmbeddingOutputFieldIDs(schema)
	if err != nil {
		return nil, err
	}
	iNode.functionOutputFieldIDs[schemaVersion] = outputFieldIDs
	return outputFieldIDs, nil
}

func (iNode *insertNode) Close() {
	for _, runners := range iNode.functionRunners {
		function.CloseRunners(runners)
	}
	iNode.functionRunners = make(map[int32][]function.FunctionRunner)
	iNode.functionOutputFieldIDs = make(map[int32][]int64)
}

// Insert task
func (iNode *insertNode) Operate(in Msg) Msg {
	metrics.QueryNodeWaitProcessingMsgCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.InsertLabel).Dec()
	nodeMsg := in.(*insertNodeMsg)

	if len(nodeMsg.insertMsgs) > 0 {
		sort.Slice(nodeMsg.insertMsgs, func(i, j int) bool {
			return nodeMsg.insertMsgs[i].BeginTs() < nodeMsg.insertMsgs[j].BeginTs()
		})

		collection := iNode.manager.Collection.Get(iNode.collectionID)
		if collection == nil {
			mlog.Error(context.TODO(), "insertNode with collection not exist", mlog.Int64("collection", iNode.collectionID))
			panic("insertNode with collection not exist")
		}
		schema := collection.Schema()
		functionOutputFieldIDs, err := iNode.getEmbeddingOutputFieldIDs(schema)
		if err != nil {
			mlog.Error(context.TODO(), "failed to get embedding output fields", mlog.String("channel", iNode.channel), mlog.Err(err))
			panic(err)
		}

		insertDatas := make(map[UniqueID]*delegator.InsertData)
		for _, msg := range nodeMsg.insertMsgs {
			if len(functionOutputFieldIDs) > 0 && !function.HasAllFieldDataByID(msg.GetFieldsData(), functionOutputFieldIDs) {
				if err := iNode.fillEmbeddingData(schema, msg); err != nil {
					mlog.Error(context.TODO(), "failed to fill embedding data for insert message", mlog.String("channel", iNode.channel), mlog.Err(err))
					panic(err)
				}
			}
			iNode.addInsertData(insertDatas, msg, collection)
		}

		iNode.delegator.ProcessInsert(insertDatas)
	}
	metrics.QueryNodeWaitProcessingMsgCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.DeleteLabel).Inc()

	return &deleteNodeMsg{
		deleteMsgs:      nodeMsg.deleteMsgs,
		timeRange:       nodeMsg.timeRange,
		schema:          nodeMsg.schema,
		schemaBarrierTs: nodeMsg.schemaBarrierTs,
	}
}

func newInsertNode(
	collectionID UniqueID,
	channel string,
	manager *DataManager,
	delegator delegator.ShardDelegator,
	schema *schemapb.CollectionSchema,
	maxQueueLength int32,
) (*insertNode, error) {
	iNode := &insertNode{
		BaseNode:               base.NewBaseNode(fmt.Sprintf("InsertNode-%s", channel), maxQueueLength),
		collectionID:           collectionID,
		channel:                channel,
		manager:                manager,
		delegator:              delegator,
		functionRunners:        make(map[int32][]function.FunctionRunner),
		functionOutputFieldIDs: make(map[int32][]int64),
	}
	if _, err := iNode.getEmbeddingOutputFieldIDs(schema); err != nil {
		return nil, err
	}
	return iNode, nil
}

func (iNode *insertNode) appendBM25Stats(iData *delegator.InsertData, msg *InsertMsg, schema *schemapb.CollectionSchema) error {
	outputFieldIDs, err := getBM25OutputFieldIDs(schema)
	if err != nil {
		return err
	}
	if len(outputFieldIDs) == 0 {
		return nil
	}
	if iData.BM25Stats == nil {
		iData.BM25Stats = make(map[int64]*storage.BM25Stats)
	}

	for _, outputFieldID := range outputFieldIDs {
		outputData := getFieldData(msg.FieldsData, outputFieldID)
		if outputData == nil {
			return merr.WrapErrFunctionFailedMsg("BM25 output field %d not found in insert message", outputFieldID)
		}
		if err := appendBM25StatsFromFieldData(iData.BM25Stats, outputFieldID, outputData); err != nil {
			return err
		}
	}
	return nil
}

func getBM25OutputFieldIDs(schema *schemapb.CollectionSchema) ([]int64, error) {
	outputFieldIDs := make([]int64, 0)
	for _, fn := range schema.GetFunctions() {
		if fn.GetType() != schemapb.FunctionType_BM25 {
			continue
		}

		outputField := typeutil.GetFunctionOutputField(schema, fn)
		if outputField == nil {
			return nil, merr.WrapErrFunctionFailedMsg("function %s output field not found", fn.GetName())
		}

		outputFieldIDs = append(outputFieldIDs, outputField.GetFieldID())
	}
	return outputFieldIDs, nil
}

func appendBM25StatsFromFieldData(stats map[int64]*storage.BM25Stats, outputFieldID int64, fieldData *schemapb.FieldData) error {
	sparseArray := fieldData.GetVectors().GetSparseFloatVector()
	if sparseArray == nil {
		return merr.WrapErrFunctionFailedMsg("BM25 output field %d is not sparse float vector data", outputFieldID)
	}
	if _, ok := stats[outputFieldID]; !ok {
		stats[outputFieldID] = storage.NewBM25Stats()
	}
	stats[outputFieldID].AppendBytes(sparseArray.GetContents()...)
	return nil
}

func getFieldData(datas []*schemapb.FieldData, fieldID int64) *schemapb.FieldData {
	for _, data := range datas {
		if data.GetFieldId() == fieldID {
			return data
		}
	}
	return nil
}
