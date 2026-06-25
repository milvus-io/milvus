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

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function"
	base "github.com/milvus-io/milvus/internal/util/pipeline"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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

	functionStore *function.FunctionRunnerLocalStore
}

func (iNode *insertNode) addInsertData(insertDatas map[UniqueID]*delegator.InsertData, msg *InsertMsg, collection *Collection) {
	ctx := msg.TraceCtx()
	schema := collection.Schema()
	insertRecord, skippedFields, err := storage.TransferInsertMsgToInsertRecord(schema, msg)
	if err != nil {
		err = merr.Wrap(err, "failed to get primary keys")
		mlog.Error(ctx, err.Error(), mlog.Int64("collectionID", iNode.collectionID), mlog.String("channel", iNode.channel))
		panic(err)
	}
	if len(skippedFields) > 0 {
		// Attributing the skip to dropped fields is safe only because a SchemaChange
		// message never shares a pack with inserts: the WAL adaptor emits every V1/V2
		// message as its own pack, msgdispatcher never merges packs, and the DML
		// micro-batcher refuses to merge non-Insert/Delete messages. If that
		// pack-granularity invariant is ever relaxed, filtering against the current
		// schema could silently drop fields that exist in a newer schema.
		mlog.Warn(ctx, "skip insert payload fields absent from current schema, fields are dropped since the message was written",
			mlog.FieldCollectionID(iNode.collectionID),
			mlog.FieldSegmentID(msg.SegmentID),
			mlog.String("channel", iNode.channel),
			mlog.Int32("schemaVersion", schema.GetVersion()),
			mlog.Int64s("skippedFieldIDs", skippedFields))
		metrics.QueryNodeSkippedInsertFieldCount.
			WithLabelValues(paramtable.GetStringNodeID(), fmt.Sprint(iNode.collectionID)).
			Add(float64(len(skippedFields)))
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
			mlog.Error(ctx, "failed to merge field data", mlog.String("channel", iNode.channel), mlog.Err(err))
			panic(err)
		}
		iData.InsertRecord.NumRows += insertRecord.NumRows
	}

	if err := iNode.appendBM25Stats(iData, msg, schema); err != nil {
		mlog.Error(ctx, "failed to append BM25 stats from insert message", mlog.String("channel", iNode.channel), mlog.Err(err))
		panic(err)
	}

	pks, err := segments.GetPrimaryKeys(msg, schema)
	if err != nil {
		mlog.Error(ctx, "failed to get primary keys from insert message", mlog.Err(err))
		panic(err)
	}

	iData.PrimaryKeys = append(iData.PrimaryKeys, pks...)
	iData.RowIDs = append(iData.RowIDs, msg.RowIDs...)
	iData.Timestamps = append(iData.Timestamps, msg.Timestamps...)
	mlog.Debug(ctx, "pipeline fetch insert msg",
		mlog.Int64("collectionID", iNode.collectionID),
		mlog.Int64("segmentID", msg.SegmentID),
		mlog.Int("insertRowNum", len(pks)),
		mlog.Uint64("timestampMin", msg.BeginTimestamp),
		mlog.Uint64("timestampMax", msg.EndTimestamp))
}

func (iNode *insertNode) Close() {
	iNode.functionStore.Close()
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
		functionOutputFieldIDs, err := iNode.functionStore.OutputFieldIDs(schema)
		if err != nil {
			mlog.Error(context.TODO(), "failed to get embedding output fields", mlog.String("channel", iNode.channel), mlog.Err(err))
			panic(err)
		}

		insertDatas := make(map[UniqueID]*delegator.InsertData)
		for _, msg := range nodeMsg.insertMsgs {
			if len(functionOutputFieldIDs) > 0 && !function.HasAllFieldDataByID(msg.GetFieldsData(), functionOutputFieldIDs) {
				if err := iNode.functionStore.FillEmbeddingData(iNode.collectionID, schema, msg.InsertRequest); err != nil {
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
		BaseNode:      base.NewBaseNode(fmt.Sprintf("InsertNode-%s", channel), maxQueueLength),
		collectionID:  collectionID,
		channel:       channel,
		manager:       manager,
		delegator:     delegator,
		functionStore: function.NewFunctionRunnerLocalStore(),
	}
	if _, err := iNode.functionStore.OutputFieldIDs(schema); err != nil {
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
