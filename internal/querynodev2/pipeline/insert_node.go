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
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/registry"
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

// verifyPayloadFields checks the payload columns against the message's own era
// schema. Era-consistent producers (proxy validation plus WAL-append
// materialization) never emit a column outside the schema the message was
// written under, so a payload field missing from its era schema can only be a
// corrupted payload and must not be silently dropped. When no era schema is
// available (fallback to the current schema), this check is skipped by the
// caller: an unknown field there is either a since-dropped field skipped by the
// downstream filter, or left to segcore's own invariant check.
func (iNode *insertNode) verifyPayloadFields(msg *InsertMsg, eraSchema *schemapb.CollectionSchema, eraFields typeutil.Set[int64]) error {
	for _, fieldData := range msg.GetFieldsData() {
		if !eraFields.Contain(fieldData.GetFieldId()) {
			return merr.WrapErrServiceInternal(fmt.Sprintf(
				"insert payload of segment %d carries field %d absent from its era schema (version %d), payload is corrupted",
				msg.SegmentID, fieldData.GetFieldId(), eraSchema.GetVersion()))
		}
	}
	return nil
}

func (iNode *insertNode) addInsertData(insertDatas map[UniqueID]*delegator.InsertData, msg *InsertMsg, schemaForMsg *schemapb.CollectionSchema, eraForCreation *schemapb.CollectionSchema) {
	insertRecord, err := storage.TransferInsertMsgToInsertRecord(schemaForMsg, msg)
	if err != nil {
		err = merr.Wrap(err, "failed to get primary keys")
		mlog.Error(context.TODO(), err.Error(), mlog.Int64("collectionID", iNode.collectionID), mlog.String("channel", iNode.channel))
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
			// The first message decides the creation schema of a not-yet-known
			// growing segment; the fence on schema change guarantees all its
			// messages share one era.
			Schema: eraForCreation,
		}
		insertDatas[msg.SegmentID] = iData
	} else {
		err := typeutil.MergeFieldData(iData.InsertRecord.FieldsData, insertRecord.FieldsData)
		if err != nil {
			mlog.Error(context.TODO(), "failed to merge field data", mlog.String("channel", iNode.channel), mlog.Err(err))
			panic(err)
		}
		iData.InsertRecord.NumRows += insertRecord.NumRows
	}

	if err := iNode.appendBM25Stats(iData, msg, schemaForMsg); err != nil {
		mlog.Error(context.TODO(), "failed to append BM25 stats from insert message", mlog.String("channel", iNode.channel), mlog.Err(err))
		panic(err)
	}

	pks, err := segments.GetPrimaryKeys(msg, schemaForMsg)
	if err != nil {
		mlog.Error(context.TODO(), "failed to get primary keys from insert message", mlog.Err(err))
		panic(err)
	}

	iData.PrimaryKeys = append(iData.PrimaryKeys, pks...)
	iData.RowIDs = append(iData.RowIDs, msg.RowIDs...)
	iData.Timestamps = append(iData.Timestamps, msg.Timestamps...)
	mlog.Debug(context.TODO(), "pipeline fetch insert msg",
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

		// The era-schema resolver exists only when this process serves the
		// channel's WAL (streaming node). The era schema equals the current
		// schema on the live path and differs only while replaying messages
		// written before later schema changes.
		resolver, resolverErr := registry.GetLocalSchemaResolver(iNode.channel)

		// Field-id sets memoized per distinct schema snapshot in this batch.
		fieldSets := make(map[*schemapb.CollectionSchema]typeutil.Set[int64])
		getFieldSet := func(s *schemapb.CollectionSchema) typeutil.Set[int64] {
			set, ok := fieldSets[s]
			if !ok {
				set = typeutil.NewSet[int64]()
				for _, field := range typeutil.GetAllFieldSchemas(s) {
					set.Insert(field.GetFieldID())
				}
				fieldSets[s] = set
			}
			return set
		}

		insertDatas := make(map[UniqueID]*delegator.InsertData)
		for _, msg := range nodeMsg.insertMsgs {
			// Interpret each message under the schema of its own era so that
			// payload verification, function-output fill and segment creation
			// all share one schema snapshot; fall back to the current schema
			// when no local schema history is available.
			schemaForMsg, isEra := schema, false
			if resolverErr == nil {
				eraSchema, err := resolver.GetSchema(context.TODO(), iNode.channel, msg.EndTs())
				if err != nil || eraSchema == nil {
					mlog.Warn(context.TODO(), "failed to resolve era schema for insert message, fall back to current schema",
						mlog.String("channel", iNode.channel),
						mlog.Uint64("timetick", msg.EndTs()),
						mlog.Err(err))
				} else {
					schemaForMsg, isEra = eraSchema, true
				}
			}
			if isEra {
				if err := iNode.verifyPayloadFields(msg, schemaForMsg, getFieldSet(schemaForMsg)); err != nil {
					mlog.Error(context.TODO(), "corrupted insert payload detected", mlog.String("channel", iNode.channel), mlog.Err(err))
					panic(err)
				}
			}
			functionOutputFieldIDs, err := iNode.functionStore.OutputFieldIDs(schemaForMsg)
			if err != nil {
				mlog.Error(context.TODO(), "failed to get embedding output fields", mlog.String("channel", iNode.channel), mlog.Err(err))
				panic(err)
			}
			if len(functionOutputFieldIDs) > 0 && !function.HasAllFieldDataByID(msg.GetFieldsData(), functionOutputFieldIDs) {
				if err := iNode.functionStore.FillEmbeddingData(iNode.collectionID, schemaForMsg, msg.InsertRequest); err != nil {
					mlog.Error(context.TODO(), "failed to fill embedding data for insert message", mlog.String("channel", iNode.channel), mlog.Err(err))
					panic(err)
				}
			}
			// Only a genuinely older era schema is carried to segment creation;
			// the live path (era == current) keeps the existing creation flow.
			eraForCreation := schemaForMsg
			if !isEra || schemaForMsg.GetVersion() == schema.GetVersion() {
				eraForCreation = nil
			}
			iNode.addInsertData(insertDatas, msg, schemaForMsg, eraForCreation)
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
