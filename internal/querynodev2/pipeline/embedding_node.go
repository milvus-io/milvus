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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function"
	base "github.com/milvus-io/milvus/internal/util/pipeline"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type embeddingNode struct {
	*BaseNode

	collectionID int64
	channel      string

	manager *DataManager

	functionRunners []function.FunctionRunner
}

func newEmbeddingNode(collectionID int64, channelName string, manager *DataManager, maxQueueLength int32) (*embeddingNode, error) {
	collection := manager.Collection.Get(collectionID)
	if collection == nil {
		log.Error("embeddingNode init failed with collection not exist", zap.Int64("collection", collectionID))
		return nil, merr.WrapErrCollectionNotFound(collectionID)
	}

	if len(collection.Schema().GetFunctions()) == 0 {
		return nil, nil
	}

	node := &embeddingNode{
		BaseNode:        base.NewBaseNode(fmt.Sprintf("EmbeddingNode-%s", channelName), maxQueueLength),
		collectionID:    collectionID,
		channel:         channelName,
		manager:         manager,
		functionRunners: make([]function.FunctionRunner, 0),
	}

	for _, tf := range collection.Schema().GetFunctions() {
		functionRunner, err := function.NewFunctionRunner(collection.Schema(), tf)
		if err != nil {
			return nil, err
		}
		if functionRunner == nil {
			continue
		}
		node.functionRunners = append(node.functionRunners, functionRunner)
	}
	return node, nil
}

func (eNode *embeddingNode) Name() string {
	return fmt.Sprintf("embeddingNode-%s", eNode.channel)
}

func (eNode *embeddingNode) addInsertData(insertDatas map[UniqueID]*delegator.InsertData, msg *InsertMsg, collection *Collection) error {
	iData, ok := insertDatas[msg.SegmentID]
	if !ok {
		iData = &delegator.InsertData{
			PartitionID: msg.PartitionID,
			BM25Stats:   make(map[int64]*storage.BM25Stats),
			StartPosition: &msgpb.MsgPosition{
				Timestamp:   msg.BeginTs(),
				ChannelName: msg.GetShardName(),
			},
		}
		insertDatas[msg.SegmentID] = iData
	}

	err := eNode.embedding(msg, iData.BM25Stats)
	if err != nil {
		log.Error("failed to function data", zap.Error(err))
		return err
	}

	insertRecord, err := storage.TransferInsertMsgToInsertRecord(collection.Schema(), msg)
	if err != nil {
		err = fmt.Errorf("failed to get primary keys, err = %d", err)
		log.Error(err.Error(), zap.String("channel", eNode.channel))
		return err
	}

	if iData.InsertRecord == nil {
		iData.InsertRecord = insertRecord
	} else {
		err := typeutil.MergeFieldData(iData.InsertRecord.FieldsData, insertRecord.FieldsData)
		if err != nil {
			log.Warn("failed to merge field data", zap.String("channel", eNode.channel), zap.Error(err))
			return err
		}
		iData.InsertRecord.NumRows += insertRecord.NumRows
	}

	pks, err := segments.GetPrimaryKeys(msg, collection.Schema())
	if err != nil {
		log.Warn("failed to get primary keys from insert message", zap.String("channel", eNode.channel), zap.Error(err))
		return err
	}

	iData.PrimaryKeys = append(iData.PrimaryKeys, pks...)
	iData.RowIDs = append(iData.RowIDs, msg.RowIDs...)
	iData.Timestamps = append(iData.Timestamps, msg.Timestamps...)
	log.Ctx(context.TODO()).Debug("pipeline embedding insert msg",
		zap.Int64("collectionID", eNode.collectionID),
		zap.Int64("segmentID", msg.SegmentID),
		zap.Int("insertRowNum", len(pks)),
		zap.Uint64("timestampMin", msg.BeginTimestamp),
		zap.Uint64("timestampMax", msg.EndTimestamp))
	return nil
}

func (eNode *embeddingNode) bm25Embedding(runner function.FunctionRunner, msg *msgstream.InsertMsg, stats map[int64]*storage.BM25Stats) error {
	functionSchema := runner.GetSchema()
	inputFieldID := functionSchema.GetInputFieldIds()[0]
	outputFieldID := functionSchema.GetOutputFieldIds()[0]
	outputField := runner.GetOutputFields()[0]

	data, err := GetEmbeddingFieldData(msg.GetFieldsData(), inputFieldID)
	if data == nil || err != nil {
		return merr.WrapErrFieldNotFound(fmt.Sprint(inputFieldID))
	}

	output, err := runner.BatchRun(data)
	if err != nil {
		return err
	}

	sparseArray, ok := output[0].(*schemapb.SparseFloatArray)
	if !ok {
		return fmt.Errorf("BM25 runner return unknown type output")
	}

	if _, ok := stats[outputFieldID]; !ok {
		stats[outputFieldID] = storage.NewBM25Stats()
	}
	stats[outputFieldID].AppendBytes(sparseArray.GetContents()...)
	msg.FieldsData = append(msg.FieldsData, delegator.BuildSparseFieldData(outputField, sparseArray))
	return nil
}

func (eNode *embeddingNode) embedding(msg *msgstream.InsertMsg, stats map[int64]*storage.BM25Stats) error {
	for _, functionRunner := range eNode.functionRunners {
		functionSchema := functionRunner.GetSchema()
		switch functionSchema.GetType() {
		case schemapb.FunctionType_BM25:
			err := eNode.bm25Embedding(functionRunner, msg, stats)
			if err != nil {
				return err
			}
		default:
			log.Warn("pipeline embedding with unknown function type", zap.Any("type", functionSchema.GetType()))
			return fmt.Errorf("unknown function type")
		}
	}

	return nil
}

func (eNode *embeddingNode) Operate(in Msg) Msg {
	nodeMsg := in.(*insertNodeMsg)
	nodeMsg.insertDatas = make(map[int64]*delegator.InsertData)

	collection := eNode.manager.Collection.Get(eNode.collectionID)
	if collection == nil {
		log.Error("embeddingNode with collection not exist", zap.Int64("collection", eNode.collectionID))
		panic("embeddingNode with collection not exist")
	}

	for _, msg := range nodeMsg.insertMsgs {
		err := eNode.addInsertData(nodeMsg.insertDatas, msg, collection)
		if err != nil {
			panic(err)
		}
	}

	return nodeMsg
}

func GetEmbeddingFieldData(datas []*schemapb.FieldData, fieldID int64) ([]string, error) {
	for _, data := range datas {
		if data.GetFieldId() == fieldID {
			return data.GetScalars().GetStringData().GetData(), nil
		}
	}
	return nil, fmt.Errorf("field %d not found", fieldID)
}
