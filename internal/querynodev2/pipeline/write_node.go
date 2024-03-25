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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	base "github.com/milvus-io/milvus/internal/util/pipeline"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type writeNode struct {
	*BaseNode
	collectionID int64
	channel      string
	manager      *DataManager // segments.SegmentManager
	delegator    delegator.ShardDelegator
	tSafeManager TSafeManager
}

// organizeInsertMsgs organizes insert messages into segment based insert data batches.
func (wNode *writeNode) organizeInsertMsgs(insertMsgs []*msgstream.InsertMsg) ([]*delegator.InsertData, error) {
	schema := wNode.manager.Collection.Get(wNode.collectionID).Schema()

	groups := lo.GroupBy(insertMsgs, func(msg *msgstream.InsertMsg) int64 { return msg.GetSegmentID() })
	segmentPartition := lo.SliceToMap(insertMsgs, func(msg *msgstream.InsertMsg) (int64, int64) { return msg.GetSegmentID(), msg.GetPartitionID() })

	batches := make([]*delegator.InsertData, 0, len(groups))

	for segment, msgs := range groups {
		inData := &delegator.InsertData{
			SegmentID:   segment,
			PartitionID: segmentPartition[segment],
			Data:        make([]*storage.InsertData, 0, len(msgs)),
			PkData:      make([]storage.FieldData, 0, len(msgs)),
			TsData:      make([]*storage.Int64FieldData, 0, len(msgs)),
		}
		for _, msg := range msgs {
			insertRecord, err := storage.TransferInsertMsgToInsertRecord(schema, msg)
			if err != nil {
				return nil, err
			}
			if inData.InsertRecord == nil {
				inData.InsertRecord = insertRecord
			} else {
				err = typeutil.MergeFieldData(inData.InsertRecord.FieldsData, insertRecord.FieldsData)
				if err != nil {
					return nil, err
				}
				inData.InsertRecord.NumRows += insertRecord.NumRows
			}
			pks, err := segments.GetPrimaryKeys(msg, schema)
			if err != nil {
				log.Error("failed to get primary keys from insert message", zap.Error(err))
				return nil, err
			}

			inData.PrimaryKeys = append(inData.PrimaryKeys, pks...)
			inData.RowIDs = append(inData.RowIDs, msg.GetRowIDs()...)
			inData.Timestamps = append(inData.Timestamps, msg.GetTimestamps()...)
		}
		inData.GeneratePkStats()
		batches = append(batches, inData)
	}
	return batches, nil
}

// organizeDeleteMsgs organizes delete messages into partition based groups.
func (wNode *writeNode) organizeDeleteMsgs(deleteMsgs []*msgstream.DeleteMsg) []*delegator.DeleteData {
	deleteData := make(map[int64]*delegator.DeleteData)
	for _, msg := range deleteMsgs {
		entry, ok := deleteData[msg.PartitionID]
		if !ok {
			entry = &delegator.DeleteData{
				PartitionID: msg.PartitionID,
			}
			deleteData[msg.PartitionID] = entry
		}
		pks := storage.ParseIDs2PrimaryKeys(msg.PrimaryKeys)
		entry.PrimaryKeys = append(entry.PrimaryKeys, pks...)
		entry.Timestamps = append(entry.Timestamps, msg.Timestamps...)
		entry.RowCount += int64(len(pks))

		log.Info("pipeline fetch delete msg",
			zap.Int64("collectionID", wNode.collectionID),
			zap.Int64("partitionID", msg.PartitionID),
			zap.Int("insertRowNum", len(pks)),
			zap.Uint64("timestampMin", msg.BeginTimestamp),
			zap.Uint64("timestampMax", msg.EndTimestamp))
	}

	return lo.Values(deleteData)
}

func (wNode *writeNode) Operate(in Msg) Msg {
	metrics.QueryNodeWaitProcessingMsgCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.InsertLabel).Dec()
	nodeMsg := in.(*insertNodeMsg)

	sort.Slice(nodeMsg.insertMsgs, func(i, j int) bool {
		return nodeMsg.insertMsgs[i].BeginTs() < nodeMsg.insertMsgs[j].BeginTs()
	})
	insertData, err := wNode.organizeInsertMsgs(nodeMsg.insertMsgs)
	if err != nil {
		log.Error("failed to organize insert message into InsertData", zap.Error(err))
		panic(err)
	}
	deleteData := wNode.organizeDeleteMsgs(nodeMsg.deleteMsgs)

	err = wNode.delegator.ProcessData(context.Background(), insertData, deleteData, nodeMsg.timeRange.timestampMin, nodeMsg.timeRange.timestampMax)
	if err != nil {
		log.Error("failed to process msg patch data", zap.Error(err))
		panic(err)
	}

	// update tSafe
	err = wNode.tSafeManager.Set(wNode.channel, nodeMsg.timeRange.timestampMax)
	if err != nil {
		// should not happen, QueryNode should addTSafe before start pipeline
		panic(fmt.Errorf("serviceTimeNode setTSafe timeout, collectionID = %d, err = %s", wNode.collectionID, err))
	}
	return nil
}

func newWriteNode(
	collectionID UniqueID,
	channel string,
	manager *DataManager,
	tSafeManager TSafeManager,
	delegator delegator.ShardDelegator,
	maxQueueLength int32,
) *writeNode {
	return &writeNode{
		BaseNode:     base.NewBaseNode(fmt.Sprintf("WriteNode-%s", channel), maxQueueLength),
		collectionID: collectionID,
		channel:      channel,
		manager:      manager,
		tSafeManager: tSafeManager,
		delegator:    delegator,
	}
}
