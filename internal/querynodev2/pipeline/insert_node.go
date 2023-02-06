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
	"fmt"
	"sort"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	base "github.com/milvus-io/milvus/internal/util/pipeline"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

type insertNode struct {
	*BaseNode
	collectionID int64
	channel      string
	manager      *DataManager
	delegator    delegator.ShardDelegator
}

func (iNode *insertNode) addInsertData(insertDatas map[UniqueID]*delegator.InsertData, msg *InsertMsg, collection *Collection) {
	insertRecord, err := storage.TransferInsertMsgToInsertRecord(collection.Schema(), msg)
	if err != nil {
		err = fmt.Errorf("failed to get primary keys, err = %d", err)
		log.Error(err.Error(), zap.Int64("collectionID", iNode.collectionID), zap.String("channel", iNode.channel))
		panic(err)
	}
	iData, ok := insertDatas[msg.SegmentID]
	if !ok {
		iData = &delegator.InsertData{
			PartitionID:  msg.PartitionID,
			InsertRecord: insertRecord,
			StartPosition: &internalpb.MsgPosition{
				Timestamp:   msg.BeginTs(),
				ChannelName: msg.GetShardName(),
			},
		}
		insertDatas[msg.SegmentID] = iData
	} else {
		typeutil.MergeFieldData(iData.InsertRecord.FieldsData, insertRecord.FieldsData)
		iData.InsertRecord.NumRows += insertRecord.NumRows
	}

	pks, err := segments.GetPrimaryKeys(msg, collection.Schema())
	if err != nil {
		log.Error("failed to get primary keys from insert message", zap.Error(err))
		panic(err)
	}

	iData.PrimaryKeys = append(iData.PrimaryKeys, pks...)
	iData.RowIDs = append(iData.RowIDs, msg.RowIDs...)
	iData.Timestamps = append(iData.Timestamps, msg.Timestamps...)
}

//Insert task
func (iNode *insertNode) Operate(in Msg) Msg {
	nodeMsg := in.(*insertNodeMsg)

	sort.Slice(nodeMsg.insertMsgs, func(i, j int) bool {
		return nodeMsg.insertMsgs[i].BeginTs() < nodeMsg.insertMsgs[j].BeginTs()
	})

	insertDatas := make(map[UniqueID]*delegator.InsertData)
	collection := iNode.manager.Collection.Get(iNode.collectionID)
	if collection == nil {
		log.Error("insertNode with collection not exist", zap.Int64("collection", iNode.collectionID))
		panic("insertNode with collection not exist")
	}

	//get InsertData and merge datas of same segment
	for _, msg := range nodeMsg.insertMsgs {
		iNode.addInsertData(insertDatas, msg, collection)
	}

	iNode.delegator.ProcessInsert(insertDatas)

	return &deleteNodeMsg{
		deleteMsgs: nodeMsg.deleteMsgs,
		timeRange:  nodeMsg.timeRange,
	}
}

func newInsertNode(
	collectionID UniqueID,
	channel string,
	manager *DataManager,
	delegator delegator.ShardDelegator,
	maxQueueLength int32,
) *insertNode {
	return &insertNode{
		BaseNode:     base.NewBaseNode(fmt.Sprintf("InsertNode-%s", channel), maxQueueLength),
		collectionID: collectionID,
		channel:      channel,
		manager:      manager,
		delegator:    delegator,
	}
}
