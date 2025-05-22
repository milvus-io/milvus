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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/storage"
	base "github.com/milvus-io/milvus/internal/util/pipeline"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type deleteNode struct {
	*BaseNode
	collectionID UniqueID
	channel      string

	manager   *DataManager
	delegator delegator.ShardDelegator
}

// addDeleteData find the segment of delete column in DeleteMsg and save in deleteData
func (dNode *deleteNode) addDeleteData(deleteDatas map[UniqueID]*delegator.DeleteData, msg *DeleteMsg) {
	deleteData, ok := deleteDatas[msg.PartitionID]
	if !ok {
		deleteData = &delegator.DeleteData{
			PartitionID: msg.PartitionID,
		}
		deleteDatas[msg.PartitionID] = deleteData
	}
	pks := storage.ParseIDs2PrimaryKeys(msg.PrimaryKeys)
	deleteData.PrimaryKeys = append(deleteData.PrimaryKeys, pks...)
	deleteData.Timestamps = append(deleteData.Timestamps, msg.Timestamps...)
	deleteData.RowCount += int64(len(pks))

	log.Info("pipeline fetch delete msg",
		zap.Int64("collectionID", dNode.collectionID),
		zap.Int64("partitionID", msg.PartitionID),
		zap.Int("deleteRowNum", len(pks)),
		zap.Uint64("timestampMin", msg.BeginTimestamp),
		zap.Uint64("timestampMax", msg.EndTimestamp))
}

func (dNode *deleteNode) Operate(in Msg) Msg {
	metrics.QueryNodeWaitProcessingMsgCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.DeleteLabel).Dec()
	nodeMsg := in.(*deleteNodeMsg)

	if len(nodeMsg.deleteMsgs) > 0 {
		// partition id = > DeleteData
		deleteDatas := make(map[UniqueID]*delegator.DeleteData)

		for _, msg := range nodeMsg.deleteMsgs {
			dNode.addDeleteData(deleteDatas, msg)
		}
		// do Delete, use ts range max as ts
		dNode.delegator.ProcessDelete(lo.Values(deleteDatas), nodeMsg.timeRange.timestampMax)
	}

	if nodeMsg.schema != nil {
		dNode.delegator.UpdateSchema(context.Background(), nodeMsg.schema, nodeMsg.schemaVersion)
	}

	// update tSafe
	dNode.delegator.UpdateTSafe(nodeMsg.timeRange.timestampMax)
	return nil
}

func newDeleteNode(
	collectionID UniqueID, channel string,
	manager *DataManager, delegator delegator.ShardDelegator,
	maxQueueLength int32,
) *deleteNode {
	return &deleteNode{
		BaseNode:     base.NewBaseNode(fmt.Sprintf("DeleteNode-%s", channel), maxQueueLength),
		collectionID: collectionID,
		channel:      channel,
		manager:      manager,
		delegator:    delegator,
	}
}
