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
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/storage"
	base "github.com/milvus-io/milvus/internal/util/pipeline"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
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

	mlog.Info(context.TODO(), "pipeline fetch delete msg",
		mlog.FieldCollectionID(dNode.collectionID),
		mlog.FieldPartitionID(msg.PartitionID),
		mlog.Int("deleteRowNum", len(pks)),
		mlog.Uint64("timestampMin", msg.BeginTimestamp),
		mlog.Uint64("timestampMax", msg.EndTimestamp))
}

func (dNode *deleteNode) Operate(in Msg) Msg {
	metrics.QueryNodeWaitProcessingMsgCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.DeleteLabel).Dec()
	nodeMsg := in.(*deleteNodeMsg)

	if len(nodeMsg.deleteMsgs) > 0 {
		deleteDataByTs := make(map[uint64]map[UniqueID]*delegator.DeleteData)
		// deleteMsgs are ordered by WAL timetick within a vchannel; keep first-seen EndTs order
		// because the delete buffer expects non-decreasing timestamps on Put.
		tsOrder := make([]uint64, 0)

		for _, msg := range nodeMsg.deleteMsgs {
			ts := msg.EndTs()
			deleteDatas, ok := deleteDataByTs[ts]
			if !ok {
				deleteDatas = make(map[UniqueID]*delegator.DeleteData)
				deleteDataByTs[ts] = deleteDatas
				tsOrder = append(tsOrder, ts)
			}
			dNode.addDeleteData(deleteDatas, msg)
		}

		batches := make([]delegator.DeleteBatch, 0, len(tsOrder))
		for _, ts := range tsOrder {
			batches = append(batches, delegator.DeleteBatch{
				Ts:   ts,
				Data: lo.Values(deleteDataByTs[ts]),
			})
		}
		dNode.delegator.ProcessDeleteBatches(batches)
	}

	if nodeMsg.schema != nil {
		ctx := context.TODO()
		// A schema-change message must NOT be skipped: WAL order guarantees that
		// inserts carrying the new schema follow it on this vchannel, and
		// processing them against the old schema would silently drop the new
		// field. Retry in place with backoff — this intentionally blocks the
		// vchannel's consumption (tsafe stops advancing; reads keep serving the
		// previous ready schema) until the schema is applied. If retries are
		// exhausted the querynode panics and recovers by replaying the WAL.
		attempt := 0
		err := retry.Do(ctx, func() error {
			attempt++
			if err := dNode.delegator.UpdateSchema(ctx, nodeMsg.schema, nodeMsg.schemaBarrierTs); err != nil {
				mlog.Warn(ctx, "failed to update schema in delete node, retrying",
					mlog.Int64("collectionID", dNode.collectionID),
					mlog.String("channel", dNode.channel),
					mlog.Int32("schemaVersion", nodeMsg.schema.GetVersion()),
					mlog.Uint64("schemaBarrierTs", nodeMsg.schemaBarrierTs),
					mlog.Int("attempt", attempt),
					mlog.Err(err))
				return err
			}
			return nil
		}, retry.Attempts(paramtable.Get().QueryNodeCfg.SchemaUpdateRetryTimes.GetAsUint()),
			retry.Sleep(time.Second), retry.MaxSleepTime(time.Second*30))
		if err != nil {
			mlog.Fatal(ctx, "failed to apply schema change after retries, panicking to recover via WAL replay",
				mlog.Int64("collectionID", dNode.collectionID),
				mlog.String("channel", dNode.channel),
				mlog.Int32("schemaVersion", nodeMsg.schema.GetVersion()),
				mlog.Uint64("schemaBarrierTs", nodeMsg.schemaBarrierTs),
				mlog.Err(err))
		}
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
