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
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/storage"
	base "github.com/milvus-io/milvus/internal/util/pipeline"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type deleteNode struct {
	*BaseNode
	collectionID UniqueID
	channel      string

	manager   *DataManager
	delegator delegator.ShardDelegator
	closed    atomic.Bool
	ctx       context.Context
	cancel    context.CancelFunc
	closeCh   chan struct{}
	closeOnce sync.Once
}

var (
	deleteNodeUpdateSchemaRetryInterval    = 200 * time.Millisecond
	deleteNodeUpdateSchemaMaxRetryDuration = 5 * time.Minute
)

// addDeleteData find the segment of delete column in DeleteMsg and save in deleteData
func (dNode *deleteNode) addDeleteData(deleteDatas map[UniqueID]*delegator.DeleteData, msg *DeleteMsg) {
	ctx := msg.TraceCtx()
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

	mlog.Info(ctx, "pipeline fetch delete msg",
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
		if !dNode.updateSchemaUntilApplied(dNode.ctx, nodeMsg) {
			return nil
		}
	}

	// update tSafe
	dNode.delegator.UpdateTSafe(nodeMsg.timeRange.timestampMax)
	return nil
}

func (dNode *deleteNode) updateSchemaUntilApplied(ctx context.Context, nodeMsg *deleteNodeMsg) bool {
	start := time.Now()
	applyCtx, cancel := context.WithTimeout(ctx, deleteNodeUpdateSchemaMaxRetryDuration)
	defer cancel()

	panicRetryLimit := func(err error) {
		wrapped := merr.Wrap(err, "schema update retry limit reached in delete node")
		mlog.Error(ctx, "schema update retry limit reached in delete node, stop process to replay WAL after restart",
			mlog.Int64("collectionID", dNode.collectionID),
			mlog.String("channel", dNode.channel),
			mlog.Int32("schemaVersion", nodeMsg.schema.GetVersion()),
			mlog.Uint64("schemaBarrierTs", nodeMsg.schemaBarrierTs),
			mlog.Duration("retryDuration", time.Since(start)),
			mlog.Err(wrapped))
		panic(wrapped)
	}

	for {
		if dNode.closed.Load() {
			return false
		}
		err := dNode.delegator.UpdateSchema(applyCtx, nodeMsg.schema, nodeMsg.schemaBarrierTs)
		if err == nil {
			return true
		}

		if dNode.closed.Load() {
			return false
		}
		if errors.Is(applyCtx.Err(), context.Canceled) {
			return false
		}
		if errors.Is(applyCtx.Err(), context.DeadlineExceeded) {
			panicRetryLimit(err)
		}
		if !isUpdateSchemaRetryable(err) {
			wrapped := merr.Wrap(err, "non-retryable schema update failure in delete node")
			mlog.Error(ctx, "non-retryable schema update failure in delete node, stop process to replay WAL after restart",
				mlog.Int64("collectionID", dNode.collectionID),
				mlog.String("channel", dNode.channel),
				mlog.Int32("schemaVersion", nodeMsg.schema.GetVersion()),
				mlog.Uint64("schemaBarrierTs", nodeMsg.schemaBarrierTs),
				mlog.Err(wrapped))
			panic(wrapped)
		}
		if time.Since(start) >= deleteNodeUpdateSchemaMaxRetryDuration {
			panicRetryLimit(err)
		}

		mlog.RatedWarn(ctx, rate.Limit(1), "failed to update schema in delete node, retrying before advancing tsafe",
			mlog.Int64("collectionID", dNode.collectionID),
			mlog.String("channel", dNode.channel),
			mlog.Int32("schemaVersion", nodeMsg.schema.GetVersion()),
			mlog.Uint64("schemaBarrierTs", nodeMsg.schemaBarrierTs),
			mlog.Err(err))

		if dNode.closed.Load() {
			return false
		}
		timer := time.NewTimer(deleteNodeUpdateSchemaRetryInterval)
		select {
		case <-timer.C:
		case <-dNode.closeCh:
			timer.Stop()
			return false
		case <-applyCtx.Done():
			timer.Stop()
			if dNode.closed.Load() || errors.Is(applyCtx.Err(), context.Canceled) {
				return false
			}
			panicRetryLimit(applyCtx.Err())
		}
		timer.Stop()
	}
}

func isUpdateSchemaRetryable(err error) bool {
	if err == nil {
		return false
	}
	if merr.IsRetryableErr(err) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, grpc.ErrClientConnClosing) ||
		errors.Is(err, merr.ErrChannelNotAvailable) ||
		errors.Is(err, merr.ErrNodeNotAvailable) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return true
	default:
		return false
	}
}

func (dNode *deleteNode) PreClose() {
	dNode.closeOnce.Do(func() {
		dNode.closed.Store(true)
		dNode.cancel()
		close(dNode.closeCh)
	})
}

func (dNode *deleteNode) Close() { dNode.PreClose() }

func newDeleteNode(
	collectionID UniqueID, channel string,
	manager *DataManager, delegator delegator.ShardDelegator,
	maxQueueLength int32,
) *deleteNode {
	// #nosec G118 -- cancel is stored on deleteNode and called by PreClose/Close.
	ctx, cancel := context.WithCancel(context.Background())
	return &deleteNode{
		BaseNode:     base.NewBaseNode(fmt.Sprintf("DeleteNode-%s", channel), maxQueueLength),
		collectionID: collectionID,
		channel:      channel,
		manager:      manager,
		delegator:    delegator,
		ctx:          ctx,
		cancel:       cancel,
		closeCh:      make(chan struct{}),
	}
}
