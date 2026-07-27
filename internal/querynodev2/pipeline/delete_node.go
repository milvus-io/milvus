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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/samber/lo"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

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
	for {
		if dNode.closed.Load() || ctx.Err() != nil {
			return false
		}
		err := dNode.delegator.UpdateSchema(ctx, nodeMsg.schema, nodeMsg.schemaBarrierTs)
		if err == nil {
			return true
		}

		if dNode.closed.Load() || ctx.Err() != nil {
			return false
		}
		if !isRetryableSchemaUpdateError(err) {
			wrapped := merr.Wrap(err, "non-retryable schema update failure in delete node")
			mlog.Error(ctx, "non-retryable schema update failure in delete node, stop process to replay WAL after restart",
				mlog.FieldCollectionID(dNode.collectionID),
				mlog.FieldVChannel(dNode.channel),
				mlog.Int32("schemaVersion", nodeMsg.schema.GetVersion()),
				mlog.Uint64("schemaBarrierTs", nodeMsg.schemaBarrierTs),
				mlog.Err(wrapped))
			panic(wrapped)
		}
		if time.Since(start) >= deleteNodeUpdateSchemaMaxRetryDuration {
			wrapped := merr.Wrap(err, "schema update retry limit reached in delete node")
			mlog.Error(ctx, "schema update retry limit reached in delete node, stop process to replay WAL after restart",
				mlog.FieldCollectionID(dNode.collectionID),
				mlog.FieldVChannel(dNode.channel),
				mlog.Int32("schemaVersion", nodeMsg.schema.GetVersion()),
				mlog.Uint64("schemaBarrierTs", nodeMsg.schemaBarrierTs),
				mlog.Duration("retryDuration", time.Since(start)),
				mlog.Err(wrapped))
			panic(wrapped)
		}

		mlog.RatedWarn(ctx, rate.Limit(1), "failed to update schema in delete node, retrying before advancing tsafe",
			mlog.FieldCollectionID(dNode.collectionID),
			mlog.FieldVChannel(dNode.channel),
			mlog.Int32("schemaVersion", nodeMsg.schema.GetVersion()),
			mlog.Uint64("schemaBarrierTs", nodeMsg.schemaBarrierTs),
			mlog.Err(err))

		if dNode.closed.Load() {
			return false
		}
		timer := time.NewTimer(deleteNodeUpdateSchemaRetryInterval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return false
		}
		timer.Stop()
	}
}

func isRetryableSchemaUpdateError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, merr.ErrOperationNotSupported) ||
		errors.Is(err, merr.ErrCollectionIllegalSchema) ||
		errors.Is(err, merr.ErrCollectionSchemaMismatch) ||
		errors.Is(err, merr.ErrParameterInvalid) ||
		errors.Is(err, merr.ErrParameterMissing) ||
		errors.Is(err, merr.ErrParameterTooLarge) ||
		errors.Is(err, merr.ErrServiceUnimplemented) {
		return false
	}
	// Segcore classifies schema parsing/construction failures at the CGO
	// boundary. Honor that classification so corrupt or unsupported schemas
	// fail-stop, while explicitly transient storage/resource failures retry.
	code := merr.Code(err)
	if code >= merr.Code(merr.ErrSegcore) && code <= merr.Code(merr.KnowhereError) {
		return merr.IsRetryableErr(err)
	}
	if merr.IsMilvusError(err) {
		return true
	}

	// Untyped errors at this boundary are normally transport failures. Fail
	// fast only for gRPC codes that unambiguously describe a permanent request
	// or protocol problem; retry the rest so worker movement and raw network
	// failures can converge.
	switch grpcstatus.Code(err) {
	case codes.InvalidArgument,
		codes.OutOfRange,
		codes.PermissionDenied,
		codes.Unauthenticated,
		codes.Unimplemented,
		codes.DataLoss:
		return false
	default:
		return true
	}
}

func (dNode *deleteNode) PreClose() {
	dNode.closeOnce.Do(func() {
		dNode.closed.Store(true)
		dNode.cancel()
	})
}

func (dNode *deleteNode) Close() { dNode.PreClose() }

func newDeleteNode(
	collectionID UniqueID, channel string,
	manager *DataManager, delegator delegator.ShardDelegator,
	maxQueueLength int32,
) *deleteNode {
	ctx, cancel := context.WithCancel(context.Background())
	return &deleteNode{
		BaseNode:     base.NewBaseNode(fmt.Sprintf("DeleteNode-%s", channel), maxQueueLength),
		collectionID: collectionID,
		channel:      channel,
		manager:      manager,
		delegator:    delegator,
		ctx:          ctx,
		cancel:       cancel,
	}
}
