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

package flusherimpl

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
)

func newMsgHandler(wbMgr writebuffer.BufferManager) *msgHandlerImpl {
	return &msgHandlerImpl{
		wbMgr: wbMgr,
	}
}

type msgHandlerImpl struct {
	wbMgr writebuffer.BufferManager
}

func (impl *msgHandlerImpl) HandleCreateSegment(ctx context.Context, createSegmentMsg message.ImmutableCreateSegmentMessageV2) error {
	vchannel := createSegmentMsg.VChannel()
	h := createSegmentMsg.Header()
	if err := impl.createNewGrowingSegment(ctx, vchannel, h); err != nil {
		return err
	}
	logger := log.With(log.FieldMessage(createSegmentMsg))
	if err := impl.wbMgr.CreateNewGrowingSegment(ctx, vchannel, h.PartitionId, h.SegmentId); err != nil {
		logger.Warn("fail to create new growing segment")
		return err
	}
	log.Info("create new growing segment")
	return nil
}

func (impl *msgHandlerImpl) createNewGrowingSegment(ctx context.Context, vchannel string, h *message.CreateSegmentMessageHeader) error {
	if h.Level == datapb.SegmentLevel_L0 {
		// L0 segment should not be flushed directly, but not create than flush.
		// the create segment operation is used to protect the binlog from garbage collection.
		// L0 segment's binlog upload and flush operation is handled once.
		// so we can skip the create segment operation here. (not strict promise exactly)
		return nil
	}
	// Transfer the pending segment into growing state.
	// Alloc the growing segment at datacoord first.
	mix, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		return err
	}
	logger := log.With(zap.Int64("collectionID", h.CollectionId), zap.Int64("partitionID", h.PartitionId), zap.Int64("segmentID", h.SegmentId))
	return retry.Do(ctx, func() (err error) {
		resp, err := mix.AllocSegment(ctx, &datapb.AllocSegmentRequest{
			CollectionId:         h.CollectionId,
			PartitionId:          h.PartitionId,
			SegmentId:            h.SegmentId,
			Vchannel:             vchannel,
			StorageVersion:       h.StorageVersion,
			IsCreatedByStreaming: true,
		})
		if err := merr.CheckRPCCall(resp, err); err != nil {
			logger.Warn("failed to alloc growing segment at datacoord")
			return errors.Wrap(err, "failed to alloc growing segment at datacoord")
		}
		logger.Info("alloc growing segment at datacoord")
		return nil
	}, retry.AttemptAlways())
}

func (impl *msgHandlerImpl) HandleFlush(flushMsg message.ImmutableFlushMessageV2) error {
	vchannel := flushMsg.VChannel()
	if err := impl.wbMgr.SealSegments(context.Background(), vchannel, []int64{flushMsg.Header().SegmentId}); err != nil {
		return errors.Wrap(err, "failed to seal segments")
	}
	return nil
}

func (impl *msgHandlerImpl) HandleManualFlush(flushMsg message.ImmutableManualFlushMessageV2) error {
	vchannel := flushMsg.VChannel()
	if err := impl.wbMgr.SealSegments(context.Background(), vchannel, flushMsg.Header().SegmentIds); err != nil {
		return errors.Wrap(err, "failed to seal segments")
	}
	if err := impl.wbMgr.FlushChannel(context.Background(), vchannel, flushMsg.Header().FlushTs); err != nil {
		return errors.Wrap(err, "failed to flush channel")
	} // may be redundant.
	return nil
}

func (impl *msgHandlerImpl) HandleSchemaChange(ctx context.Context, msg message.ImmutableSchemaChangeMessageV2) error {
	return impl.wbMgr.SealSegments(context.Background(), msg.VChannel(), msg.Header().FlushedSegmentIds)
}
