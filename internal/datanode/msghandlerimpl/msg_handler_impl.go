/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package msghandlerimpl

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
)

type msgHandlerImpl struct {
	broker broker.Broker
}

func (m *msgHandlerImpl) HandleCreateSegment(ctx context.Context, vchannel string, createSegmentMsg message.ImmutableCreateSegmentMessageV2) error {
	panic("unreachable code")
}

func (m *msgHandlerImpl) HandleFlush(vchannel string, flushMsg message.ImmutableFlushMessageV2) error {
	panic("unreachable code")
}

func (m *msgHandlerImpl) HandleManualFlush(vchannel string, flushMsg message.ImmutableManualFlushMessageV2) error {
	panic("unreachable code")
}

func (m *msgHandlerImpl) HandleImport(ctx context.Context, vchannel string, importMsg *msgpb.ImportMsg) error {
	return retry.Do(ctx, func() (err error) {
		defer func() {
			if err == nil {
				err = streaming.WAL().Broadcast().Ack(ctx, types.BroadcastAckRequest{
					BroadcastID: uint64(importMsg.GetJobID()),
					VChannel:    vchannel,
				})
			}
		}()
		importResp, err := m.broker.ImportV2(ctx, &internalpb.ImportRequestInternal{
			CollectionID:   importMsg.GetCollectionID(),
			CollectionName: importMsg.GetCollectionName(),
			PartitionIDs:   importMsg.GetPartitionIDs(),
			ChannelNames:   []string{vchannel},
			Schema:         importMsg.GetSchema(),
			Files:          lo.Map(importMsg.GetFiles(), util.ConvertInternalImportFile),
			Options:        funcutil.Map2KeyValuePair(importMsg.GetOptions()),
			DataTimestamp:  importMsg.GetBase().GetTimestamp(),
			JobID:          importMsg.GetJobID(),
		})
		err = merr.CheckRPCCall(importResp, err)
		if errors.Is(err, merr.ErrCollectionNotFound) {
			log.Ctx(ctx).Warn("import message failed because of collection not found, skip it", zap.String("job_id", importResp.GetJobID()), zap.Error(err))
			return nil
		}
		if err != nil {
			log.Ctx(ctx).Warn("import message failed", zap.String("job_id", importResp.GetJobID()), zap.Error(err))
			return err
		}
		log.Ctx(ctx).Info("import message handled", zap.String("job_id", importResp.GetJobID()))
		return nil
	}, retry.AttemptAlways())
}

func (impl *msgHandlerImpl) HandleSchemaChange(ctx context.Context, vchannel string, msg *adaptor.SchemaChangeMessageBody) error {
	return streaming.WAL().Broadcast().Ack(ctx, types.BroadcastAckRequest{
		BroadcastID: msg.BroadcastID,
		VChannel:    vchannel,
	})
}

func NewMsgHandlerImpl(broker broker.Broker) *msgHandlerImpl {
	return &msgHandlerImpl{
		broker: broker,
	}
}
