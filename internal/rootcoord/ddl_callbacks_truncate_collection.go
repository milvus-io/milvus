// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootcoord

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (c *Core) broadcastTruncateCollection(ctx context.Context, req *milvuspb.TruncateCollectionRequest) error {
	broadcaster, err := c.startBroadcastWithCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	// get collection info
	coll, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	header := &messagespb.TruncateCollectionMessageHeader{
		DbId:         coll.DBID,
		CollectionId: coll.CollectionID,
	}
	body := &messagespb.TruncateCollectionMessageBody{}

	channels := make([]string, 0, len(coll.VirtualChannelNames)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	channels = append(channels, coll.VirtualChannelNames...)
	msg := message.NewTruncateCollectionMessageBuilderV2().
		WithHeader(header).
		WithBody(body).
		WithBroadcast(channels, message.OptBuildBroadcastAckSyncUp()).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}

// truncateCollectionV2AckCallback is called when the truncate collection message is acknowledged
func (c *DDLCallback) truncateCollectionV2AckCallback(ctx context.Context, result message.BroadcastResultTruncateCollectionMessageV2) error {
	msg := result.Message
	header := msg.Header()

	flushTsList := make(map[string]uint64)
	for vchannel, result := range result.Results {
		if funcutil.IsControlChannel(vchannel) {
			continue
		}
		flushTs := result.TimeTick
		flushTsList[vchannel] = flushTs
	}

	// Drop segments that were updated before the flush timestamp
	if err := c.mixCoord.DropSegmentsByTime(ctx, header.CollectionId, flushTsList); err != nil {
		return errors.Wrap(err, "when dropping segments by time")
	}

	// manually update current target to sync QueryCoord's view
	if err := c.mixCoord.ManualUpdateCurrentTarget(ctx, header.CollectionId); err != nil {
		return errors.Wrap(err, "when manually updating current target")
	}

	if err := c.meta.TruncateCollection(ctx, result); err != nil {
		if errors.Is(err, errAlterCollectionNotFound) {
			log.Ctx(ctx).Warn("truncate a non-existent collection, ignore it", log.FieldMessage(result.Message))
			return nil
		}
		return errors.Wrap(err, "when truncating collection")
	}

	// notify datacoord to update their meta cache
	if err := c.broker.BroadcastAlteredCollection(ctx, header.CollectionId); err != nil {
		return errors.Wrap(err, "when broadcasting altered collection")
	}
	return nil
}

// truncateCollectionV2AckOnceCallback is called when the truncate collection message is acknowledged once
func (c *DDLCallback) truncateCollectionV2AckOnceCallback(ctx context.Context, result message.AckResultTruncateCollectionMessageV2) error {
	msg := result.Message
	// When the ack callback of truncate collection operation is executing,
	// the compaction and flush operation may be executed in parallel.
	// So if some vchannel flush a new segment which order after the truncate collection operation,
	// the segment should not be dropped, but the compaction may compact it with the segment which order before the truncate collection operation.
	// the new compacted segment can not be dropped as whole, break the design of truncate collection operation.
	// we need to forbid the compaction of current collection here.
	collectionID := msg.Header().CollectionId
	if err := c.meta.BeginTruncateCollection(ctx, collectionID); err != nil {
		if errors.Is(err, errAlterCollectionNotFound) {
			log.Ctx(ctx).Warn("begin to truncate a non-existent collection, ignore it", log.FieldMessage(result.Message))
			return nil
		}
		return errors.Wrap(err, "when beginning truncate collection")
	}

	// notify datacoord to update their meta cache
	if err := c.broker.BroadcastAlteredCollection(ctx, collectionID); err != nil {
		return errors.Wrap(err, "when broadcasting altered collection")
	}
	return nil
}
