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

package rootcoord

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (c *Core) broadcastDropCollectionV1(ctx context.Context, req *milvuspb.DropCollectionRequest) error {
	broadcaster, err := c.startBroadcastWithCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	dropCollectionTask := &dropCollectionTask{
		Core: c,
		Req:  req,
	}
	if err := dropCollectionTask.Prepare(ctx); err != nil {
		return err
	}

	channels := make([]string, 0, len(dropCollectionTask.vchannels)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	channels = append(channels, dropCollectionTask.vchannels...)
	msg := message.NewDropCollectionMessageBuilderV1().
		WithHeader(dropCollectionTask.header).
		WithBody(dropCollectionTask.body).
		WithBroadcast(channels).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}

// dropCollectionV1AckCallback is called when the drop collection message is acknowledged
func (c *DDLCallback) dropCollectionV1AckCallback(ctx context.Context, result message.BroadcastResultDropCollectionMessageV1) error {
	msg := result.Message
	header := msg.Header()
	body := msg.MustBody()
	for vchannel, result := range result.Results {
		collectionID := msg.Header().CollectionId
		if funcutil.IsControlChannel(vchannel) {
			// when the control channel is acknowledged, we should do the following steps:

			// 1. release the collection from querycoord first.
			dropLoadConfigMsg := message.NewDropLoadConfigMessageBuilderV2().
				WithHeader(&message.DropLoadConfigMessageHeader{
					DbId:         msg.Header().DbId,
					CollectionId: collectionID,
				}).
				WithBody(&message.DropLoadConfigMessageBody{}).
				WithBroadcast([]string{streaming.WAL().ControlChannel()}).
				MustBuildBroadcast().
				WithBroadcastID(msg.BroadcastHeader().BroadcastID)
			if err := registry.CallMessageAckCallback(ctx, dropLoadConfigMsg, map[string]*message.AppendResult{
				streaming.WAL().ControlChannel(): result,
			}); err != nil {
				return errors.Wrap(err, "failed to release collection")
			}

			// 2. drop the collection index.
			dropIndexMsg := message.NewDropIndexMessageBuilderV2().
				WithHeader(&message.DropIndexMessageHeader{
					CollectionId: collectionID,
				}).
				WithBody(&message.DropIndexMessageBody{}).
				WithBroadcast([]string{streaming.WAL().ControlChannel()}).
				MustBuildBroadcast().
				WithBroadcastID(msg.BroadcastHeader().BroadcastID)

			if err := registry.CallMessageAckCallback(ctx, dropIndexMsg, map[string]*message.AppendResult{
				streaming.WAL().ControlChannel(): result,
			}); err != nil {
				return errors.Wrap(err, "failed to drop collection index")
			}

			// 2.5. best-effort drop all snapshots of this collection
			dropSnapshotsMsg := message.NewDropSnapshotsByCollectionMessageBuilderV2().
				WithHeader(&message.DropSnapshotsByCollectionMessageHeader{
					CollectionId: collectionID,
				}).
				WithBody(&message.DropSnapshotsByCollectionMessageBody{}).
				WithBroadcast([]string{streaming.WAL().ControlChannel()}).
				MustBuildBroadcast().
				WithBroadcastID(msg.BroadcastHeader().BroadcastID)

			if err := registry.CallMessageAckCallback(ctx, dropSnapshotsMsg, map[string]*message.AppendResult{
				streaming.WAL().ControlChannel(): result,
			}); err != nil {
				log.Ctx(ctx).Warn("best-effort drop collection snapshots failed, will be cleaned up by GC",
					zap.Int64("collectionID", collectionID), zap.Error(err))
			}

			// 3. drop the collection meta itself.
			if err := c.meta.DropCollection(ctx, collectionID, result.TimeTick); err != nil {
				return errors.Wrap(err, "failed to drop collection")
			}
			continue
		}
		// Drop virtual channel data when the vchannel is acknowledged.
		resp, err := c.mixCoord.DropVirtualChannel(ctx, &datapb.DropVirtualChannelRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			ChannelName: vchannel,
		})
		if err := merr.CheckRPCCall(resp, err); err != nil {
			return errors.Wrap(err, "failed to drop virtual channel")
		}
	}
	// add the collection tombstone to the sweeper.
	c.tombstoneSweeper.AddTombstone(newCollectionTombstone(c.meta, c.broker, header.CollectionId))
	// DropCollection already deleted grants for the dropped collection.
	// Refresh the RBAC policy cache on all proxies so they stop using stale grant entries.
	if err := c.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
		OpType: int32(typeutil.CacheRefresh),
	}); err != nil {
		log.Ctx(ctx).Warn("failed to refresh RBAC policy cache after collection drop, skipping",
			zap.Int64("collectionID", header.CollectionId), zap.Error(err))
	}
	// expire the collection meta cache on proxy.
	return c.ExpireCaches(ctx, ce.NewBuilder().WithLegacyProxyCollectionMetaCache(
		ce.OptLPCMDBName(body.DbName),
		ce.OptLPCMCollectionName(body.CollectionName),
		ce.OptLPCMCollectionID(header.CollectionId),
		ce.OptLPCMMsgType(commonpb.MsgType_DropCollection)).Build())
}

// newCollectionTombstone creates a new collection tombstone.
func newCollectionTombstone(meta IMetaTable, broker Broker, collectionID int64) *collectionTombstone {
	return &collectionTombstone{
		meta:         meta,
		broker:       broker,
		collectionID: collectionID,
	}
}

type collectionTombstone struct {
	meta         IMetaTable
	broker       Broker
	collectionID int64
}

func (t *collectionTombstone) ID() string {
	return fmt.Sprintf("c:%d", t.collectionID)
}

func (t *collectionTombstone) ConfirmCanBeRemoved(ctx context.Context) (bool, error) {
	return t.broker.GcConfirm(ctx, t.collectionID, common.AllPartitionsID), nil
}

func (t *collectionTombstone) Remove(ctx context.Context) error {
	return t.meta.RemoveCollection(ctx, t.collectionID, 0)
}
