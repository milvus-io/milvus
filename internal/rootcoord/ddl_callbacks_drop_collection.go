package rootcoord

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func (c *Core) broadcastDropCollectionV1(ctx context.Context, req *milvuspb.DropCollectionRequest) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(req.GetDbName()),
		message.NewExclusiveCollectionNameResourceKey(req.GetDbName(), req.GetCollectionName()),
	)
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	dropCollectionTask := &dropCollectionTask{
		Core: c,
		Req:  req,
	}
	if err := dropCollectionTask.Prepare(ctx); err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) || errors.Is(err, merr.ErrDatabaseNotFound) {
			// make dropping collection idempotent.
			log.Ctx(ctx).Warn("drop non-existent collection", zap.String("collection", req.GetCollectionName()), zap.String("database", req.GetDbName()))
			return nil
		}
		return err
	}

	channels := make([]string, 0, len(dropCollectionTask.vchannels)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	for _, vchannel := range dropCollectionTask.vchannels {
		channels = append(channels, vchannel)
	}
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
				return err
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
				return err
			}

			// 3. drop the collection meta itself.
			if err := c.meta.DropCollection(ctx, collectionID, result.TimeTick); err != nil {
				return err
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
			return err
		}
	}
	return c.ExpireCaches(ctx, ce.NewBuilder().WithLegacyProxyCollectionMetaCache(
		ce.OptLPCMDBName(body.DbName),
		ce.OptLPCMCollectionName(body.CollectionName),
		ce.OptLPCMCollectionID(header.CollectionId),
		ce.OptLPCMMsgType(commonpb.MsgType_DropCollection)).Build(),
		result.GetControlChannelResult().TimeTick)
}
