package rootcoord

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"go.uber.org/zap"
)

func (c *Core) broadcastDropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) error {
	if err := CheckMsgType(in.GetBase().GetMsgType(), commonpb.MsgType_DropPartition); err != nil {
		return err
	}
	if in.GetPartitionName() == Params.CommonCfg.DefaultPartitionName.GetValue() {
		return errors.New("default partition cannot be deleted")
	}

	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(in.GetDbName()),
		message.NewExclusiveCollectionNameResourceKey(in.GetDbName(), in.GetCollectionName()))
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	collMeta, err := c.meta.GetCollectionByName(ctx, in.GetDbName(), in.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		// Is this idempotent?
		return err
	}

	partID := common.InvalidPartitionID
	for _, partition := range collMeta.Partitions {
		if partition.PartitionName == in.GetPartitionName() {
			partID = partition.PartitionID
			break
		}
	}
	if partID == common.InvalidPartitionID {
		log.Ctx(ctx).Warn("drop an non-existent partition", zap.String("collection", in.GetCollectionName()), zap.String("partition", in.GetPartitionName()))
		// make dropping partition idempotent.
		return nil
	}

	channels := make([]string, 0, collMeta.ShardsNum+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	for i := 0; i < int(collMeta.ShardsNum); i++ {
		channels = append(channels, collMeta.VirtualChannelNames[i])
	}
	msg := message.NewDropPartitionMessageBuilderV1().
		WithHeader(&message.DropPartitionMessageHeader{
			CollectionId: collMeta.CollectionID,
			PartitionId:  partID,
		}).
		WithBody(&message.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_DropPartition,
			},
			DbName:         in.GetDbName(),
			CollectionName: in.GetCollectionName(),
			PartitionName:  in.GetPartitionName(),
			DbID:           collMeta.DBID,
			CollectionID:   collMeta.CollectionID,
			PartitionID:    partID,
		}).
		WithBroadcast(channels).
		MustBuildBroadcast()

	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallback) dropPartitionV1AckCallback(ctx context.Context, result message.BroadcastResultDropPartitionMessageV1) error {
	header := result.Message.Header()
	body := result.Message.MustBody()

	for vchannel := range result.Results {
		if funcutil.IsControlChannel(vchannel) {
			continue
		}
		// drop all historical partition data when the vchannel is acknowledged.
		if err := c.mixCoord.NotifyDropPartition(ctx, vchannel, []int64{header.PartitionId}); err != nil {
			return err
		}
	}
	if err := c.meta.DropPartition(ctx, header.CollectionId, header.PartitionId, result.GetControlChannelResult().TimeTick); err != nil {
		return err
	}
	return c.ExpireCaches(ctx, ce.NewBuilder().
		WithLegacyProxyCollectionMetaCache(
			ce.OptLPCMDBName(body.DbName),
			ce.OptLPCMCollectionName(body.CollectionName),
			ce.OptLPCMCollectionID(header.CollectionId),
			ce.OptLPCMPartitionName(body.PartitionName),
			ce.OptLPCMMsgType(commonpb.MsgType_DropPartition),
		),
		result.GetControlChannelResult().TimeTick)
}
