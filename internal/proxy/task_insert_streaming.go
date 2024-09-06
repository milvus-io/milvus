package proxy

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type insertTaskByStreamingService struct {
	*insertTask
}

// we only overwrite the Execute function
func (it *insertTaskByStreamingService) Execute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Insert-Execute")
	defer sp.End()

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute insert streaming %d", it.ID()))

	collectionName := it.insertMsg.CollectionName
	collID, err := globalMetaCache.GetCollectionID(it.ctx, it.insertMsg.GetDbName(), collectionName)
	log := log.Ctx(ctx)
	if err != nil {
		log.Warn("fail to get collection id", zap.Error(err))
		return err
	}
	it.insertMsg.CollectionID = collID

	getCacheDur := tr.RecordSpan()
	channelNames, err := it.chMgr.getVChannels(collID)
	if err != nil {
		log.Warn("get vChannels failed", zap.Int64("collectionID", collID), zap.Error(err))
		it.result.Status = merr.Status(err)
		return err
	}

	log.Debug("send insert request to virtual channels",
		zap.String("partition", it.insertMsg.GetPartitionName()),
		zap.Int64("collectionID", collID),
		zap.Strings("virtual_channels", channelNames),
		zap.Int64("task_id", it.ID()),
		zap.Bool("is_parition_key", it.partitionKeys != nil),
		zap.Duration("get cache duration", getCacheDur))

	// start to repack insert data
	var msgs []message.MutableMessage
	if it.partitionKeys == nil {
		msgs, err = repackInsertDataForStreamingService(it.TraceCtx(), channelNames, it.insertMsg, it.result)
	} else {
		msgs, err = repackInsertDataWithPartitionKeyForStreamingService(it.TraceCtx(), channelNames, it.insertMsg, it.result, it.partitionKeys)
	}
	if err != nil {
		log.Warn("assign segmentID and repack insert data failed", zap.Error(err))
		it.result.Status = merr.Status(err)
		return err
	}
	resp := streaming.WAL().AppendMessages(ctx, msgs...)
	if err := resp.UnwrapFirstError(); err != nil {
		log.Warn("append messages to wal failed", zap.Error(err))
		it.result.Status = merr.Status(err)
	}
	// Update result.Timestamp for session consistency.
	it.result.Timestamp = resp.MaxTimeTick()
	return nil
}

func repackInsertDataForStreamingService(
	ctx context.Context,
	channelNames []string,
	insertMsg *msgstream.InsertMsg,
	result *milvuspb.MutationResult,
) ([]message.MutableMessage, error) {
	messages := make([]message.MutableMessage, 0)

	channel2RowOffsets := assignChannelsByPK(result.IDs, channelNames, insertMsg)
	for channel, rowOffsets := range channel2RowOffsets {
		partitionName := insertMsg.PartitionName
		partitionID, err := globalMetaCache.GetPartitionID(ctx, insertMsg.GetDbName(), insertMsg.CollectionName, partitionName)
		if err != nil {
			return nil, err
		}
		// segment id is assigned at streaming node.
		msgs, err := genInsertMsgsByPartition(ctx, 0, partitionID, partitionName, rowOffsets, channel, insertMsg)
		if err != nil {
			return nil, err
		}
		for _, msg := range msgs {
			newMsg, err := message.NewInsertMessageBuilderV1().
				WithVChannel(channel).
				WithHeader(&message.InsertMessageHeader{
					CollectionId: insertMsg.CollectionID,
					Partitions: []*message.PartitionSegmentAssignment{
						{
							PartitionId: partitionID,
							Rows:        uint64(len(rowOffsets)),
							BinarySize:  0, // TODO: current not used, message estimate size is used.
						},
					},
				}).
				WithBody(msg.(*msgstream.InsertMsg).InsertRequest).
				BuildMutable()
			if err != nil {
				return nil, err
			}
			messages = append(messages, newMsg)
		}
	}
	return messages, nil
}

func repackInsertDataWithPartitionKeyForStreamingService(
	ctx context.Context,
	channelNames []string,
	insertMsg *msgstream.InsertMsg,
	result *milvuspb.MutationResult,
	partitionKeys *schemapb.FieldData,
) ([]message.MutableMessage, error) {
	messages := make([]message.MutableMessage, 0)

	channel2RowOffsets := assignChannelsByPK(result.IDs, channelNames, insertMsg)
	partitionNames, err := getDefaultPartitionsInPartitionKeyMode(ctx, insertMsg.GetDbName(), insertMsg.CollectionName)
	if err != nil {
		log.Warn("get default partition names failed in partition key mode",
			zap.String("collectionName", insertMsg.CollectionName),
			zap.Error(err))
		return nil, err
	}

	// Get partition ids
	partitionIDs := make(map[string]int64, 0)
	for _, partitionName := range partitionNames {
		partitionID, err := globalMetaCache.GetPartitionID(ctx, insertMsg.GetDbName(), insertMsg.CollectionName, partitionName)
		if err != nil {
			log.Warn("get partition id failed",
				zap.String("collectionName", insertMsg.CollectionName),
				zap.String("partitionName", partitionName),
				zap.Error(err))
			return nil, err
		}
		partitionIDs[partitionName] = partitionID
	}

	hashValues, err := typeutil.HashKey2Partitions(partitionKeys, partitionNames)
	if err != nil {
		log.Warn("has partition keys to partitions failed",
			zap.String("collectionName", insertMsg.CollectionName),
			zap.Error(err))
		return nil, err
	}
	for channel, rowOffsets := range channel2RowOffsets {
		partition2RowOffsets := make(map[string][]int)
		for _, idx := range rowOffsets {
			partitionName := partitionNames[hashValues[idx]]
			if _, ok := partition2RowOffsets[partitionName]; !ok {
				partition2RowOffsets[partitionName] = []int{}
			}
			partition2RowOffsets[partitionName] = append(partition2RowOffsets[partitionName], idx)
		}

		for partitionName, rowOffsets := range partition2RowOffsets {
			msgs, err := genInsertMsgsByPartition(ctx, 0, partitionIDs[partitionName], partitionName, rowOffsets, channel, insertMsg)
			if err != nil {
				return nil, err
			}
			for _, msg := range msgs {
				newMsg, err := message.NewInsertMessageBuilderV1().
					WithVChannel(channel).
					WithHeader(&message.InsertMessageHeader{
						CollectionId: insertMsg.CollectionID,
						Partitions: []*message.PartitionSegmentAssignment{
							{
								PartitionId: partitionIDs[partitionName],
								Rows:        uint64(len(rowOffsets)),
								BinarySize:  0, // TODO: current not used, message estimate size is used.
							},
						},
					}).
					WithBody(msg.(*msgstream.InsertMsg).InsertRequest).
					BuildMutable()
				if err != nil {
					return nil, err
				}
				messages = append(messages, newMsg)
			}
		}
	}
	return messages, nil
}
