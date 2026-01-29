package proxy

import (
	"context"
	"fmt"
	"sort"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// we only overwrite the Execute function
func (it *insertTask) Execute(ctx context.Context) error {
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

	var ez *message.CipherConfig
	if hookutil.IsClusterEncryptionEnabled() {
		ez = hookutil.GetEzByCollProperties(it.schema.GetProperties(), it.collectionID).AsMessageConfig()
	}

	// start to repack insert data
	var msgs []message.MutableMessage
	if it.partitionKeys == nil {
		msgs, err = repackInsertDataForStreamingService(it.TraceCtx(), channelNames, it.insertMsg, it.result, ez)
	} else {
		msgs, err = repackInsertDataWithPartitionKeyForStreamingService(it.TraceCtx(), channelNames, it.insertMsg, it.result, it.partitionKeys, ez)
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
	ez *message.CipherConfig,
) ([]message.MutableMessage, error) {
	messages := make([]message.MutableMessage, 0)

	channel2RowOffsets := assignChannelsByPK(result.IDs, channelNames, insertMsg)
	partitionName := insertMsg.PartitionName
	partitionID, err := globalMetaCache.GetPartitionID(ctx, insertMsg.GetDbName(), insertMsg.CollectionName, partitionName)
	if err != nil {
		return nil, err
	}

	for channel, rowOffsets := range channel2RowOffsets {
		// segment id is assigned at streaming node.
		msgs, err := genInsertMsgsByPartition(ctx, 0, partitionID, partitionName, rowOffsets, channel, insertMsg)
		if err != nil {
			return nil, err
		}
		for _, msg := range msgs {
			insertRequest := msg.(*msgstream.InsertMsg).InsertRequest
			newMsg, err := message.NewInsertMessageBuilderV1().
				WithVChannel(channel).
				WithHeader(&message.InsertMessageHeader{
					CollectionId: insertMsg.CollectionID,
					Partitions: []*message.PartitionSegmentAssignment{
						{
							PartitionId: partitionID,
							Rows:        insertRequest.GetNumRows(),
							BinarySize:  0, // TODO: current not used, message estimate size is used.
						},
					},
				}).
				WithBody(insertRequest).
				WithCipher(ez).
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
	ez *message.CipherConfig,
) ([]message.MutableMessage, error) {
	messages := make([]message.MutableMessage, 0)

	channel2RowOffsets := assignChannelsByPK(result.IDs, channelNames, insertMsg)
	partitionNames, err := getDefaultPartitionsInPartitionKeyMode(ctx, insertMsg.GetDbName(), insertMsg.CollectionName)
	if err != nil {
		log.Ctx(ctx).Warn("get default partition names failed in partition key mode",
			zap.String("collectionName", insertMsg.CollectionName),
			zap.Error(err))
		return nil, err
	}

	// Get partition ids
	partitionIDs := make(map[string]int64, 0)
	for _, partitionName := range partitionNames {
		partitionID, err := globalMetaCache.GetPartitionID(ctx, insertMsg.GetDbName(), insertMsg.CollectionName, partitionName)
		if err != nil {
			log.Ctx(ctx).Warn("get partition id failed",
				zap.String("collectionName", insertMsg.CollectionName),
				zap.String("partitionName", partitionName),
				zap.Error(err))
			return nil, err
		}
		partitionIDs[partitionName] = partitionID
	}

	hashValues, err := typeutil.HashKey2Partitions(partitionKeys, partitionNames)
	if err != nil {
		log.Ctx(ctx).Warn("has partition keys to partitions failed",
			zap.String("collectionName", insertMsg.CollectionName),
			zap.Error(err))
		return nil, err
	}
	for channel, rowOffsets := range channel2RowOffsets {
		// Group rows by partition
		partition2RowOffsets := make(map[int64][]int)
		for _, idx := range rowOffsets {
			partitionName := partitionNames[hashValues[idx]]
			partitionID := partitionIDs[partitionName]
			partition2RowOffsets[partitionID] = append(partition2RowOffsets[partitionID], idx)
		}

		// Sort partitions by ID for deterministic ordering
		sortedPartitionIDs := make([]int64, 0, len(partition2RowOffsets))
		for partitionID := range partition2RowOffsets {
			sortedPartitionIDs = append(sortedPartitionIDs, partitionID)
		}
		sort.Slice(sortedPartitionIDs, func(i, j int) bool {
			return sortedPartitionIDs[i] < sortedPartitionIDs[j]
		})

		// Merge all rows from all partitions in partition order (contiguous ranges)
		allRowOffsets := make([]int, 0)
		partitionAssignments := make([]*message.PartitionSegmentAssignment, 0, len(sortedPartitionIDs))

		for _, partitionID := range sortedPartitionIDs {
			rows := partition2RowOffsets[partitionID]
			allRowOffsets = append(allRowOffsets, rows...)

			partitionAssignments = append(partitionAssignments, &message.PartitionSegmentAssignment{
				PartitionId: partitionID,
				Rows:        uint64(len(rows)),
				BinarySize:  0, // TODO: current not used, message estimate size is used.
			})
		}

		// Create single merged InsertRequest with all rows from all partitions
		// Use partitionID=0 and partitionName="" since this is multi-partition
		mergedInsertMsgs, err := genInsertMsgsByPartition(ctx, 0, 0, "", allRowOffsets, channel, insertMsg)
		if err != nil {
			return nil, err
		}

		// Should only be 1 message since we're not splitting by partition
		if len(mergedInsertMsgs) != 1 {
			return nil, errors.Errorf("expected 1 merged insert message, got %d", len(mergedInsertMsgs))
		}

		mergedInsertRequest := mergedInsertMsgs[0].(*msgstream.InsertMsg).InsertRequest

		// Create InsertMessage with multi-partition header
		newMsg, err := message.NewInsertMessageBuilderV1().
			WithVChannel(channel).
			WithHeader(&message.InsertMessageHeader{
				CollectionId: insertMsg.CollectionID,
				Partitions:   partitionAssignments,
			}).
			WithBody(mergedInsertRequest).
			WithCipher(ez).
			BuildMutable()
		if err != nil {
			return nil, err
		}

		messages = append(messages, newMsg)
	}
	return messages, nil
}
