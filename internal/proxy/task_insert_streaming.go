package proxy

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// we only overwrite the Execute function
// TODO: InsertMessageHeader does not carry SchemaVersion, which means the consistency gate
// in StreamingNode cannot tell whether an insert was produced before or after a schema change.
// This can cause a deadlock when the gate waits for inserts at the new schema version that
// will never arrive. The companion PR https://github.com/milvus-io/milvus/pull/48139
// resolves this by propagating SchemaVersion through the insert path.
func (it *insertTask) Execute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Insert-Execute")
	defer sp.End()

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute insert streaming %d", it.ID()))

	collectionName := it.insertMsg.CollectionName
	collID, err := globalMetaCache.GetCollectionID(it.ctx, it.insertMsg.GetDbName(), collectionName)
	if err != nil {
		mlog.Warn(ctx, "fail to get collection id", mlog.Err(err))
		return err
	}
	it.insertMsg.CollectionID = collID

	getCacheDur := tr.RecordSpan()
	channelNames, err := it.chMgr.getVChannels(collID)
	if err != nil {
		mlog.Warn(ctx, "get vChannels failed", mlog.FieldCollectionID(collID), mlog.Err(err))
		it.result.Status = merr.Status(err)
		return err
	}

	var ez *message.CipherConfig
	if hookutil.IsClusterEncryptionEnabled() {
		ez = hookutil.GetEzByCollProperties(it.schema.GetProperties(), it.collectionID).AsMessageConfig()
	}

	// Resolve routing, repack and append in a bounded loop. For a range-routed
	// (namespace) collection the channel set is narrowed to the single shard owning the
	// request's namespace; if that shard was fenced by a concurrent shard split the
	// streamingnode rejects the append with ShardFenced. The proxy then drops its stale
	// routing cache and retries, so the split is transparent to the client. Because a
	// range request lands wholly on one shard, a rejected append wrote nothing, so the
	// retry cannot double-write.
	var resp streaming.AppendResponses
	appendErr := retry.Handle(ctx, func() (bool, error) {
		collInfo, err := globalMetaCache.GetCollectionInfo(ctx, it.insertMsg.GetDbName(), collectionName, collID)
		if err != nil {
			return false, err
		}
		channelNames, err := resolveRangeRoutingChannels(collInfo, it.insertMsg.GetNamespace(), channelNames)
		if err != nil {
			return false, err
		}

		mlog.Debug(ctx, "send insert request to virtual channels",
			mlog.String("partition", it.insertMsg.GetPartitionName()),
			mlog.FieldCollectionID(collID),
			mlog.Strings("virtual_channels", channelNames),
			mlog.FieldTaskID(it.ID()),
			mlog.Bool("is_parition_key", it.partitionKeys != nil),
			mlog.Duration("get cache duration", getCacheDur))

		// start to repack insert data
		var msgs []message.MutableMessage
		if it.partitionKeys == nil {
			msgs, err = repackInsertDataForStreamingService(it.TraceCtx(), channelNames, it.insertMsg, it.result, ez, it.schemaVersion)
		} else {
			msgs, err = repackInsertDataWithPartitionKeyForStreamingService(it.TraceCtx(), channelNames, it.insertMsg, it.result, it.partitionKeys, ez, it.schema, it.schemaVersion)
		}
		if err != nil {
			return false, err
		}
		resp = streaming.WAL().AppendMessages(ctx, msgs...)
		if err := resp.UnwrapFirstError(); err != nil {
			if status.AsStreamingError(err).IsShardFenced() {
				// The target shard was fenced by a shard split; drop the stale routing
				// cache so the next attempt routes against the post-split topology.
				globalMetaCache.RemoveCollection(ctx, it.insertMsg.GetDbName(), collectionName, 0)
				return true, err
			}
			return false, err
		}
		return false, nil
	}, retry.Attempts(shardFencedRetryAttempts))
	if appendErr != nil {
		mlog.Warn(ctx, "append messages to wal failed", mlog.Err(appendErr))
		if status.AsStreamingError(appendErr).IsSchemaVersionMismatch() {
			it.result.Status = merr.Status(merr.ErrCollectionSchemaMismatch)
		} else {
			it.result.Status = merr.Status(appendErr)
		}
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
	schemaVersion int32,
) ([]message.MutableMessage, error) {
	messages := make([]message.MutableMessage, 0)

	channel2RowOffsets, err := assignChannelsByPK(result.IDs, channelNames, insertMsg)
	if err != nil {
		return nil, err
	}
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
					SchemaVersion: &schemaVersion,
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
	schema *schemapb.CollectionSchema,
	schemaVersion int32,
) ([]message.MutableMessage, error) {
	messages := make([]message.MutableMessage, 0)

	var channel2RowOffsets map[string][]int
	var err error
	if namespacePartitionKeyModeEnabled(schema) && insertMsg.Namespace != nil {
		channel2RowOffsets, err = assignChannelsByNamespace(*insertMsg.Namespace, channelNames, insertMsg)
	} else {
		channel2RowOffsets, err = assignChannelsByPK(result.IDs, channelNames, insertMsg)
	}
	if err != nil {
		return nil, err
	}
	partitionNames, err := getDefaultPartitionsInPartitionKeyMode(ctx, insertMsg.GetDbName(), insertMsg.CollectionName)
	if err != nil {
		mlog.Warn(ctx, "get default partition names failed in partition key mode",
			mlog.FieldCollectionName(insertMsg.CollectionName),
			mlog.Err(err))
		return nil, err
	}

	// Get partition ids
	partitionIDs := make(map[string]int64, 0)
	for _, partitionName := range partitionNames {
		partitionID, err := globalMetaCache.GetPartitionID(ctx, insertMsg.GetDbName(), insertMsg.CollectionName, partitionName)
		if err != nil {
			mlog.Warn(ctx, "get partition id failed",
				mlog.FieldCollectionName(insertMsg.CollectionName),
				mlog.FieldPartitionName(partitionName),
				mlog.Err(err))
			return nil, err
		}
		partitionIDs[partitionName] = partitionID
	}

	hashValues, err := typeutil.HashKey2Partitions(partitionKeys, partitionNames)
	if err != nil {
		mlog.Warn(ctx, "has partition keys to partitions failed",
			mlog.FieldCollectionName(insertMsg.CollectionName),
			mlog.Err(err))
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
				insertRequest := msg.(*msgstream.InsertMsg).InsertRequest
				newMsg, err := message.NewInsertMessageBuilderV1().
					WithVChannel(channel).
					WithHeader(&message.InsertMessageHeader{
						CollectionId: insertMsg.CollectionID,
						Partitions: []*message.PartitionSegmentAssignment{
							{
								PartitionId: partitionIDs[partitionName],
								Rows:        insertRequest.GetNumRows(),
								BinarySize:  0, // TODO: current not used, message estimate size is used.
							},
						},
						SchemaVersion: &schemaVersion,
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
	}
	return messages, nil
}
