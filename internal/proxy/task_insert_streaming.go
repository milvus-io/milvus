package proxy

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/proxy/channelmgr"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
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
	collID, err := it.getMetaCache().GetCollectionID(it.ctx, it.insertMsg.GetDbName(), collectionName)
	if err != nil {
		mlog.Warn(ctx, "fail to get collection id", mlog.Err(err))
		return err
	}
	it.insertMsg.CollectionID = collID

	getCacheDur := tr.RecordSpan()
	channelNames, err := it.chMgr.GetVChannels(collID)
	if err != nil {
		mlog.Warn(ctx, "get vChannels failed", mlog.FieldCollectionID(collID), mlog.Err(err))
		it.result.Status = merr.Status(err)
		return err
	}

	mlog.Debug(ctx, "send insert request to virtual channels",
		mlog.String("partition", it.insertMsg.GetPartitionName()),
		mlog.FieldCollectionID(collID),
		mlog.Strings("virtual_channels", channelNames),
		mlog.FieldTaskID(it.ID()),
		mlog.Bool("is_parition_key", it.partitionKeys != nil),
		mlog.Duration("get cache duration", getCacheDur))

	var ez *message.CipherConfig
	if hookutil.IsClusterEncryptionEnabled() {
		ez = hookutil.GetEzByCollProperties(it.schema.GetProperties(), it.collectionID).AsMessageConfig()
	}

	// Route, repack and append in a bounded loop. A concurrent shard split fences
	// the source shard, and the streamingnode then rejects an append to it with
	// ShardFenced; the proxy drops its stale routing cache and retries, so the
	// split is transparent to the client.
	//
	// Only the rows the WAL actually refused are retried. A request spans several
	// shards and each one commits independently, so re-sending the whole request
	// after a single shard was fenced would write the committed rows twice (see
	// shard_fenced_retry.go).
	var resp streaming.AppendResponses
	pending := newRowSet(allRowOffsets(int(it.insertMsg.NumRows)))
	appendErr := retry.Handle(ctx, func() (bool, error) {
		collInfo, err := it.getMetaCache().GetCollectionInfo(ctx, it.insertMsg.GetDbName(), collectionName, collID)
		if err != nil {
			return false, err
		}
		table := routingOf(collInfo)

		// start to repack insert data
		var msgs []message.MutableMessage
		var messageOffsets [][]int
		if it.partitionKeys == nil {
			msgs, messageOffsets, err = repackInsertDataForStreamingService(it.TraceCtx(), it.getMetaCache(), table, channelNames, it.insertMsg, it.result, ez, it.schemaVersion, nil, pending)
		} else {
			msgs, messageOffsets, err = repackInsertDataWithPartitionKeyForStreamingService(it.TraceCtx(), it.getMetaCache(), table, channelNames, it.insertMsg, it.result, it.partitionKeys, ez, it.schema, it.schemaVersion, nil, pending)
		}
		if err != nil {
			return false, err
		}
		resp = streaming.WAL().AppendMessages(ctx, msgs...)
		refused, fenceErr, fatalErr := refusedRows(resp, messageOffsets)
		if fatalErr != nil {
			return false, fatalErr
		}
		if len(refused) == 0 {
			return false, nil
		}
		// Those shards were fenced by a shard split; the rows that did land are
		// done and drop out of the set. Drop the stale routing cache so the next
		// attempt places the rest against the post-split topology.
		pending = newRowSet(refused)
		it.getMetaCache().RemoveCollection(ctx, it.insertMsg.GetDbName(), collectionName)
		return true, fenceErr
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

// repackInsertDataForStreamingService returns the messages to append and, for
// each one, the row offsets it carries. The caller needs that mapping to know
// which rows a refused message failed to write.
func repackInsertDataForStreamingService(
	ctx context.Context,
	metaCache Cache,
	table *routing.Table,
	channelNames []string,
	insertMsg *msgstream.InsertMsg,
	result *milvuspb.MutationResult,
	ez *message.CipherConfig,
	schemaVersion int32,
	partialUpdateCASGroups map[string]*messagespb.PartialUpdateCAS,
	pending rowSet,
) ([]message.MutableMessage, [][]int, error) {
	messages := make([]message.MutableMessage, 0)
	messageOffsets := make([][]int, 0)
	walName := channelmgr.GetActiveWALName()

	channel2RowOffsets, err := assignChannelsByPK(table, result.IDs, channelNames, insertMsg)
	if err != nil {
		return nil, nil, err
	}
	channel2RowOffsets = pending.retain(channel2RowOffsets)
	partitionName := insertMsg.PartitionName
	partitionID, err := metaCache.GetPartitionID(ctx, insertMsg.GetDbName(), insertMsg.CollectionName, partitionName)
	if err != nil {
		return nil, nil, err
	}

	for channel, rowOffsets := range channel2RowOffsets {
		partialUpdateCAS, err := getPartialUpdateCASForStreamingService(partialUpdateCASGroups, channel)
		if err != nil {
			return nil, nil, err
		}

		// segment id is assigned at streaming node.
		msgs, msgOffsets, err := repackInsertDataByPartitionForStreamingService(
			ctx,
			partitionID,
			partitionName,
			rowOffsets,
			channel,
			insertMsg,
			ez,
			schemaVersion,
			partialUpdateCAS,
			walName,
		)
		if err != nil {
			return nil, nil, err
		}
		messages = append(messages, msgs...)
		messageOffsets = append(messageOffsets, msgOffsets...)
	}
	return messages, messageOffsets, nil
}

func repackInsertDataWithPartitionKeyForStreamingService(
	ctx context.Context,
	metaCache Cache,
	table *routing.Table,
	channelNames []string,
	insertMsg *msgstream.InsertMsg,
	result *milvuspb.MutationResult,
	partitionKeys *schemapb.FieldData,
	ez *message.CipherConfig,
	schema *schemapb.CollectionSchema,
	schemaVersion int32,
	partialUpdateCASGroups map[string]*messagespb.PartialUpdateCAS,
	pending rowSet,
) ([]message.MutableMessage, [][]int, error) {
	messages := make([]message.MutableMessage, 0)
	messageOffsets := make([][]int, 0)
	walName := channelmgr.GetActiveWALName()

	var channel2RowOffsets map[string][]int
	var err error
	if namespacePartitionKeyModeEnabled(schema) && insertMsg.Namespace != nil {
		channel2RowOffsets, err = assignChannelsByNamespace(table, *insertMsg.Namespace, channelNames, insertMsg)
	} else {
		channel2RowOffsets, err = assignChannelsByPK(table, result.IDs, channelNames, insertMsg)
	}
	if err != nil {
		return nil, nil, err
	}
	channel2RowOffsets = pending.retain(channel2RowOffsets)
	partitionNames, err := getDefaultPartitionsInPartitionKeyMode(ctx, metaCache, insertMsg.GetDbName(), insertMsg.CollectionName)
	if err != nil {
		mlog.Warn(ctx, "get default partition names failed in partition key mode",
			mlog.FieldCollectionName(insertMsg.CollectionName),
			mlog.Err(err))
		return nil, nil, err
	}

	// Get partition ids
	partitionIDs := make(map[string]int64, 0)
	for _, partitionName := range partitionNames {
		partitionID, err := metaCache.GetPartitionID(ctx, insertMsg.GetDbName(), insertMsg.CollectionName, partitionName)
		if err != nil {
			mlog.Warn(ctx, "get partition id failed",
				mlog.FieldCollectionName(insertMsg.CollectionName),
				mlog.FieldPartitionName(partitionName),
				mlog.Err(err))
			return nil, nil, err
		}
		partitionIDs[partitionName] = partitionID
	}

	hashValues, err := typeutil.HashKey2Partitions(partitionKeys, partitionNames)
	if err != nil {
		mlog.Warn(ctx, "has partition keys to partitions failed",
			mlog.FieldCollectionName(insertMsg.CollectionName),
			mlog.Err(err))
		return nil, nil, err
	}
	for channel, rowOffsets := range channel2RowOffsets {
		partialUpdateCAS, err := getPartialUpdateCASForStreamingService(partialUpdateCASGroups, channel)
		if err != nil {
			return nil, nil, err
		}

		partition2RowOffsets := make(map[string][]int)
		for _, idx := range rowOffsets {
			partitionName := partitionNames[hashValues[idx]]
			if _, ok := partition2RowOffsets[partitionName]; !ok {
				partition2RowOffsets[partitionName] = []int{}
			}
			partition2RowOffsets[partitionName] = append(partition2RowOffsets[partitionName], idx)
		}

		for partitionName, rowOffsets := range partition2RowOffsets {
			msgs, msgOffsets, err := repackInsertDataByPartitionForStreamingService(
				ctx,
				partitionIDs[partitionName],
				partitionName,
				rowOffsets,
				channel,
				insertMsg,
				ez,
				schemaVersion,
				partialUpdateCAS,
				walName,
			)
			if err != nil {
				return nil, nil, err
			}
			messages = append(messages, msgs...)
			messageOffsets = append(messageOffsets, msgOffsets...)
		}
	}
	return messages, messageOffsets, nil
}

func getPartialUpdateCASForStreamingService(
	groups map[string]*messagespb.PartialUpdateCAS,
	channel string,
) (*messagespb.PartialUpdateCAS, error) {
	if groups == nil {
		return nil, nil
	}
	meta, ok := groups[channel]
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("partial update insert has no CAS metadata for vchannel %s", channel)
	}
	if meta == nil {
		return nil, merr.WrapErrServiceInternalMsg("partial update insert has nil CAS metadata for vchannel %s", channel)
	}
	return meta, nil
}

func repackInsertDataByPartitionForStreamingService(
	ctx context.Context,
	partitionID int64,
	partitionName string,
	rowOffsets []int,
	channel string,
	insertMsg *msgstream.InsertMsg,
	ez *message.CipherConfig,
	schemaVersion int32,
	partialUpdateCAS *messagespb.PartialUpdateCAS,
	walName message.WALName,
) ([]message.MutableMessage, [][]int, error) {
	type pendingInsertPack struct {
		rowOffsets []int
		insertMsg  *msgstream.InsertMsg
	}

	maxMessageSize := Params.PulsarCfg.MaxMessageSize.GetAsInt()
	messages := make([]message.MutableMessage, 0)
	// Each built message carries exactly its pack's row offsets, which is what
	// lets a refused message hand its rows back for a re-route.
	messageOffsets := make([][]int, 0)
	pending := []pendingInsertPack{{rowOffsets: rowOffsets}}
	for len(pending) > 0 {
		pack := pending[0]
		pending = pending[1:]
		if pack.insertMsg == nil {
			packedMsgs, err := channelmgr.GenInsertMsgsByPartition(
				ctx,
				0,
				partitionID,
				partitionName,
				pack.rowOffsets,
				channel,
				insertMsg,
				walName,
			)
			if err != nil {
				return nil, nil, err
			}

			generated := make([]pendingInsertPack, 0, len(packedMsgs))
			rowOffsetCursor := 0
			for _, packedMsg := range packedMsgs {
				packedInsertMsg := packedMsg.(*msgstream.InsertMsg)
				nextRowOffsetCursor := rowOffsetCursor + int(packedInsertMsg.GetNumRows())
				generated = append(generated, pendingInsertPack{
					rowOffsets: pack.rowOffsets[rowOffsetCursor:nextRowOffsetCursor],
					insertMsg:  packedInsertMsg,
				})
				rowOffsetCursor = nextRowOffsetCursor
			}
			pending = append(generated, pending...)
			continue
		}

		msg, err := buildInsertMessageForStreamingService(
			pack.insertMsg.InsertRequest,
			insertMsg.CollectionID,
			partitionID,
			channel,
			schemaVersion,
			ez,
			partialUpdateCAS,
		)
		if err != nil {
			return nil, nil, err
		}

		// Entity-size packing does not include the streaming header or CAS
		// metadata. Validate the fully built message and split the original row
		// offsets again when that final envelope crosses the transport limit.
		if partialUpdateCAS == nil || msg.EstimateSize() <= maxMessageSize {
			messages = append(messages, msg)
			messageOffsets = append(messageOffsets, pack.rowOffsets)
			continue
		}
		if len(pack.rowOffsets) == 1 {
			return nil, nil, merr.WrapErrParameterTooLarge("single partial update row exceeds max message size")
		}

		middle := len(pack.rowOffsets) / 2
		split := []pendingInsertPack{
			{rowOffsets: pack.rowOffsets[:middle]},
			{rowOffsets: pack.rowOffsets[middle:]},
		}
		pending = append(split, pending...)
	}
	return messages, messageOffsets, nil
}

func buildInsertMessageForStreamingService(
	insertRequest *message.InsertRequest,
	collectionID int64,
	partitionID int64,
	channel string,
	schemaVersion int32,
	ez *message.CipherConfig,
	partialUpdateCAS *messagespb.PartialUpdateCAS,
) (message.MutableMessage, error) {
	builder := message.NewInsertMessageBuilderV1().
		WithVChannel(channel).
		WithHeader(&message.InsertMessageHeader{
			CollectionId: collectionID,
			Partitions: []*message.PartitionSegmentAssignment{
				{
					PartitionId: partitionID,
					Rows:        insertRequest.GetNumRows(),
					BinarySize:  0, // TODO: current not used, message estimate size is used.
				},
			},
			SchemaVersion: &schemaVersion,
		}).
		WithBody(insertRequest)
	if partialUpdateCAS != nil {
		if err := builder.AddPartialUpdateCAS(partialUpdateCAS); err != nil {
			return nil, err
		}
	}
	return builder.
		WithCipher(ez).
		BuildMutable()
}
