package proxy

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/proxy/channelmgr"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/fastpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	collID, err := it.GetMetaCache().GetCollectionID(it.ctx, it.insertMsg.GetDbName(), collectionName)
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

	// start to repack insert data
	var msgs []message.MutableMessage
	if it.partitionKeys == nil {
		msgs, err = repackInsertDataForStreamingService(it.TraceCtx(), it.GetMetaCache(), channelNames, it.insertMsg, it.result, ez, it.schemaVersion, nil)
	} else {
		msgs, err = repackInsertDataWithPartitionKeyForStreamingService(it.TraceCtx(), it.GetMetaCache(), channelNames, it.insertMsg, it.result, it.partitionKeys, ez, it.schema, it.schemaVersion, nil)
	}
	if err != nil {
		mlog.Warn(ctx, "assign segmentID and repack insert data failed", mlog.Err(err))
		it.result.Status = merr.Status(err)
		return err
	}
	resp := streaming.WAL().AppendMessages(ctx, msgs...)
	if err := resp.UnwrapFirstError(); err != nil {
		mlog.Warn(ctx, "append messages to wal failed", mlog.Err(err))
		if status.AsStreamingError(err).IsSchemaVersionMismatch() {
			it.result.Status = merr.Status(merr.ErrCollectionSchemaMismatch)
		} else {
			it.result.Status = merr.Status(err)
		}
	}
	// Update result.Timestamp for session consistency.
	it.result.Timestamp = resp.MaxTimeTick()
	return nil
}

func repackInsertDataForStreamingService(
	ctx context.Context,
	metaCache Cache,
	channelNames []string,
	insertMsg *msgstream.InsertMsg,
	result *milvuspb.MutationResult,
	ez *message.CipherConfig,
	schemaVersion int32,
	partialUpdateCASGroups map[string]*messagespb.PartialUpdateCAS,
) ([]message.MutableMessage, error) {
	messages := make([]message.MutableMessage, 0)
	channel2RowOffsets, err := assignChannelsByPK(result.IDs, channelNames, insertMsg)
	if err != nil {
		return nil, err
	}
	partitionName := insertMsg.PartitionName
	partitionID, err := metaCache.GetPartitionID(ctx, insertMsg.GetDbName(), insertMsg.CollectionName, partitionName)
	if err != nil {
		return nil, err
	}

	for channel, rowOffsets := range channel2RowOffsets {
		partialUpdateCAS, err := getPartialUpdateCASForStreamingService(partialUpdateCASGroups, channel)
		if err != nil {
			return nil, err
		}

		// segment id is assigned at streaming node.
		msgs, err := repackInsertDataByPartitionForStreamingService(
			ctx,
			partitionID,
			partitionName,
			rowOffsets,
			channel,
			insertMsg,
			ez,
			schemaVersion,
			partialUpdateCAS,
		)
		if err != nil {
			return nil, err
		}
		messages = append(messages, msgs...)
	}
	return messages, nil
}

func repackInsertDataWithPartitionKeyForStreamingService(
	ctx context.Context,
	metaCache Cache,
	channelNames []string,
	insertMsg *msgstream.InsertMsg,
	result *milvuspb.MutationResult,
	partitionKeys *schemapb.FieldData,
	ez *message.CipherConfig,
	schema *schemapb.CollectionSchema,
	schemaVersion int32,
	partialUpdateCASGroups map[string]*messagespb.PartialUpdateCAS,
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
	partitionNames, err := getDefaultPartitionsInPartitionKeyMode(ctx, metaCache, insertMsg.GetDbName(), insertMsg.CollectionName)
	if err != nil {
		mlog.Warn(ctx, "get default partition names failed in partition key mode",
			mlog.FieldCollectionName(insertMsg.CollectionName),
			mlog.Err(err))
		return nil, err
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
		partialUpdateCAS, err := getPartialUpdateCASForStreamingService(partialUpdateCASGroups, channel)
		if err != nil {
			return nil, err
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
			msgs, err := repackInsertDataByPartitionForStreamingService(
				ctx,
				partitionIDs[partitionName],
				partitionName,
				rowOffsets,
				channel,
				insertMsg,
				ez,
				schemaVersion,
				partialUpdateCAS,
			)
			if err != nil {
				return nil, err
			}
			messages = append(messages, msgs...)
		}
	}
	return messages, nil
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
) ([]message.MutableMessage, error) {
	if Params.ProxyCfg.SplitChunkProxy.GetAsBool() {
		return repackInsertDataAtProxyForStreamingService(
			ctx,
			partitionID,
			partitionName,
			rowOffsets,
			channel,
			insertMsg,
			ez,
			schemaVersion,
			partialUpdateCAS,
			channelmgr.GetActiveWALName(),
		)
	}
	return buildSingleInsertMessageForStreamingService(
		partitionID,
		partitionName,
		rowOffsets,
		channel,
		insertMsg,
		ez,
		schemaVersion,
		partialUpdateCAS,
	)
}

// buildSingleInsertMessageForStreamingService builds exactly one logical V1
// insert message per (channel, partition) group. The view encoder writes the
// selected rows directly into the final protobuf payload without materializing
// copied RowIDs, Timestamps, or FieldsData columns in Proxy.
func buildSingleInsertMessageForStreamingService(
	partitionID int64,
	partitionName string,
	rowOffsets []int,
	channel string,
	insertMsg *msgstream.InsertMsg,
	ez *message.CipherConfig,
	schemaVersion int32,
	partialUpdateCAS *messagespb.PartialUpdateCAS,
) ([]message.MutableMessage, error) {
	if len(rowOffsets) == 0 {
		return nil, nil
	}
	if err := insertMsg.CheckAligned(); err != nil {
		return nil, err
	}

	template := &msgpb.InsertRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_Insert),
			commonpbutil.WithTimeStamp(insertMsg.BeginTimestamp),
			commonpbutil.WithSourceID(insertMsg.GetBase().GetSourceID()),
		),
		DbID:           insertMsg.GetDbID(),
		CollectionID:   insertMsg.GetCollectionID(),
		PartitionID:    partitionID,
		DbName:         insertMsg.GetDbName(),
		CollectionName: insertMsg.GetCollectionName(),
		PartitionName:  partitionName,
		SegmentID:      0, // segment id is assigned at StreamingNode.
		ShardName:      channel,
		NumRows:        uint64(len(rowOffsets)),
		Version:        msgpb.InsertDataVersion_ColumnBased,
		Namespace:      insertMsg.Namespace,
	}
	if partialUpdateCAS != nil {
		// CAS lives in Base.Properties and must be present before the encoder
		// computes the exact body size.
		if err := message.EncodePartialUpdateCASIntoInsertTemplate(partialUpdateCAS, template); err != nil {
			return nil, err
		}
	}

	encoder, err := fastpb.NewInsertRequestViewEncoder(template, insertMsg.InsertRequest, rowOffsets)
	if err != nil {
		return nil, err
	}
	builder := message.NewInsertMessageBuilderV1().
		WithVChannel(channel).
		WithHeader(&message.InsertMessageHeader{
			CollectionId: insertMsg.GetCollectionID(),
			Partitions: []*message.PartitionSegmentAssignment{
				{
					PartitionId: partitionID,
					Rows:        uint64(len(rowOffsets)),
					BinarySize:  0, // StreamingNode uses the encoded message size when absent.
				},
			},
			SchemaVersion: &schemaVersion,
		}).
		WithBodyEncoder(encoder)
	if partialUpdateCAS != nil {
		if err := builder.MarkPartialUpdateCASForBodyEncoder(); err != nil {
			return nil, err
		}
	}
	msg, err := builder.
		WithCipher(ez).
		BuildMutable()
	if err != nil {
		return nil, err
	}
	return []message.MutableMessage{msg}, nil
}

func repackInsertDataAtProxyForStreamingService(
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
) ([]message.MutableMessage, error) {
	type pendingInsertPack struct {
		rowOffsets []int
		insertMsg  *msgstream.InsertMsg
	}

	maxMessageSize := Params.PulsarCfg.MaxMessageSize.GetAsInt()
	messages := make([]message.MutableMessage, 0)
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
				return nil, err
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
			return nil, err
		}

		// Entity-size packing does not include the streaming header or CAS
		// metadata. Validate the fully built partial-update message and split the
		// original row offsets again when that final envelope crosses the legacy
		// Proxy transport limit.
		if partialUpdateCAS == nil || msg.EstimateSize() <= maxMessageSize {
			messages = append(messages, msg)
			continue
		}
		if len(pack.rowOffsets) == 1 {
			return nil, merr.WrapErrParameterTooLarge("single partial update row exceeds max message size")
		}

		middle := len(pack.rowOffsets) / 2
		pending = append([]pendingInsertPack{
			{rowOffsets: pack.rowOffsets[:middle]},
			{rowOffsets: pack.rowOffsets[middle:]},
		}, pending...)
	}
	return messages, nil
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
					BinarySize:  0, // StreamingNode uses the encoded message size when absent.
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
