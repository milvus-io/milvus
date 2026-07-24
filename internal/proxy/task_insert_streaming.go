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
	channelNames := it.vChannels
	if len(channelNames) == 0 {
		channelNames, err = it.chMgr.getVChannels(collID)
		if err != nil {
			mlog.Warn(ctx, "get vChannels failed", mlog.FieldCollectionID(collID), mlog.Err(err))
			it.result.Status = merr.Status(err)
			return err
		}
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
	decorateHeader := it.idempotentInsertHeaderDecorator()
	if it.partitionKeys == nil {
		msgs, err = repackInsertDataForStreamingService(it.TraceCtx(), channelNames, it.insertMsg, it.result, ez, it.schemaVersion, decorateHeader)
	} else {
		msgs, err = repackInsertDataWithPartitionKeyForStreamingService(it.TraceCtx(), channelNames, it.insertMsg, it.result, it.partitionKeys, ez, it.schema, it.schemaVersion, decorateHeader)
	}
	if err != nil {
		mlog.Warn(ctx, "assign segmentID and repack insert data failed", mlog.Err(err))
		it.result.Status = merr.Status(err)
		return err
	}
	resp := streaming.WAL().AppendMessagesWithOptions(ctx, msgs, streaming.AppendOption{
		IdempotencyKey: it.idempotencyKey,
	})
	if err := resp.UnwrapFirstError(); err != nil {
		mlog.Warn(ctx, "append messages to wal failed", mlog.Err(err))
		if status.AsStreamingError(err).IsSchemaVersionMismatch() {
			it.result.Status = merr.Status(merr.ErrCollectionSchemaMismatch)
		} else {
			it.result.Status = merr.Status(err)
		}
		return nil
	}
	// Update result.Timestamp for session consistency.
	it.result.Timestamp = resp.MaxTimeTick()

	if it.idempotencyEnabled {
		warnOnPartialIdempotentDuplicate(ctx, it.idempotencyKey, resp)
		if err := mergeDuplicateInsertResults(it.result, resp); err != nil {
			// The append itself already committed (or deduplicated) durably; the
			// only reachable cause of a merge failure is an EXPLICIT idempotency
			// key reused with a payload of a DIFFERENT SHAPE (row count / PK
			// type), so the stored duplicate result does not line up with this
			// request structurally (auto keys hash the payload and cannot
			// diverge). Surface that as an input error naming the misuse —
			// reporting an internal failure here would tell the client an insert
			// failed when its data exists, deterministically on every retry. The
			// mismatch detail goes to the log.
			//
			// NOTE: this is best-effort, not a payload-equality guarantee. The
			// key is trusted by design (no request fingerprint is stored): a key
			// reused with a same-shape but different payload merges cleanly and
			// returns the original insert's result. Key uniqueness per logical
			// request is the client's contract; see the WithIdempotencyKey docs.
			mlog.Warn(ctx, "idempotent duplicate insert result does not match this request", mlog.Err(err))
			it.result.Status = merr.Status(merr.WrapErrParameterInvalidMsg(
				"idempotency key was reused with a different payload; the server kept the original insert result"))
		}
	}
	return nil
}

func repackInsertDataForStreamingService(
	ctx context.Context,
	channelNames []string,
	insertMsg *msgstream.InsertMsg,
	result *milvuspb.MutationResult,
	ez *message.CipherConfig,
	schemaVersion int32,
	decorateHeader func(*message.InsertMessageHeader, []int) error,
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
		msgs, msgRowOffsets, err := genInsertMsgsByPartition(ctx, 0, partitionID, partitionName, rowOffsets, channel, insertMsg)
		if err != nil {
			return nil, err
		}
		for idx, msg := range msgs {
			insertRequest := msg.(*msgstream.InsertMsg).InsertRequest
			header := &message.InsertMessageHeader{
				CollectionId: insertMsg.CollectionID,
				Partitions: []*message.PartitionSegmentAssignment{
					{
						PartitionId: partitionID,
						Rows:        insertRequest.GetNumRows(),
						BinarySize:  0, // TODO: current not used, message estimate size is used.
					},
				},
				SchemaVersion: &schemaVersion,
			}
			if decorateHeader != nil {
				if err := decorateHeader(header, msgRowOffsets[idx]); err != nil {
					return nil, err
				}
			}
			newMsg, err := message.NewInsertMessageBuilderV1().
				WithVChannel(channel).
				WithHeader(header).
				WithBody(insertRequest).
				WithCipher(ez).
				BuildMutable()
			if err != nil {
				return nil, err
			}
			if err := validateStreamingInsertMessageSize(newMsg); err != nil {
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
	decorateHeader func(*message.InsertMessageHeader, []int) error,
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
			msgs, msgRowOffsets, err := genInsertMsgsByPartition(ctx, 0, partitionIDs[partitionName], partitionName, rowOffsets, channel, insertMsg)
			if err != nil {
				return nil, err
			}
			for idx, msg := range msgs {
				insertRequest := msg.(*msgstream.InsertMsg).InsertRequest
				header := &message.InsertMessageHeader{
					CollectionId: insertMsg.CollectionID,
					Partitions: []*message.PartitionSegmentAssignment{
						{
							PartitionId: partitionIDs[partitionName],
							Rows:        insertRequest.GetNumRows(),
							BinarySize:  0, // TODO: current not used, message estimate size is used.
						},
					},
					SchemaVersion: &schemaVersion,
				}
				if decorateHeader != nil {
					if err := decorateHeader(header, msgRowOffsets[idx]); err != nil {
						return nil, err
					}
				}
				newMsg, err := message.NewInsertMessageBuilderV1().
					WithVChannel(channel).
					WithHeader(header).
					WithBody(insertRequest).
					WithCipher(ez).
					BuildMutable()
				if err != nil {
					return nil, err
				}
				if err := validateStreamingInsertMessageSize(newMsg); err != nil {
					return nil, err
				}
				messages = append(messages, newMsg)
			}
		}
	}
	return messages, nil
}

func validateStreamingInsertMessageSize(msg message.MutableMessage) error {
	if msg == nil {
		return nil
	}
	maxSize := Params.PulsarCfg.MaxMessageSize.GetAsInt()
	if maxSize <= 0 {
		return nil
	}
	messageSize := msg.EstimateSize()
	if messageSize <= maxSize {
		return nil
	}
	return merr.WrapErrParameterInvalidMsg(
		"insert message size %d exceeds max message size %d after adding streaming headers; reduce insert batch size",
		messageSize,
		maxSize,
	)
}
