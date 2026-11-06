package dml

import (
	"context"

	"go.opentelemetry.io/otel"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// we only overwrite the Execute function
// TODO: InsertMessageHeader does not carry SchemaVersion, which means the consistency gate
// in StreamingNode cannot tell whether an insert was produced before or after a schema change.
// This can cause a deadlock when the gate waits for inserts at the new schema version that
// will never arrive. The companion PR https://github.com/milvus-io/milvus/pull/48139
// resolves this by propagating SchemaVersion through the insert path.
func (it *InsertTask) Execute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Insert-Execute")
	defer sp.End()

	collID := it.collectionID
	it.insertMsg.CollectionID = collID

	channelNames := it.vChannels
	if len(channelNames) == 0 {
		var err error
		channelNames, err = it.chMgr.GetVChannels(collID)
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
		mlog.Bool("is_parition_key", it.partitionKeys != nil))

	var ez *message.CipherConfig
	if hookutil.IsClusterEncryptionEnabled() {
		ez = hookutil.GetEzByCollProperties(it.schema.GetProperties(), it.collectionID).AsMessageConfig()
	}

	// start to repack insert data
	var msgs []message.MutableMessage
	var err error
	idempotency := it.idempotentInsertDecoration()
	if paramtable.Get().StreamingCfg.EnableUpsertMessage.GetAsBool() {
		err = validateUpsertNamespace(it.insertMsg.Namespace, it.schema)
		if err == nil {
			msgs, err = repackUpsertMessages(ctx, it.GetMetaCache(), channelNames, it.insertMsg, it.result.GetIDs(), it.partitionKeys, it.schema, it.schemaVersion, ez, nil, idempotency, nil, 0)
		}
	} else if it.partitionKeys == nil {
		msgs, err = repackInsertDataForStreamingService(ctx, it.GetMetaCache(), channelNames, it.insertMsg, it.result, ez, it.schemaVersion, nil, idempotency)
	} else {
		msgs, err = repackInsertDataWithPartitionKeyForStreamingService(ctx, it.GetMetaCache(), channelNames, it.insertMsg, it.result, it.partitionKeys, ez, it.schema, it.schemaVersion, nil, idempotency)
	}
	if err != nil {
		mlog.Warn(ctx, "assign segmentID and repack insert data failed", mlog.Err(err))
		it.result.Status = merr.Status(err)
		return err
	}
	resp := streaming.WAL().AppendMessagesWithOptions(ctx, msgs, streaming.AppendOption{
		IdempotencyKey: it.idempotencyKey,
	})
	invalidateMismatchedWriteSchema(ctx, it.GetMetaCache(), collID, resp)
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
				"idempotency key was reused with a different payload; the server kept the original insert result",
			))
		}
	}
	return nil
}
