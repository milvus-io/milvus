package shard

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const interceptorName = "shard"

var _ interceptors.InterceptorWithMetrics = (*shardInterceptor)(nil)

// shardInterceptor is the implementation of shard management interceptor.
type shardInterceptor struct {
	shardManager shards.ShardManager
	ops          map[message.MessageType]interceptors.AppendInterceptorCall
	// nameGates lists the message types whose handler is keyed by collection id
	// and must therefore only run for the vchannel this pchannel holds. See
	// passNameGate.
	nameGates map[message.MessageType]nameGate
}

// notHeldPolicy is what the name gate does with a message addressed to a
// vchannel this pchannel does not hold.
type notHeldPolicy int

const (
	// notHeldAppend appends it without effect, fenced or never held. For
	// broadcast replicas: refusing one would wedge the broadcaster.
	notHeldAppend notHeldPolicy = iota
	// notHeldRefuseUnknown appends it without effect on a fenced source and
	// refuses it (unrecoverable) on a vchannel that was never held, which is
	// what the handler did before the gate existed.
	notHeldRefuseUnknown
	// notHeldRefuse refuses it either way: SHARD_FENCED on a fenced source,
	// unrecoverable otherwise. For messages this node generates itself for a
	// registration it no longer has, whose workers stop on either error.
	notHeldRefuse
)

// nameGate describes how DoAppend treats a collection-keyed message before its
// handler runs.
type nameGate struct {
	// collectionID reads the collection the message is keyed by.
	collectionID func(msg message.MutableMessage) int64
	// withoutEffect appends a replica addressed to a vchannel this pchannel
	// does not hold. nil means a plain append. Unused under notHeldRefuse.
	withoutEffect interceptors.AppendInterceptorCall
	// notHeld decides between appending without effect and refusing.
	notHeld notHeldPolicy
	// exempt, when set and true for the append, skips the gate: the message is
	// not managed by the shard manager at all.
	exempt func(ctx context.Context) bool
}

// initOpTable initializes the operation table for the segment interceptor.
func (impl *shardInterceptor) initOpTable() {
	impl.ops = map[message.MessageType]interceptors.AppendInterceptorCall{
		message.MessageTypeCreateCollection:   impl.handleCreateCollection,
		message.MessageTypeDropCollection:     impl.handleDropCollection,
		message.MessageTypeCreatePartition:    impl.handleCreatePartition,
		message.MessageTypeDropPartition:      impl.handleDropPartition,
		message.MessageTypeInsert:             impl.handleInsertMessage,
		message.MessageTypeDelete:             impl.handleDeleteMessage,
		message.MessageTypeManualFlush:        impl.handleManualFlushMessage,
		message.MessageTypeSchemaChange:       impl.handleSchemaChange,
		message.MessageTypeAlterCollection:    impl.handleAlterCollection,
		message.MessageTypeCreateSegment:      impl.handleCreateSegment,
		message.MessageTypeFlush:              impl.handleFlushSegment,
		message.MessageTypeFlushAll:           impl.handleFlushAllMessage,
		message.MessageTypeTruncateCollection: impl.handleTruncateCollectionMessage,
		message.MessageTypeSplitShard:         impl.handleSplitShardMessage,
	}
	// Every message whose handler is keyed by collection id (and partition id).
	// Not listed, on purpose:
	//   - CreateCollection and SplitShard: genesis (and fence) messages, which
	//     create or release the registration themselves.
	//   - Insert and Delete: DML keeps its own admission, which maps a fenced
	//     vchannel to SHARD_FENCED (rejectUnwritableVChannel); the insert path
	//     answers it under the same read lock as the schema version check.
	//   - FlushAll: a pchannel-level message on the control channel, keyed by
	//     nothing but the pchannel.
	//   - Import: has no handler; it takes no shard-manager action on any
	//     vchannel, held or not.
	impl.nameGates = map[message.MessageType]nameGate{
		message.MessageTypeDropCollection: {
			collectionID: func(msg message.MutableMessage) int64 {
				return message.MustAsMutableDropCollectionMessageV1(msg).Header().GetCollectionId()
			},
			withoutEffect: impl.appendDropCollectionWithoutEffect,
		},
		message.MessageTypeCreatePartition: {
			collectionID: func(msg message.MutableMessage) int64 {
				return message.MustAsMutableCreatePartitionMessageV1(msg).Header().GetCollectionId()
			},
		},
		message.MessageTypeDropPartition: {
			collectionID: func(msg message.MutableMessage) int64 {
				return message.MustAsMutableDropPartitionMessageV1(msg).Header().GetCollectionId()
			},
		},
		message.MessageTypeManualFlush: {
			collectionID: func(msg message.MutableMessage) int64 {
				return message.MustAsMutableManualFlushMessageV2(msg).Header().GetCollectionId()
			},
			withoutEffect: impl.appendManualFlushWithoutEffect,
			// A vchannel this pchannel never held has always been refused with
			// CollectionNotFound; only a fenced source becomes a no-op.
			notHeld: notHeldRefuseUnknown,
		},
		message.MessageTypeSchemaChange: {
			collectionID: func(msg message.MutableMessage) int64 {
				return message.MustAsMutableSchemaChangeMessageV2(msg).Header().GetCollectionId()
			},
		},
		message.MessageTypeAlterCollection: {
			collectionID: func(msg message.MutableMessage) int64 {
				return message.MustAsMutableAlterCollectionMessageV2(msg).Header().GetCollectionId()
			},
		},
		message.MessageTypeTruncateCollection: {
			collectionID: func(msg message.MutableMessage) int64 {
				return message.MustAsMutableTruncateCollectionMessageV2(msg).Header().GetCollectionId()
			},
		},
		// CreateSegment and Flush are appended by a partition manager's own
		// segment workers, and their handlers look the partition up by
		// (collection, partition) -- not by segment id, and not by vchannel. A
		// worker started for a source before its fence can still be retrying when
		// a split target of the same collection has registered the same partition
		// on this pchannel, and without the gate its message would be applied to
		// the target: a CreateSegment panics an idle target partition manager or
		// completes a pending allocation with the source's segment. Both are
		// refused rather than appended: they are not broadcast replicas, nothing
		// is waiting on them, and the workers stop on SHARD_FENCED and on an
		// unrecoverable error alike. The segments of a fenced source were sealed
		// by the fence record itself.
		message.MessageTypeCreateSegment: {
			collectionID: func(msg message.MutableMessage) int64 {
				return message.MustAsMutableCreateSegmentMessageV2(msg).Header().GetCollectionId()
			},
			notHeld: notHeldRefuse,
		},
		message.MessageTypeFlush: {
			collectionID: func(msg message.MutableMessage) int64 {
				return message.MustAsMutableFlushMessageV2(msg).Header().GetCollectionId()
			},
			notHeld: notHeldRefuse,
			// A flush from the old architecture is not managed by the shard
			// manager, so it is appended as it always was.
			exempt: utility.GetFlushFromOldArch,
		},
	}
}

// Name returns the name of the interceptor.
func (impl *shardInterceptor) Name() string {
	return interceptorName
}

// DoAppend assigns segment for every partition in the message.
func (impl *shardInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (msgID message.MessageID, err error) {
	op, ok := impl.ops[msg.MessageType()]
	if !ok || (funcutil.IsControlChannel(msg.VChannel()) && !msg.IsPChannelLevel()) {
		// Only a registered message type has shard-manager work. A control
		// channel message is only used to determine the DDL/DCL order and
		// performs no effect on the shard manager, so skip it.
		return appendOp(ctx, msg)
	}
	if gate, gated := impl.nameGates[msg.MessageType()]; gated {
		return impl.passNameGate(ctx, msg, gate, op, appendOp)
	}
	return op(ctx, msg, appendOp)
}

// passNameGate runs op only when this pchannel holds the vchannel msg is
// addressed to.
//
// The shard manager keeps one registration per collection per pchannel, and
// the handlers behind this gate act by collection id. A split fence releases
// the source's registration on the spot, so two kinds of message reach a
// pchannel that does not hold their vchannel: one addressed to the fenced
// source, which stays on the collection's vchannel list until adoption retires
// it, and one addressed to a source whose slot a split target has since taken.
// Acting on either would flush, re-schema, re-partition or allocate segments
// on whatever vchannel does hold the entry.
//
// A broadcast replica is appended with no effect on shard state, deliberately
// not refused: the broadcaster retries an unrecoverable replica forever while
// it holds the collection's exclusive key, so a refusal would wedge the
// collection's DDL until adoption. ManualFlush is refused only for a vchannel
// that was never held and never fenced, which is what it has always done.
// CreateSegment and Flush, which this node's own segment workers append, are
// refused either way (see initOpTable).
//
// The fence and a drop of a vchannel take its exclusive lock, and every gated
// message takes that vchannel's lock (exclusive for DDL, shared for
// CreateSegment and Flush) for its whole append, so the answer cannot change
// between this check and the handler.
func (impl *shardInterceptor) passNameGate(ctx context.Context, msg message.MutableMessage, gate nameGate, op interceptors.AppendInterceptorCall, appendOp interceptors.Append) (message.MessageID, error) {
	if gate.exempt != nil && gate.exempt(ctx) {
		return op(ctx, msg, appendOp)
	}
	collectionID := gate.collectionID(msg)
	err := impl.shardManager.CheckIfVChannelCanBeWritten(collectionID, msg.VChannel())
	if err == nil {
		return op(ctx, msg, appendOp)
	}
	fenced := errors.Is(err, shards.ErrVChannelFenced)
	switch {
	case gate.notHeld == notHeldRefuse:
		impl.shardManager.Logger().Warn(ctx, "message addressed to a vchannel this pchannel does not hold, refused",
			mlog.FieldCollectionID(collectionID), mlog.FieldVChannel(msg.VChannel()),
			mlog.String("messageType", msg.MessageType().String()), mlog.Bool("fenced", fenced), mlog.Err(err))
		return nil, rejectUnwritableVChannel(msg.VChannel(), collectionID, err)
	case gate.notHeld == notHeldRefuseUnknown && !fenced:
		return nil, status.NewUnrecoverableError("vchannel %s of collection %d is not held by this pchannel: %s", msg.VChannel(), collectionID, err.Error())
	}
	impl.shardManager.Logger().Info(ctx, "message addressed to a vchannel this pchannel does not hold, appending without effect",
		mlog.FieldCollectionID(collectionID), mlog.FieldVChannel(msg.VChannel()),
		mlog.String("messageType", msg.MessageType().String()), mlog.Bool("fenced", fenced), mlog.Err(err))
	if gate.withoutEffect != nil {
		return gate.withoutEffect(ctx, msg, appendOp)
	}
	return appendOp(ctx, msg)
}

// rejectUnwritableVChannel is the single mapping from a failed DML admission
// check to the error the client sees.
func rejectUnwritableVChannel(vchannel string, collectionID int64, err error) error {
	if errors.Is(err, shards.ErrVChannelFenced) {
		// the vchannel is fenced by shard split. The intended client reaction
		// is to refresh the routing table and write to the new shards; the
		// proxy side of that is not implemented on this branch. T_switch is
		// not carried here: a DML client would refresh routing, never read it.
		return status.NewShardFenced(vchannel, 0, 0)
	}
	// This pchannel has never held the vchannel, so no routing refresh sends the
	// write anywhere -- terminal, unlike the fence above. It must still be a
	// rejection rather than a pass: every check after this one is keyed by
	// collection (schema version) or by (collection, partition) (segment
	// assignment), so a message let through would be appended carrying vchannel
	// X while its segment belongs to vchannel Y. The flusher finds no data sync
	// service for X and drops the batch, recovery counts the rows against Y's
	// segment, and the client is told the append succeeded.
	return status.NewUnrecoverableError("vchannel %s of collection %d cannot be written: %s", vchannel, collectionID, err.Error())
}

// handleCreateCollection handles the create collection message.
func (impl *shardInterceptor) handleCreateCollection(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	createCollectionMsg := message.MustAsMutableCreateCollectionMessageV1(msg)
	body := createCollectionMsg.MustBody()
	if body.GetCollectionSchema() == nil && len(body.GetSchema()) == 0 {
		return nil, status.NewUnrecoverableError("create collection message does not contain collection schema")
	}
	schema := messageutil.MustGetSchemaFromCreateCollectionMessageBody(body)
	header := createCollectionMsg.Header()
	if err := impl.shardManager.CheckIfCollectionCanBeCreated(header.GetCollectionId()); err != nil {
		impl.shardManager.Logger().Warn(ctx, "collection already exists when creating collection", mlog.FieldCollectionID(header.GetCollectionId()))
		// The collection can not be created at current shard, ignored
		// TODO: idompotent for wal is required in future, but current milvus state is not recovered from wal.
		// return nil, status.NewUnrecoverableError(err.Error())
	}

	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return msgID, err
	}
	impl.shardManager.CreateCollection(message.MustAsImmutableCreateCollectionMessageV1(msg.IntoImmutableMessage(msgID)))
	// Legacy CreateCollection messages keep the schema in the serialized Schema
	// field instead of CollectionSchema. Resolve both formats so Alloc always
	// registers the WAL lifecycle key before later schema updates.
	impl.allocFunctionRunners(header.GetCollectionId(), createCollectionMsg.VChannel(), schema)
	return msgID, nil
}

// handleSplitShardOnTarget handles the TARGET replica of a split broadcast: the
// genesis message of a new vchannel. It registers the collection on this
// pchannel for DML and segment assignment exactly as create collection.
func (impl *shardInterceptor) handleSplitShardOnTarget(ctx context.Context, msg message.MutableMessage, splitShardMsg message.MutableSplitShardMessageV2, appendOp interceptors.Append) (message.MessageID, error) {
	body := splitShardMsg.MustBody().GetGenesis()
	if body.GetCollectionSchema() == nil && len(body.GetSchema()) == 0 {
		// The same guard CreateCollection has, for the same reason and one more.
		// Without a schema the shard manager registers a nil one -- every
		// versioned insert to the new target then fails with
		// ErrCollectionSchemaNotFound -- while the recovery storage seeds an
		// empty non-nil one from the shared parser, so the shard behaves
		// differently before and after a restart. Refuse at the only point that
		// can enforce it against any coordinator version.
		return nil, status.NewUnrecoverableError("split shard target replica does not contain collection schema")
	}
	// Resolved through the same helper the recovery storage uses, so all three
	// genesis consumers read the body the same way -- which is the point of the
	// target genesis reusing CreateCollection's body shape.
	schema := messageutil.MustGetSchemaFromCreateCollectionMessageBody(body)
	header := splitShardMsg.Header()
	if err := impl.shardManager.CheckIfVChannelCanBeCreated(header.GetCollectionId(), msg.VChannel()); err != nil {
		if errors.Is(err, shards.ErrVChannelConflict) {
			// Refuse rather than warn-and-continue. The shard manager holds one
			// entry per collection per pchannel, so appending anyway would give
			// the new shard a WAL genesis and a recovery-storage entry while
			// leaving it with no segment assignment at all -- and, if the
			// incumbent is a fenced split source, an inherited fence that makes
			// the new shard permanently unwritable. The coordinator must retire
			// the source (the delisting routing commit) before placing a
			// successor here.
			impl.shardManager.Logger().Warn(ctx, "cannot create vchannel on this pchannel",
				mlog.FieldCollectionID(header.GetCollectionId()), mlog.Err(err))
			return nil, status.NewUnrecoverableError("%s", err.Error())
		}
		// ErrCollectionExists: the same vchannel is already registered. The
		// genesis is still appended and applied; every consumer is idempotent.
		impl.shardManager.Logger().Warn(ctx, "vchannel already exists when creating vchannel",
			mlog.FieldCollectionID(header.GetCollectionId()))
	}

	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return msgID, err
	}
	impl.shardManager.CreateVChannel(message.MustAsImmutableSplitShardMessageV2(msg.IntoImmutableMessage(msgID)))
	// The apply re-checks under the write lock and may skip the registration --
	// the append-time check above and it are not one critical section. Allocating
	// the function-runner key anyway would leak it: Close releases by REGISTERED
	// vchannel, and an unregistered one is never released.
	if err := impl.shardManager.CheckIfVChannelCanBeWritten(header.GetCollectionId(), msg.VChannel()); err != nil {
		impl.shardManager.Logger().Warn(ctx, "vchannel genesis appended but not registered, skipping function runner alloc",
			mlog.FieldCollectionID(header.GetCollectionId()), mlog.FieldVChannel(msg.VChannel()), mlog.Err(err))
		return msgID, nil
	}
	// "Exactly as create collection" has to include the WAL's function-runner
	// lifecycle key. Without it every insert to the new target is rejected at
	// materializeFunctionFields with "function runner schema for key
	// WAL-<vchannel> is not available" — even for a collection that declares no
	// function, because the key is what carries the schema snapshot the
	// materializer resolves against. The split's targets are created live, so
	// nothing else registers them until the WAL is next recovered.
	impl.allocFunctionRunners(header.GetCollectionId(), msg.VChannel(), schema)
	return msgID, nil
}

// handleDropCollection handles the drop collection message.
func (impl *shardInterceptor) handleDropCollection(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	dropCollectionMessage := message.MustAsMutableDropCollectionMessageV1(msg)
	if err := impl.shardManager.CheckIfCollectionExists(dropCollectionMessage.Header().GetCollectionId()); err != nil {
		impl.shardManager.Logger().Warn(ctx, "collection not found when dropping collection", mlog.FieldCollectionID(dropCollectionMessage.Header().GetCollectionId()))
		// The collection can not be dropped at current shard, ignored
		// TODO: idompotent for wal is required in future, but current milvus state is not recovered from wal.
		// return nil, status.NewUnrecoverableError(err.Error())
	}

	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return msgID, err
	}
	impl.shardManager.DropCollection(message.MustAsImmutableDropCollectionMessageV1(msg.IntoImmutableMessage(msgID)))
	function.GetManager().Release(dropCollectionMessage.Header().GetCollectionId(), walFunctionRunnerKey(dropCollectionMessage.VChannel()))
	return msgID, nil
}

// appendDropCollectionWithoutEffect appends a DropCollection replica addressed
// to a vchannel this pchannel does not hold. The shard manager is not touched --
// its DropCollection is keyed by collection id and would tear down whichever
// vchannel holds the entry -- but the addressed vchannel's WAL function-runner
// key is still released, exactly as handleDropCollection does: that key is
// keyed by vchannel, and Release is idempotent.
func (impl *shardInterceptor) appendDropCollectionWithoutEffect(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return msgID, err
	}
	collectionID := message.MustAsMutableDropCollectionMessageV1(msg).Header().GetCollectionId()
	function.GetManager().Release(collectionID, walFunctionRunnerKey(msg.VChannel()))
	return msgID, nil
}

// appendManualFlushWithoutEffect appends a ManualFlush addressed to a fenced
// source: nothing is growing there (the fence sealed it), so the flushed set is
// empty. The extra response is still set, because the proxy decodes it from
// every ManualFlush append result and fails the whole Flush without it.
func (impl *shardInterceptor) appendManualFlushWithoutEffect(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	manualFlushMsg := message.MustAsMutableManualFlushMessageV2(msg)
	header := manualFlushMsg.Header()
	header.SegmentIds = nil
	manualFlushMsg.OverwriteHeader(header)
	utility.ModifyAppendResultExtra(ctx, func(old *message.ManualFlushExtraResponse) *message.ManualFlushExtraResponse {
		return &messagespb.ManualFlushExtraResponse{SegmentIds: []int64{}}
	})
	return appendOp(ctx, msg)
}

// handleCreatePartition handles the create partition message.
func (impl *shardInterceptor) handleCreatePartition(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	createPartitionMessage := message.MustAsMutableCreatePartitionMessageV1(msg)
	h := createPartitionMessage.Header()
	if err := impl.shardManager.CheckIfPartitionCanBeCreated(shards.PartitionUniqueKey{CollectionID: h.GetCollectionId(), PartitionID: h.GetPartitionId()}); err != nil {
		impl.shardManager.Logger().Warn(ctx, "partition already exists when creating partition", mlog.FieldCollectionID(h.GetCollectionId()), mlog.FieldPartitionID(h.GetPartitionId()))
		// TODO: idompotent for wal is required in future, but current milvus state is not recovered from wal.
		// return nil, status.NewUnrecoverableError(err.Error())
	}

	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return nil, err
	}
	impl.shardManager.CreatePartition(message.MustAsImmutableCreatePartitionMessageV1(msg.IntoImmutableMessage(msgID)))
	return msgID, nil
}

// handleDropPartition handles the drop partition message.
func (impl *shardInterceptor) handleDropPartition(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	dropPartitionMessage := message.MustAsMutableDropPartitionMessageV1(msg)
	h := dropPartitionMessage.Header()
	if err := impl.shardManager.CheckIfPartitionExists(shards.PartitionUniqueKey{CollectionID: h.GetCollectionId(), PartitionID: h.GetPartitionId()}); err != nil {
		impl.shardManager.Logger().Warn(ctx, "partition not found when dropping partition", mlog.FieldCollectionID(h.GetCollectionId()), mlog.FieldPartitionID(h.GetPartitionId()))
		// The partition can not be dropped at current shard, ignored
		// TODO: idompotent for wal is required in future, but current milvus state is not recovered from wal.
		// return nil, status.NewUnrecoverableError(err.Error())
	}

	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return msgID, err
	}
	impl.shardManager.DropPartition(message.MustAsImmutableDropPartitionMessageV1(msg.IntoImmutableMessage(msgID)))
	return msgID, nil
}

// handleInsertMessage handles the insert message.
func (impl *shardInterceptor) handleInsertMessage(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	insertMsg := message.MustAsMutableInsertMessageV1(msg)
	// Assign segment for insert message.
	// !!! Current implementation a insert message only has one parition, but we need to merge the message for partition-key in future.
	header := insertMsg.Header()

	collectionID := header.GetCollectionId()
	schemaVersion := header.GetSchemaVersion()
	// Both admission questions -- may this vchannel be written, and does the
	// header's schema version match -- under one read lock. They were already
	// consistent with each other (the vchannel-exclusive lock upstream keeps the
	// fence from flipping between them); this only stops paying for the second
	// acquisition on the hot path.
	correctSchemaVersion, err := impl.shardManager.CheckWritableAndSchemaVersion(msg.VChannel(), header)
	if err != nil {
		if errors.IsAny(err, shards.ErrVChannelFenced, shards.ErrCollectionNotFound) {
			return nil, rejectUnwritableVChannel(msg.VChannel(), collectionID, err)
		}
		if errors.Is(err, shards.ErrCollectionSchemaNotFound) {
			return nil, status.NewUnrecoverableError("collection %d schema not provided by create collection message", collectionID)
		}
		if errors.Is(err, shards.ErrCollectionSchemaVersionNotMatch) {
			impl.shardManager.Logger().Warn(ctx, "insertMessage schema version mismatch",
				mlog.FieldCollectionID(collectionID),
				mlog.Bool("schemaVersionProvided", header.SchemaVersion != nil),
				mlog.Int32("schemaVersion", schemaVersion),
				mlog.Int32("collectionSchemaVersion", correctSchemaVersion),
				mlog.Err(err))
			return nil, status.NewSchemaVersionMismatch("schema version mismatch, input schema version: %d, collection schema version: %d",
				schemaVersion, correctSchemaVersion)
		}
		impl.shardManager.Logger().Error(ctx, "unexpected error from CheckWritableAndSchemaVersion",
			mlog.FieldCollectionID(collectionID),
			mlog.Bool("schemaVersionProvided", header.SchemaVersion != nil),
			mlog.Int32("schemaVersion", schemaVersion),
			mlog.Err(err))
		return nil, status.NewUnrecoverableError("unexpected error from CheckIfCollectionSchemaVersionMatch: %s", err.Error())
	}
	schemaVersion = correctSchemaVersion
	if header.SchemaVersion == nil {
		schemaVersion = function.LatestFunctionRunnerVersion
	}
	// Write-before function materialization is version-gated: it only runs once
	// the whole cluster has confirmed version >= GateVersion (config default
	// "auto"); before that the write path keeps the legacy format. The gate is
	// applied at the config-item read layer, so GetAsBool already returns the
	// effective value.
	if paramtable.Get().FunctionCfg.EnableWriteBeforeMaterialization.GetAsBool() {
		if err := impl.materializeFunctionFields(ctx, insertMsg, header.GetCollectionId(), schemaVersion); err != nil {
			impl.shardManager.Logger().Warn(ctx, "failed to materialize function fields before WAL append",
				mlog.Int64("collectionID", header.GetCollectionId()),
				mlog.Int32("schemaVersion", schemaVersion),
				mlog.Err(err))
			return nil, status.NewUnrecoverableError("failed to materialize function fields before WAL append: %s", err.Error())
		}
	}
	for _, partition := range header.GetPartitions() {
		if partition.BinarySize == 0 {
			// Proxy does not estimate binary size today. Use the payload size;
			// note this excludes materialized function output fields while the
			// version gate is off (matching pre-feature behavior).
			partition.BinarySize = uint64(msg.EstimateSize())
		}
		req := &shards.AssignSegmentRequest{
			CollectionID: header.GetCollectionId(),
			PartitionID:  partition.GetPartitionId(),
			ModifiedMetrics: stats.ModifiedMetrics{
				Rows:       partition.GetRows(),
				BinarySize: partition.GetBinarySize(),
			},
			TimeTick: msg.TimeTick(),
		}
		if session := txn.GetTxnSessionFromContext(ctx); session != nil {
			// because the shard manager use the interface, txn is a struct,
			// so we need to check nil before the assignment.
			req.TxnSession = session
		}
		result, err := impl.shardManager.AssignSegment(req)
		if errors.IsAny(err, shards.ErrTimeTickTooOld, shards.ErrWaitForNewSegment, shards.ErrFencedAssign) {
			// 1. time tick is too old for segment assignment.
			// 2. partition is fenced.
			// 3. segment is not ready.
			// we just redo it to refresh a new latest timetick.
			return nil, redo.ErrRedo
		}
		if errors.IsAny(err, shards.ErrPartitionNotFound, shards.ErrCollectionNotFound) {
			// The target metadata no longer exists, so retrying cannot recover the operation.
			impl.shardManager.Logger().Warn(ctx, "unrecoverable insert operation", mlog.Object("message", msg), mlog.Err(err))
			return nil, status.NewUnrecoverableError("fail to assign segment, %s", err.Error())
		}
		if err != nil {
			return nil, err
		}
		// once the segment assignment is done, we need to ack the result,
		// if other partitions failed to assign segment or wal write failure,
		// the segment assignment will not rolled back for simple implementation.
		defer result.Ack()

		// Attach segment assignment to message.
		partition.SegmentAssignment = &message.SegmentAssignment{
			SegmentId: result.SegmentID,
		}
	}
	// Update the insert message headers.
	insertMsg.OverwriteHeader(header)
	return appendOp(ctx, msg)
}

// handleDeleteMessage handles the delete message.
func (impl *shardInterceptor) handleDeleteMessage(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	deleteMessage := message.MustAsMutableDeleteMessageV1(msg)
	header := deleteMessage.Header()
	if err := impl.shardManager.CheckIfVChannelCanBeWritten(header.GetCollectionId(), msg.VChannel()); err != nil {
		return nil, rejectUnwritableVChannel(msg.VChannel(), header.GetCollectionId(), err)
	}
	// No separate existence check: the admission above already answers
	// ErrCollectionNotFound when this pchannel holds no entry for the
	// collection, so a second lock acquisition would ask the same question.

	impl.shardManager.ApplyDelete(deleteMessage)
	return appendOp(ctx, msg)
}

// handleSplitShardMessage handles one replica of a SplitShard broadcast by the
// role its vchannel plays. The control channel replica never reaches here
// (DoAppend skips it).
func (impl *shardInterceptor) handleSplitShardMessage(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	splitShardMsg := message.MustAsMutableSplitShardMessageV2(msg)
	switch message.SplitShardRoleOf(splitShardMsg.Header(), msg.VChannel()) {
	case message.SplitShardRoleSource:
		return impl.handleSplitShardOnSource(ctx, msg, splitShardMsg, appendOp)
	case message.SplitShardRoleTarget:
		return impl.handleSplitShardOnTarget(ctx, msg, splitShardMsg, appendOp)
	default:
		// The broadcast reaches only the source, the targets and the control
		// channel, so a replica on any other vchannel -- another shard of the
		// same collection included -- is a coordinator bug; refuse it rather
		// than fence or register a stranger.
		return nil, status.NewUnrecoverableError("split shard replica landed on vchannel %s, which is neither a source nor a target of task %d",
			msg.VChannel(), splitShardMsg.Header().GetSplitTaskId())
	}
}

// reportSplitSwitchTimeTick hands the source's T_switch back to whoever
// acknowledges this replica, on both paths an ack can take: the append result's
// extra (what the broadcaster's own append receives) and the record itself
// (what a consumer-side ack reads off the WAL -- the only ack a secondary
// cluster ever sees). The two carry the same SplitShardExtraResponse, so the
// ack callback reads one value however the replica was acknowledged.
func reportSplitSwitchTimeTick(ctx context.Context, msg message.MutableMessage, switchTimeTick uint64) {
	resp := &message.SplitShardExtraResponse{SplitTimeTick: switchTimeTick}
	extra, err := anypb.New(resp)
	if err != nil {
		panic("unreachable: failed to marshal the split shard extra response")
	}
	message.SetAppendExtra(msg, extra)
	utility.ModifyAppendResultExtra(ctx, func(*message.SplitShardExtraResponse) *message.SplitShardExtraResponse {
		return resp
	})
}

// handleSplitShardOnSource handles the SOURCE replica of a split broadcast.
// The message is the write fence of the source vchannel: it must be appended
// exclusively (ExclusiveRequired), and after it is persisted the vchannel
// never accepts new DML again.
func (impl *shardInterceptor) handleSplitShardOnSource(ctx context.Context, msg message.MutableMessage, splitShardMsg message.MutableSplitShardMessageV2, appendOp interceptors.Append) (message.MessageID, error) {
	header := splitShardMsg.Header()
	collectionID := header.GetCollectionId()
	// Every fence names the task that places it: the task id is what tells a
	// re-driven fence of the same task from a fence another split placed, and a
	// zero id would make a first fence unattributable and a re-fence
	// indistinguishable from any other zero. The builder refuses it before the
	// broadcast; a message that reached the WAL some other way is refused here,
	// before the source is sealed or fenced, on the first fence and a re-fence
	// alike.
	if header.GetSplitTaskId() == 0 {
		return nil, status.NewUnrecoverableError("split shard fence on vchannel %s carries no split task id", msg.VChannel())
	}
	if err := impl.shardManager.CheckIfVChannelCanBeWritten(collectionID, msg.VChannel()); err != nil {
		if !errors.Is(err, shards.ErrVChannelFenced) {
			return nil, status.NewUnrecoverableError("%s", err.Error())
		}
		// Already fenced. The broadcaster re-drives a split whose source
		// landed but was not yet persisted, so this task's own fence must
		// append again and succeed -- but it moves nothing: T_switch stays the
		// tick of the task's FIRST fence record, and that recorded tick is what
		// this replica reports back. Raising it would be unsafe, not just
		// unnecessary: the source's data sync service may already have drained
		// past the first fence and closed, leaving nothing to advance its
		// checkpoint to a later tick, so a raised T_switch could never drain
		// and would pin WAL truncation for good. Every fence record of one task
		// seals the same data, because the vchannel took no DML in between.
		//
		// A fence recorded by any other task -- including one whose TaskID
		// reads zero -- is a coordinator invariant violation (one active task
		// per source; every fence SplitShard places carries the placing task's
		// id, so zero is a coordinator bug, not a legacy fence); refusing it is
		// what keeps two splits from carving one source twice.
		fence := impl.shardManager.GetSplitFence(collectionID, msg.VChannel())
		if fence.TaskID != header.GetSplitTaskId() {
			return nil, status.NewShardFenced(msg.VChannel(), fence.TimeTick, fence.TaskID)
		}
		//
		// The re-fence is also how a first fence whose append FAILED reaches the
		// WAL: the fence below is installed before its append and never rolled
		// back, so the re-drive lands here and reports the first attempt's tick
		// even when no record carries it. Every consumer of this record reads
		// T_switch from the tick reported here, never from the record's own
		// tick: the ack callback from the extra response, the recovery storage
		// from the stamped record.
		impl.shardManager.Logger().Info(ctx, "source vchannel already fenced by this task, appending the fence again",
			mlog.FieldCollectionID(collectionID), mlog.FieldVChannel(msg.VChannel()), mlog.Uint64("fencedTimeTick", fence.TimeTick))
		reportSplitSwitchTimeTick(ctx, msg, fence.TimeTick)
		// Idempotent; the first attempt released it already unless this node
		// restarted since, when a SPLITTED vchannel holds no key to release.
		function.GetManager().Release(collectionID, walFunctionRunnerKey(msg.VChannel()))
		return appendOp(ctx, msg)
	}
	// Auto-flush every growing segment of the vchannel as of the fence time
	// tick and embed the sealed segment ids into the message header, exactly
	// as the AlterCollection schema-change path does. This SplitShard message
	// is the single authoritative seal record for T_switch — there is no
	// separate ManualFlush anymore — so the downstream consumers (flusher,
	// delegator, recovery) learn the sealed set only from here.
	//
	// The seal happens before the append, as it must: the ids have to be in the
	// header the append persists.
	segmentIDs, err := impl.shardManager.FlushAndFenceSegmentAllocUntil(collectionID, msg.TimeTick())
	if err != nil {
		return nil, status.NewUnrecoverableError("%s", err.Error())
	}
	header.FlushedSegmentIds = segmentIDs
	splitShardMsg.OverwriteHeader(header)
	// The first fence: this replica's own tick is T_switch. The time tick
	// interceptor runs before this one, so the tick is already assigned.
	reportSplitSwitchTimeTick(ctx, msg, msg.TimeTick())

	// Fence BEFORE the append, and keep the fence whatever the append returns.
	//
	// An append error does not mean the record is absent: the WAL adaptor
	// returns a canceled or expired context, or a fenced WAL term, as it gets
	// it, and the backend may have written the record first. Were the fence
	// installed only after a successful append, such a record would be in the
	// WAL -- the recovery storage and the flusher treating the vchannel as
	// fenced at this tick -- while this node kept accepting DML after it, and
	// the broadcaster's re-drive would then be a first fence at a later tick.
	// Installed here, DML after this tick is refused either way, and the
	// re-drive takes the re-fence branch above and reports this tick. If the
	// append definitely failed, no record carries this tick, but nothing was
	// accepted after it either, so it is still a valid T_switch for the record
	// the re-drive persists; if this node restarts first, the fence is lost with
	// its memory, nothing recorded it, and the re-drive is a genuine first
	// fence. The price of a definite failure is DML refused with SHARD_FENCED
	// until the re-drive lands.
	impl.shardManager.SplitShard(splitShardMsg)
	// The fence is where the vchannel stops taking writes and loses its
	// registration, so it is where the WAL function-runner key creation took per
	// VCHANNEL goes back -- releasing exactly this one and no other, as
	// handleDropCollection does. Leaving it for the retire (hours later, at
	// adoption) would leak it across any WAL close in between: Close releases by
	// REGISTERED vchannel, and a fenced source has no registration left to be
	// found under. Release is idempotent, so a re-driven fence releasing again
	// costs nothing.
	function.GetManager().Release(collectionID, walFunctionRunnerKey(msg.VChannel()))
	return appendOp(ctx, msg)
}

// handleManualFlushMessage handles the manual flush message.
func (impl *shardInterceptor) handleManualFlushMessage(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	maunalFlushMsg := message.MustAsMutableManualFlushMessageV2(msg)
	header := maunalFlushMsg.Header()
	segmentIDs, err := impl.shardManager.FlushAndFenceSegmentAllocUntil(header.GetCollectionId(), msg.TimeTick())
	if err != nil {
		return nil, status.NewUnrecoverableError(err.Error())
	}

	// Modify the extra response for manual flush message.
	utility.ModifyAppendResultExtra(ctx, func(old *message.ManualFlushExtraResponse) *message.ManualFlushExtraResponse {
		return &messagespb.ManualFlushExtraResponse{SegmentIds: segmentIDs}
	})
	header.SegmentIds = segmentIDs
	maunalFlushMsg.OverwriteHeader(header)

	return appendOp(ctx, msg)
}

// handleSchemaChange handles the schema change message.
func (impl *shardInterceptor) handleSchemaChange(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	schemaChangeMsg := message.MustAsMutableSchemaChangeMessageV2(msg)
	header := schemaChangeMsg.Header()
	segmentIDs, err := impl.shardManager.FlushAndFenceSegmentAllocUntil(header.GetCollectionId(), msg.TimeTick())
	if err != nil {
		return nil, status.NewUnrecoverableError(err.Error())
	}

	// Modify the header of schema change message, carry with the all flushed segment ids.
	header.FlushedSegmentIds = segmentIDs
	schemaChangeMsg.OverwriteHeader(header)
	return appendOp(ctx, msg)
}

// handleAlterCollection handles the alter collection message.
func (impl *shardInterceptor) handleAlterCollection(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	putCollectionMsg := message.MustAsMutableAlterCollectionMessageV2(msg)
	header := putCollectionMsg.Header()

	// A routing commit that no longer names this vchannel retires it: the same
	// broadcast that grows the collection's vchannel list is what delists the
	// spent source, so this replica is about one retired shard, not about the
	// collection's state. The collection-wide apply below must not run for it.
	if messageutil.RetiresVChannel(header, putCollectionMsg.MustBody().GetUpdates(), msg.VChannel()) {
		// Nothing is left to do: the registration went at the fence and so did
		// the vchannel's function-runner key. The replica is still appended --
		// the recovery storage and the flusher, both keyed by vchannel, finish
		// their own teardown from it -- but it must not fall through to the
		// collection-wide apply below, which is keyed by collection id and
		// would flush and re-schema whichever vchannel now holds the entry.
		return appendOp(ctx, msg)
	}

	// AlterCollection atomically flushes+fences segments (if schema change) and updates
	// in-memory schema — all within one critical region of the shard manager.
	segmentIDs, err := impl.shardManager.AlterCollection(putCollectionMsg)
	if err != nil {
		return nil, status.NewUnrecoverableError(err.Error())
	}

	// Embed flushed segment IDs into the WAL message header before appending.
	if len(segmentIDs) > 0 {
		header.FlushedSegmentIds = segmentIDs
		putCollectionMsg.OverwriteHeader(header)
	}

	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return msgID, err
	}
	if messageutil.IsSchemaChange(header) {
		if schema := putCollectionMsg.MustBody().GetUpdates().GetSchema(); schema != nil {
			impl.updateFunctionRunners(header.GetCollectionId(), putCollectionMsg.VChannel(), schema)
		}
	}
	return msgID, nil
}

// handleCreateSegment handles the create segment message.
func (impl *shardInterceptor) handleCreateSegment(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	createSegmentMsg := message.MustAsMutableCreateSegmentMessageV2(msg)
	h := createSegmentMsg.Header()
	if err := impl.shardManager.CheckIfSegmentCanBeCreated(shards.PartitionUniqueKey{CollectionID: h.GetCollectionId(), PartitionID: h.GetPartitionId()}, h.GetSegmentId()); err != nil {
		// The segment can not be created at current shard, ignored
		return nil, status.NewUnrecoverableError(err.Error())
	}

	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return nil, err
	}
	impl.shardManager.CreateSegment(message.MustAsImmutableCreateSegmentMessageV2(msg.IntoImmutableMessage(msgID)))
	return msgID, nil
}

func (impl *shardInterceptor) handleFlushSegment(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	flushMsg := message.MustAsMutableFlushMessageV2(msg)
	h := flushMsg.Header()
	if utility.GetFlushFromOldArch(ctx) {
		// The flush message come from old arch, so it's not managed by shard manager.
		// We need to flush it into wal directly.
		impl.shardManager.Logger().Info(ctx, "flush segment from old arch, skip checking of shard manager", mlog.FieldMessage(msg))
		return appendOp(ctx, msg)
	}

	if err := impl.shardManager.CheckIfSegmentCanBeFlushed(shards.PartitionUniqueKey{CollectionID: h.GetCollectionId(), PartitionID: h.GetPartitionId()}, h.GetSegmentId()); err != nil {
		// The segment can not be flushed at current shard, ignored
		return nil, status.NewUnrecoverableError(err.Error())
	}
	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return nil, err
	}
	impl.shardManager.FlushSegment(message.MustAsImmutableFlushMessageV2(msg.IntoImmutableMessage(msgID)))
	return msgID, nil
}

// handleFlushAllMessage handles the flush all message.
func (impl *shardInterceptor) handleFlushAllMessage(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	_, err := impl.shardManager.FlushAllAndFenceSegmentAllocUntil(msg.TimeTick())
	if err != nil {
		return nil, status.NewUnrecoverableError(err.Error())
	}
	return appendOp(ctx, msg)
}

// handleTruncateCollectionMessage handles the truncate collection message.
func (impl *shardInterceptor) handleTruncateCollectionMessage(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	truncateCollectionMsg := message.MustAsMutableTruncateCollectionMessageV2(msg)
	header := truncateCollectionMsg.Header()
	segmentIDs, err := impl.shardManager.FlushAndFenceSegmentAllocUntil(header.GetCollectionId(), msg.TimeTick())
	if err != nil {
		return nil, status.NewUnrecoverableError(err.Error())
	}

	header.SegmentIds = segmentIDs
	truncateCollectionMsg.OverwriteHeader(header)

	return appendOp(ctx, msg)
}

// Close closes the segment interceptor.
func (impl *shardInterceptor) Close() {
	if schemaProvider, ok := impl.shardManager.(collectionSchemaProvider); ok {
		for collectionID, schemaInfo := range schemaProvider.GetAllCollectionSchemaInfos() {
			function.GetManager().Release(collectionID, walFunctionRunnerKey(schemaInfo.VChannel))
		}
	}
}
