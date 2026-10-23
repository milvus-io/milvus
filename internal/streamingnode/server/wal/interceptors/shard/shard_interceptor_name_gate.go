package shard

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
)

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
	// appendIfDropped, under notHeldRefuseUnknown, appends without effect
	// instead of refusing a message that it accepts, when this pchannel no
	// longer registers the collection at all (rather than under another
	// vchannel).
	appendIfDropped func(msg message.MutableMessage) bool
}

// initNameGates initializes the name gate table (see passNameGate).
func (impl *shardInterceptor) initNameGates() {
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
			// CollectionNotFound; only a fenced source becomes a no-op, and so
			// does a cluster-local flush that a drop overtook (see
			// handleManualFlushMessage).
			notHeld:         notHeldRefuseUnknown,
			appendIfDropped: isLocalManualFlush,
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
			withoutEffect: impl.appendAlterCollectionWithoutEffect,
		},
		message.MessageTypeTruncateCollection: {
			collectionID: func(msg message.MutableMessage) int64 {
				return message.MustAsMutableTruncateCollectionMessageV2(msg).Header().GetCollectionId()
			},
		},
		// CreateSnapshot flushes and fences the segments of whatever vchannel
		// holds this pchannel's entry for the collection. A replica addressed
		// to a fenced source -- which stays on the collection's vchannel list
		// until adoption -- would therefore seal a split target's segments
		// instead. It is a broadcast replica, so it is appended without effect
		// rather than refused: nothing is growing on a fenced source (the
		// fence sealed it), and refusing would wedge the broadcaster. The
		// consuming-side Ack waits for the flushed output of the vchannels
		// that did act.
		message.MessageTypeCreateSnapshot: {
			collectionID: func(msg message.MutableMessage) int64 {
				return message.MustAsMutableCreateSnapshotMessageV2(msg).Header().GetCollectionId()
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
	case gate.notHeld == notHeldRefuseUnknown && !fenced && !impl.isDroppedAndAllowed(msg, gate, collectionID):
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

// isDroppedAndAllowed reports whether the gate appends msg without effect
// although its vchannel was never held: the gate accepts it for a dropped
// collection, and this pchannel registers the collection under no vchannel at
// all. A collection registered under another vchannel (a split target that took
// the slot) is not dropped, so a message for it is still refused.
func (impl *shardInterceptor) isDroppedAndAllowed(msg message.MutableMessage, gate nameGate, collectionID int64) bool {
	if gate.appendIfDropped == nil || !gate.appendIfDropped(msg) {
		return false
	}
	return errors.Is(impl.shardManager.CheckIfCollectionExists(collectionID), shards.ErrCollectionNotFound)
}

// isLocalManualFlush reports whether msg is a cluster-local ManualFlush, the
// only kind DataCoord admits before a drop can overtake it; never a replicated
// or historical one.
func isLocalManualFlush(msg message.MutableMessage) bool {
	return msg.IsUnreplicable() && msg.ReplicateHeader() == nil
}

// rejectUnwritableVChannel is the single mapping from a failed DML admission
// check to the error the client sees.
func rejectUnwritableVChannel(vchannel string, collectionID int64, err error) error {
	if errors.Is(err, shards.ErrVChannelFenced) {
		// the vchannel is fenced by shard split. The intended client reaction
		// is to refresh the routing table and write to the new shards, which
		// the proxy does per row (shard-split-write-path). T_switch is not
		// carried here: a DML client would refresh routing, never read it.
		return status.NewShardFenced(vchannel, 0, 0)
	}
	// This pchannel has never held the vchannel, so no routing refresh sends the
	// write anywhere -- terminal, unlike the fence above. It must still be a
	// rejection rather than a pass: every check after this one is keyed by
	// collection (schema version) or by (collection, partition) (segment
	// assignment), so a message let through would be appended carrying vchannel
	// X while its segment belongs to vchannel Y. The vchannel module for X then
	// finds no such segment and drops the batch, Y's module counts the rows
	// against its own segment, and the client is told the append succeeded.
	return status.NewUnrecoverableError("vchannel %s of collection %d cannot be written: %s", vchannel, collectionID, err.Error())
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
	function.GetManager().Release(collectionID, WALFunctionRunnerKey(msg.VChannel()))
	// The drop is the fenced source's end: nothing routes to it any more.
	impl.shardManager.ReleaseSplitFence(msg.VChannel())
	return msgID, nil
}

// appendAlterCollectionWithoutEffect appends an AlterCollection addressed to a
// vchannel this pchannel does not hold. When it is the routing commit that
// retires that vchannel -- a fenced source's adoption -- the source is gone
// once it lands, so its fence tombstone goes with it.
func (impl *shardInterceptor) appendAlterCollectionWithoutEffect(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return msgID, err
	}
	alterMsg := message.MustAsMutableAlterCollectionMessageV2(msg)
	header := alterMsg.Header()
	if messageutil.IsShardSplitRouting(header) &&
		messageutil.RetiresVChannel(header, alterMsg.MustBody().GetUpdates(), msg.VChannel()) {
		impl.shardManager.ReleaseSplitFence(msg.VChannel())
	}
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
