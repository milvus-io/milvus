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
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"google.golang.org/protobuf/types/known/anypb"
)

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
		// unnecessary: DataCoord has recorded the first tick as T_switch and
		// the drain is judged against it, so a raised T_switch would be a
		// second, later boundary nothing ever crosses. Every fence record of
		// one task seals the same data, because the vchannel took no DML in
		// between.
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
		function.GetManager().Release(collectionID, WALFunctionRunnerKey(msg.VChannel()))
		return appendOp(ctx, msg)
	}
	// Auto-flush every growing segment of the vchannel as of the fence time
	// tick and embed the sealed segment ids into the message header, exactly
	// as the AlterCollection schema-change path does. This SplitShard message
	// is the single authoritative seal record for T_switch — there is no
	// separate ManualFlush anymore — so the downstream consumers (the vchannel
	// recovery module, the delegator) learn the sealed set only from here.
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
	// WAL -- the vchannel recovery module treating the vchannel as fenced at
	// this tick -- while this node kept accepting DML after it, and
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
	function.GetManager().Release(collectionID, WALFunctionRunnerKey(msg.VChannel()))
	return appendOp(ctx, msg)
}
