package vchannel

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
)

// NewVChannelMetaFromSplitShardTargetMessage seeds the meta of a vchannel a
// shard split creates. The TARGET replica of a SplitShard broadcast is that
// vchannel's genesis, and its body carries the genesis in the CreateCollection
// body shape, so the same parser resolves the schema.
//
// The target's first checkpoint is the message itself, exactly as a fresh
// CreateCollection's is. No genesis position is persisted beside it: a split
// target is an ordinary vchannel from here on, and whoever needs its start
// position (DataCoord, through the SplitShard ack callback) learns it the same
// way it learns a new collection's.
func NewVChannelMetaFromSplitShardTargetMessage(msg message.ImmutableSplitShardMessageV2) *streamingpb.VChannelMeta {
	genesis := msg.MustBody().GetGenesis()
	schema := messageutil.MustGetSchemaFromCreateCollectionMessageBody(genesis)
	partitions := make([]*streamingpb.PartitionInfoOfVChannel, 0, len(msg.Header().GetPartitionIds()))
	for _, partitionID := range msg.Header().GetPartitionIds() {
		partitions = append(partitions, &streamingpb.PartitionInfoOfVChannel{
			PartitionId: partitionID,
			State:       streamingpb.PartitionState_PARTITION_STATE_NORMAL,
		})
	}
	return &streamingpb.VChannelMeta{
		Vchannel: msg.VChannel(),
		State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: msg.Header().GetCollectionId(),
			Partitions:   partitions,
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             schema,
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					CheckpointTimeTick: msg.TimeTick(),
				},
			},
		},
		CheckpointTimeTick: msg.TimeTick(),
	}
}

// NewVChannelViewFromSplitShardTargetMessage creates the vchannel-level
// recovery view of a split target from its genesis replica.
func NewVChannelViewFromSplitShardTargetMessage(msg message.ImmutableSplitShardMessageV2) *VChannelView {
	return NewVChannelView(NewVChannelMetaFromSplitShardTargetMessage(msg), 0, true)
}

// SplitFence returns the shard split write fence recorded for this vchannel:
// T_switch and the task that placed it. Zero values when the vchannel is not a
// fenced split source.
func (info *VChannelView) SplitFence() (timeTick uint64, taskID int64) {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.meta.GetSplitFenceTimeTick(), info.meta.GetSplitFenceTaskId()
}

// ObserveSplitShardSourceMessageV2 records the shard split write fence of this
// vchannel. After it the vchannel never accepts new DML again, and the record
// must survive a restart so the write path's gate keeps holding.
//
// split_fence_time_tick is T_switch as the SOURCE handler reported it on the
// record (splitSwitchTimeTickOf), not necessarily the record's own tick: the
// handler fences in memory before it appends, at the first attempt's tick, and
// never rolls that back, so a re-driven first fence carries a later tick while
// reporting the first attempt's. checkpoint_time_tick keeps the record's own
// tick, as every other observation does.
//
// An already-fenced vchannel is left exactly as it is. A same-task re-fence
// seals the same data -- the vchannel took no DML in between -- and moving
// T_switch forward would invalidate what DataCoord already recorded. A fence
// from another task cannot reach here: the append path refuses it.
//
// The vchannel stays VCHANNEL_STATE_NORMAL. It is still on the collection's
// vchannel list until adoption, still has to observe the collection's DDL, and
// still has its own data to drain; the fence is one field, and the drop that
// retires it is a separate, later observation (ObserveRetireVChannel).
func (info *VChannelView) ObserveSplitShardSourceMessageV2(msg message.ImmutableSplitShardMessageV2) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	if !info.shouldObserveLocked(msg.TimeTick()) {
		return false
	}
	if info.closingLocked(0) || info.meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
		return false
	}
	if info.meta.GetSplitFenceTimeTick() != 0 {
		// make it idempotent: only the first fence of a task is recorded.
		return false
	}
	info.meta.SplitFenceTimeTick = splitSwitchTimeTickOf(msg)
	info.meta.SplitFenceTaskId = msg.Header().GetSplitTaskId()
	// Write the fence through every pending drop's stable pre-image, exactly as
	// the L0 materialization frontier is written through
	// (SetTransformMaterializedTimeTick), and for the same reason.
	//
	// A pending PARTITION drop does not close the vchannel, so a fence can land
	// while one is outstanding -- and while it is, stableMetaLocked publishes
	// pendingDrops[0].before, a clone taken before this fence. Without the
	// write-through the fence would not be persisted until that drop completes,
	// and the recovery checkpoint may pass this record first: a crash in between
	// would then recover a source with no fence, writable again for keys its
	// targets already own. A fence is set once and never cleared, so writing it
	// into a pre-image cannot make that pre-image describe a state the vchannel
	// was never in.
	for _, drop := range info.pendingDrops {
		drop.before.SplitFenceTimeTick = info.meta.SplitFenceTimeTick
		drop.before.SplitFenceTaskId = info.meta.SplitFenceTaskId
	}
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
	return true
}

// ObserveRetireVChannel begins the drop of a vchannel a routing commit
// delisted. The same AlterCollection broadcast that grows a collection's
// vchannel list with the split's targets is what delists the spent source;
// there is no separate message.
//
// This is an ordinary runtime drop, the one DropCollection uses: metadata
// publication is fenced at this tick, and the module completes the drop only
// once its own dependencies have finished -- the L0 materialization frontier
// has passed the tick and every segment created before it has a durable
// tombstone. After that the vchannel is TOMBSTONED and its catalog row is
// collected by the ordinary cleanup pass. Nothing of the split's waits for it.
//
// Only a fenced source is retirable this way. A vchannel that carries no fence
// takes no action: the delisting commit is the routing post-image of a split,
// and applying a drop to a vchannel the commit merely does not mention -- a
// stale replica, a collection whose list never contained it -- would drop a
// live shard. A replayed retire is likewise harmless, because a vchannel that
// is already closing is left alone.
func (info *VChannelView) ObserveRetireVChannel(msg message.ImmutableAlterCollectionMessageV2) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	// The header gate first: it is the cheap half, and it keeps every
	// AlterCollection that is not a routing commit from paying a body decode.
	if !messageutil.IsShardSplitRouting(msg.Header()) {
		return false
	}
	if !messageutil.RetiresVChannel(msg.Header(), msg.MustBody().GetUpdates(), info.meta.GetVchannel()) {
		return false
	}
	if !info.shouldObserveLocked(msg.TimeTick()) {
		return false
	}
	if info.closingLocked(0) || isVChannelClosed(info.meta.GetState()) {
		return false
	}
	if info.meta.GetSplitFenceTimeTick() == 0 {
		return false
	}
	info.beginDropLocked(0, msg.TimeTick())
	return true
}

// splitSwitchTimeTickOf returns T_switch for a source fence record: the tick
// the source handler stamped on it as SplitShardExtraResponse (the task's FIRST
// fence, which the shard manager recorded before the append), or the record's
// own tick for a record that carries none or a zero one.
func splitSwitchTimeTickOf(msg message.ImmutableSplitShardMessageV2) uint64 {
	if extra := message.AppendExtraOf(msg); extra != nil {
		resp := &message.SplitShardExtraResponse{}
		if err := extra.UnmarshalTo(resp); err == nil && resp.GetSplitTimeTick() != 0 {
			return resp.GetSplitTimeTick()
		}
	}
	return msg.TimeTick()
}

// RequiresSummarySeal reports whether a message is the last one its own
// vchannel may ever take, so the pchannel's WAL summary has to seal the records
// that vchannel staged before it.
//
// The WAL summary only seals a staged span when it crosses FlushMaxBytes or
// when somebody asks (walsummary.Manager.RequestFlushThrough); there is no
// age-based seal. Until the span is sealed and its manifest published,
// Manager.LastAcked() stays at the previous coverage end -- and LastAcked is a
// hard cap on the PUBLISHED recovery checkpoint
// (recoveryStorageImpl.consumeDirtySnapshot -> Tracker.CheckpointThrough).
// A vchannel that goes quiet forever therefore pins the whole pchannel's
// published checkpoint at a tick before its own last records, however far the
// in-memory ack frontier has moved on.
//
// For a shard split that is not a latency problem, it is a deadlock: the
// published checkpoint is the only one DataCoord sees, and the split's drain
// gate waits for it to pass T_switch before the targets may be adopted. The
// source takes no DML after the fence and no message at all after the routing
// commit that retires it, so nothing of its own would ever seal the span.
//
// Two messages qualify, and both are already explicit L0 boundaries
// (isL0Boundary) for the same reason:
//
//   - the SOURCE replica of a SplitShard: the write fence. Its tick is
//     T_switch, which is exactly the value the drain gate waits for.
//   - the routing commit that delists this vchannel: its drop. The source's
//     catalog row is collected only once the summary confirms the drop tick
//     (walsummary.Manager.CanCleanupVChannel reads LastAcked), so without a
//     seal the row, the module and the vchannel's transform history all leak.
//
// Unconditional on purpose. What pins the frontier is that ANY record was
// staged, not what staged it: a delete stages one, and so does an insert
// appended with a client idempotency key. The shape that escaped in testing
// did so only because unkeyed inserts stage nothing at all.
func RequiresSummarySeal(msg message.ImmutableMessage) bool {
	switch msg.MessageType() {
	case message.MessageTypeSplitShard:
		header := message.MustAsImmutableSplitShardMessageV2(msg).Header()
		return message.SplitShardRoleOf(header, msg.VChannel()) == message.SplitShardRoleSource
	case message.MessageTypeAlterCollection:
		alter := message.MustAsImmutableAlterCollectionMessageV2(msg)
		// Header gate first; only a routing commit's body is worth decoding.
		if !messageutil.IsShardSplitRouting(alter.Header()) {
			return false
		}
		return messageutil.RetiresVChannel(alter.Header(), alter.MustBody().GetUpdates(), msg.VChannel())
	default:
		return false
	}
}
