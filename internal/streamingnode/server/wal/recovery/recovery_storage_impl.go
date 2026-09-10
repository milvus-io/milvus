package recovery

import (
	"context"
	"sync"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

const (
	componentRecoveryStorage = "recovery-storage"

	recoveryStorageStatePersistRecovering = "persist-recovering"
	recoveryStorageStateStreamRecovering  = "stream-recovering"
	recoveryStorageStateWorking           = "working"
)

// RecoverRecoveryStorage creates a new recovery storage.
func RecoverRecoveryStorage(
	ctx context.Context,
	recoveryStreamBuilder RecoveryStreamBuilder,
	cp *utility.WALCheckpoint,
	lastTimeTickMessage message.ImmutableMessage,
) (RecoveryStorage, *RecoverySnapshot, error) {
	rs := newRecoveryStorage(recoveryStreamBuilder.Channel(), cp)
	if err := rs.recoverRecoveryInfoFromMeta(ctx, recoveryStreamBuilder.Channel(), lastTimeTickMessage); err != nil {
		rs.Logger().Warn(ctx, "recovery storage failed", mlog.Err(err))
		return nil, nil, err
	}
	// recover the state from wal and start the background task to persist the state.
	snapshot, err := rs.recoverFromStream(ctx, recoveryStreamBuilder, lastTimeTickMessage)
	if err != nil {
		rs.Logger().Warn(ctx, "recovery storage failed", mlog.Err(err))
		return nil, nil, err
	}
	// recovery storage start work.
	rs.metrics.ObserveStateChange(recoveryStorageStateWorking)
	rs.SetLogger(resource.Resource().Logger().With(
		mlog.Int64("nodeID", paramtable.GetNodeID()),
		mlog.FieldComponent(componentRecoveryStorage),
		mlog.String("channel", recoveryStreamBuilder.Channel().String()),
		mlog.String("state", recoveryStorageStateWorking)))
	rs.truncator = recoveryStreamBuilder.RWWALImpls()
	go rs.backgroundTask()
	return rs, snapshot, nil
}

// newRecoveryStorage creates a new recovery storage.
func newRecoveryStorage(channel types.PChannelInfo, cp *utility.WALCheckpoint) *recoveryStorageImpl {
	cfg := newConfig()
	return &recoveryStorageImpl{
		backgroundTaskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		cfg:                    cfg,
		mu:                     sync.Mutex{},
		currentClusterID:       paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
		channel:                channel,
		checkpoint:             cp,
		dirtyCounter:           0,
		persistNotifier:        make(chan struct{}, 1),
		gracefulClosed:         false,
		metrics:                newRecoveryStorageMetrics(channel),
		retiredVChannels:       make(map[string]struct{}),
	}
}

// recoveryStorageImpl is a component that manages the recovery info for the streaming service.
// It will consume the message from the wal, consume the message in wal, and update the checkpoint for it.
type recoveryStorageImpl struct {
	mlog.Binder
	backgroundTaskNotifier *syncutil.AsyncTaskNotifier[struct{}]
	cfg                    *config
	mu                     sync.Mutex
	currentClusterID       string
	channel                types.PChannelInfo
	segments               map[int64]*segmentRecoveryInfo
	vchannels              map[string]*vchannelRecoveryInfo
	checkpoint             *WALCheckpoint
	dirtyCounter           int // records the message count since last persist snapshot.
	// used to trigger the recovery persist operation.
	persistNotifier        chan struct{}
	gracefulClosed         bool
	truncator              walimpls.WALImpls
	metrics                *recoveryMetrics
	pendingPersistSnapshot *RecoverySnapshot
	// used to mark switch MQ msg found
	alterWALInfo *AlterWALInfo
	// pendingSalvageCheckpoint holds the salvage checkpoint captured during force promote.
	// Set under r.mu; consumed and persisted by the background task to avoid holding the lock.
	pendingSalvageCheckpoint *utility.ReplicateCheckpoint
	// retiredVChannels is the set of vchannel names that are SPLITTED and
	// Retired but not yet removed from vchannels -- still waiting for the
	// flusher checkpoint to drain past their fence (see
	// ConsumeDirtyAndGetSnapshot). Populated when ObserveRetire is applied
	// and on reload from the catalog; an entry is deleted the moment its
	// vchannel is actually removed from vchannels. Set under r.mu.
	//
	// Its sole purpose is to keep the persist gate (see consumeDirtySnapshot)
	// from staying shut on an otherwise quiet pchannel: UpdateFlusherCheckpoint
	// is the only signal that the flusher has passed a fence, and it neither
	// bumps dirtyCounter nor is itself a WAL message, so without this a
	// retired vchannel could sit collectable forever if nothing else ever
	// touched the pchannel again.
	retiredVChannels map[string]struct{}
}

// Metrics gets the metrics of the wal.
func (r *recoveryStorageImpl) Metrics() RecoveryMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()

	return RecoveryMetrics{
		RecoveryTimeTick: r.checkpoint.TimeTick,
	}
}

// UpdateFlusherCheckpoint updates the checkpoint of flusher.
// TODO: should be removed in future, after merge the flusher logic into recovery storage.
func (r *recoveryStorageImpl) UpdateFlusherCheckpoint(vchannel string, checkpoint *WALCheckpoint) {
	r.mu.Lock()
	defer r.mu.Unlock()
	vchannelInfo, ok := r.vchannels[vchannel]
	if !ok {
		r.Logger().Warn(context.TODO(), "vchannel not found", mlog.String("vchannel", vchannel))
		return
	}
	if err := vchannelInfo.UpdateFlushCheckpoint(checkpoint); err != nil {
		r.Logger().Warn(context.TODO(), "failed to update flush checkpoint", mlog.Err(err))
		return
	}
	r.Logger().Info(context.TODO(), "update flush checkpoint", mlog.String("vchannel", vchannel), mlog.String("messageID", checkpoint.MessageID.String()), mlog.Uint64("timeTick", checkpoint.TimeTick))

	// A retired SPLITTED vchannel only becomes collectable once the
	// pchannel-wide minimum flusher checkpoint -- not just this one
	// vchannel's own -- passes its fence, because that minimum is exactly
	// what persistDirtySnapshot hands to ConsumeDirtyAndGetSnapshot. This is
	// the only place that signal ever advances, so wake the persist loop the
	// moment that minimum crosses any pending retirement's SplitTimeTick,
	// instead of waiting on unrelated traffic to bump dirtyCounter or on the
	// next periodic tick.
	if r.hasCollectableRetiredVChannelLocked() {
		r.notifyPersist()
	}
}

// hasCollectableRetiredVChannelLocked reports whether some pending
// retirement has now drained: the pchannel-wide minimum flusher checkpoint --
// the very tick persistDirtySnapshot hands to ConsumeDirtyAndGetSnapshot --
// has reached its fence, so the next snapshot would actually remove it from
// the catalog. A retirement that has not drained yet is deliberately NOT
// collectable: it is nothing the persist loop can act on, and treating it as
// dirty would spin the graceful-shutdown loop (which persists for as long as
// isDirty holds) until the timeout.
// The caller must already hold r.mu.
func (r *recoveryStorageImpl) hasCollectableRetiredVChannelLocked() bool {
	if len(r.retiredVChannels) == 0 {
		return false
	}
	minimumCheckpoint := r.getFlusherCheckpointLocked()
	if minimumCheckpoint == nil {
		return false
	}
	for retiredVChannel := range r.retiredVChannels {
		if info, ok := r.vchannels[retiredVChannel]; ok && minimumCheckpoint.TimeTick >= info.meta.SplitTimeTick {
			return true
		}
	}
	return false
}

// GetSchema gets the schema of the collection at the given timetick.
func (r *recoveryStorageImpl) GetSchema(ctx context.Context, vchannel string, timetick uint64) (*schemapb.CollectionSchema, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if vchannelInfo, ok := r.vchannels[vchannel]; ok {
		_, schema := vchannelInfo.GetSchema(timetick)
		if schema == nil {
			r.Logger().DPanic(context.TODO(), "schema not found, fallback to latest schema", mlog.String("vchannel", vchannel), mlog.Uint64("timetick", timetick))
			if _, schema = vchannelInfo.GetSchema(0); schema != nil {
				return schema, nil
			}
			return nil, status.NewInner("critical error: schema not found, vchannel: %s, timetick: %d", vchannel, timetick)
		}
		return schema, nil
	}
	return nil, status.NewInner("critical error: vchannel not found, vchannel: %s, timetick: %d", vchannel, timetick)
}

// ObserveMessage is called when a new message is observed.
func (r *recoveryStorageImpl) ObserveMessage(ctx context.Context, msg message.ImmutableMessage) (err error) {
	ctx = message.ExtractTraceContext(ctx, msg)

	if h := msg.BroadcastHeader(); h != nil {
		if err := streaming.WAL().Broadcast().Ack(ctx, msg); err != nil {
			r.Logger().Warn(ctx, "failed to ack broadcast message", mlog.Err(err))
			return err
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.observeMessage(ctx, msg)
	return nil
}

// Close closes the recovery storage and wait the background task stop.
func (r *recoveryStorageImpl) Close() {
	r.backgroundTaskNotifier.Cancel()
	r.backgroundTaskNotifier.BlockUntilFinish()
	r.metrics.Close()
}

// notifyPersist notifies a persist operation.
func (r *recoveryStorageImpl) notifyPersist() {
	select {
	case r.persistNotifier <- struct{}{}:
	default:
	}
}

// consumeDirtySnapshot consumes the dirty state and returns a snapshot to persist.
// A snapshot is always a consistent state (fully consume a message or a txn message) of the recovery storage.
// flusherCheckpointTimeTick is the tick up to which the flusher has actually
// drained this pchannel (0 if not yet known); it decides whether a retired
// SPLITTED vchannel has drained past its fence and can be collected.
func (r *recoveryStorageImpl) consumeDirtySnapshot(flusherCheckpointTimeTick uint64) *RecoverySnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	// A pending retirement keeps the gate open even when nothing else is
	// dirty: it is the only way a retired SPLITTED vchannel, sitting on an
	// otherwise quiet pchannel, ever gets re-evaluated once the flusher
	// checkpoint independently catches up to its fence (see
	// retiredVChannels and UpdateFlusherCheckpoint).
	if r.dirtyCounter == 0 && r.pendingSalvageCheckpoint == nil && len(r.retiredVChannels) == 0 {
		return nil
	}

	segments := make(map[int64]*streamingpb.SegmentAssignmentMeta)
	vchannels := make(map[string]*streamingpb.VChannelMeta)
	for _, segment := range r.segments {
		dirtySnapshot, shouldBeRemoved := segment.ConsumeDirtyAndGetSnapshot()
		if shouldBeRemoved {
			delete(r.segments, segment.meta.SegmentId)
		}
		if dirtySnapshot != nil {
			segments[segment.meta.SegmentId] = dirtySnapshot
		}
	}
	for _, vchannel := range r.vchannels {
		dirtySnapshot, shouldBeRemoved := vchannel.ConsumeDirtyAndGetSnapshot(flusherCheckpointTimeTick)
		if shouldBeRemoved {
			delete(r.vchannels, vchannel.meta.Vchannel)
			// No-op (and safe) if this vchannel was never a pending
			// retirement -- e.g. a genuine DROPPED vchannel.
			delete(r.retiredVChannels, vchannel.meta.Vchannel)
		}
		if dirtySnapshot != nil {
			vchannels[vchannel.meta.Vchannel] = dirtySnapshot
		}
	}
	// Atomically capture the salvage checkpoint alongside other dirty state.
	// Clearing it here (under r.mu) ensures it is only consumed once.
	salvageCP := r.pendingSalvageCheckpoint
	r.pendingSalvageCheckpoint = nil
	// clear the dirty counter.
	r.dirtyCounter = 0
	return &RecoverySnapshot{
		VChannels:          vchannels,
		SegmentAssignments: segments,
		Checkpoint:         r.checkpoint.Clone(),
		SalvageCheckpoint:  salvageCP,
	}
}

// observeMessage observes a message and update the recovery storage.
func (r *recoveryStorageImpl) observeMessage(ctx context.Context, msg message.ImmutableMessage) {
	if msg.TimeTick() <= r.checkpoint.TimeTick {
		if r.Logger().Level().Enabled(mlog.DebugLevel) {
			r.Logger().Debug(ctx, "skip the message before the checkpoint",
				mlog.FieldMessage(msg),
				mlog.Uint64("checkpoint", r.checkpoint.TimeTick),
				mlog.Uint64("incoming", msg.TimeTick()),
			)
		}
		return
	}
	r.handleMessage(ctx, msg)

	r.updateCheckpoint(ctx, msg)
	r.metrics.ObServeInMemMetrics(r.checkpoint.TimeTick)

	if !msg.IsPersisted() {
		// only trigger persist when the message is persisted.
		return
	}
	r.dirtyCounter++
	if r.dirtyCounter > r.cfg.maxDirtyMessages {
		r.notifyPersist()
	}
}

// updateCheckpoint updates the checkpoint of the recovery storage.
func (r *recoveryStorageImpl) updateCheckpoint(ctx context.Context, msg message.ImmutableMessage) {
	if msg.MessageType() == message.MessageTypeAlterReplicateConfig {
		cfg := message.MustAsImmutableAlterReplicateConfigMessageV2(msg)
		header := cfg.Header()

		// Check ignore field - if true, skip updating ReplicateConfig and ReplicateCheckpoint
		// This is used for incomplete switchover messages that should be ignored after force promote
		if header.Ignore {
			r.Logger().Info(ctx, "AlterReplicateConfig message has ignore flag set, skipping checkpoint update",
				mlog.Bool("forcePromote", header.ForcePromote))
		} else {
			r.checkpoint.ReplicateConfig = header.ReplicateConfiguration
			clusterRole := replicateutil.MustNewConfigHelper(r.currentClusterID, header.ReplicateConfiguration).GetCurrentCluster()
			switch clusterRole.Role() {
			case replicateutil.RolePrimary:
				if header.GetForcePromote() && r.checkpoint.ReplicateCheckpoint != nil {
					// Store for background task to persist; never call etcd while holding r.mu.
					r.pendingSalvageCheckpoint = r.checkpoint.ReplicateCheckpoint
					r.notifyPersist()
				}
				r.checkpoint.ReplicateCheckpoint = nil
			case replicateutil.RoleSecondary:
				// Update the replicate checkpoint if the cluster role is secondary.
				sourceClusterID := clusterRole.SourceCluster().GetClusterId()
				sourcePChannel := clusterRole.MustGetSourceChannel(r.channel.Name)
				if r.checkpoint.ReplicateCheckpoint == nil || r.checkpoint.ReplicateCheckpoint.ClusterID != sourceClusterID {
					r.checkpoint.ReplicateCheckpoint = &utility.ReplicateCheckpoint{
						ClusterID: sourceClusterID,
						PChannel:  sourcePChannel,
						MessageID: nil,
						TimeTick:  0,
					}
				}
			}
		}
	}
	r.checkpoint.MessageID = msg.LastConfirmedMessageID()
	r.checkpoint.TimeTick = msg.TimeTick()
	if r.alterWALInfo != nil && r.alterWALInfo.FoundAlterWALMsg && (r.checkpoint.AlterWalState == nil || r.checkpoint.AlterWalState.Stage == streamingpb.AlterWALStage_NONE) {
		r.checkpoint.AlterWalState = &streamingpb.AlterWALState{
			TargetWalName: r.alterWALInfo.TargetWALName,
			TimeTick:      r.alterWALInfo.AlterWALTs,
			Configs:       r.alterWALInfo.AlterWALConfig,
			Stage:         streamingpb.AlterWALStage_FLUSHING,
		}
	}

	// update the replicate checkpoint.
	replicateHeader := msg.ReplicateHeader()
	if replicateHeader == nil {
		return
	}
	if r.checkpoint.ReplicateCheckpoint == nil {
		r.detectInconsistency(ctx, msg, "replicate checkpoint is nil when incoming replicate message")
		return
	}
	if replicateHeader.ClusterID != r.checkpoint.ReplicateCheckpoint.ClusterID {
		r.detectInconsistency(ctx, msg,
			"replicate header cluster id mismatch",
			mlog.String("expected", r.checkpoint.ReplicateCheckpoint.ClusterID),
			mlog.String("actual", replicateHeader.ClusterID))
		return
	}
	r.checkpoint.ReplicateCheckpoint.MessageID = replicateHeader.LastConfirmedMessageID
	r.checkpoint.ReplicateCheckpoint.TimeTick = replicateHeader.TimeTick
}

// The incoming message id is always sorted with timetick.
func (r *recoveryStorageImpl) handleMessage(ctx context.Context, msg message.ImmutableMessage) {
	if funcutil.IsControlChannel(msg.VChannel()) && !msg.IsPChannelLevel() {
		// message on control channel except pchannel-level messages is just used to determine the DDL/DCL order,
		// will not affect the recovery storage, so skip it.
		return
	}

	if msg.VChannel() != "" && !msg.IsPChannelLevel() && r.vchannels[msg.VChannel()] == nil &&
		!funcutil.IsControlChannel(msg.VChannel()) && !exemptFromVChannelNotFound(msg) {
		r.detectInconsistency(ctx, msg, "vchannel not found")
	}

	switch msg.MessageType() {
	case message.MessageTypeInsert:
		immutableMsg := message.MustAsImmutableInsertMessageV1(msg)
		r.handleInsert(ctx, immutableMsg)
	case message.MessageTypeDelete:
		immutableMsg := message.MustAsImmutableDeleteMessageV1(msg)
		r.handleDelete(immutableMsg)
	case message.MessageTypeCreateSegment:
		immutableMsg := message.MustAsImmutableCreateSegmentMessageV2(msg)
		r.handleCreateSegment(ctx, immutableMsg)
	case message.MessageTypeFlush:
		immutableMsg := message.MustAsImmutableFlushMessageV2(msg)
		r.handleFlush(ctx, immutableMsg)
	case message.MessageTypeManualFlush:
		immutableMsg := message.MustAsImmutableManualFlushMessageV2(msg)
		r.handleManualFlush(ctx, immutableMsg)
	case message.MessageTypeFlushAll:
		immutableMsg := message.MustAsImmutableFlushAllMessageV2(msg)
		r.handleFlushAll(ctx, immutableMsg)
	case message.MessageTypeCreateCollection:
		immutableMsg := message.MustAsImmutableCreateCollectionMessageV1(msg)
		r.handleCreateCollection(ctx, immutableMsg)
	case message.MessageTypeDropCollection:
		immutableMsg := message.MustAsImmutableDropCollectionMessageV1(msg)
		r.handleDropCollection(ctx, immutableMsg)
	case message.MessageTypeCreatePartition:
		immutableMsg := message.MustAsImmutableCreatePartitionMessageV1(msg)
		r.handleCreatePartition(ctx, immutableMsg)
	case message.MessageTypeDropPartition:
		immutableMsg := message.MustAsImmutableDropPartitionMessageV1(msg)
		r.handleDropPartition(ctx, immutableMsg)
	case message.MessageTypeTxn:
		immutableMsg := message.AsImmutableTxnMessage(msg)
		r.handleTxn(ctx, immutableMsg)
	case message.MessageTypeImport:
		immutableMsg := message.MustAsImmutableImportMessageV1(msg)
		r.handleImport(immutableMsg)
	case message.MessageTypeSchemaChange:
		immutableMsg := message.MustAsImmutableSchemaChangeMessageV2(msg)
		r.handleSchemaChange(ctx, immutableMsg)
	case message.MessageTypeAlterCollection:
		immutableMsg := message.MustAsImmutableAlterCollectionMessageV2(msg)
		r.handleAlterCollection(ctx, immutableMsg)
	case message.MessageTypeTruncateCollection:
		immutableMsg := message.MustAsImmutableTruncateCollectionMessageV2(msg)
		r.handleTruncateCollection(ctx, immutableMsg)
	case message.MessageTypeSplitShard:
		immutableMsg := message.MustAsImmutableSplitShardMessageV2(msg)
		r.handleSplitShard(ctx, immutableMsg)
	case message.MessageTypeTimeTick:
		// nothing, the time tick message make no recovery operation.
	case message.MessageTypeAlterWAL:
		immutableMsg := message.MustAsImmutableAlterWALMessageV2(msg)
		r.handleAlterWAL(ctx, immutableMsg)
	}
}

// exemptFromVChannelNotFound lists the messages that may legitimately arrive on
// a vchannel this recovery storage does not hold: the genesis messages, and the
// teardowns, whose replay after the meta was dropped is not an inconsistency.
func exemptFromVChannelNotFound(msg message.ImmutableMessage) bool {
	switch msg.MessageType() {
	case message.MessageTypeCreateCollection, message.MessageTypeDropCollection:
		return true
	case message.MessageTypeSplitShard:
		header := message.MustAsImmutableSplitShardMessageV2(msg).Header()
		return message.SplitShardRoleOf(header, msg.VChannel()) == message.SplitShardRoleTarget
	case message.MessageTypeAlterCollection:
		alter := message.MustAsImmutableAlterCollectionMessageV2(msg)
		return messageutil.RetiresVChannel(alter.Header(), alter.MustBody().GetUpdates(), msg.VChannel())
	}
	return false
}

// handleSplitShard handles the split shard message.
//
// A SplitShard broadcast lands on more than one vchannel, and each replica's
// role -- decided from the header, not from which switch case dispatched it --
// says what it means here:
//
//   - the source replica fences the vchannel: no new DML is appended after it,
//     so only the vchannel state flips here. The growing segments were already
//     sealed by the interceptor while the fence was being built (there is no
//     ManualFlush before it -- this message IS the seal record); flushing them
//     again keeps the replay idempotent, since a replay can recreate GROWING
//     segments after the fence was persisted.
//
//   - the target replica is the genesis of a new vchannel: it seeds the
//     vchannel meta exactly as create collection does, so the new vchannel
//     survives a streamingnode restart.
//
// Scoped by vchannel, not by collection: the fenced source is one shard of a
// collection whose other shards are still taking writes, and flushing those
// here would seal segments no message asked to seal.
func (r *recoveryStorageImpl) handleSplitShard(ctx context.Context, msg message.ImmutableSplitShardMessageV2) {
	switch message.SplitShardRoleOf(msg.Header(), msg.VChannel()) {
	case message.SplitShardRoleSource:
		r.flushAllSegmentOfVChannel(ctx, msg)
		if vchannelInfo, ok := r.vchannels[msg.VChannel()]; ok {
			vchannelInfo.ObserveSplitShard(msg)
		}
		r.Logger().Info(ctx, "split shard", mlog.FieldMessage(msg))
	case message.SplitShardRoleTarget:
		if _, ok := r.vchannels[msg.VChannel()]; ok {
			return
		}
		r.vchannels[msg.VChannel()] = newVChannelRecoveryInfoFromSplitShardMessage(msg)
		r.Logger().Info(ctx, "create vchannel from split shard genesis", mlog.FieldMessage(msg))
	case message.SplitShardRoleBystander:
		// The broadcast now covers every vchannel of the collection, so a
		// bystander shard -- neither fenced nor created by this split -- is
		// expected to receive a replica. It takes no recovery-storage action:
		// nothing about its own vchannel meta changes, and this is not a
		// misroute worth reporting.
	default:
		// A replica landing on a vchannel that is neither a source, a target
		// nor even the same collection is a misroute, symmetric with the
		// source and target arms above: report it instead of silently doing
		// nothing.
		r.detectInconsistency(ctx, msg, "split shard replica of unknown role")
	}
}

// handleAlterWAL handles the alter WAL message.
// Flushes all growing segments to ensure segment data does not span across different WAL implementations.
func (r *recoveryStorageImpl) handleAlterWAL(ctx context.Context, msg message.ImmutableAlterWALMessageV2) {
	header := msg.Header()

	segmentIDs := make([]int64, 0)
	rows := make([]uint64, 0)
	binarySize := make([]uint64, 0)

	// Flush all growing segments before WAL switch
	for segmentID, segment := range r.segments {
		if segment.IsGrowing() {
			segment.ObserveFlush(msg.TimeTick())
			segmentIDs = append(segmentIDs, segmentID)
			rows = append(rows, segment.Rows())
			binarySize = append(binarySize, segment.BinarySize())
		}
	}

	if len(segmentIDs) > 0 {
		r.Logger().Info(ctx, "flush all growing segments for WAL switch",
			mlog.FieldMessage(msg),
			mlog.Stringer("targetWALName", header.TargetWalName),
			mlog.Int64s("segmentIDs", segmentIDs),
			mlog.Uint64s("rows", rows),
			mlog.Uint64s("binarySize", binarySize))
	} else {
		r.Logger().Info(ctx, "no growing segments to flush for WAL switch",
			mlog.FieldMessage(msg),
			mlog.Stringer("targetWALName", header.TargetWalName))
	}

	// Record alter WAL information for snapshot persistence
	r.alterWALInfo = &AlterWALInfo{
		FoundAlterWALMsg: true,
		TargetWALName:    header.TargetWalName,
		AlterWALConfig:   header.Config,
		AlterWALTs:       msg.TimeTick(),
	}
}

// handleInsert handles the insert message.
func (r *recoveryStorageImpl) handleInsert(ctx context.Context, msg message.ImmutableInsertMessageV1) {
	for _, partition := range msg.Header().GetPartitions() {
		if segment, ok := r.segments[partition.SegmentAssignment.SegmentId]; ok && segment.IsGrowing() {
			segment.ObserveInsert(msg.TimeTick(), partition)
		} else {
			r.detectInconsistency(ctx, msg, "segment not found")
		}
	}
}

// handleDelete handles the delete message.
func (r *recoveryStorageImpl) handleDelete(msg message.ImmutableDeleteMessageV1) {
}

// handleCreateSegment handles the create segment message.
func (r *recoveryStorageImpl) handleCreateSegment(ctx context.Context, msg message.ImmutableCreateSegmentMessageV2) {
	// Skip segment creation if the vchannel does not exist (collection was dropped).
	// During WAL replay (e.g., Kafka offset reset), CreateSegment messages may appear
	// for collections whose vchannels have already been cleaned up.
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; !ok || vchannelInfo.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
		r.Logger().Warn(ctx, "skip create segment for non-active vchannel",
			mlog.FieldMessage(msg),
			mlog.String("vchannel", msg.VChannel()),
			mlog.Int64("segmentID", msg.Header().SegmentId),
		)
		return
	}
	segment := newSegmentRecoveryInfoFromCreateSegmentMessage(msg)
	r.segments[segment.meta.SegmentId] = segment
	r.Logger().Info(ctx, "create segment", mlog.FieldMessage(msg))
}

// handleFlush handles the flush message.
func (r *recoveryStorageImpl) handleFlush(ctx context.Context, msg message.ImmutableFlushMessageV2) {
	header := msg.Header()
	if segment, ok := r.segments[header.SegmentId]; ok {
		segment.ObserveFlush(msg.TimeTick())
		r.Logger().Info(ctx, "flush segment", mlog.FieldMessage(msg), mlog.Uint64("rows", segment.Rows()), mlog.Uint64("binarySize", segment.BinarySize()))
	}
}

// handleManualFlush handles the manual flush message.
func (r *recoveryStorageImpl) handleManualFlush(ctx context.Context, msg message.ImmutableManualFlushMessageV2) {
	segments := make(map[int64]struct{}, len(msg.Header().SegmentIds))
	for _, segmentID := range msg.Header().SegmentIds {
		segments[segmentID] = struct{}{}
	}
	r.flushSegments(ctx, msg, segments)
}

// handleFlushAll handles the flush all message.
func (r *recoveryStorageImpl) handleFlushAll(ctx context.Context, msg message.ImmutableFlushAllMessageV2) {
	segments := lo.MapValues(r.segments, func(segment *segmentRecoveryInfo, _ int64) struct{} {
		return struct{}{}
	})
	r.flushSegments(ctx, msg, segments)
}

// flushSegments flushes the segments in the recovery storage.
func (r *recoveryStorageImpl) flushSegments(ctx context.Context, msg message.ImmutableMessage, sealSegmentIDs map[int64]struct{}) {
	segmentIDs := make([]int64, 0)
	rows := make([]uint64, 0)
	binarySize := make([]uint64, 0)
	for segmentID := range sealSegmentIDs {
		if segment, ok := r.segments[segmentID]; ok {
			segment.ObserveFlush(msg.TimeTick())
			segmentIDs = append(segmentIDs, segment.meta.SegmentId)
			rows = append(rows, segment.Rows())
			binarySize = append(binarySize, segment.BinarySize())
		}
	}
	if len(segmentIDs) != len(sealSegmentIDs) {
		r.detectInconsistency(ctx, msg, "flush segments not exist", mlog.Int64s("wanted", lo.Keys(sealSegmentIDs)), mlog.Int64s("actually", segmentIDs))
	}
	r.Logger().Info(ctx, "flush segments of collection by flush", mlog.FieldMessage(msg),
		mlog.Uint64s("rows", rows),
		mlog.Uint64s("binarySize", binarySize),
		mlog.Int("flushedSegmentCount", len(segmentIDs)),
	)
}

// handleCreateCollection handles the create collection message.
func (r *recoveryStorageImpl) handleCreateCollection(ctx context.Context, msg message.ImmutableCreateCollectionMessageV1) {
	if _, ok := r.vchannels[msg.VChannel()]; ok {
		return
	}
	r.vchannels[msg.VChannel()] = newVChannelRecoveryInfoFromCreateCollectionMessage(msg)
	r.Logger().Info(ctx, "create collection", mlog.FieldMessage(msg))
}

// flushAllSegmentOfVChannel flushes every segment sitting on one vchannel.
//
// Scoped by InsertChannel rather than by collection: a reclaimed vchannel is
// one shard of a collection that is still very much alive, and flushing the
// collection's other shards here would be wrong.
func (r *recoveryStorageImpl) flushAllSegmentOfVChannel(ctx context.Context, msg message.ImmutableMessage) {
	segmentIDs := make([]int64, 0)
	for _, segment := range r.segments {
		if segment.meta.GetVchannel() != msg.VChannel() {
			continue
		}
		segment.ObserveFlush(msg.TimeTick())
		segmentIDs = append(segmentIDs, segment.meta.SegmentId)
	}
	if len(segmentIDs) > 0 {
		r.Logger().Info(ctx, "flush all segments of vchannel", mlog.FieldMessage(msg),
			mlog.Int64s("segmentIDs", segmentIDs))
	}
}

// handleDropCollection handles the drop collection message.
func (r *recoveryStorageImpl) handleDropCollection(ctx context.Context, msg message.ImmutableDropCollectionMessageV1) {
	// Always flush first: during WAL replay, CreateSegment/Insert messages may have recreated
	// GROWING segments after the vchannel was marked DROPPED (non-atomic etcd persistence or
	// Kafka offset compaction). Flushing unconditionally ensures idempotent replay.
	r.flushAllSegmentOfCollection(ctx, msg, msg.Header().CollectionId)
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; ok && vchannelInfo.meta.State != streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
		vchannelInfo.ObserveDropCollection(msg)
	}
	r.Logger().Info(ctx, "drop collection", mlog.FieldMessage(msg))
}

// flushAllSegmentOfCollection flushes all segments of the collection.
func (r *recoveryStorageImpl) flushAllSegmentOfCollection(ctx context.Context, msg message.ImmutableMessage, collectionID int64) {
	segmentIDs := make([]int64, 0)
	rows := make([]uint64, 0)
	for _, segment := range r.segments {
		if segment.meta.CollectionId == collectionID {
			segment.ObserveFlush(msg.TimeTick())
			segmentIDs = append(segmentIDs, segment.meta.SegmentId)
			rows = append(rows, segment.Rows())
		}
	}
	r.Logger().Info(ctx, "flush all segments of collection", mlog.FieldMessage(msg), mlog.Int64s("segmentIDs", segmentIDs), mlog.Uint64s("rows", rows))
}

// handleCreatePartition handles the create partition message.
func (r *recoveryStorageImpl) handleCreatePartition(ctx context.Context, msg message.ImmutableCreatePartitionMessageV1) {
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; !ok || vchannelInfo.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
		return
	}
	r.vchannels[msg.VChannel()].ObserveCreatePartition(msg)
	r.Logger().Info(ctx, "create partition", mlog.FieldMessage(msg))
}

// handleDropPartition handles the drop partition message.
func (r *recoveryStorageImpl) handleDropPartition(ctx context.Context, msg message.ImmutableDropPartitionMessageV1) {
	// Always flush first: same rationale as handleDropCollection — orphaned GROWING segments
	// may exist for this partition due to non-atomic etcd persistence or WAL offset reset.
	r.flushAllSegmentOfPartition(ctx, msg, msg.Header().PartitionId)
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; ok && vchannelInfo.meta.State != streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
		vchannelInfo.ObserveDropPartition(msg)
	}
	r.Logger().Info(ctx, "drop partition", mlog.FieldMessage(msg))
}

// flushAllSegmentOfPartition flushes all segments of the partition.
func (r *recoveryStorageImpl) flushAllSegmentOfPartition(ctx context.Context, msg message.ImmutableMessage, partitionID int64) {
	segmentIDs := make([]int64, 0)
	rows := make([]uint64, 0)
	for _, segment := range r.segments {
		if segment.meta.PartitionId == partitionID {
			segment.ObserveFlush(msg.TimeTick())
			segmentIDs = append(segmentIDs, segment.meta.SegmentId)
			rows = append(rows, segment.Rows())
		}
	}
	r.Logger().Info(ctx, "flush all segments of partition", mlog.FieldMessage(msg), mlog.Int64s("segmentIDs", segmentIDs), mlog.Uint64s("rows", rows))
}

// handleTxn handles the txn message.
func (r *recoveryStorageImpl) handleTxn(ctx context.Context, msg message.ImmutableTxnMessage) {
	msg.RangeOver(func(im message.ImmutableMessage) error {
		r.handleMessage(message.ExtractTraceContext(ctx, im), im)
		return nil
	})
}

// handleImport handles the import message.
func (r *recoveryStorageImpl) handleImport(_ message.ImmutableImportMessageV1) {
}

// handleSchemaChange handles the schema change message.
func (r *recoveryStorageImpl) handleSchemaChange(ctx context.Context, msg message.ImmutableSchemaChangeMessageV2) {
	// when schema change happens, we need to flush all segments in the collection.
	segments := make(map[int64]struct{}, len(msg.Header().FlushedSegmentIds))
	for _, segmentID := range msg.Header().FlushedSegmentIds {
		segments[segmentID] = struct{}{}
	}
	r.flushSegments(ctx, msg, segments)

	// persist the schema change into recovery info.
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; ok {
		vchannelInfo.ObserveSchemaChange(msg)
	}
}

// handlePutCollection handles the put collection message.
//
// A retiring replica -- a shard-split routing commit that delists this
// vchannel -- marks the vchannel retired, not dropped: unlike
// handleDropCollection's teardown, there is no flush here and the state
// never moves to DROPPED. The vchannel was already fenced (SPLITTED) and had
// its segments flushed by the earlier SplitShard broadcast, so there is
// nothing left to flush by the time a retire lands; the source is left
// SPLITTED so it can never be picked up by dropAllVirtualChannel and
// mistakenly reported to DataCoord as dropped. ConsumeDirtyAndGetSnapshot is
// what later collects the meta from the catalog, once the flusher checkpoint
// proves the fence has fully drained.
func (r *recoveryStorageImpl) handleAlterCollection(ctx context.Context, msg message.ImmutableAlterCollectionMessageV2) {
	if messageutil.RetiresVChannel(msg.Header(), msg.MustBody().GetUpdates(), msg.VChannel()) {
		if vchannelInfo, ok := r.vchannels[msg.VChannel()]; ok {
			vchannelInfo.ObserveRetire(msg.TimeTick())
			if vchannelInfo.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED && vchannelInfo.meta.Retired {
				// Track it so the persist gate (consumeDirtySnapshot) keeps
				// re-checking removability even if nothing else ever
				// touches this pchannel again -- see retiredVChannels.
				r.retiredVChannels[msg.VChannel()] = struct{}{}
			}
		}
		r.Logger().Info(ctx, "retire vchannel", mlog.FieldMessage(msg))
		return
	}

	// when put collection happens, we need to flush all segments in the collection.
	segments := make(map[int64]struct{}, len(msg.Header().FlushedSegmentIds))
	for _, segmentID := range msg.Header().FlushedSegmentIds {
		segments[segmentID] = struct{}{}
	}
	r.flushSegments(ctx, msg, segments)

	// persist the schema change into recovery info.
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; ok {
		vchannelInfo.ObserveAlterCollection(msg)
	}
}

// handleTruncateCollection handles the truncate collection message.
func (r *recoveryStorageImpl) handleTruncateCollection(ctx context.Context, msg message.ImmutableTruncateCollectionMessageV2) {
	// when truncate collection happens, we need to flush all segments in the collection.
	segments := make(map[int64]struct{}, len(msg.Header().SegmentIds))
	for _, segmentID := range msg.Header().SegmentIds {
		segments[segmentID] = struct{}{}
	}
	r.flushSegments(ctx, msg, segments)
}

// detectInconsistency detects the inconsistency in the recovery storage.
func (r *recoveryStorageImpl) detectInconsistency(ctx context.Context, msg message.ImmutableMessage, reason string, extra ...mlog.Field) {
	fields := make([]mlog.Field, 0, len(extra)+2)
	fields = append(fields, mlog.FieldMessage(msg), mlog.String("reason", reason))
	fields = append(fields, extra...)
	// The log is not fatal in some cases.
	// because our meta is not atomic-updated, so these error may be logged if crashes when meta updated partially.
	r.Logger().Warn(ctx, "inconsistency detected", fields...)
	r.metrics.ObserveInconsitentEvent()
}

// GetFlusherCheckpointByTimeTick returns the minimum flush checkpoint among all vchannels based on time tick.
// This method is used to determine the earliest checkpoint that can be safely flushed.
func (r *recoveryStorageImpl) GetFlusherCheckpointByTimeTick(ctx context.Context) *WALCheckpoint {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.vchannels) == 0 {
		r.Logger().Info(context.TODO(), "get flush checkpoint fast return pChan cp, due to no vChan", mlog.String("pChannel", r.channel.String()))
		return r.checkpoint
	}

	var minimumCheckpoint *WALCheckpoint
	for _, vchannel := range r.vchannels {
		if vchannel.GetFlushCheckpoint() == nil {
			// If any flush checkpoint is not set, not ready.
			return nil
		}
		if minimumCheckpoint == nil || vchannel.GetFlushCheckpoint().TimeTick < minimumCheckpoint.TimeTick {
			minimumCheckpoint = vchannel.GetFlushCheckpoint()
		}
	}
	return minimumCheckpoint
}

// getFlusherCheckpoint returns flusher checkpoint concurrent-safe
// NOTE: shall not be called with r.mu.Lock()!
func (r *recoveryStorageImpl) getFlusherCheckpoint() *WALCheckpoint {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.getFlusherCheckpointLocked()
}

// getFlusherCheckpointLocked is the lock-free core of getFlusherCheckpoint.
// The caller must already hold r.mu.
func (r *recoveryStorageImpl) getFlusherCheckpointLocked() *WALCheckpoint {
	var minimumCheckpoint *WALCheckpoint
	for _, vchannel := range r.vchannels {
		if vchannel.GetFlushCheckpoint() == nil {
			// If any flush checkpoint is not set, not ready.
			return nil
		}
		if minimumCheckpoint == nil || vchannel.GetFlushCheckpoint().MessageID.LTE(minimumCheckpoint.MessageID) {
			minimumCheckpoint = vchannel.GetFlushCheckpoint()
		}
	}
	return minimumCheckpoint
}
