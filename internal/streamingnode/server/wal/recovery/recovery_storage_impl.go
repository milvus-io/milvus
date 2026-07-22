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
	// Recover the idempotency window cache before WAL replay. The chunk store is
	// the source of truth when a chunk write succeeded but catalog metadata was
	// not advanced before crash. recoverWindows is a no-op when idempotency is
	// disabled and tolerates a corrupted (rebuildable) window store; it returns
	// the (possibly rewound) checkpoint to resume consuming from.
	rewoundCheckpoint, err := rs.windowManager.recoverWindows(ctx, recoveryStreamBuilder.Channel().Name, rs.checkpoint, rs.vchannels)
	if err != nil {
		rs.Logger().Warn(ctx, "recovery storage failed", mlog.Err(err))
		return nil, nil, err
	}
	rs.checkpoint = rewoundCheckpoint
	// recover the state from wal and start the background task to persist the state.
	snapshot, err := rs.recoverFromStream(ctx, recoveryStreamBuilder, lastTimeTickMessage)
	if err != nil {
		rs.Logger().Warn(ctx, "recovery storage failed", mlog.Err(err))
		return nil, nil, err
	}
	// recovery storage start work.
	rs.metrics.ObserveStateChange(recoveryStorageStateWorking)
	logger := resource.Resource().Logger().With(
		mlog.Int64("nodeID", paramtable.GetNodeID()),
		mlog.FieldComponent(componentRecoveryStorage),
		mlog.String("channel", recoveryStreamBuilder.Channel().String()),
		mlog.String("state", recoveryStorageStateWorking))
	rs.SetLogger(logger)
	rs.windowManager.SetLogger(logger)
	rs.truncator = recoveryStreamBuilder.RWWALImpls()
	rs.windowManager.setNormalMode()
	go rs.backgroundTask()
	//nolint:gosec // G118: the window background task is a WAL-lifetime goroutine; it must outlive the recovery request context.
	go rs.windowManager.windowBackgroundTask()
	return rs, snapshot, nil
}

// newRecoveryStorage creates a new recovery storage.
func newRecoveryStorage(channel types.PChannelInfo, cp *utility.WALCheckpoint) *recoveryStorageImpl {
	cfg := newConfig()
	rs := &recoveryStorageImpl{
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
	}
	rs.windowManager = newWindowManager(channel.Name, cfg, rs.metrics, cp, windowEvictionConfig{
		windowTTL:  cfg.idempotencyWindowTTL,
		minEntries: cfg.idempotencyMinEntries,
		maxEntries: cfg.idempotencyMaxEntries,
	})
	return rs
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
	windowManager          *windowManager
	checkpoint             *WALCheckpoint
	dirtyCounter           int // records the message count since last persist snapshot.
	// used to trigger the recovery persist operation.
	persistNotifier                chan struct{}
	gracefulClosed                 bool
	truncator                      walimpls.WALImpls
	metrics                        *recoveryMetrics
	pendingRecoveryPersistSnapshot *RecoverySnapshot
	// used to mark switch MQ msg found
	alterWALInfo *AlterWALInfo
	// pendingSalvageCheckpoint holds the salvage checkpoint captured during force promote.
	// Set under r.mu; consumed and persisted by the background task to avoid holding the lock.
	pendingSalvageCheckpoint *utility.ReplicateCheckpoint
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
	if vchannelInfo, ok := r.vchannels[vchannel]; ok {
		if err := vchannelInfo.UpdateFlushCheckpoint(checkpoint); err != nil {
			r.Logger().Warn(context.TODO(), "failed to update flush checkpoint", mlog.Err(err))
			return
		}
		r.Logger().Info(context.TODO(), "update flush checkpoint", mlog.String("vchannel", vchannel), mlog.String("messageID", checkpoint.MessageID.String()), mlog.Uint64("timeTick", checkpoint.TimeTick))
		return
	}
	r.Logger().Warn(context.TODO(), "vchannel not found", mlog.String("vchannel", vchannel))
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
	// observeMessage mutates window state that the window background task also
	// touches, so hold windowManager.mu for the whole message: the per-message
	// window updates stay atomic as a group and mutually exclusive with window
	// persistence. The lock order is always rs.mu -> windowManager.mu.
	r.windowManager.mu.Lock()
	defer r.windowManager.mu.Unlock()
	r.observeMessage(ctx, msg)
	return nil
}

// Close closes the recovery storage and wait the background task stop.
func (r *recoveryStorageImpl) Close() {
	if r.windowManager != nil {
		r.windowManager.windowBackgroundTaskNotifier.Cancel()
		r.windowManager.windowBackgroundTaskNotifier.BlockUntilFinish()
	}
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
func (r *recoveryStorageImpl) consumeDirtySnapshot() *RecoverySnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.consumeDirtySnapshotLocked()
}

func (r *recoveryStorageImpl) consumeDirtySnapshotLocked() *RecoverySnapshot {
	if !r.hasDirtyRecoveryStateUnsafe() {
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
		dirtySnapshot, shouldBeRemoved := vchannel.ConsumeDirtyAndGetSnapshot()
		if shouldBeRemoved {
			delete(r.vchannels, vchannel.meta.Vchannel)
			// The vchannel is fully reclaimed; drop its idempotency window too so
			// m.windows does not grow without bound under create/drop churn.
			r.windowManager.removeIdempotencyWindow(vchannel.meta.Vchannel)
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

func (r *recoveryStorageImpl) hasDirtyRecoveryStateUnsafe() bool {
	return r.dirtyCounter > 0 || r.pendingSalvageCheckpoint != nil ||
		r.windowManager.canPersistConsumeCheckpoint(r.checkpoint, r.getFlusherCheckpointUnsafe())
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
	r.windowManager.advanceIdempotencyWindowCheckpoints(r.checkpoint)
	r.windowManager.observeMessage(msg)

	r.updateCheckpoint(ctx, msg)
	r.windowManager.advancePChannelWindowSnapshotCheckpoint(r.checkpoint)
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

	if msg.VChannel() != "" && !msg.IsPChannelLevel() && msg.MessageType() != message.MessageTypeCreateCollection &&
		msg.MessageType() != message.MessageTypeDropCollection && r.vchannels[msg.VChannel()] == nil && !funcutil.IsControlChannel(msg.VChannel()) {
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
	case message.MessageTypeTimeTick:
		// nothing, the time tick message make no recovery operation.
	case message.MessageTypeAlterWAL:
		immutableMsg := message.MustAsImmutableAlterWALMessageV2(msg)
		r.handleAlterWAL(ctx, immutableMsg)
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
	// The vchannel just became active; create its idempotency window here rather
	// than rescanning every active vchannel on each observed message.
	r.windowManager.ensureIdempotencyWindow(msg.VChannel(), r.checkpoint)
	r.Logger().Info(ctx, "create collection", mlog.FieldMessage(msg))
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
func (r *recoveryStorageImpl) handleAlterCollection(ctx context.Context, msg message.ImmutableAlterCollectionMessageV2) {
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
	return r.getFlusherCheckpointUnsafe()
}

func (r *recoveryStorageImpl) getFlusherCheckpointUnsafe() *WALCheckpoint {
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
