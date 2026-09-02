package recovery

import (
	"context"
	"math"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
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
	opts ...RecoveryStorageOption,
) (RecoveryStorage, *RecoverySnapshot, error) {
	if cp == nil {
		cp = initialCheckpointFromLastTimeTickMessage(lastTimeTickMessage)
	}
	rs := newRecoveryStorage(recoveryStreamBuilder.Channel(), cp, opts...)
	if err := rs.recoverRecoveryInfoFromMeta(ctx, recoveryStreamBuilder.Channel()); err != nil {
		rs.Logger().Warn(context.TODO(), "recovery storage failed", mlog.Err(err))
		return nil, nil, err
	}
	snapshot, err := rs.runBoundedRecovery(ctx, recoveryStreamBuilder, lastTimeTickMessage)
	if err != nil {
		rs.Logger().Warn(context.TODO(), "recovery storage failed", mlog.Err(err))
		// The recovery modules are already started at this point; release them
		// so no goroutine or scheduler keeps a half-built store alive. The full
		// Close() cannot be used: the background task has not started yet, so
		// BlockUntilFinish would never return.
		rs.closeRecoveryResources()
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
	rs.startAckTracker()
	rs.startLiveScanner(recoveryStreamBuilder, lastTimeTickMessage)
	return rs, snapshot, nil
}

type RecoveryStorageOption func(*recoveryStorageImpl)

func WithNodeScheduler(scheduler nodescheduler.Scheduler) RecoveryStorageOption {
	return func(r *recoveryStorageImpl) {
		r.nodeScheduler = scheduler
	}
}

func WithRecoveryTailRateLimiter(rateLimiter RecoveryTailRateLimiter) RecoveryStorageOption {
	return func(r *recoveryStorageImpl) {
		r.recoveryTailRateLimiter = rateLimiter
	}
}

func initialCheckpointFromLastTimeTickMessage(lastTimeTickMessage message.ImmutableMessage) *utility.WALCheckpoint {
	return &utility.WALCheckpoint{
		MessageID: lastTimeTickMessage.LastConfirmedMessageID(),
		TimeTick:  lastTimeTickMessage.TimeTick(),
		Magic:     utility.RecoveryMagicRecoveryStorageV2,
	}
}

// newRecoveryStorage creates a new recovery storage.
func newRecoveryStorage(channel types.PChannelInfo, cp *utility.WALCheckpoint, opts ...RecoveryStorageOption) *recoveryStorageImpl {
	cfg := newConfig()
	metrics := newRecoveryStorageMetrics(channel)
	rs := &recoveryStorageImpl{
		backgroundTaskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		cfg:                    cfg,
		mu:                     sync.Mutex{},
		currentClusterID:       paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
		channel:                channel,
		dirtyCounter:           0,
		persistNotifier:        make(chan struct{}, 1),
		metrics:                metrics,
	}
	if cp != nil {
		rs.installCheckpoint(cp)
	}
	// The pchannel control state is embedded in the checkpoint: it advances
	// atomically with it, so the checkpoint is the single source of truth for
	// the control state after a crash (see utility.WALCheckpoint).
	rs.installPChannelControl(utility.PChannelControlFromCheckpoint(cp))
	for _, opt := range opts {
		opt(rs)
	}
	rs.tailController = newRecoveryTailController(cfg, rs.recoveryTailRateLimiter, metrics)
	rs.refreshRecoveryTail()
	if rs.nodeScheduler == nil {
		rs.nodeScheduler = nodescheduler.Get()
	}
	rs.taskScheduler = newScopedTaskScheduler(rs.nodeScheduler, cfg.taskConcurrency)
	rs.broadcastAck = newBroadcastAckModule(moduleapi.Runtime{
		Scheduler: rs.taskScheduler,
		Notifier:  rs,
	})
	return rs
}

// recoveryStorageImpl is a component that manages the recovery info for the streaming service.
// It will consume the message from the wal, consume the message in wal, and update the checkpoint for it.
type recoveryStorageImpl struct {
	mlog.Binder
	backgroundTaskNotifier  *syncutil.AsyncTaskNotifier[struct{}]
	cfg                     *config
	mu                      sync.Mutex
	currentClusterID        string
	channel                 types.PChannelInfo
	checkpoint              *WALCheckpoint
	pchannelControl         *streamingpb.PChannelRecoveryControlMeta
	ackTracker              *messageack.Tracker
	tailController          *recoveryTailController
	recoveryTailRateLimiter RecoveryTailRateLimiter
	broadcastAck            *broadcastAckModule
	vchannelManager         *vchannel.PChannelRecoveryManager
	summaryManager          *walsummary.Manager
	nodeScheduler           nodescheduler.Scheduler
	taskScheduler           *scopedTaskScheduler
	dirtyCounter            int // records the message count since last persist snapshot.
	// used to trigger the recovery persist operation.
	persistNotifier        chan struct{}
	truncator              walimpls.WALImpls
	metrics                *recoveryMetrics
	pendingPersistSnapshot *dirtyPersistSnapshot
	scannerWG              sync.WaitGroup
	ackTrackerWG           sync.WaitGroup
	// pendingSalvageCheckpoint holds the salvage checkpoint captured during force promote.
	// Set under r.mu; consumed and persisted by the background task to avoid holding the lock.
	pendingSalvageCheckpoint *utility.ReplicateCheckpoint
}

func (r *recoveryStorageImpl) installCheckpoint(checkpoint *WALCheckpoint) {
	if checkpoint == nil {
		checkpoint = &WALCheckpoint{}
	}
	r.checkpoint = checkpoint.Clone()
	if r.metrics != nil {
		r.metrics.ObserveObservedTimeTick(checkpoint.TimeTick)
		r.metrics.ObServeInMemMetrics(checkpoint.TimeTick)
		r.metrics.ObServePersistedMetrics(checkpoint.TimeTick)
	}
	point := utility.WALCheckpoint{
		MessageID: checkpoint.MessageID,
		TimeTick:  checkpoint.TimeTick,
		Magic:     checkpoint.Magic,
	}
	var tracker *messageack.Tracker
	tracker = messageack.NewTracker(point, func(utility.WALCheckpoint) {
		observed, completed := tracker.LogicalOffsets()
		if r.tailController != nil {
			r.tailController.UpdateTrackerFrontiers(observed, completed)
		}
		r.notifyPersist()
	}, composedPersistRequester{
		vchannelManager: r.vchannelManager,
		summaryManager:  r.summaryManager,
	})
	r.ackTracker = tracker
	if r.tailController != nil {
		r.tailController.Reset()
	}
	r.refreshRecoveryTail()
}

func (r *recoveryStorageImpl) installPChannelControl(control *streamingpb.PChannelRecoveryControlMeta) {
	if control == nil {
		control = &streamingpb.PChannelRecoveryControlMeta{}
	}
	r.pchannelControl = proto.Clone(control).(*streamingpb.PChannelRecoveryControlMeta)
}

func (r *recoveryStorageImpl) initRecoveryModules(
	ctx context.Context,
	vchannels map[string]*streamingpb.VChannelMeta,
	segments map[int64]*streamingpb.SegmentAssignmentMeta,
	summaryManager *walsummary.Manager,
) error {
	coord, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		return err
	}
	moduleRuntime := moduleapi.Runtime{
		Scheduler: r.taskScheduler,
		Notifier:  r,
	}
	transformLogMaterializer := transformlog.NewSyncMaterializer(
		resource.Resource().ChunkManager(),
		idalloc.NewMAllocator(resource.Resource().IDAllocator()),
		syncmgr.BrokerMetaWriter(broker.NewCoordBroker(coord, paramtable.GetNodeID()), paramtable.GetNodeID()),
	)
	// Load the initial materialization window of every vchannel: the durable
	// records after the restored transform_materialized_time_tick. This is
	// the only read of the summary store the transform consumer relies on; at
	// runtime it observes the vchannel's messages directly instead.
	// The persisted frontier is the single source of truth for recovery: a
	// crash only loses in-flight registrations, whose records are still in
	// the summary window below and are re-materialized (duplicate L0 output
	// is idempotent), and a crash never loses materialized-but-unregistered
	// records because the frontier advances only after registration succeeds.
	pendingTransformEntries := make(map[string][]*streamingpb.TransformLogEntry, len(vchannels))
	for vchannel, meta := range vchannels {
		// The summary was already restored (chunk index, durable frontiers and
		// GC positions) by Restore; only the transform consumer's recovery
		// window is loaded here: the durable records after the restored
		// materialization frontier. This is the only read of the summary store
		// the transform consumer relies on; at runtime it observes the
		// vchannel's messages directly instead.
		//
		// The persisted frontier is the single source of truth for recovery: a
		// crash only loses in-flight registrations, whose records are still in
		// the summary window below and are re-materialized (duplicate L0
		// output is idempotent), and a crash never loses
		// materialized-but-unregistered records because the frontier advances
		// only after registration succeeds.
		if state := meta.GetState(); state == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED ||
			state == streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED {
			// A dropped vchannel consumes nothing after its cleanup; it needs
			// no materialization window.
			continue
		}
		entries, err := summaryManager.ReadTransformEntries(
			ctx, vchannel, meta.GetTransformMaterializedTimeTick(), math.MaxUint64,
		)
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			pendingTransformEntries[vchannel] = entries
		}
	}
	// Deprecated: the manager periodically reports the pchannel recovery
	// checkpoint to DataCoord (DataCoord.UpdateChannelCheckpoint) so that
	// GetFlushState can observe flush progress. The recovery storage write
	// path itself never calls UpdateChannelCheckpoint; remove this wiring
	// together with PChannelCheckpointUpdater once the new
	// checkpoint-propagation path lands.
	coordinatorBroker := broker.NewCoordBroker(coord, paramtable.GetNodeID())
	manager, err := vchannel.NewPChannelRecoveryManager(vchannel.PChannelManagerConfig{
		PChannel:         r.channel.Name,
		VChannelMetas:    vchannels,
		Segments:         segments,
		Runtime:          moduleRuntime,
		Logger:           r.Logger(),
		SegmentLifecycle: segment.NewSegmentLifecycleWriter(coord, paramtable.GetNodeID()),
		SegmentPackWriter: segment.NewBulkPackWriter(
			resource.Resource().ChunkManager(),
			idalloc.NewMAllocator(resource.Resource().IDAllocator()),
			packed.CreateStorageConfig(),
		),
		SummaryManager:            summaryManager,
		PendingTransformEntries:   pendingTransformEntries,
		TransformLogMaterializer:  transformLogMaterializer,
		TransformLogMaterialRows:  uint64(paramtable.Get().StreamingCfg.FlushL0MaxRowNum.GetAsInt()),
		TransformLogMaterialBytes: uint64(paramtable.Get().StreamingCfg.FlushL0MaxSize.GetAsSize()),
		GetRecoveryCheckpoint:     func() *utility.WALCheckpoint { return r.GetCheckpoint(context.TODO()) },
		CoordinatorBroker:         coordinatorBroker,
	})
	if err != nil {
		return err
	}
	manager.Start()
	r.vchannelManager = manager
	r.summaryManager = summaryManager
	r.installCheckpoint(r.checkpoint)
	// Seed the summary's confirmation frontier from the restored checkpoint so
	// the frontier never starts behind a checkpoint that was already
	// persisted; the WAL replay re-observes messages right after.
	if summaryManager != nil {
		summaryManager.InitLastAcked(r.checkpoint)
	}
	return nil
}

// newSummaryManager creates the pchannel-scoped WALSummary manager and wires
// it to the recovery runtime.
func (r *recoveryStorageImpl) newSummaryManager(runtime moduleapi.Runtime) *walsummary.Manager {
	return walsummary.NewManager(walsummary.ManagerConfig{
		PChannel: r.channel.Name,
		Term:     r.channel.Term,
		Store: walsummary.NewStore(
			resource.Resource().ChunkManager(),
			r.channel.Name,
			r.channel.Term,
		),
		Runtime:           runtime,
		FlushMaxBytes:     uint64(paramtable.Get().StreamingCfg.FlushL0MaxSize.GetAsSize()),
		RetentionMaxBytes: uint64(paramtable.Get().StreamingCfg.SummaryMaxBytesPerPChannel.GetAsSize()),
		Logger:            r.Logger(),
	})
}

func (r *recoveryStorageImpl) NotifyModuleUpdated(moduleapi.ModuleName) {
	r.notifyPersist()
}

// Metrics gets the metrics of the wal.
func (r *recoveryStorageImpl) Metrics() RecoveryMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	checkpoint := r.checkpoint
	if r.ackTracker != nil {
		completed := r.ackTracker.CompletedPoint()
		checkpoint = &completed
	}
	tail := recoveryTailSnapshot{}
	if r.tailController != nil {
		tail = r.tailController.Snapshot()
	}
	return RecoveryMetrics{
		RecoveryTimeTick:  checkpoint.TimeTick,
		RecoveryTailBytes: tail.RecoveryTail,
		BlockingBytes:     tail.Blocking,
		PublishLagBytes:   tail.PublishLag,
	}
}

func (r *recoveryStorageImpl) VChannelManager() *vchannel.PChannelRecoveryManager {
	return r.vchannelManager
}

// Close closes the recovery storage and wait the background task stop.
func (r *recoveryStorageImpl) Close() {
	r.backgroundTaskNotifier.Cancel()
	r.backgroundTaskNotifier.BlockUntilFinish()
	r.scannerWG.Wait()
	r.ackTrackerWG.Wait()
	r.closeRecoveryResources()
}

// closeRecoveryResources releases the goroutines and schedulers started during
// recovery. It is shared by Close (after the background task finished) and by
// the failed-recovery path (where the background task never started, so
// BlockUntilFinish must not be called).
func (r *recoveryStorageImpl) closeRecoveryResources() {
	if r.broadcastAck != nil {
		r.broadcastAck.Close()
	}
	if r.taskScheduler != nil {
		r.taskScheduler.Close()
	}
	if r.vchannelManager != nil {
		// Stops the deprecated DataCoord channel-checkpoint reporting loop.
		r.vchannelManager.Close()
	}
	r.metrics.Close()
}

func (r *recoveryStorageImpl) startAckTracker() {
	if r.ackTracker == nil {
		return
	}
	r.ackTrackerWG.Add(1)
	go func() {
		defer r.ackTrackerWG.Done()
		var underPressure func() bool
		if r.tailController != nil {
			underPressure = r.tailController.UnderSoftPressure
		}
		r.ackTracker.Run(r.backgroundTaskNotifier.Context(), r.cfg.ackStallTimeout, underPressure)
	}()
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
func (r *recoveryStorageImpl) consumeDirtySnapshot() *dirtyPersistSnapshot {
	r.mu.Lock()
	if r.checkpoint == nil {
		r.installCheckpoint(nil)
	}
	if r.pchannelControl == nil {
		r.installPChannelControl(nil)
	}
	var checkpoint *WALCheckpoint
	if r.checkpoint != nil {
		checkpoint = r.checkpoint.Clone()
	}
	completedPoint, completedLogicalOffset := r.ackTracker.Completed()
	// The summary consumes the WAL without retaining message references, so
	// the tracker's completed point does not include it. Merge the summary's
	// own confirmation frontier: the persisted checkpoint must never advance
	// past a staged-but-not-yet-durable delete record (the WAL truncation
	// would delete it and recovery would never replay it). Both points are
	// WAL positions, so the comparison is on the message ID, which orders
	// strictly with the timetick on a single WAL.
	if r.summaryManager != nil {
		if summaryAcked := r.summaryManager.LastAcked(); summaryAcked != nil {
			if completedPoint.MessageID == nil || summaryAcked.MessageID.LT(completedPoint.MessageID) {
				completedPoint = *summaryAcked
			}
		}
	}
	if checkpoint != nil && !shouldAdvanceConsumePoint(*checkpoint, completedPoint) {
		completedPoint = *checkpoint.Clone()
		if r.tailController != nil {
			completedLogicalOffset = r.tailController.Snapshot().PublishedOffset
		}
	}
	frozenCheckpoint := &WALCheckpoint{
		MessageID: completedPoint.MessageID,
		TimeTick:  completedPoint.TimeTick,
		Magic:     utility.RecoveryMagicRecoveryStorageV2,
	}
	// The control state advances atomically with the checkpoint: freeze the
	// current in-memory control into the checkpoint so a control-only change
	// rewrites the checkpoint and is never lost on crash.
	frozenCheckpoint.ApplyControl(r.pchannelControl)
	checkpointDirty := checkpoint == nil ||
		!consumeCheckpointEqual(checkpoint, frozenCheckpoint)
	salvageCP := r.pendingSalvageCheckpoint
	r.pendingSalvageCheckpoint = nil
	r.dirtyCounter = 0
	r.mu.Unlock()

	cleanup := moduleapi.CleanupContext{}
	if checkpoint != nil {
		cleanup.PhysicalTimeTick = checkpoint.TimeTick
	}
	moduleSnapshots := make([]moduleapi.DirtySnapshot, 0)
	if r.vchannelManager != nil {
		moduleSnapshots = append(moduleSnapshots, r.vchannelManager.ConsumeCleanupSnapshots(cleanup)...)
		moduleSnapshots = append(moduleSnapshots, r.vchannelManager.ConsumeDirtySnapshots()...)
	}
	if !checkpointDirty && salvageCP == nil && len(moduleSnapshots) == 0 {
		return nil
	}
	return &dirtyPersistSnapshot{
		Checkpoint:        frozenCheckpoint,
		LogicalEndOffset:  completedLogicalOffset,
		CheckpointDirty:   checkpointDirty,
		SalvageCheckpoint: salvageCP,
		ModuleDirtySnaps:  moduleSnapshots,
	}
}

func consumeCheckpointEqual(left, right *utility.WALCheckpoint) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	if left.TimeTick != right.TimeTick {
		return false
	}
	if left.Magic != right.Magic {
		return false
	}
	if !proto.Equal(left.ReplicateConfig, right.ReplicateConfig) ||
		!proto.Equal(left.ReplicateCheckpoint, right.ReplicateCheckpoint) ||
		!proto.Equal(left.AlterWalState, right.AlterWalState) {
		return false
	}
	if left.MessageID == nil || right.MessageID == nil {
		return left.MessageID == nil && right.MessageID == nil
	}
	return left.MessageID.EQ(right.MessageID)
}

func shouldAdvanceConsumePoint(current, next utility.WALCheckpoint) bool {
	if next.TimeTick != current.TimeTick {
		return next.TimeTick > current.TimeTick
	}
	if current.MessageID == nil {
		return next.MessageID != nil
	}
	return next.MessageID != nil && current.MessageID.LT(next.MessageID)
}

// observeMessage observes a message and update the recovery storage.
func (r *recoveryStorageImpl) observeMessage(ctx context.Context, msg message.ImmutableMessage) {
	// Non-persisted time tick heartbeats carry no data and are not written to
	// the WAL; consuming them would advance the tracker (and thus persist a
	// recovery snapshot) on every heartbeat even while the pchannel is fully
	// idle. Skip them entirely: the time tick sync inspector appends a
	// persisted time tick every forcePersistedInterval, which keeps the
	// checkpoint fresh on a bounded cadence instead.
	if !msg.IsPersisted() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	owner := r.ackTracker.Track(msg)
	r.refreshRecoveryTail()
	r.metrics.ObserveObservedTimeTick(msg.TimeTick())
	dispatch := owner.Clone()
	r.observeModulesMessage(ctx, dispatch)
	dispatch.Release()
	r.updatePChannelControl(msg)
	r.broadcastAck.Accept(owner)
	completed := r.ackTracker.CompletedPoint()
	r.metrics.ObServeInMemMetrics(completed.TimeTick)

	r.dirtyCounter++
	if r.dirtyCounter > r.cfg.maxDirtyMessages {
		r.notifyPersist()
	}
}

func (r *recoveryStorageImpl) refreshRecoveryTail() {
	if r.ackTracker == nil || r.tailController == nil {
		return
	}
	observed, completed := r.ackTracker.LogicalOffsets()
	r.tailController.UpdateTrackerFrontiers(observed, completed)
}

func (r *recoveryStorageImpl) observeModulesMessage(
	ctx context.Context,
	retained message.RetainedImmutableMessage,
) {
	if r.vchannelManager == nil {
		panic("recovery modules are not initialized")
	}
	r.vchannelManager.ObserveMessage(ctx, retained)
	// The summary observes the same message stream independently of the
	// vchannel modules: its pchannel-level statistics (staging bytes, flush
	// threshold) are maintained by the summary manager itself.
	if r.summaryManager != nil {
		r.summaryManager.ObserveMessage(ctx, retained)
	}
}

// composedPersistRequester fans a tracker stall / under-pressure request out
// to both the vchannel segments and the pchannel summary: the WAL checkpoint
// must not advance until the buffered data of the stalled vchannel is durable
// on every write path. The summary handles the request at the pchannel level
// (a chunk is pchannel-scoped, see walsummary.Manager.RequestFlushThrough).
type composedPersistRequester struct {
	vchannelManager *vchannel.PChannelRecoveryManager
	summaryManager  *walsummary.Manager
}

func (r composedPersistRequester) RequestPersistThrough(vchannelName string, targetTimeTick uint64) {
	r.vchannelManager.RequestPersistThrough(vchannelName, targetTimeTick)
	if r.summaryManager != nil {
		r.summaryManager.RequestFlushThrough(targetTimeTick)
	}
}

func (r *recoveryStorageImpl) startLiveScanner(
	recoveryStreamBuilder RecoveryStreamBuilder,
	recoveryBarrier message.ImmutableMessage,
) {
	if recoveryBarrier == nil || recoveryBarrier.MessageID() == nil {
		r.Logger().Warn(context.TODO(), "skip live scanner because recovery barrier is nil")
		return
	}
	rs := recoveryStreamBuilder.Build(BuildRecoveryStreamParam{
		StartCheckpoint:     recoveryBarrier.MessageID(),
		EndTimeTick:         0,
		UseWriteAheadBuffer: true,
	})
	r.scannerWG.Add(1)
	go func() {
		defer r.scannerWG.Done()
		r.runLiveScanner(rs)
	}()
}

func (r *recoveryStorageImpl) runLiveScanner(rs RecoveryStream) {
	defer rs.Close()
	ctx := r.backgroundTaskNotifier.Context()
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-rs.Chan():
			if !ok {
				if err := rs.Error(); err != nil {
					r.Logger().Warn(context.TODO(), "wal recovery live scanner stopped with error", mlog.Err(err))
				}
				return
			}
			r.observeMessage(ctx, msg)
		}
	}
}

// updatePChannelControl applies pchannel-scoped recovery control effects. The
// global WAL checkpoint is advanced exclusively by AckTracker completion.
func (r *recoveryStorageImpl) updatePChannelControl(msg message.ImmutableMessage) {
	if r.pchannelControl == nil {
		r.installPChannelControl(nil)
	}
	if msg.TimeTick() <= r.pchannelControl.GetCheckpointTimeTick() {
		return
	}
	changed := false
	if msg.MessageType() == message.MessageTypeAlterReplicateConfig {
		cfg := message.MustAsImmutableAlterReplicateConfigMessageV2(msg)
		header := cfg.Header()

		// Check ignore field - if true, skip updating ReplicateConfig and ReplicateCheckpoint
		// This is used for incomplete switchover messages that should be ignored after force promote
		if header.Ignore {
			r.Logger().Info(context.TODO(), "AlterReplicateConfig message has ignore flag set, skipping checkpoint update",
				mlog.Bool("forcePromote", header.ForcePromote))
		} else {
			r.pchannelControl.ReplicateConfig = proto.Clone(header.ReplicateConfiguration).(*commonpb.ReplicateConfiguration)
			changed = true
			clusterRole := replicateutil.MustNewConfigHelper(r.currentClusterID, header.ReplicateConfiguration).GetCurrentCluster()
			switch clusterRole.Role() {
			case replicateutil.RolePrimary:
				if header.GetForcePromote() && r.pchannelControl.ReplicateCheckpoint != nil {
					// Store for background task to persist; never call etcd while holding r.mu.
					r.pendingSalvageCheckpoint = utility.NewReplicateCheckpointFromProto(r.pchannelControl.ReplicateCheckpoint)
					r.notifyPersist()
				}
				r.pchannelControl.ReplicateCheckpoint = nil
			case replicateutil.RoleSecondary:
				// Update the replicate checkpoint if the cluster role is secondary.
				sourceClusterID := clusterRole.SourceCluster().GetClusterId()
				sourcePChannel := clusterRole.MustGetSourceChannel(r.channel.Name)
				if r.pchannelControl.ReplicateCheckpoint == nil || r.pchannelControl.ReplicateCheckpoint.GetClusterId() != sourceClusterID {
					r.pchannelControl.ReplicateCheckpoint = (&utility.ReplicateCheckpoint{
						ClusterID: sourceClusterID,
						PChannel:  sourcePChannel,
						MessageID: nil,
						TimeTick:  0,
					}).IntoProto()
					changed = true
				}
			}
		}
	}
	if msg.MessageType() == message.MessageTypeAlterWAL {
		alterWAL := message.MustAsImmutableAlterWALMessageV2(msg)
		header := alterWAL.Header()
		if r.pchannelControl.AlterWalState == nil || r.pchannelControl.AlterWalState.Stage == streamingpb.AlterWALStage_NONE {
			r.pchannelControl.AlterWalState = &streamingpb.AlterWALState{
				TargetWalName: header.TargetWalName,
				TimeTick:      msg.TimeTick(),
				Configs:       header.Config,
				Stage:         streamingpb.AlterWALStage_FLUSHING,
			}
			changed = true
		}
	}
	// update the replicate checkpoint.
	replicateHeader := msg.ReplicateHeader()
	if replicateHeader != nil && r.pchannelControl.ReplicateCheckpoint == nil {
		r.Logger().Warn(context.TODO(), "replicate checkpoint is nil when incoming replicate message", mlog.FieldMessage(msg))
	} else if replicateHeader != nil && replicateHeader.ClusterID != r.pchannelControl.ReplicateCheckpoint.GetClusterId() {
		r.Logger().Warn(context.TODO(), "replicate header cluster id mismatch",
			mlog.FieldMessage(msg),
			mlog.String("expected", r.pchannelControl.ReplicateCheckpoint.GetClusterId()),
			mlog.String("actual", replicateHeader.ClusterID))
	} else if replicateHeader != nil {
		r.pchannelControl.ReplicateCheckpoint.MessageId = message.MustMarshalMessageID(replicateHeader.LastConfirmedMessageID)
		r.pchannelControl.ReplicateCheckpoint.TimeTick = replicateHeader.TimeTick
		changed = true
	}
	if changed {
		r.pchannelControl.CheckpointTimeTick = msg.TimeTick()
	}
}

// GetCheckpoint returns the latest catalog-published recovery checkpoint.
func (r *recoveryStorageImpl) GetCheckpoint(_ context.Context) *WALCheckpoint {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.checkpoint == nil {
		return nil
	}
	return r.checkpoint.Clone()
}

func (r *recoveryStorageImpl) getCompletedCheckpoint() *WALCheckpoint {
	if r.ackTracker == nil {
		return nil
	}
	point := r.ackTracker.CompletedPoint()
	if point.MessageID == nil {
		return nil
	}
	return &WALCheckpoint{
		MessageID: point.MessageID,
		TimeTick:  point.TimeTick,
		Magic:     utility.RecoveryMagicRecoveryStorageV2,
	}
}
