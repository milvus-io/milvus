package recovery

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/transformlog"
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
	snapshot, err := rs.runBoundedMetaScannerAndSwitchModules(ctx, recoveryStreamBuilder, lastTimeTickMessage)
	if err != nil {
		rs.Logger().Warn(context.TODO(), "recovery storage failed", mlog.Err(err))
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
	rs.startDataLiveScanner(recoveryStreamBuilder)
	return rs, snapshot, nil
}

type RecoveryStorageOption func(*recoveryStorageImpl)

func WithQueryRuntimeModuleBuilders(builders ...queryresource.QueryRuntimeModuleBuilder) RecoveryStorageOption {
	return func(r *recoveryStorageImpl) {
		r.queryRuntimeModuleBuilders = append([]queryresource.QueryRuntimeModuleBuilder(nil), builders...)
	}
}

func WithQueryViewLoadInfoProvider(provider queryresource.LoadInfoProvider) RecoveryStorageOption {
	return func(r *recoveryStorageImpl) {
		r.queryViewLoadInfoProvider = provider
	}
}

func WithNodeScheduler(scheduler nodescheduler.Scheduler) RecoveryStorageOption {
	return func(r *recoveryStorageImpl) {
		r.nodeScheduler = scheduler
	}
}

func initialCheckpointFromLastTimeTickMessage(lastTimeTickMessage message.ImmutableMessage) *utility.WALCheckpoint {
	point := utility.WALConsumeCheckpoint{
		MessageID: lastTimeTickMessage.LastConfirmedMessageID(),
		TimeTick:  lastTimeTickMessage.TimeTick(),
	}
	return &utility.WALCheckpoint{
		MessageID:      point.MessageID,
		TimeTick:       point.TimeTick,
		DataCheckpoint: point.Clone(),
	}
}

// newRecoveryStorage creates a new recovery storage.
func newRecoveryStorage(channel types.PChannelInfo, cp *utility.WALCheckpoint, opts ...RecoveryStorageOption) *recoveryStorageImpl {
	cfg := newConfig()
	rs := &recoveryStorageImpl{
		backgroundTaskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		cfg:                    cfg,
		mu:                     sync.Mutex{},
		currentClusterID:       paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
		channel:                channel,
		dirtyCounter:           0,
		persistNotifier:        make(chan struct{}, 1),
		gracefulClosed:         false,
		metrics:                newRecoveryStorageMetrics(channel),
	}
	if cp != nil {
		rs.installCheckpoint(cp)
	}
	for _, opt := range opts {
		opt(rs)
	}
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
	backgroundTaskNotifier *syncutil.AsyncTaskNotifier[struct{}]
	cfg                    *config
	mu                     sync.Mutex
	currentClusterID       string
	channel                types.PChannelInfo
	checkpoint             *WALCheckpoint
	persistedCheckpoint    *WALCheckpoint
	ackTracker             *messageack.Tracker
	broadcastAck           *broadcastAckModule
	checkpointDirty        bool
	vchannelManager        *vchannel.PChannelRecoveryManager
	nodeScheduler          nodescheduler.Scheduler
	taskScheduler          *scopedTaskScheduler
	dirtyCounter           int // records the message count since last persist snapshot.
	moduleDirty            bool
	// used to trigger the recovery persist operation.
	persistNotifier        chan struct{}
	gracefulClosed         bool
	truncator              walimpls.WALImpls
	metrics                *recoveryMetrics
	pendingPersistSnapshot *dirtyPersistSnapshot
	// used to mark switch MQ msg found
	alterWALInfo *AlterWALInfo
	// pendingSalvageCheckpoint holds the salvage checkpoint captured during force promote.
	// Set under r.mu; consumed and persisted by the background task to avoid holding the lock.
	pendingSalvageCheckpoint   *utility.ReplicateCheckpoint
	queryRuntimeModuleBuilders []queryresource.QueryRuntimeModuleBuilder
	queryViewLoadInfoProvider  queryresource.LoadInfoProvider
}

func (r *recoveryStorageImpl) installCheckpoint(checkpoint *WALCheckpoint) {
	if checkpoint == nil {
		checkpoint = &WALCheckpoint{}
	}
	r.checkpoint = checkpoint.Clone()
	if r.persistedCheckpoint == nil && checkpoint != nil {
		r.persistedCheckpoint = checkpoint.Clone()
	}
	dataPoint := utility.WALConsumeCheckpoint{
		MessageID: checkpoint.MessageID,
		TimeTick:  checkpoint.TimeTick,
	}
	if checkpoint.DataCheckpoint != nil {
		dataPoint = *checkpoint.DataCheckpoint.Clone()
	}
	r.ackTracker = messageack.NewTracker(dataPoint, func(utility.WALConsumeCheckpoint) {
		r.notifyPersist()
	})
}

func (r *recoveryStorageImpl) initRecoveryModules(
	ctx context.Context,
	vchannels map[string]*streamingpb.VChannelMeta,
	segments map[int64]*streamingpb.SegmentAssignmentMeta,
	transformLogMetas map[string]*streamingpb.VChannelTransformLogMeta,
) error {
	coord, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		return err
	}
	moduleRuntime := moduleapi.Runtime{
		Scheduler: r.taskScheduler,
		Notifier:  r,
	}
	transformLogStore := transformlog.NewObjectChunkStore(
		resource.Resource().ChunkManager(),
		r.channel.Name,
	)
	transformLogMaterializer := transformlog.NewSyncMaterializer(
		resource.Resource().ChunkManager(),
		idalloc.NewMAllocator(resource.Resource().IDAllocator()),
		syncmgr.BrokerMetaWriter(broker.NewCoordBroker(coord, paramtable.GetNodeID()), paramtable.GetNodeID()),
	)
	manager, err := vchannel.NewPChannelRecoveryManager(vchannel.PChannelManagerConfig{
		PChannel:               r.channel.Name,
		DataCheckpointTimeTick: r.checkpoint.DataCheckpoint.TimeTick,
		VChannelMetas:          vchannels,
		Segments:               segments,
		TransformLogMetas:      transformLogMetas,
		Runtime:                moduleRuntime,
		Logger:                 r.Logger(),
		SegmentLifecycle:       segment.NewSegmentLifecycleWriter(coord, paramtable.GetNodeID()),
		SegmentPackWriter: segment.NewBulkPackWriter(
			resource.Resource().ChunkManager(),
			idalloc.NewMAllocator(resource.Resource().IDAllocator()),
			packed.CreateStorageConfig(),
		),
		TransformLogStore:          transformLogStore,
		TransformLogMaterializer:   transformLogMaterializer,
		TransformLogMaxRows:        uint64(paramtable.Get().StreamingCfg.FlushL0MaxRowNum.GetAsInt()),
		TransformLogMaterialRows:   uint64(paramtable.Get().StreamingCfg.FlushL0MaxRowNum.GetAsInt()),
		TransformLogMaterialBytes:  uint64(paramtable.Get().StreamingCfg.FlushL0MaxSize.GetAsSize()),
		QueryRuntimeModuleBuilders: r.queryRuntimeModuleBuilders,
		QueryViewLoadInfoProvider:  r.queryViewLoadInfoProvider,
		NodeScheduler:              r.nodeScheduler,
	})
	if err != nil {
		return err
	}
	r.vchannelManager = manager
	return nil
}

func (r *recoveryStorageImpl) NotifyModuleUpdated(moduleapi.ModuleName) {
	r.mu.Lock()
	r.moduleDirty = true
	r.mu.Unlock()
	r.notifyPersist()
}

// Metrics gets the metrics of the wal.
func (r *recoveryStorageImpl) Metrics() RecoveryMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()

	return RecoveryMetrics{
		RecoveryTimeTick: r.checkpoint.TimeTick,
	}
}

func (r *recoveryStorageImpl) TransformLog() wal.TransformLogAccesser {
	if r.vchannelManager != nil {
		return r.vchannelManager
	}
	return wal.NewTransformLogErrorAccesser(wal.ErrTransformLogVChannelUnavailable)
}

func (r *recoveryStorageImpl) VChannelManager() *vchannel.PChannelRecoveryManager {
	return r.vchannelManager
}

// Close closes the recovery storage and wait the background task stop.
func (r *recoveryStorageImpl) Close() {
	r.backgroundTaskNotifier.Cancel()
	r.backgroundTaskNotifier.BlockUntilFinish()
	if r.broadcastAck != nil {
		r.broadcastAck.Close()
	}
	if r.taskScheduler != nil {
		r.taskScheduler.Close()
	}
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
func (r *recoveryStorageImpl) consumeDirtySnapshot() *dirtyPersistSnapshot {
	r.mu.Lock()
	if r.checkpoint == nil {
		r.installCheckpoint(nil)
	}
	var persistedCheckpoint *WALCheckpoint
	if r.persistedCheckpoint != nil {
		persistedCheckpoint = r.persistedCheckpoint.Clone()
	}
	frozenCheckpoint := r.checkpoint.Clone()
	completedPoint := r.ackTracker.CompletedPoint()
	metaPoint := utility.WALConsumeCheckpoint{
		MessageID: frozenCheckpoint.MessageID,
		TimeTick:  frozenCheckpoint.TimeTick,
	}
	if !consumePointReached(metaPoint, completedPoint) {
		completedPoint = utility.WALConsumeCheckpoint{
			MessageID: frozenCheckpoint.MessageID,
			TimeTick:  frozenCheckpoint.TimeTick,
		}
	}
	if persistedCheckpoint != nil && persistedCheckpoint.DataCheckpoint != nil &&
		!shouldAdvanceConsumePoint(*persistedCheckpoint.DataCheckpoint, completedPoint) {
		completedPoint = *persistedCheckpoint.DataCheckpoint.Clone()
	}
	frozenCheckpoint.DataCheckpoint = completedPoint.Clone()
	checkpointDirty := r.checkpointDirty || r.dirtyCounter > 0 ||
		persistedCheckpoint == nil ||
		!consumeCheckpointEqual(persistedCheckpoint.DataCheckpoint, frozenCheckpoint.DataCheckpoint)
	salvageCP := r.pendingSalvageCheckpoint
	r.pendingSalvageCheckpoint = nil
	r.dirtyCounter = 0
	r.moduleDirty = false
	r.checkpointDirty = false
	r.mu.Unlock()

	cleanup := moduleapi.CleanupContext{}
	if persistedCheckpoint != nil {
		cleanup.MetaPhysicalTimeTick = persistedCheckpoint.TimeTick
		if persistedCheckpoint.DataCheckpoint != nil {
			cleanup.DataPhysicalTimeTick = persistedCheckpoint.DataCheckpoint.TimeTick
		}
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
		Checkpoint:         frozenCheckpoint,
		CheckpointDirty:    checkpointDirty,
		SalvageCheckpoint:  salvageCP,
		ModuleDirtySnaps:   moduleSnapshots,
		ModuleSnapshotsAck: false,
	}
}

func consumeCheckpointEqual(left, right *utility.WALConsumeCheckpoint) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	if left.TimeTick != right.TimeTick {
		return false
	}
	if left.MessageID == nil || right.MessageID == nil {
		return left.MessageID == nil && right.MessageID == nil
	}
	return left.MessageID.EQ(right.MessageID)
}

func shouldAdvanceConsumePoint(current, next utility.WALConsumeCheckpoint) bool {
	if next.TimeTick != current.TimeTick {
		return next.TimeTick > current.TimeTick
	}
	if current.MessageID == nil {
		return next.MessageID != nil
	}
	return next.MessageID != nil && current.MessageID.LT(next.MessageID)
}

func consumePointReached(current, point utility.WALConsumeCheckpoint) bool {
	if current.TimeTick != point.TimeTick {
		return current.TimeTick > point.TimeTick
	}
	if point.MessageID == nil {
		return true
	}
	return current.MessageID != nil && point.MessageID.LTE(current.MessageID)
}

// observeMessage observes a message and update the recovery storage.
func (r *recoveryStorageImpl) observeMessage(ctx context.Context, msg message.ImmutableMessage) {
	owner := r.ackTracker.Track(msg)
	dispatch := owner.Clone()
	r.observeModulesMessage(ctx, dispatch)
	dispatch.Release()
	r.updateCheckpoint(msg)
	r.broadcastAck.Accept(owner)
	r.metrics.ObServeInMemMetrics(r.checkpoint.TimeTick)

	r.dirtyCounter++
	if r.dirtyCounter > r.cfg.maxDirtyMessages {
		r.notifyPersist()
	}
}

func (r *recoveryStorageImpl) observeMetaOnlyMessage(ctx context.Context, msg message.ImmutableMessage) {
	owner := message.NewOwnedImmutableMessage(msg, nil)
	dispatch := owner.Clone()
	r.observeModulesMessage(ctx, dispatch)
	dispatch.Release()
	owner.Release()
	r.updateCheckpoint(msg)
	r.metrics.ObServeInMemMetrics(r.checkpoint.TimeTick)

	r.dirtyCounter++
	if r.dirtyCounter > r.cfg.maxDirtyMessages {
		r.notifyPersist()
	}
}

func (r *recoveryStorageImpl) observeMetaScannerMessage(ctx context.Context, msg message.ImmutableMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observeMetaOnlyMessage(ctx, msg)
}

func (r *recoveryStorageImpl) observeDataScannerMessage(ctx context.Context, msg message.ImmutableMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observeMessage(ctx, msg)
}

func (r *recoveryStorageImpl) observeModulesMessage(
	ctx context.Context,
	retained message.RetainedImmutableMessage,
) {
	if r.vchannelManager == nil {
		panic("recovery modules are not initialized")
	}
	r.vchannelManager.ObserveMessage(ctx, retained)
}

func consumePointFromMessage(msg message.ImmutableMessage) utility.WALConsumeCheckpoint {
	return utility.WALConsumeCheckpoint{
		MessageID: msg.LastConfirmedMessageID(),
		TimeTick:  msg.TimeTick(),
	}
}

func (r *recoveryStorageImpl) startDataLiveScanner(recoveryStreamBuilder RecoveryStreamBuilder) {
	checkpoint := r.checkpoint.DataCheckpoint
	if checkpoint == nil || checkpoint.MessageID == nil {
		r.Logger().Warn(context.TODO(), "skip data scanner because wal data checkpoint is nil")
		return
	}
	rs := recoveryStreamBuilder.Build(BuildRecoveryStreamParam{
		StartCheckpoint:     checkpoint.MessageID,
		EndTimeTick:         0,
		UseWriteAheadBuffer: true,
	})
	go r.runDataLiveScanner(rs)
}

func (r *recoveryStorageImpl) runDataLiveScanner(rs RecoveryStream) {
	defer rs.Close()
	ctx := r.backgroundTaskNotifier.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-rs.Chan():
			if !ok {
				if err := rs.Error(); err != nil {
					r.Logger().Warn(context.TODO(), "wal recovery data scanner stopped with error", mlog.Err(err))
				}
				return
			}
			r.observeDataScannerMessage(ctx, msg)
		}
	}
}

// updateCheckpoint updates the checkpoint of the recovery storage.
func (r *recoveryStorageImpl) updateCheckpoint(msg message.ImmutableMessage) {
	if r.checkpoint == nil {
		r.installCheckpoint(nil)
	}
	point := consumePointFromMessage(msg)
	currentPoint := utility.WALConsumeCheckpoint{
		MessageID: r.checkpoint.MessageID,
		TimeTick:  r.checkpoint.TimeTick,
	}
	if !shouldAdvanceConsumePoint(currentPoint, point) {
		return
	}
	checkpointDirty := false
	if msg.MessageType() == message.MessageTypeAlterReplicateConfig {
		cfg := message.MustAsImmutableAlterReplicateConfigMessageV2(msg)
		header := cfg.Header()

		// Check ignore field - if true, skip updating ReplicateConfig and ReplicateCheckpoint
		// This is used for incomplete switchover messages that should be ignored after force promote
		if header.Ignore {
			r.Logger().Info(context.TODO(), "AlterReplicateConfig message has ignore flag set, skipping checkpoint update",
				mlog.Bool("forcePromote", header.ForcePromote))
		} else {
			r.checkpoint.ReplicateConfig = header.ReplicateConfiguration
			checkpointDirty = true
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
					checkpointDirty = true
				}
			}
		}
	}
	r.checkpoint.MessageID = point.MessageID
	r.checkpoint.TimeTick = point.TimeTick
	if r.alterWALInfo != nil && r.alterWALInfo.FoundAlterWALMsg && (r.checkpoint.AlterWalState == nil || r.checkpoint.AlterWalState.Stage == streamingpb.AlterWALStage_NONE) {
		r.checkpoint.AlterWalState = &streamingpb.AlterWALState{
			TargetWalName: r.alterWALInfo.TargetWALName,
			TimeTick:      r.alterWALInfo.AlterWALTs,
			Configs:       r.alterWALInfo.AlterWALConfig,
			Stage:         streamingpb.AlterWALStage_FLUSHING,
		}
		checkpointDirty = true
	}
	if msg.MessageType() == message.MessageTypeAlterWAL {
		alterWAL := message.MustAsImmutableAlterWALMessageV2(msg)
		header := alterWAL.Header()
		r.alterWALInfo = &AlterWALInfo{
			FoundAlterWALMsg: true,
			TargetWALName:    header.TargetWalName,
			AlterWALConfig:   header.Config,
			AlterWALTs:       msg.TimeTick(),
		}
		if r.checkpoint.AlterWalState == nil || r.checkpoint.AlterWalState.Stage == streamingpb.AlterWALStage_NONE {
			r.checkpoint.AlterWalState = &streamingpb.AlterWALState{
				TargetWalName: header.TargetWalName,
				TimeTick:      msg.TimeTick(),
				Configs:       header.Config,
				Stage:         streamingpb.AlterWALStage_FLUSHING,
			}
			checkpointDirty = true
		}
	}
	if checkpointDirty {
		r.checkpointDirty = true
	}

	// update the replicate checkpoint.
	replicateHeader := msg.ReplicateHeader()
	if replicateHeader == nil {
		return
	}
	if r.checkpoint.ReplicateCheckpoint == nil {
		r.Logger().Warn(context.TODO(), "replicate checkpoint is nil when incoming replicate message", mlog.FieldMessage(msg))
		return
	}
	if replicateHeader.ClusterID != r.checkpoint.ReplicateCheckpoint.ClusterID {
		r.Logger().Warn(context.TODO(), "replicate header cluster id mismatch",
			mlog.FieldMessage(msg),
			mlog.String("expected", r.checkpoint.ReplicateCheckpoint.ClusterID),
			mlog.String("actual", replicateHeader.ClusterID))
		return
	}
	r.checkpoint.ReplicateCheckpoint.MessageID = replicateHeader.LastConfirmedMessageID
	r.checkpoint.ReplicateCheckpoint.TimeTick = replicateHeader.TimeTick
	r.checkpointDirty = true
}

// GetDataCheckpoint returns the recovery-owned data checkpoint.
func (r *recoveryStorageImpl) GetDataCheckpoint(ctx context.Context) *WALCheckpoint {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.getDataCheckpointLocked()
}

func (r *recoveryStorageImpl) getDataCheckpointLocked() *WALCheckpoint {
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
	}
}
