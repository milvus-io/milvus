package recovery

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/segment"
	waltransformlog "github.com/milvus-io/milvus/internal/streamingnode/server/wal/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/transformlog"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
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
	if cp == nil {
		cp = initialCheckpointFromLastTimeTickMessage(lastTimeTickMessage)
	}
	rs := newRecoveryStorage(recoveryStreamBuilder.Channel(), cp)
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
func newRecoveryStorage(channel types.PChannelInfo, cp *utility.WALCheckpoint) *recoveryStorageImpl {
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
	rs.taskScheduler = scheduler.New(context.Background())
	if cp != nil {
		rs.installCheckpointManager(cp)
	}
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
	checkpointManager      *walcheckpoint.Manager
	metaObservedCheckpoint utility.WALConsumeCheckpoint
	vchannelModule         *vchannel.Module
	segmentModule          *segment.Module
	transformLogModule     *waltransformlog.Module
	modules                []moduleapi.Module
	taskScheduler          *scheduler.Scheduler
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
	pendingSalvageCheckpoint *utility.ReplicateCheckpoint
}

func (r *recoveryStorageImpl) installCheckpointManager(checkpoint *WALCheckpoint) {
	r.checkpointManager = walcheckpoint.NewManager(checkpoint)
	r.checkpoint = r.checkpointManager.Checkpoint()
	r.metaObservedCheckpoint = utility.WALConsumeCheckpoint{
		MessageID: r.checkpoint.MessageID,
		TimeTick:  r.checkpoint.TimeTick,
	}
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
	r.vchannelModule = vchannel.NewModule(
		r.channel.Name,
		vchannels,
		vchannel.WithModuleRuntime(r.Logger(), moduleRuntime),
	)
	r.segmentModule = segment.NewModule(
		r.channel.Name,
		segments,
		r.vchannelModule,
		segment.NewSegmentLifecycleWriter(coord, paramtable.GetNodeID()),
		segment.WithPackWriter(segment.NewBulkPackWriter(
			resource.Resource().ChunkManager(),
			idalloc.NewMAllocator(resource.Resource().IDAllocator()),
			nil,
		)),
		segment.WithModuleRuntime(r.Logger(), moduleRuntime),
	)
	transformLogStore := waltransformlog.NewObjectChunkStore(
		resource.Resource().ChunkManager(),
		r.channel.Name,
	)
	r.transformLogModule = waltransformlog.NewModule(
		r.channel.Name,
		transformLogMetas,
		transformLogStore,
		waltransformlog.WithModuleRuntime(moduleRuntime),
	)
	if err := r.transformLogModule.Recover(ctx); err != nil {
		return err
	}
	frontierProvider := newDataFrontierProvider(r.segmentModule, r.transformLogModule)
	r.modules = []moduleapi.Module{
		r.vchannelModule,
		r.segmentModule,
		r.transformLogModule,
		newBroadcastAckModule(r.channel.Name, frontierProvider, moduleRuntime),
	}
	return nil
}

func (r *recoveryStorageImpl) NotifyBarrierUpdated() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.moduleDirty = true
	if r.checkpointManager == nil {
		return
	}
	r.checkpointManager.TryAdvanceMetaCheckpoint()
	r.checkpointManager.TryAdvanceDataCheckpoint()
	r.notifyPersist()
	if r.taskScheduler != nil {
		r.taskScheduler.Notify()
	}
}

func (r *recoveryStorageImpl) NotifyModuleUpdated(moduleapi.ModuleName) {
	r.mu.Lock()
	r.moduleDirty = true
	r.mu.Unlock()
	r.notifyPersist()
	if r.taskScheduler != nil {
		r.taskScheduler.Notify()
	}
}

// Metrics gets the metrics of the wal.
func (r *recoveryStorageImpl) Metrics() RecoveryMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()

	return RecoveryMetrics{
		RecoveryTimeTick: r.checkpoint.TimeTick,
	}
}

func (r *recoveryStorageImpl) TransformLog() transformlog.Accesser {
	if r.transformLogModule != nil {
		return r.transformLogModule
	}
	return transformlog.NewErrorAccesser(transformlog.ErrVChannelUnavailable)
}

// Close closes the recovery storage and wait the background task stop.
func (r *recoveryStorageImpl) Close() {
	r.backgroundTaskNotifier.Cancel()
	r.backgroundTaskNotifier.BlockUntilFinish()
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
	if r.checkpointManager == nil {
		r.installCheckpointManager(r.checkpoint)
	}
	r.mu.Unlock()

	moduleSnapshots := make([]moduleapi.DirtySnapshot, 0)
	for _, module := range r.modules {
		moduleSnapshots = append(moduleSnapshots, module.ConsumeDirtySnapshots()...)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	checkpointDirty := r.checkpointManager != nil && r.checkpointManager.HasDirty()
	if r.dirtyCounter == 0 && !r.moduleDirty && r.pendingSalvageCheckpoint == nil && !checkpointDirty && len(moduleSnapshots) == 0 {
		return nil
	}
	// Atomically capture the salvage checkpoint alongside other dirty state.
	// Clearing it here (under r.mu) ensures it is only consumed once.
	salvageCP := r.pendingSalvageCheckpoint
	r.pendingSalvageCheckpoint = nil
	// clear the dirty counter.
	r.dirtyCounter = 0
	r.moduleDirty = false
	checkpointDirty = r.checkpointManager.ConsumeDirty() || salvageCP != nil
	if !checkpointDirty && salvageCP == nil && len(moduleSnapshots) == 0 {
		return nil
	}
	return &dirtyPersistSnapshot{
		Checkpoint:         r.checkpointManager.Snapshot(),
		CheckpointDirty:    checkpointDirty,
		SalvageCheckpoint:  salvageCP,
		ModuleDirtySnaps:   moduleSnapshots,
		ModuleSnapshotsAck: false,
	}
}

// observeMessage observes a message and update the recovery storage.
func (r *recoveryStorageImpl) observeMessage(ctx context.Context, msg message.ImmutableMessage) {
	result := r.observeModulesMessage(ctx, msg)
	r.updateCheckpoint(msg, result.Meta)
	r.updateDataCheckpoint(msg, result.Data)
	r.metrics.ObServeInMemMetrics(r.checkpoint.TimeTick)

	r.dirtyCounter++
	if r.dirtyCounter > r.cfg.maxDirtyMessages {
		r.notifyPersist()
	}
}

func (r *recoveryStorageImpl) observeMetaOnlyMessage(ctx context.Context, msg message.ImmutableMessage) {
	result := r.observeModulesMessage(ctx, msg)
	r.updateCheckpoint(msg, result.Meta)
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

func (r *recoveryStorageImpl) observeModulesMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	if len(r.modules) == 0 {
		panic("recovery modules are not initialized")
	}
	results := make([]moduleapi.ObserveResult, 0, len(r.modules))
	for _, module := range r.modules {
		result := module.ObserveMessage(ctx, msg)
		results = append(results, result)
	}
	return moduleapi.ComposeBarriers(results)
}

func (r *recoveryStorageImpl) updateDataCheckpoint(msg message.ImmutableMessage, barrier walcheckpoint.Barrier) {
	point := utility.WALConsumeCheckpoint{
		MessageID: msg.LastConfirmedMessageID(),
		TimeTick:  msg.TimeTick(),
	}
	r.checkpointManager.AddDataBarrier(point, barrier)
}

func (r *recoveryStorageImpl) startDataLiveScanner(recoveryStreamBuilder RecoveryStreamBuilder) {
	checkpoint := r.checkpoint.DataCheckpoint
	if checkpoint == nil || checkpoint.MessageID == nil {
		r.Logger().Warn(context.TODO(), "skip data scanner because wal data checkpoint is nil")
		return
	}
	rs := recoveryStreamBuilder.Build(BuildRecoveryStreamParam{
		StartCheckpoint: checkpoint.MessageID,
		EndTimeTick:     0,
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
func (r *recoveryStorageImpl) updateCheckpoint(msg message.ImmutableMessage, metaBarriers ...walcheckpoint.Barrier) {
	if r.checkpointManager == nil {
		r.installCheckpointManager(r.checkpoint)
	}
	var metaBarrier walcheckpoint.Barrier
	if len(metaBarriers) > 0 {
		metaBarrier = metaBarriers[0]
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
	point := utility.WALConsumeCheckpoint{
		MessageID: msg.LastConfirmedMessageID(),
		TimeTick:  msg.TimeTick(),
	}
	r.advanceMetaObservedCheckpoint(point)
	r.checkpointManager.AddMetaBarrier(point, metaBarrier)
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
		r.checkpointManager.MarkDirty()
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
	r.checkpointManager.MarkDirty()
}

func (r *recoveryStorageImpl) advanceMetaObservedCheckpoint(point utility.WALConsumeCheckpoint) {
	if !walcheckpoint.ShouldAdvance(r.metaObservedCheckpoint.MessageID, r.metaObservedCheckpoint.TimeTick, point) {
		return
	}
	r.metaObservedCheckpoint.MessageID = point.MessageID
	r.metaObservedCheckpoint.TimeTick = point.TimeTick
}

// GetDataCheckpoint returns the recovery-owned data checkpoint.
func (r *recoveryStorageImpl) GetDataCheckpoint(ctx context.Context) *WALCheckpoint {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.getDataCheckpointLocked()
}

func (r *recoveryStorageImpl) getDataCheckpointLocked() *WALCheckpoint {
	if r.checkpoint.DataCheckpoint == nil || r.checkpoint.DataCheckpoint.MessageID == nil {
		return nil
	}
	return &WALCheckpoint{
		MessageID: r.checkpoint.DataCheckpoint.MessageID,
		TimeTick:  r.checkpoint.DataCheckpoint.TimeTick,
	}
}
