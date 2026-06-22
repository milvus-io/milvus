package adaptor

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/viewresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/viewresource/idf"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	msgadaptor "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ wal.Opener = (*openerAdaptorImpl)(nil)

// adaptImplsToOpener creates a new wal opener with opener impls.
// Test Only
// Deprecated: Use NewOpenerAdaptor instead.
func adaptImplsToOpener(basicOpener walimpls.OpenerImpls, interceptorBuilders []interceptors.InterceptorBuilder) wal.Opener {
	o := &openerAdaptorImpl{
		lifetime:            typeutil.NewLifetime(),
		openerCache:         make(map[message.WALName]walimpls.OpenerImpls),
		idAllocator:         typeutil.NewIDAllocator(),
		walInstances:        typeutil.NewConcurrentMap[int64, wal.WAL](),
		interceptorBuilders: interceptorBuilders,
	}
	o.openerCache[message.WALNameTest] = basicOpener
	o.SetLogger(resource.Resource().Logger().With(mlog.FieldComponent("wal-opener")))
	return o
}

// NewOpenerAdaptor creates a new dynamic wal opener that can open different MQ types at runtime.
// It doesn't bind to a specific walName at construction time, instead it selects the appropriate
// wal implementation based on the walName in OpenOption when Open() is called.
func NewOpenerAdaptor(builders []interceptors.InterceptorBuilder) wal.Opener {
	o := &openerAdaptorImpl{
		lifetime:            typeutil.NewLifetime(),
		openerCache:         make(map[message.WALName]walimpls.OpenerImpls),
		idAllocator:         typeutil.NewIDAllocator(),
		walInstances:        typeutil.NewConcurrentMap[int64, wal.WAL](),
		interceptorBuilders: builders,
	}
	o.SetLogger(resource.Resource().Logger().With(mlog.FieldComponent("wal-opener")))
	return o
}

// openerAdaptorImpl is the wrapper that adapts walimpls.OpenerImpls to wal.Opener.
// It supports opening different WALImpls dynamically at runtime.
type openerAdaptorImpl struct {
	mlog.Binder

	lifetime            *typeutil.Lifetime
	mu                  sync.Mutex                               // protects openerCache
	openerCache         map[message.WALName]walimpls.OpenerImpls // cache of opened walimpls.OpenerImpls, dynamically created based on walName
	idAllocator         *typeutil.IDAllocator
	walInstances        *typeutil.ConcurrentMap[int64, wal.WAL] // store all wal instances allocated by these allocator.
	interceptorBuilders []interceptors.InterceptorBuilder
}

var (
	walSwitchFlushCheckInterval = time.Second
	walSwitchFlushTimeout       = time.Minute
)

// Open opens a wal instance for the channel.
func (o *openerAdaptorImpl) Open(ctx context.Context, opt *wal.OpenOption) (wal.WAL, error) {
	if !o.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("wal opener is on shutdown")
	}
	defer o.lifetime.Done()

	// Determine which walName to use
	walName, err := o.determineWALName(ctx, opt)
	if err != nil {
		return nil, err
	}

	logger := mlog.With().With(mlog.String("channel", opt.Channel.String()), mlog.Stringer("walName", walName))

	// Get or create the underlying walimpls.OpenerImpls for this walName
	openerImpl, err := o.getOrCreateOpenerImpl(ctx, walName)
	if err != nil {
		logger.Warn(context.TODO(), "get or create underlying wal impls opener failed", mlog.Err(err))
		return nil, err
	}

	// Open the underlying WAL implementation
	l, err := openerImpl.Open(ctx, &walimpls.OpenOption{
		Channel: opt.Channel,
	})
	if err != nil {
		logger.Warn(context.TODO(), "open wal impls failed", mlog.Err(err))
		return nil, err
	}

	var wal wal.WAL
	switch opt.Channel.AccessMode {
	case types.AccessModeRW:
		wal, err = o.openRWWAL(ctx, l, opt)
	case types.AccessModeRO:
		wal, err = o.openROWAL(l)
	default:
		panic("unknown access mode")
	}
	if err != nil {
		logger.Warn(context.TODO(), "open wal failed", mlog.Err(err))
		return nil, err
	}
	logger.Info(context.TODO(), "open wal done", mlog.Stringer("walName", walName), mlog.String("pchannel", opt.Channel.Name))
	return wal, nil
}

// determineWALName determines which walName to use for the given channel.
func (o *openerAdaptorImpl) determineWALName(ctx context.Context, opt *wal.OpenOption) (message.WALName, error) {
	walName := message.WALNameUnknown
	catalog := resource.Resource().StreamingNodeCatalog()
	cpProto, err := catalog.GetConsumeCheckpoint(ctx, opt.Channel.Name)
	if err != nil {
		return message.WALNameUnknown, errors.Wrap(err, "failed to get checkpoint from catalog")
	}
	if cpProto != nil {
		checkpoint := utility.NewWALCheckpointFromProto(cpProto)
		mlog.With().Info(context.TODO(), "get checkpoint from catalog",
			mlog.String("channel", opt.Channel.Name),
			mlog.Stringer("checkpoint", checkpoint.MessageID),
			mlog.Uint64("checkpointTimeTick", checkpoint.TimeTick),
			mlog.Stringer("currentWAL", checkpoint.MessageID.WALName()),
			mlog.Any("AlterWalState", checkpoint.AlterWalState))
		walName = checkpoint.MessageID.WALName()
	}

	if walName == message.WALNameUnknown {
		// Use default WAL if already register
		walName = message.GetDefaultWALName()
	}

	if walName == message.WALNameUnknown {
		// Use wal selector to choose one
		walName = util.MustSelectWALName()
	}
	return walName, nil
}

// getOrCreateOpenerImpl gets an existing opener from cache or creates a new one.
func (o *openerAdaptorImpl) getOrCreateOpenerImpl(ctx context.Context, walName message.WALName) (walimpls.OpenerImpls, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if opener, ok := o.openerCache[walName]; ok {
		return opener, nil
	}

	// Double-check after acquiring write lock
	if opener, ok := o.openerCache[walName]; ok {
		return opener, nil
	}

	// Build and cache new opener
	builderImpl := registry.MustGetBuilder(walName)
	opener, err := builderImpl.Build()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to build walimpls opener for %s", walName)
	}

	o.openerCache[walName] = opener
	mlog.With().Info(context.TODO(), "created and cached new walimpls opener", mlog.Stringer("walName", walName))
	return opener, nil
}

// openRWWAL opens a read write wal instance for the channel.
func (o *openerAdaptorImpl) openRWWAL(ctx context.Context, l walimpls.WALImpls, opt *wal.OpenOption) (wal.WAL, error) {
	id := o.idAllocator.Allocate()
	roWAL := adaptImplsToROWAL(l, func() {
		o.walInstances.Remove(id)
	})
	resources := &walOpenResources{roWAL: roWAL}
	defer resources.Close()

	cpProto, err := resource.Resource().StreamingNodeCatalog().GetConsumeCheckpoint(ctx, opt.Channel.Name)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get checkpoint from catalog")
	}
	cp := utility.NewWALCheckpointFromProto(cpProto)

	// recover the wal state.
	param, err := buildInterceptorParams(ctx, l, cp)
	if err != nil {
		return nil, errors.Wrap(err, "when building interceptor params")
	}
	resources.param = param
	queryViewCatalog := resource.Resource().StreamingNodeCatalog()
	persistedViews, err := queryViewCatalog.ListQueryViews(ctx, opt.Channel.Name)
	if err != nil {
		return nil, errors.Wrap(err, "when loading streaming node query view meta")
	}
	resourceBaseByVChannel := snview.OldestUpDataVersions(persistedViews)
	recoveredLoadConfigs := snview.RecoveredLoadConfigs(persistedViews)
	resMgr := viewresource.NewManager(
		viewresource.NewGrowingRuntimeModuleBuilder(nil),
		idf.NewFutureProvider(
			resource.Resource().MixCoordClient(),
			idf.WithChunkManager(resource.Resource().ChunkManager()),
		),
	)
	rs, snapshot, err := recovery.RecoverRecoveryStorage(
		ctx,
		newRecoveryStreamBuilder(roWAL),
		cp,
		param.LastTimeTickMessage,
		recovery.WithLoadConfigListener(resMgr),
		recovery.WithResourceRecoveryBaseSelector(func(vchannel string) (qviews.DataVersion, bool) {
			version, ok := resourceBaseByVChannel[vchannel]
			return version, ok
		}),
		recovery.WithRecoveredLoadConfigProvider(func(vchannel string) *streamingpb.VChannelLoadConfig {
			return recoveredLoadConfigs[vchannel]
		}),
	)
	if err != nil {
		resMgr.Close()
		return nil, errors.Wrap(err, "when recovering recovery storage")
	}
	param.RecoveryStorage = rs
	snHandler := snview.RecoverPChannelSNQueryViewHandler(opt.Channel.Name, queryViewCatalog, resMgr, persistedViews)
	unregisterQueryViewHandler := resource.Resource().QueryViewRouter().Register(opt.Channel.Name, snHandler)

	// Handle alter WAL if found in snapshot
	// This flushes all remaining data and triggers WAL switch to the target implementation
	if snapshot.AlterWALInfo != nil && snapshot.AlterWALInfo.FoundAlterWALMsg {
		unregisterQueryViewHandler()
		rs.DetachLoadConfigListener()
		snHandler.CloseForHandoff()
		resMgr.Close()
		return o.handleAlterWAL(ctx, opt, roWAL, rs, resources, snapshot)
	}

	param.LastConfirmedMessageID = determineLastConfirmedMessageID(param.LastTimeTickMessage.MessageID(), snapshot.TxnBuffer)
	param.InitialRecoverSnapshot = snapshot
	param.TxnManager = txn.NewTxnManager(param.ChannelInfo, snapshot.TxnBuffer.GetUncommittedMessageBuilder())
	param.ShardManager = shards.RecoverShardManager(&shards.ShardManagerRecoverParam{
		ChannelInfo:            param.ChannelInfo,
		WAL:                    param.WAL,
		InitialRecoverSnapshot: snapshot,
		TxnManager:             param.TxnManager,
	})
	// Load salvage checkpoints from etcd (one per source cluster that was force-promoted from).
	var salvageCheckpoints []*utility.ReplicateCheckpoint
	if salvageCPProtos, err := resource.Resource().StreamingNodeCatalog().GetSalvageCheckpoint(ctx, param.ChannelInfo.Name); err != nil {
		mlog.With().Info(context.TODO(), "failed to load salvage checkpoints", mlog.Err(err))
	} else {
		for _, proto := range salvageCPProtos {
			salvageCheckpoints = append(salvageCheckpoints, utility.NewReplicateCheckpointFromProto(proto))
		}
	}

	if param.ReplicateManager, err = replicates.RecoverReplicateManager(
		&replicates.ReplicateManagerRecoverParam{
			ChannelInfo:            param.ChannelInfo,
			CurrentClusterID:       paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
			InitialRecoverSnapshot: snapshot,
			SalvageCheckpoints:     salvageCheckpoints,
		},
	); err != nil {
		unregisterQueryViewHandler()
		rs.DetachLoadConfigListener()
		snHandler.CloseForHandoff()
		resMgr.Close()
		return nil, err
	}

	wal := adaptImplsToRWWAL(roWAL, o.interceptorBuilders, param)
	wal.queryViewHandler = snHandler
	wal.viewResourceManager = resMgr
	wal.unregisterQueryViewHandler = unregisterQueryViewHandler
	o.walInstances.Insert(id, wal)
	resources.Release()
	return wal, nil
}

// determineLastConfirmedMessageID determines the last confirmed message id after recovery.
// The last confirmed message id is the minimum last confirmed message id of all uncommitted txn messages.
func determineLastConfirmedMessageID(lastTimeTickMessageID message.MessageID, txnBuffer *utility.TxnBuffer) message.MessageID {
	// From here, we can read all messages which timetick is greater than timetick of LastTimeTickMessage sent at these term.
	lastConfirmedMessageID := lastTimeTickMessageID
	for _, builder := range txnBuffer.GetUncommittedMessageBuilder() {
		if builder.LastConfirmedMessageID().LT(lastConfirmedMessageID) {
			// use the minimum last confirmed message id of all uncommitted txn messages to protect the `LastConfirmedMessageID` promise.
			lastConfirmedMessageID = builder.LastConfirmedMessageID()
		}
	}
	return lastConfirmedMessageID
}

// handleAlterWAL handles WAL switch operation in two stages:
// Stage 1 (FLUSHING): Flush all growing segments and wait for completion
// Stage 2 (ADVANCE_CHECKPOINT): Update vchannel checkpoints and pchannel consume checkpoint
// Returns an error to trigger WAL re-opening after successful switch
func (o *openerAdaptorImpl) handleAlterWAL(ctx context.Context, opt *wal.OpenOption,
	roWAL *roWALAdaptorImpl, rs recovery.RecoveryStorage,
	resources *walOpenResources, snapshot *recovery.RecoverySnapshot,
) (wal.WAL, error) {
	mlog.With().Info(context.TODO(), "detected alter WAL message in snapshot",
		mlog.String("channel", opt.Channel.String()),
		mlog.Bool("foundAlterWAL", snapshot.AlterWALInfo.FoundAlterWALMsg),
		mlog.Stringer("targetWAL", snapshot.AlterWALInfo.TargetWALName),
		mlog.String("checkpointMessageID", snapshot.Checkpoint.MessageID.String()),
		mlog.Uint64("checkpointTimeTick", snapshot.Checkpoint.TimeTick),
		mlog.Any("alterWALConfig", snapshot.AlterWALInfo.AlterWALConfig))

	if snapshot.Checkpoint.AlterWalState != nil && snapshot.Checkpoint.AlterWalState.Stage == streamingpb.AlterWALStage_FLUSHING {
		flushingErr := o.handleAlterWALFlushingStage(ctx, opt, roWAL, rs, resources, snapshot)
		if flushingErr != nil {
			return nil, errors.Wrap(flushingErr, "failed to handle alter WAL flushing stage")
		}
	}

	if snapshot.Checkpoint.AlterWalState != nil && snapshot.Checkpoint.AlterWalState.Stage == streamingpb.AlterWALStage_ADVANCE_CHECKPOINT {
		advanceCheckpointsErr := o.handleAlterWALAdvanceCheckpointsStage(ctx, opt, snapshot)
		if advanceCheckpointsErr != nil {
			return nil, errors.Wrap(advanceCheckpointsErr, "failed to handle alter WAL advance checkpoints stage")
		}
	}

	targetWALName := snapshot.AlterWALInfo.TargetWALName
	return nil, status.NewInner("WAL switch success: %s switch to %s finish, re-opening required", opt.Channel.Name, targetWALName)
}

func (o *openerAdaptorImpl) handleAlterWALFlushingStage(ctx context.Context, opt *wal.OpenOption, roWAL *roWALAdaptorImpl,
	rs recovery.RecoveryStorage,
	resources *walOpenResources, snapshot *recovery.RecoverySnapshot,
) error {
	// Wait for all data up to target time tick to be flushed
	targetTimeTick := snapshot.AlterWALInfo.AlterWALTs
	targetWALName := snapshot.AlterWALInfo.TargetWALName
	mlog.With().Info(context.TODO(), "waiting for recovery data checkpoint before WAL switch",
		mlog.String("channel", opt.Channel.Name),
		mlog.Uint64("targetTimeTick", targetTimeTick))

	ticker := time.NewTicker(walSwitchFlushCheckInterval)
	defer ticker.Stop()
	timeout := time.NewTimer(walSwitchFlushTimeout)
	defer timeout.Stop()

	// Periodically check recovery-owned data progress until target time tick is reached.
	var dataCP *utility.WALCheckpoint
	for dataCP == nil || dataCP.TimeTick < targetTimeTick {
		select {
		case <-ticker.C:
			dataCP = rs.GetDataCheckpoint(ctx)
			if dataCP == nil {
				mlog.With().Info(context.TODO(), "waiting for recovery data checkpoint initialization")
				continue
			}
			if dataCP.TimeTick >= targetTimeTick {
				mlog.With().Info(context.TODO(), "recovery data checkpoint reached, ready for WAL switch",
					mlog.String("channel", opt.Channel.Name),
					mlog.Uint64("dataCheckpointTS", dataCP.TimeTick),
					mlog.Uint64("targetTimeTick", targetTimeTick),
					mlog.Stringer("targetWAL", targetWALName))
				break
			}
			remaining := targetTimeTick - dataCP.TimeTick
			mlog.With().Info(context.TODO(), "recovery data checkpoint in progress",
				mlog.String("channel", opt.Channel.Name),
				mlog.Uint64("currentTS", dataCP.TimeTick),
				mlog.Uint64("targetTS", targetTimeTick),
				mlog.Uint64("remainingTS", remaining))
		case <-timeout.C:
			mlog.With().Warn(context.TODO(), "timeout waiting for flush completion",
				mlog.String("channel", opt.Channel.Name),
				mlog.Duration("timeout", walSwitchFlushTimeout))
			return status.NewInner("timeout waiting for flush completion during WAL switch")
		case <-ctx.Done():
			mlog.Warn(ctx, "context canceled while waiting for flush completion",
				mlog.String("channel", opt.Channel.Name),
				mlog.Err(ctx.Err()))
			return errors.Wrap(ctx.Err(), "context canceled during WAL switch flush waiting")
		}
	}

	// Close recovery storage and related resources to persist final state
	mlog.Info(ctx, "closing recovery storage to persist WAL switch snapshot")
	resources.Close()

	// Update checkpoint stage to ADVANCE_CHECKPOINT and persist to catalog
	snapshot.Checkpoint.DataCheckpoint = &utility.WALConsumeCheckpoint{
		MessageID: dataCP.MessageID,
		TimeTick:  dataCP.TimeTick,
	}
	snapshot.Checkpoint.AlterWalState.Stage = streamingpb.AlterWALStage_ADVANCE_CHECKPOINT

	catalog := resource.Resource().StreamingNodeCatalog()
	if err := catalog.SaveConsumeCheckpoint(ctx, opt.Channel.Name, snapshot.Checkpoint.IntoProto()); err != nil {
		mlog.Warn(ctx, "failed to persist checkpoint after flushing stage",
			mlog.String("channel", opt.Channel.Name),
			mlog.Err(err))
		return errors.Wrap(err, "failed to persist checkpoint after flushing stage")
	}

	mlog.With().Info(context.TODO(), "checkpoint stage updated to ADVANCE_CHECKPOINT",
		mlog.String("channel", opt.Channel.Name),
		mlog.String("checkpoint", snapshot.Checkpoint.MessageID.String()),
		mlog.Uint64("checkpointTS", snapshot.Checkpoint.TimeTick))
	return nil
}

func (o *openerAdaptorImpl) handleAlterWALAdvanceCheckpointsStage(ctx context.Context, opt *wal.OpenOption, snapshot *recovery.RecoverySnapshot) error {
	// Update all vchannel checkpoints to new WAL initial position, then update pchannel checkpoint
	catalog := resource.Resource().StreamingNodeCatalog()
	vchannels, err := catalog.ListVChannel(ctx, opt.Channel.Name)
	if err != nil {
		return errors.Wrap(err, "failed to list vchannels")
	}

	// Build new WAL initial position
	newWALInitialTimeTick := snapshot.Checkpoint.TimeTick
	newWALInitialMsgID, newWALName := msgadaptor.MustGetEarliestMessageIDFromMQType(snapshot.Checkpoint.AlterWalState.TargetWalName)

	if len(vchannels) > 0 {
		// Get MixCoordClient to update vchannel checkpoints
		mixCoordClient, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to get mix coord client")
		}

		// Build checkpoint positions for all vchannels
		channelCheckpoints := make([]*msgpb.MsgPosition, 0, len(vchannels))
		for _, vchannel := range vchannels {
			msgIDBytes := newWALInitialMsgID.Serialize()
			vChannelName := vchannel.Vchannel
			pos := &msgpb.MsgPosition{
				ChannelName: vChannelName,
				MsgID:       msgIDBytes,
				Timestamp:   newWALInitialTimeTick,
				WALName:     newWALName,
			}
			channelCheckpoints = append(channelCheckpoints, pos)
		}

		// Batch update all vchannel checkpoints to DataCoord
		req := &datapb.UpdateChannelCheckpointRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			ChannelCheckpoints: channelCheckpoints,
		}

		resp, err := mixCoordClient.UpdateChannelCheckpoint(ctx, req)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			mlog.With().Warn(context.TODO(), "failed to update vchannel checkpoints",
				mlog.String("channel", opt.Channel.Name),
				mlog.Int("vchannelCount", len(channelCheckpoints)),
				mlog.Err(err))
			return errors.Wrap(err, "failed to update vchannel checkpoints")
		}

		mlog.With().Info(context.TODO(), "vchannel checkpoints updated to new WAL initial position",
			mlog.String("channel", opt.Channel.Name),
			mlog.Int("vchannelCount", len(channelCheckpoints)),
			mlog.Uint64("newWALInitialTS", newWALInitialTimeTick))

		// Verify checkpoint updates
		for _, vchannel := range vchannels {
			resp2, err2 := mixCoordClient.GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{Vchannel: vchannel.Vchannel})
			if err2 != nil {
				mlog.Warn(ctx, "failed to verify vchannel checkpoint update",
					mlog.String("vchannel", vchannel.Vchannel),
					mlog.Err(err2))
				return errors.Wrap(err2, "failed to verify vchannel checkpoint update")
			}
			mlog.With().Info(context.TODO(), "verified vchannel checkpoint update",
				mlog.String("vchannel", vchannel.Vchannel),
				mlog.Binary("seekPositionMsgID", resp2.Info.SeekPosition.MsgID))
		}
	} else {
		mlog.Info(ctx, "no vchannels found, skipping vchannel checkpoint update",
			mlog.String("channel", opt.Channel.Name))
	}

	// Update pchannel checkpoint: reset alterWALState and set position to new WAL initial position
	finalCheckpoint := snapshot.Checkpoint.Clone()
	finalCheckpoint.AlterWalState = nil
	// Only the local checkpoint moves to the new backend. The replicate checkpoint
	// holds the position this cluster has reached in the SOURCE cluster's WAL, so a
	// local backend migration must leave it alone: the two message IDs belong to
	// different clusters and therefore to different ID spaces.
	finalCheckpoint.MessageID = msgadaptor.MustGetMessageIDFromMQWrapperID(newWALInitialMsgID)
	// Keep the replicate checkpoint untouched: it holds the position this
	// cluster has reached in the SOURCE cluster's WAL, and the two message IDs
	// belong to different ID spaces.
	finalCheckpoint.DataCheckpoint = &utility.WALConsumeCheckpoint{
		MessageID: finalCheckpoint.MessageID,
		TimeTick:  finalCheckpoint.TimeTick,
	}

	// Persist final checkpoint to catalog
	if err := catalog.SaveConsumeCheckpoint(ctx, opt.Channel.Name, finalCheckpoint.IntoProto()); err != nil {
		mlog.Warn(ctx, "failed to persist checkpoint after advance checkpoint stage",
			mlog.String("channel", opt.Channel.Name),
			mlog.Err(err))
		return errors.Wrap(err, "failed to persist checkpoint after advance checkpoint stage")
	}

	// Register default WAL name for delegator to track seek position changes
	message.RegisterDefaultWALName(finalCheckpoint.MessageID.WALName())

	mlog.With().Info(context.TODO(), "pchannel checkpoint updated to new WAL initial position",
		mlog.String("channel", opt.Channel.Name),
		mlog.String("newCheckpoint", finalCheckpoint.MessageID.String()),
		mlog.String("newWAL", finalCheckpoint.MessageID.WALName().String()),
		mlog.Uint64("newCheckpointTS", finalCheckpoint.TimeTick))

	return nil
}

// openROWAL opens a read only wal instance for the channel.
func (o *openerAdaptorImpl) openROWAL(l walimpls.WALImpls) (wal.WAL, error) {
	id := o.idAllocator.Allocate()
	wal := adaptImplsToROWAL(l, func() {
		o.walInstances.Remove(id)
	})
	o.walInstances.Insert(id, wal)
	return wal, nil
}

// Close the wal opener, release the underlying resources.
func (o *openerAdaptorImpl) Close() {
	o.lifetime.SetState(typeutil.LifetimeStateStopped)
	o.lifetime.Wait()

	o.Logger().Info(context.TODO(), "wal opener closing...")

	// close all wal instances.
	o.walInstances.Range(func(id int64, l wal.WAL) bool {
		l.Close()
		o.Logger().Info(context.TODO(), "close wal by opener", mlog.Int64("id", id), mlog.String("channel", l.Channel().String()))
		return true
	})

	// close all cached opener impls
	o.mu.Lock()
	defer o.mu.Unlock()
	for walName, opener := range o.openerCache {
		o.Logger().Info(context.TODO(), "closing underlying walimpls opener", mlog.Stringer("walName", walName))
		opener.Close()
	}
	o.openerCache = nil

	o.Logger().Info(context.TODO(), "wal opener closed")
}
