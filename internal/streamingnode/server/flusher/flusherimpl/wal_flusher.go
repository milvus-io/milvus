package flusherimpl

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/adaptor/rate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

var errChannelLifetimeUnrecoverable = errors.New("channel lifetime unrecoverable")

// RecoverWALFlusherParam is the parameter for building wal flusher.
type RecoverWALFlusherParam struct {
	ChannelInfo        types.PChannelInfo
	WAL                *syncutil.Future[wal.WAL]
	RecoverySnapshot   *recovery.RecoverySnapshot
	RecoveryStorage    recovery.RecoveryStorage
	RateLimitComponent *rate.WALRateLimitComponent
}

// RecoverWALFlusher recovers the wal flusher.
func RecoverWALFlusher(param *RecoverWALFlusherParam) *WALFlusherImpl {
	flusher := &WALFlusherImpl{
		notifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		wal:      param.WAL,
		logger: resource.Resource().Logger().With(
			mlog.FieldComponent("flusher"),
			mlog.FieldPChannel(param.ChannelInfo.String())),
		metrics:              newFlusherMetrics(param.ChannelInfo),
		emptyTimeTickCounter: metrics.WALFlusherEmptyTimeTickFilteredTotal.WithLabelValues(paramtable.GetStringNodeID(), param.ChannelInfo.Name),
		rateLimitComponent:   param.RateLimitComponent,
		RecoveryStorage:      param.RecoveryStorage,
	}
	go flusher.Execute(param.RecoverySnapshot)
	return flusher
}

type WALFlusherImpl struct {
	notifier             *syncutil.AsyncTaskNotifier[struct{}]
	wal                  *syncutil.Future[wal.WAL]
	flusherComponents    *flusherComponents
	logger               *mlog.Logger
	metrics              *flusherMetrics
	lastDispatchTimeTick uint64 // The last time tick that the message is dispatched.
	emptyTimeTickCounter prometheus.Counter
	rateLimitComponent   *rate.WALRateLimitComponent
	recovery.RecoveryStorage
}

// Execute starts the wal flusher.
func (impl *WALFlusherImpl) Execute(recoverSnapshot *recovery.RecoverySnapshot) (err error) {
	defer func() {
		impl.notifier.Finish(struct{}{})
		if err == nil {
			impl.logger.Info(context.TODO(), "wal flusher stop")
			return
		}
		if !errors.Is(err, context.Canceled) {
			impl.logger.DPanic(context.TODO(), "wal flusher stop to executing with unexpected error", mlog.Err(err))
			return
		}
		impl.logger.Warn(context.TODO(), "wal flusher is canceled before executing", mlog.Err(err))
	}()

	// because current flusher is build asynchronously,
	// so we need to enter slowdown mode to protect the wal from being overloaded before the recovery-storage scanner is started.
	// recovery-storage scanner will protect the wal from being overloaded after the recovery-storage is started.
	impl.rateLimitComponent.FlusherRecovering.EnterSlowdownMode(nil)

	impl.logger.Info(context.TODO(), "wal flusher start to recovery...")
	l, err := impl.wal.GetWithContext(impl.notifier.Context())
	if err != nil {
		return errors.Wrap(err, "when get wal from future")
	}
	impl.logger.Info(context.TODO(), "wal ready for flusher recovery")

	var checkpoint message.MessageID
	impl.flusherComponents, checkpoint, err = impl.buildFlusherComponents(impl.notifier.Context(), l, recoverSnapshot)
	if err != nil {
		return errors.Wrap(err, "when build flusher components")
	}
	defer impl.flusherComponents.Close()

	scanner, err := impl.generateScanner(impl.notifier.Context(), impl.wal.Get(), checkpoint)
	if err != nil {
		return errors.Wrap(err, "when generate scanner")
	}
	defer scanner.Close()

	impl.logger.Info(context.TODO(), "wal flusher start to work")
	impl.metrics.IntoState(flusherStateInWorking)
	defer impl.metrics.IntoState(flusherStateOnClosing)
	impl.rateLimitComponent.FlusherRecovering.EnterRecoveryMode()

	for {
		select {
		case <-impl.notifier.Context().Done():
			return nil
		case msg, ok := <-scanner.Chan():
			if !ok {
				impl.logger.Warn(context.TODO(), "wal flusher is closing for closed scanner channel, which is unexpected at graceful way")
				return nil
			}
			impl.metrics.ObserveMetrics(msg.TimeTick())
			if err := impl.dispatch(msg); err != nil {
				// The error is always context canceled.
				return nil
			}
		}
	}
}

// Close closes the wal flusher and release all related resources for it.
func (impl *WALFlusherImpl) Close() {
	impl.notifier.Cancel()
	impl.notifier.BlockUntilFinish()

	impl.logger.Info(context.TODO(), "wal flusher start to close the recovery storage...")
	impl.RecoveryStorage.Close()
	impl.logger.Info(context.TODO(), "recovery storage closed")

	impl.metrics.Close()
}

// buildFlusherComponents builds the components of the flusher.
func (impl *WALFlusherImpl) buildFlusherComponents(ctx context.Context, l wal.WAL, snapshot *recovery.RecoverySnapshot) (*flusherComponents, message.MessageID, error) {
	// Get all existed vchannels of the pchannel.
	vchannels := lo.Keys(snapshot.VChannels)
	impl.logger.Info(ctx, "fetch vchannel done", mlog.Int("vchannelNum", len(vchannels)))

	// Get all the recovery info of the recoverable vchannels.
	recoverInfos, checkpoint, err := impl.getRecoveryInfos(ctx, vchannels)
	if err != nil {
		impl.logger.Warn(ctx, "get recovery info failed", mlog.Err(err))
		return nil, nil, err
	}
	impl.logger.Info(ctx, "fetch recovery info done", mlog.Int("recoveryInfoNum", len(recoverInfos)))
	if len(vchannels) == 0 && checkpoint == nil {
		impl.logger.Info(ctx, "no vchannel to recover, use the snapshot checkpoint", mlog.Stringer("checkpoint", snapshot.Checkpoint.MessageID))
		checkpoint = snapshot.Checkpoint.MessageID
	}

	mixc, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		impl.logger.Warn(ctx, "flusher recovery is canceled before data coord client ready", mlog.Err(err))
		return nil, nil, err
	}
	impl.logger.Info(ctx, "data coord client ready")

	// build all components.
	broker := broker.NewCoordBroker(mixc, paramtable.GetNodeID())
	chunkManager := resource.Resource().ChunkManager()

	cpUpdater := util.NewChannelCheckpointUpdaterWithCallback(broker, func(mp *msgpb.MsgPosition) {
		messageID := adaptor.MustGetMessageIDFromMQWrapperIDBytesWithWALName(impl.wal.Get().WALName(), mp.MsgID)
		impl.UpdateFlusherCheckpoint(mp.ChannelName, &recovery.WALCheckpoint{
			MessageID: messageID,
			TimeTick:  mp.Timestamp,
			Magic:     utility.RecoveryMagicStreamingInitialized,
		})
	})
	go cpUpdater.Start()

	fc := &flusherComponents{
		wal:                        l,
		broker:                     broker,
		cpUpdater:                  cpUpdater,
		chunkManager:               chunkManager,
		dataServices:               make(map[string]*dataSyncServiceWrapper),
		logger:                     impl.logger,
		recoveryCheckPointTimeTick: snapshot.Checkpoint.TimeTick,
		rs:                         impl.RecoveryStorage,
	}
	impl.logger.Info(ctx, "flusher components intiailizing done")
	if err := fc.recover(ctx, recoverInfos); err != nil {
		impl.logger.Warn(ctx, "flusher recovery is canceled before recovery done, recycle the resource", mlog.Err(err))
		fc.Close()
		impl.logger.Info(ctx, "flusher recycle the resource done")
		return nil, nil, err
	}
	impl.logger.Info(ctx, "flusher recovery done")
	return fc, checkpoint, nil
}

// generateScanner create a new scanner for the wal.
func (impl *WALFlusherImpl) generateScanner(ctx context.Context, l wal.WAL, checkpoint message.MessageID) (wal.Scanner, error) {
	handler := make(adaptor.ChanMessageHandler, 64)
	readOpt := wal.ReadOption{
		VChannel:         "", // We need consume all message from wal.
		MesasgeHandler:   handler,
		DeliverPolicy:    options.DeliverPolicyAll(),
		RateLimitControl: impl.rateLimitComponent.RecoveryStorage,
	}
	if checkpoint != nil {
		impl.logger.Info(ctx, "wal start to scan from minimum checkpoint", mlog.Stringer("checkpointMessageID", checkpoint))
		readOpt.DeliverPolicy = options.DeliverPolicyStartFrom(checkpoint)
	} else {
		impl.logger.Info(ctx, "wal start to scan from the earliest checkpoint")
	}
	return l.Read(ctx, readOpt)
}

// dispatch dispatches the message to the related handler for flusher components.
func (impl *WALFlusherImpl) dispatch(msg message.ImmutableMessage) (err error) {
	if msg.MessageType() == message.MessageTypeTimeTick && !msg.IsPersisted() {
		// Currently, milvus use the timetick to synchronize the system periodically,
		// so the wal will still produce empty timetick message after the last write operation is done.
		// When there're huge amount of vchannel in one pchannel, every time tick will be dispatched,
		// which will waste a lot of cpu resources.
		// So we only dispatch the timetick message when the timetick-lastDispatchTimeTick is greater than a threshold.
		timetick := msg.TimeTick()
		threshold := paramtable.Get().StreamingCfg.FlushEmptyTimeTickMaxFilterInterval.GetAsDurationByParse()
		if tsoutil.CalculateDuration(timetick, impl.lastDispatchTimeTick) < threshold.Milliseconds() {
			impl.emptyTimeTickCounter.Inc()
			return err
		}
	}
	timetick := msg.TimeTick()
	defer func() {
		impl.lastDispatchTimeTick = timetick
	}()

	if msg.MessageType() == message.MessageTypeCommitImport {
		// CommitImport must not be observed until DataCoord accepts the commit
		// fence; otherwise replay can skip the only retry signal for this vchannel.
		return impl.dispatchCommitImport(msg)
	}

	// TODO: should be removed at 3.0, after merge the flusher logic into recovery storage.
	// Only truncate collection needs to observe before the flusher handles the message.
	// Other messages should keep the deferred order so lifecycle cleanup such as
	// DropCollection can finish the flowgraph before recovery storage observes it.
	if msg.MessageType() == message.MessageTypeTruncateCollection {
		if err := impl.ObserveMessage(impl.notifier.Context(), msg); err != nil {
			impl.logger.Warn(context.TODO(), "failed to observe message", mlog.Err(err))
			return err
		}
	} else {
		// TODO: We will merge the flusher into recovery storage in future.
		// Currently, flusher works as a separate component.
		defer func() {
			if err = impl.ObserveMessage(impl.notifier.Context(), msg); err != nil {
				impl.logger.Warn(context.TODO(), "failed to observe message", mlog.Err(err))
			}
		}()
	}

	// wal flusher will not handle the control channel message unless it's a pchannel-level message.
	if funcutil.IsControlChannel(msg.VChannel()) && !msg.IsPChannelLevel() {
		return nil
	}

	// Do the data sync service management here.
	switch msg.MessageType() {
	case message.MessageTypeCreateCollection:
		createCollectionMsg, err := message.AsImmutableCreateCollectionMessageV1(msg)
		if err != nil {
			impl.logger.DPanic(context.TODO(), "the message type is not CreateCollectionMessage", mlog.Err(err))
			return nil
		}
		impl.flusherComponents.WhenCreateCollection(createCollectionMsg)
	case message.MessageTypeDropCollection:
		// defer to remove the data sync service from the components.
		// TODO: Current drop collection message will be handled by the underlying data sync service.
		defer func() {
			impl.flusherComponents.WhenDropCollection(msg.VChannel())
		}()
	case message.MessageTypeRollbackImport:
		// No-op: DataCoord DDL ack callback handles all state changes.
		impl.logger.Info(context.TODO(), "RollbackImportMessage consumed (no-op in flusher)",
			mlog.FieldVChannel(msg.VChannel()))
		return nil // don't forward to flusherComponents
	}
	return impl.flusherComponents.HandleMessage(impl.notifier.Context(), msg)
}

func (impl *WALFlusherImpl) dispatchCommitImport(msg message.ImmutableMessage) error {
	if funcutil.IsControlChannel(msg.VChannel()) && !msg.IsPChannelLevel() {
		return impl.ObserveMessage(impl.notifier.Context(), msg)
	}

	commitMsg, err := message.AsImmutableCommitImportMessageV2(msg)
	if err != nil {
		impl.logger.DPanic(context.TODO(), "failed to parse CommitImportMessage", mlog.Err(err))
		return nil
	}
	vchannel := msg.VChannel()
	jobID := commitMsg.Header().GetJobId()

	// Flush DML data before this commit fence. Panic on failure so WAL replays the message.
	if err := resource.Resource().WriteBufferManager().
		FlushChannel(context.Background(), vchannel, msg.TimeTick()); err != nil {
		if errors.Is(err, merr.ErrChannelNotFound) {
			impl.logger.Info(context.TODO(), "CommitImport targets stale vchannel, skip local flush and continue commit ack",
				mlog.FieldVChannel(vchannel), mlog.FieldJobID(jobID), mlog.Err(err))
		} else {
			impl.logger.Panic(context.TODO(), "FlushChannel on CommitImport failed, panicking to retry from WAL",
				mlog.FieldVChannel(vchannel), mlog.FieldJobID(jobID), mlog.Err(err))
		}
	}

	mixCoord, err := resource.Resource().MixCoordClient().GetWithContext(impl.notifier.Context())
	if err != nil {
		return errors.Wrap(err, "failed to get MixCoordClient for HandleCommitVchannel")
	}
	// Retry only the DataCoord ack. The local FlushChannel above is a best-effort
	// fence for this dispatch and must not be repeated on every retry.
	// This retry blocks the whole pchannel flusher until DataCoord accepts the
	// commit fence, preserving WAL replay order for later messages on the pchannel.
	impl.logger.Warn(context.TODO(), "HandleCommitVchannel may block the pchannel flusher until DataCoord accepts the commit fence",
		mlog.FieldJobID(jobID),
		mlog.FieldVChannel(vchannel),
		mlog.Uint64("commitTs", msg.TimeTick()))
	if err := retry.Do(impl.notifier.Context(), func() error {
		resp, err := mixCoord.HandleCommitVchannel(impl.notifier.Context(), &datapb.HandleCommitVchannelRequest{
			Base:            commonpbutil.NewMsgBase(commonpbutil.WithSourceID(paramtable.GetNodeID())),
			JobId:           jobID,
			Vchannel:        vchannel,
			CommitTimestamp: msg.TimeTick(),
		})
		if err := merr.CheckRPCCall(resp, err); err != nil {
			impl.logger.Debug(context.TODO(), "HandleCommitVchannel failed, retry later",
				mlog.FieldJobID(jobID),
				mlog.FieldVChannel(vchannel),
				mlog.Uint64("commitTs", msg.TimeTick()),
				mlog.Err(err))
			return err
		}
		return nil
	}, retry.AttemptAlways()); err != nil {
		return err
	}

	if err := impl.ObserveMessage(impl.notifier.Context(), msg); err != nil {
		impl.logger.Warn(context.TODO(), "failed to observe CommitImport message",
			mlog.FieldVChannel(vchannel), mlog.FieldJobID(jobID), mlog.Err(err))
		return err
	}

	impl.logger.Info(context.TODO(), "CommitImportMessage handled: vchannel committed",
		mlog.FieldVChannel(vchannel), mlog.FieldJobID(jobID))
	return nil
}
