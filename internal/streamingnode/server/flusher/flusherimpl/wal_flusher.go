package flusherimpl

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var errChannelLifetimeUnrecoverable = errors.New("channel lifetime unrecoverable")

// RecoverWALFlusherParam is the parameter for building wal flusher.
type RecoverWALFlusherParam struct {
	ChannelInfo      types.PChannelInfo
	WAL              *syncutil.Future[wal.WAL]
	RecoverySnapshot *recovery.RecoverySnapshot
	RecoveryStorage  *recovery.RecoveryStorage
}

// RecoverWALFlusher recovers the wal flusher.
func RecoverWALFlusher(param *RecoverWALFlusherParam) *WALFlusherImpl {
	flusher := &WALFlusherImpl{
		notifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		wal:      param.WAL,
		logger: resource.Resource().Logger().With(
			log.FieldComponent("flusher"),
			zap.String("pchannel", param.ChannelInfo.Name)),
		metrics: newFlusherMetrics(param.ChannelInfo),
		rs:      param.RecoveryStorage,
	}
	go flusher.Execute(param.RecoverySnapshot)
	return flusher
}

type WALFlusherImpl struct {
	notifier          *syncutil.AsyncTaskNotifier[struct{}]
	wal               *syncutil.Future[wal.WAL]
	flusherComponents *flusherComponents
	logger            *log.MLogger
	metrics           *flusherMetrics
	rs                *recovery.RecoveryStorage
}

// Execute starts the wal flusher.
func (impl *WALFlusherImpl) Execute(recoverSnapshot *recovery.RecoverySnapshot) (err error) {
	defer func() {
		impl.notifier.Finish(struct{}{})
		if err == nil {
			impl.logger.Info("wal flusher stop")
			return
		}
		if !errors.Is(err, context.Canceled) {
			impl.logger.DPanic("wal flusher stop to executing with unexpected error", zap.Error(err))
			return
		}
		impl.logger.Warn("wal flusher is canceled before executing", zap.Error(err))
	}()

	impl.logger.Info("wal flusher start to recovery...")
	l, err := impl.wal.GetWithContext(impl.notifier.Context())
	if err != nil {
		return errors.Wrap(err, "when get wal from future")
	}
	impl.logger.Info("wal ready for flusher recovery")

	impl.flusherComponents, err = impl.buildFlusherComponents(impl.notifier.Context(), l, recoverSnapshot)
	if err != nil {
		return errors.Wrap(err, "when build flusher components")
	}
	defer impl.flusherComponents.Close()

	scanner, err := impl.generateScanner(impl.notifier.Context(), impl.wal.Get())
	if err != nil {
		return errors.Wrap(err, "when generate scanner")
	}
	defer scanner.Close()

	impl.logger.Info("wal flusher start to work")
	impl.metrics.IntoState(flusherStateInWorking)
	defer impl.metrics.IntoState(flusherStateOnClosing)

	for {
		select {
		case <-impl.notifier.Context().Done():
			return nil
		case msg, ok := <-scanner.Chan():
			if !ok {
				impl.logger.Warn("wal flusher is closing for closed scanner channel, which is unexpected at graceful way")
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

	impl.logger.Info("wal flusher start to close the recovery storage...")
	impl.rs.Close()
	impl.logger.Info("recovery storage closed")

	impl.metrics.Close()
}

// buildFlusherComponents builds the components of the flusher.
func (impl *WALFlusherImpl) buildFlusherComponents(ctx context.Context, l wal.WAL, snapshot *recovery.RecoverySnapshot) (*flusherComponents, error) {
	// Get all existed vchannels of the pchannel.
	vchannels := lo.Keys(snapshot.VChannels)
	impl.logger.Info("fetch vchannel done", zap.Int("vchannelNum", len(vchannels)))

	// Get all the recovery info of the recoverable vchannels.
	recoverInfos, checkpoints, err := impl.getRecoveryInfos(ctx, vchannels)
	if err != nil {
		impl.logger.Warn("get recovery info failed", zap.Error(err))
		return nil, err
	}
	impl.logger.Info("fetch recovery info done", zap.Int("recoveryInfoNum", len(recoverInfos)))
	impl.rs.InitVChannelCheckpoint(checkpoints)

	mixc, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		impl.logger.Warn("flusher recovery is canceled before data coord client ready", zap.Error(err))
		return nil, err
	}
	impl.logger.Info("data coord client ready")

	// build all components.
	broker := broker.NewCoordBroker(mixc, paramtable.GetNodeID())
	chunkManager := resource.Resource().ChunkManager()

	cpUpdater := util.NewChannelCheckpointUpdaterWithCallback(broker, func(mp *msgpb.MsgPosition) {
		// After vchannel checkpoint updated, notify the pchannel checkpoint manager to work.
		impl.rs.UpdateVChannelCheckpoint(mp.ChannelName, adaptor.MustGetMessageIDFromMQWrapperIDBytes(l.WALName(), mp.MsgID))
	})
	go cpUpdater.Start()

	fc := &flusherComponents{
		wal:          l,
		broker:       broker,
		cpUpdater:    cpUpdater,
		chunkManager: chunkManager,
		dataServices: make(map[string]*dataSyncServiceWrapper),
		logger:       impl.logger,
	}
	impl.logger.Info("flusher components intiailizing done")
	if err := fc.recover(ctx, recoverInfos); err != nil {
		impl.logger.Warn("flusher recovery is canceled before recovery done, recycle the resource", zap.Error(err))
		fc.Close()
		impl.logger.Info("flusher recycle the resource done")
		return nil, err
	}
	impl.logger.Info("flusher recovery done")
	return fc, nil
}

// generateScanner create a new scanner for the wal.
func (impl *WALFlusherImpl) generateScanner(ctx context.Context, l wal.WAL) (wal.Scanner, error) {
	handler := make(adaptor.ChanMessageHandler, 64)
	readOpt := wal.ReadOption{
		VChannel:       "", // We need consume all message from wal.
		MesasgeHandler: handler,
		DeliverPolicy:  options.DeliverPolicyAll(),
	}
	if startMessageID := impl.rs.FlushCheckpoint(); startMessageID != nil {
		impl.logger.Info("wal start to scan from minimum checkpoint", zap.Stringer("startMessageID", startMessageID))
		// !!! we always set the deliver policy to start from the last confirmed message id.
		// because the catchup scanner at the streamingnode server must see the last confirmed message id if it's the last timetick.
		readOpt.DeliverPolicy = options.DeliverPolicyStartFrom(startMessageID)
	}
	impl.logger.Info("wal start to scan from the beginning")
	return l.Read(ctx, readOpt)
}

// dispatch dispatches the message to the related handler for flusher components.
func (impl *WALFlusherImpl) dispatch(msg message.ImmutableMessage) error {
	// TODO: We will merge the flusher into recovery storage in future.
	// Currently, flusher works as a separate component.
	defer impl.rs.ObserveMessage(msg)

	// Do the data sync service management here.
	switch msg.MessageType() {
	case message.MessageTypeCreateCollection:
		createCollectionMsg, err := message.AsImmutableCreateCollectionMessageV1(msg)
		if err != nil {
			impl.logger.DPanic("the message type is not CreateCollectionMessage", zap.Error(err))
			return nil
		}
		impl.flusherComponents.WhenCreateCollection(createCollectionMsg)
		impl.rs.AddVChannelCheckpoint(createCollectionMsg.VChannel(), createCollectionMsg.LastConfirmedMessageID())
	case message.MessageTypeDropCollection:
		// defer to remove the data sync service from the components.
		// TODO: Current drop collection message will be handled by the underlying data sync service.
		defer func() {
			impl.flusherComponents.WhenDropCollection(msg.VChannel())
			impl.rs.DropVChannelCheckpoint(msg.VChannel())
		}()
	}
	return impl.flusherComponents.HandleMessage(impl.notifier.Context(), msg)
}
