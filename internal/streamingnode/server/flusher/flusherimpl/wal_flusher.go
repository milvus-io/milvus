package flusherimpl

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

var errChannelLifetimeUnrecoverable = errors.New("channel lifetime unrecoverable")

// RecoverWALFlusher recovers the wal flusher.
func RecoverWALFlusher(param interceptors.InterceptorBuildParam) *WALFlusherImpl {
	flusher := &WALFlusherImpl{
		notifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		wal:      param.WAL,
		logger: resource.Resource().Logger().With(
			log.FieldComponent("flusher"),
			zap.String("pchannel", param.WALImpls.Channel().Name)),
	}
	go flusher.Execute()
	return flusher
}

type WALFlusherImpl struct {
	notifier          *syncutil.AsyncTaskNotifier[struct{}]
	wal               *syncutil.Future[wal.WAL]
	flusherComponents *flusherComponents
	logger            *log.MLogger
}

// Execute starts the wal flusher.
func (impl *WALFlusherImpl) Execute() (err error) {
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

	impl.flusherComponents, err = impl.buildFlusherComponents(impl.notifier.Context(), l)
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
	for {
		select {
		case <-impl.notifier.Context().Done():
			return nil
		case msg, ok := <-scanner.Chan():
			if !ok {
				impl.logger.Warn("wal flusher is closing for closed scanner channel, which is unexpected at graceful way")
				return nil
			}
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
}

// buildFlusherComponents builds the components of the flusher.
func (impl *WALFlusherImpl) buildFlusherComponents(ctx context.Context, l wal.WAL) (*flusherComponents, error) {
	// Get all existed vchannels of the pchannel.
	vchannels, err := impl.getVchannels(ctx, l.Channel().Name)
	if err != nil {
		impl.logger.Warn("get vchannels failed", zap.Error(err))
		return nil, err
	}
	impl.logger.Info("fetch vchannel done", zap.Int("vchannelNum", len(vchannels)))

	// Get all the recovery info of the recoverable vchannels.
	recoverInfos, checkpoints, err := impl.getRecoveryInfos(ctx, vchannels)
	if err != nil {
		impl.logger.Warn("get recovery info failed", zap.Error(err))
		return nil, err
	}
	impl.logger.Info("fetch recovery info done", zap.Int("recoveryInfoNum", len(recoverInfos)))

	dc, err := resource.Resource().DataCoordClient().GetWithContext(ctx)
	if err != nil {
		impl.logger.Warn("flusher recovery is canceled before data coord client ready", zap.Error(err))
		return nil, err
	}
	impl.logger.Info("data coord client ready")

	// build all components.
	broker := broker.NewCoordBroker(dc, paramtable.GetNodeID())
	chunkManager := resource.Resource().ChunkManager()
	syncMgr := syncmgr.NewSyncManager(chunkManager)
	wbMgr := writebuffer.NewManager(syncMgr)
	wbMgr.Start()

	pm, err := recoverPChannelCheckpointManager(ctx, l.WALName(), l.Channel().Name, checkpoints)
	if err != nil {
		impl.logger.Warn("recover pchannel checkpoint manager failure", zap.Error(err))
		return nil, err
	}
	cpUpdater := util.NewChannelCheckpointUpdaterWithCallback(broker, func(mp *msgpb.MsgPosition) {
		// After vchannel checkpoint updated, notify the pchannel checkpoint manager to work.
		pm.Update(mp.ChannelName, adaptor.MustGetMessageIDFromMQWrapperIDBytes(l.WALName(), mp.MsgID))
	})
	go cpUpdater.Start()

	fc := &flusherComponents{
		wal:               l,
		broker:            broker,
		syncMgr:           syncMgr,
		wbMgr:             wbMgr,
		cpUpdater:         cpUpdater,
		chunkManager:      chunkManager,
		dataServices:      make(map[string]*dataSyncServiceWrapper),
		checkpointManager: pm,
		logger:            impl.logger,
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
	if startMessageID := impl.flusherComponents.StartMessageID(); startMessageID != nil {
		impl.logger.Info("wal start to scan from minimum checkpoint", zap.Stringer("startMessageID", startMessageID))
		readOpt.DeliverPolicy = options.DeliverPolicyStartAfter(startMessageID)
	}
	impl.logger.Info("wal start to scan from the beginning")
	return l.Read(ctx, readOpt)
}

// dispatch dispatches the message to the related handler for flusher components.
func (impl *WALFlusherImpl) dispatch(msg message.ImmutableMessage) error {
	// Do the data sync service management here.
	switch msg.MessageType() {
	case message.MessageTypeCreateCollection:
		createCollectionMsg, err := message.AsImmutableCreateCollectionMessageV1(msg)
		if err != nil {
			impl.logger.DPanic("the message type is not CreateCollectionMessage", zap.Error(err))
			return nil
		}
		impl.flusherComponents.WhenCreateCollection(createCollectionMsg)
	case message.MessageTypeDropCollection:
		// defer to remove the data sync service from the components.
		// TODO: Current drop collection message will be handled by the underlying data sync service.
		defer impl.flusherComponents.WhenDropCollection(msg.VChannel())
	}
	return impl.flusherComponents.HandleMessage(impl.notifier.Context(), msg)
}
