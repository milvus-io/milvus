package flusherimpl

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/util/retry"
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

func (impl *WALFlusherImpl) generateScanner(ctx context.Context, wal wal.WAL) (wal.Scanner, error) {
	// If checkpoint is nil,
	// there may be the first time the wal built or the older version milvus that doesn't have pchannel level checkpoint.
	// Try to recover it from vchannels.
	// TODO: retry infinitely.
	var checkpoint *streamingpb.WALCheckpoint
	retryCnt := -1
	if err := retry.Do(ctx, func() error {
		var err error
		retryCnt++
		if checkpoint, err = resource.Resource().StreamingNodeCatalog().GetConsumeCheckpoint(ctx, wal.Channel().Name); err != nil {
			impl.logger.Warn("get consume checkpoint failed", zap.Error(err), zap.Int("retryCnt", retryCnt))
			return err
		}
		return nil
	}, retry.AttemptAlways()); err != nil {
		return nil, err
	}
	readOpt := impl.getReadOptions(checkpoint, wal.WALName())
	return wal.Read(ctx, readOpt)
}

func (impl *WALFlusherImpl) getReadOptions(checkpoint *streamingpb.WALCheckpoint, walName string) wal.ReadOption {
	handler := make(adaptor.ChanMessageHandler, 64)
	readOpt := wal.ReadOption{
		VChannel:       "", // We need consume all message from wal.
		MesasgeHandler: handler,
		DeliverPolicy:  options.DeliverPolicyAll(),
	}
	if checkpoint != nil {
		startMessageID := message.MustUnmarshalMessageID(walName, checkpoint.MessageID.Id)
		impl.logger.Info("wal start to scan from pchannel checkpoint", zap.Stringer("startMessageID", startMessageID))
		readOpt.DeliverPolicy = options.DeliverPolicyStartFrom(startMessageID)
		return readOpt
	}
	if startMessageID := impl.flusherComponents.GetMinimumStartMessage(); startMessageID != nil {
		impl.logger.Info("wal start to scan from minimum vchannel checkpoint", zap.Stringer("startMessageID", startMessageID))
		readOpt.DeliverPolicy = options.DeliverPolicyStartAfter(startMessageID)
		return readOpt
	}
	impl.logger.Info("wal start to scan from the beginning")
	return readOpt
}

// handleMessage handles the message from wal.
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
		defer impl.flusherComponents.WhenDropCollection(msg.VChannel())
	}
	return impl.flusherComponents.HandleMessage(impl.notifier.Context(), msg)
}

func (impl *WALFlusherImpl) Close() {
	impl.notifier.Cancel()
	impl.notifier.BlockUntilFinish()
}
