package adaptor

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/helper"
)

var _ wal.Scanner = (*scannerAdaptorImpl)(nil)

// newScannerAdaptor creates a new scanner adaptor.
func newScannerAdaptor(
	name string,
	l walimpls.WALImpls,
	readOption wal.ReadOption,
	scanMetrics *metricsutil.ScannerMetrics,
	cleanup func(),
) wal.Scanner {
	if readOption.MesasgeHandler == nil {
		readOption.MesasgeHandler = adaptor.ChanMessageHandler(make(chan message.ImmutableMessage))
	}
	options.GetFilterFunc(readOption.MessageFilter)
	logger := resource.Resource().Logger().With(
		log.FieldComponent("scanner"),
		zap.String("name", name),
		zap.String("channel", l.Channel().Name),
	)
	s := &scannerAdaptorImpl{
		logger:        logger,
		innerWAL:      l,
		readOption:    readOption,
		filterFunc:    options.GetFilterFunc(readOption.MessageFilter),
		reorderBuffer: utility.NewReOrderBuffer(),
		pendingQueue:  utility.NewPendingQueue(),
		txnBuffer:     utility.NewTxnBuffer(logger, scanMetrics),
		cleanup:       cleanup,
		ScannerHelper: helper.NewScannerHelper(name),
		metrics:       scanMetrics,
	}
	go s.execute()
	return s
}

// scannerAdaptorImpl is a wrapper of ScannerImpls to extend it into a Scanner interface.
type scannerAdaptorImpl struct {
	*helper.ScannerHelper
	logger        *log.MLogger
	innerWAL      walimpls.WALImpls
	readOption    wal.ReadOption
	filterFunc    func(message.ImmutableMessage) bool
	reorderBuffer *utility.ReOrderByTimeTickBuffer // support time tick reorder.
	pendingQueue  *utility.PendingQueue
	txnBuffer     *utility.TxnBuffer // txn buffer for txn message.
	cleanup       func()
	metrics       *metricsutil.ScannerMetrics
}

// Channel returns the channel assignment info of the wal.
func (s *scannerAdaptorImpl) Channel() types.PChannelInfo {
	return s.innerWAL.Channel()
}

// Chan returns the message channel of the scanner.
func (s *scannerAdaptorImpl) Chan() <-chan message.ImmutableMessage {
	return s.readOption.MesasgeHandler.(adaptor.ChanMessageHandler)
}

// Close the scanner, release the underlying resources.
// Return the error same with `Error`
func (s *scannerAdaptorImpl) Close() error {
	err := s.ScannerHelper.Close()
	if s.cleanup != nil {
		s.cleanup()
	}
	s.metrics.Close()
	return err
}

func (s *scannerAdaptorImpl) execute() {
	defer func() {
		s.readOption.MesasgeHandler.Close()
		s.Finish(nil)
		s.logger.Info("scanner is closed")
	}()
	s.logger.Info("scanner start background task")

	msgChan := make(chan message.ImmutableMessage)
	ch := make(chan struct{})
	// TODO: optimize the extra goroutine here after msgstream is removed.
	go func() {
		defer close(ch)
		err := s.produceEventLoop(msgChan)
		if errors.Is(err, context.Canceled) {
			s.logger.Info("the produce event loop of scanner is closed")
			return
		}
		s.logger.Warn("the produce event loop of scanner is closed with unexpected error", zap.Error(err))
	}()

	err := s.consumeEventLoop(msgChan)
	if errors.Is(err, context.Canceled) {
		s.logger.Info("the consuming event loop of scanner is closed")
		return
	}
	s.logger.Warn("the consuming event loop of scanner is closed with unexpected error", zap.Error(err))

	// waiting for the produce event loop to close.
	<-ch
}

// produceEventLoop produces the message from the wal and write ahead buffer.
func (s *scannerAdaptorImpl) produceEventLoop(msgChan chan<- message.ImmutableMessage) error {
	wb, err := resource.Resource().TimeTickInspector().MustGetOperator(s.Channel()).WriteAheadBuffer(s.Context())
	if err != nil {
		return err
	}

	scanner := newSwithableScanner(s.Name(), s.logger, s.innerWAL, wb, s.readOption.DeliverPolicy, msgChan)
	s.logger.Info("start produce loop of scanner at mode", zap.String("mode", scanner.Mode()))
	for {
		if scanner, err = scanner.Do(s.Context()); err != nil {
			return err
		}
		s.logger.Info("switch scanner mode", zap.String("mode", scanner.Mode()))
	}
}

// consumeEventLoop consumes the message from the message channel and handle it.
func (s *scannerAdaptorImpl) consumeEventLoop(msgChan <-chan message.ImmutableMessage) error {
	for {
		var upstream <-chan message.ImmutableMessage
		if s.pendingQueue.Len() > 16 {
			// If the pending queue is full, we need to wait until it's consumed to avoid scanner overloading.
			upstream = nil
		} else {
			upstream = msgChan
		}
		// generate the event channel and do the event loop.
		handleResult := s.readOption.MesasgeHandler.Handle(message.HandleParam{
			Ctx:      s.Context(),
			Upstream: upstream,
			Message:  s.pendingQueue.Next(),
		})
		if handleResult.Error != nil {
			return handleResult.Error
		}
		if handleResult.MessageHandled {
			s.pendingQueue.UnsafeAdvance()
			s.metrics.UpdatePendingQueueSize(s.pendingQueue.Bytes())
		}
		if handleResult.Incoming != nil {
			s.handleUpstream(handleResult.Incoming)
		}
	}
}

// handleUpstream handles the incoming message from the upstream.
func (s *scannerAdaptorImpl) handleUpstream(msg message.ImmutableMessage) {
	// Observe the message.
	s.metrics.ObserveMessage(msg.MessageType(), msg.EstimateSize())
	if msg.MessageType() == message.MessageTypeTimeTick {
		// If the time tick message incoming,
		// the reorder buffer can be consumed until latest confirmed timetick.
		messages := s.reorderBuffer.PopUtilTimeTick(msg.TimeTick())
		s.metrics.UpdateTimeTickBufSize(s.reorderBuffer.Bytes())

		// There's some txn message need to hold until confirmed, so we need to handle them in txn buffer.
		msgs := s.txnBuffer.HandleImmutableMessages(messages, msg.TimeTick())
		s.metrics.UpdateTxnBufSize(s.txnBuffer.Bytes())

		if len(msgs) > 0 {
			// Push the confirmed messages into pending queue for consuming.
			s.pendingQueue.Add(msgs)
		} else if s.pendingQueue.Len() == 0 {
			// If there's no new message incoming and there's no pending message in the queue.
			// Add current timetick message into pending queue to make timetick push forward.
			// TODO: current milvus can only run on timetick pushing,
			// after qview is applied, those trival time tick message can be erased.
			s.pendingQueue.Add([]message.ImmutableMessage{msg})
		}
		s.metrics.UpdatePendingQueueSize(s.pendingQueue.Bytes())
		return
	}

	// Filtering the vchannel
	// If the message is not belong to any vchannel, it should be broadcasted to all vchannels.
	// Otherwise, it should be filtered by vchannel.
	if msg.VChannel() != "" && s.readOption.VChannel != "" && s.readOption.VChannel != msg.VChannel() {
		return
	}
	// Filtering the message if needed.
	// System message should never be filtered.
	if s.filterFunc != nil && !s.filterFunc(msg) {
		return
	}
	// otherwise add message into reorder buffer directly.
	if err := s.reorderBuffer.Push(msg); err != nil {
		if errors.Is(err, utility.ErrTimeTickVoilation) {
			s.metrics.ObserveTimeTickViolation(msg.MessageType())
		}
		s.logger.Warn("failed to push message into reorder buffer",
			zap.Any("msgID", msg.MessageID()),
			zap.Uint64("timetick", msg.TimeTick()),
			zap.String("vchannel", msg.VChannel()),
			zap.Error(err))
	}
	// Observe the filtered message.
	s.metrics.UpdateTimeTickBufSize(s.reorderBuffer.Bytes())
	s.metrics.ObserveFilteredMessage(msg.MessageType(), msg.EstimateSize())
}
