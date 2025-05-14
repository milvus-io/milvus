package adaptor

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/helper"
)

var _ wal.Scanner = (*scannerAdaptorImpl)(nil)

// newRecoveryScannerAdaptor creates a new recovery scanner adaptor.
func newRecoveryScannerAdaptor(l walimpls.ROWALImpls,
	startMessageID message.MessageID,
	scanMetrics *metricsutil.ScannerMetrics,
) *scannerAdaptorImpl {
	name := "recovery"
	logger := resource.Resource().Logger().With(
		log.FieldComponent("scanner"),
		zap.String("name", name),
		zap.String("channel", l.Channel().String()),
		zap.String("startMessageID", startMessageID.String()),
	)
	readOption := wal.ReadOption{
		DeliverPolicy:  options.DeliverPolicyStartFrom(startMessageID),
		MesasgeHandler: adaptor.ChanMessageHandler(make(chan message.ImmutableMessage)),
	}

	s := &scannerAdaptorImpl{
		logger:        logger,
		recovery:      true,
		innerWAL:      l,
		readOption:    readOption,
		filterFunc:    func(message.ImmutableMessage) bool { return true },
		reorderBuffer: utility.NewReOrderBuffer(),
		pendingQueue:  utility.NewPendingQueue(),
		txnBuffer:     utility.NewTxnBuffer(logger, scanMetrics),
		cleanup:       func() {},
		ScannerHelper: helper.NewScannerHelper(name),
		metrics:       scanMetrics,
	}
	go s.execute()
	return s
}

// newScannerAdaptor creates a new scanner adaptor.
func newScannerAdaptor(
	name string,
	l walimpls.ROWALImpls,
	readOption wal.ReadOption,
	scanMetrics *metricsutil.ScannerMetrics,
	cleanup func(),
) *scannerAdaptorImpl {
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
		recovery:      false,
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
	recovery      bool
	logger        *log.MLogger
	innerWAL      walimpls.ROWALImpls
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
	defer func() { <-ch }()
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
}

// produceEventLoop produces the message from the wal and write ahead buffer.
func (s *scannerAdaptorImpl) produceEventLoop(msgChan chan<- message.ImmutableMessage) error {
	var wb wab.ROWriteAheadBuffer
	var err error
	if s.Channel().AccessMode == types.AccessModeRW && !s.recovery {
		// recovery scanner can not use the write ahead buffer, should not trigger sync.

		// Trigger a persisted time tick to make sure the timetick is pushed forward.
		// because the underlying wal may be deleted because of retention policy.
		// So we cannot get the timetick from the wal.
		// Trigger the timetick inspector to append a new persisted timetick,
		// then the catch up scanner can see the latest timetick and make a catchup.
		resource.Resource().TimeTickInspector().TriggerSync(s.Channel(), true)
		wb = resource.Resource().TimeTickInspector().MustGetOperator(s.Channel()).WriteAheadBuffer()
	}

	scanner := newSwithableScanner(s.Name(), s.logger, s.innerWAL, wb, s.readOption.DeliverPolicy, msgChan)
	s.logger.Info("start produce loop of scanner at model", zap.String("model", getScannerModel(scanner)))
	for {
		if scanner, err = scanner.Do(s.Context()); err != nil {
			return err
		}
		m := getScannerModel(scanner)
		s.metrics.SwitchModel(m)
		s.logger.Info("switch scanner model", zap.String("model", m))
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
	var isTailing bool
	msg, isTailing = isTailingScanImmutableMessage(msg)
	s.metrics.ObserveMessage(isTailing, msg.MessageType(), msg.EstimateSize())
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
		}
		if msg.IsPersisted() || s.pendingQueue.Len() == 0 {
			// If the ts message is persisted, it must can be seen by the consumer.
			//
			// Otherwise if there's no new message incoming and there's no pending message in the queue.
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
			s.metrics.ObserveTimeTickViolation(isTailing, msg.MessageType())
		}
		s.logger.Warn("failed to push message into reorder buffer",
			log.FieldMessage(msg),
			zap.Bool("tailing", isTailing),
			zap.Error(err))
	}
	// Observe the filtered message.
	s.metrics.UpdateTimeTickBufSize(s.reorderBuffer.Bytes())
	s.metrics.ObservePassedMessage(isTailing, msg.MessageType(), msg.EstimateSize())
	if s.logger.Level().Enabled(zap.DebugLevel) {
		// Log the message if the log level is debug.
		s.logger.Debug("push message into reorder buffer",
			log.FieldMessage(msg),
			zap.Bool("tailing", isTailing))
	}
}
