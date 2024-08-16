package adaptor

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/inspector"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ wal.Scanner = (*scannerAdaptorImpl)(nil)

// newScannerAdaptor creates a new scanner adaptor.
func newScannerAdaptor(
	name string,
	l walimpls.WALImpls,
	readOption wal.ReadOption,
	cleanup func(),
) wal.Scanner {
	if readOption.MesasgeHandler == nil {
		readOption.MesasgeHandler = defaultMessageHandler(make(chan message.ImmutableMessage))
	}
	options.GetFilterFunc(readOption.MessageFilter)
	logger := log.With(zap.String("name", name), zap.String("channel", l.Channel().Name))
	s := &scannerAdaptorImpl{
		logger:           logger,
		innerWAL:         l,
		readOption:       readOption,
		filterFunc:       options.GetFilterFunc(readOption.MessageFilter),
		reorderBuffer:    utility.NewReOrderBuffer(),
		pendingQueue:     typeutil.NewMultipartQueue[message.ImmutableMessage](),
		txnBuffer:        utility.NewTxnBuffer(logger),
		cleanup:          cleanup,
		ScannerHelper:    helper.NewScannerHelper(name),
		lastTimeTickInfo: inspector.TimeTickInfo{},
	}
	go s.executeConsume()
	return s
}

// scannerAdaptorImpl is a wrapper of ScannerImpls to extend it into a Scanner interface.
type scannerAdaptorImpl struct {
	*helper.ScannerHelper
	logger           *log.MLogger
	innerWAL         walimpls.WALImpls
	readOption       wal.ReadOption
	filterFunc       func(message.ImmutableMessage) bool
	reorderBuffer    *utility.ReOrderByTimeTickBuffer                   // only support time tick reorder now.
	pendingQueue     *typeutil.MultipartQueue[message.ImmutableMessage] //
	txnBuffer        *utility.TxnBuffer                                 // txn buffer for txn message.
	cleanup          func()
	lastTimeTickInfo inspector.TimeTickInfo
}

// Channel returns the channel assignment info of the wal.
func (s *scannerAdaptorImpl) Channel() types.PChannelInfo {
	return s.innerWAL.Channel()
}

// Chan returns the message channel of the scanner.
func (s *scannerAdaptorImpl) Chan() <-chan message.ImmutableMessage {
	return s.readOption.MesasgeHandler.(defaultMessageHandler)
}

// Close the scanner, release the underlying resources.
// Return the error same with `Error`
func (s *scannerAdaptorImpl) Close() error {
	err := s.ScannerHelper.Close()
	if s.cleanup != nil {
		s.cleanup()
	}
	return err
}

func (s *scannerAdaptorImpl) executeConsume() {
	defer s.readOption.MesasgeHandler.Close()

	innerScanner, err := s.innerWAL.Read(s.Context(), walimpls.ReadOption{
		Name:          s.Name(),
		DeliverPolicy: s.readOption.DeliverPolicy,
	})
	if err != nil {
		s.Finish(err)
		return
	}
	defer innerScanner.Close()

	timeTickNotifier := resource.Resource().TimeTickInspector().MustGetOperator(s.Channel()).TimeTickNotifier()

	for {
		// generate the event channel and do the event loop.
		// TODO: Consume from local cache.
		handleResult := s.readOption.MesasgeHandler.Handle(wal.HandleParam{
			Ctx:          s.Context(),
			Upstream:     s.getUpstream(innerScanner),
			TimeTickChan: s.getTimeTickUpdateChan(timeTickNotifier),
			Message:      s.pendingQueue.Next(),
		})
		if handleResult.Error != nil {
			s.Finish(err)
			return
		}
		if handleResult.MessageHandled {
			s.pendingQueue.UnsafeAdvance()
		}
		if handleResult.Incoming != nil {
			s.handleUpstream(handleResult.Incoming)
		}
		// If the timetick just updated with a non persist operation,
		// we just make a fake message to keep timetick sync if there are no more pending message.
		if handleResult.TimeTickUpdated {
			s.handleTimeTickUpdated(timeTickNotifier)
		}
	}
}

func (s *scannerAdaptorImpl) getTimeTickUpdateChan(timeTickNotifier *inspector.TimeTickNotifier) <-chan struct{} {
	if s.pendingQueue.Len() == 0 && s.reorderBuffer.Len() == 0 && !s.lastTimeTickInfo.IsZero() {
		return timeTickNotifier.WatchAtMessageID(s.lastTimeTickInfo.MessageID, s.lastTimeTickInfo.TimeTick)
	}
	return nil
}

func (s *scannerAdaptorImpl) getUpstream(scanner walimpls.ScannerImpls) <-chan message.ImmutableMessage {
	// TODO: configurable pending buffer count.
	// If the pending queue is full, we need to wait until it's consumed to avoid scanner overloading.
	if s.pendingQueue.Len() > 16 {
		return nil
	}
	return scanner.Chan()
}

func (s *scannerAdaptorImpl) handleUpstream(msg message.ImmutableMessage) {
	if msg.MessageType() == message.MessageTypeTimeTick {
		// If the time tick message incoming,
		// the reorder buffer can be consumed until latest confirmed timetick.
		messages := s.reorderBuffer.PopUtilTimeTick(msg.TimeTick())

		// There's some txn message need to hold until confirmed, so we need to handle them in txn buffer.
		msgs := s.txnBuffer.HandleImmutableMessages(messages, msg.TimeTick())

		// Push the confirmed messages into pending queue for consuming.
		// and push forward timetick info.
		s.pendingQueue.Add(msgs)
		s.lastTimeTickInfo = inspector.TimeTickInfo{
			MessageID:              msg.MessageID(),
			TimeTick:               msg.TimeTick(),
			LastConfirmedMessageID: msg.LastConfirmedMessageID(),
		}
		return
	}

	// Filtering the message if needed.
	// System message should never be filtered.
	if s.filterFunc != nil && !s.filterFunc(msg) {
		return
	}
	// otherwise add message into reorder buffer directly.
	if err := s.reorderBuffer.Push(msg); err != nil {
		s.logger.Warn("failed to push message into reorder buffer",
			zap.Any("msgID", msg.MessageID()),
			zap.Uint64("timetick", msg.TimeTick()),
			zap.String("vchannel", msg.VChannel()),
			zap.Error(err))
	}
}

func (s *scannerAdaptorImpl) handleTimeTickUpdated(timeTickNotifier *inspector.TimeTickNotifier) {
	timeTickInfo := timeTickNotifier.Get()
	if timeTickInfo.MessageID.EQ(s.lastTimeTickInfo.MessageID) && timeTickInfo.TimeTick > s.lastTimeTickInfo.TimeTick {
		s.lastTimeTickInfo.TimeTick = timeTickInfo.TimeTick
		msg, err := timetick.NewTimeTickMsg(
			s.lastTimeTickInfo.TimeTick,
			s.lastTimeTickInfo.LastConfirmedMessageID,
			paramtable.GetNodeID(),
		)
		if err != nil {
			s.logger.Warn("unreachable: a marshal timetick operation must be success")
			return
		}
		s.pendingQueue.AddOne(msg.IntoImmutableMessage(s.lastTimeTickInfo.MessageID))
	}
}
