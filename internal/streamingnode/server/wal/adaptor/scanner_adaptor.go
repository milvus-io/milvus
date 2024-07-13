package adaptor

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
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
	cleanup func(),
) wal.Scanner {
	s := &scannerAdaptorImpl{
		logger:        log.With(zap.String("name", name), zap.String("channel", l.Channel().Name)),
		innerWAL:      l,
		readOption:    readOption,
		sendingCh:     make(chan message.ImmutableMessage, 1),
		reorderBuffer: utility.NewReOrderBuffer(),
		pendingQueue:  utility.NewImmutableMessageQueue(),
		cleanup:       cleanup,
		ScannerHelper: helper.NewScannerHelper(name),
	}
	go s.executeConsume()
	return s
}

// scannerAdaptorImpl is a wrapper of ScannerImpls to extend it into a Scanner interface.
type scannerAdaptorImpl struct {
	*helper.ScannerHelper
	logger        *log.MLogger
	innerWAL      walimpls.WALImpls
	readOption    wal.ReadOption
	sendingCh     chan message.ImmutableMessage
	reorderBuffer *utility.ReOrderByTimeTickBuffer // only support time tick reorder now.
	pendingQueue  *utility.ImmutableMessageQueue   //
	cleanup       func()
}

// Channel returns the channel assignment info of the wal.
func (s *scannerAdaptorImpl) Channel() types.PChannelInfo {
	return s.innerWAL.Channel()
}

// Chan returns the channel of message.
func (s *scannerAdaptorImpl) Chan() <-chan message.ImmutableMessage {
	return s.sendingCh
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
	defer close(s.sendingCh)

	innerScanner, err := s.innerWAL.Read(s.Context(), walimpls.ReadOption{
		Name:          s.Name(),
		DeliverPolicy: s.readOption.DeliverPolicy,
	})
	if err != nil {
		s.Finish(err)
		return
	}
	defer innerScanner.Close()

	for {
		// generate the event channel and do the event loop.
		// TODO: Consume from local cache.
		upstream, sending := s.getEventCh(innerScanner)
		select {
		case <-s.Context().Done():
			s.Finish(err)
			return
		case msg, ok := <-upstream:
			if !ok {
				s.Finish(innerScanner.Error())
				return
			}
			s.handleUpstream(msg)
		case sending <- s.pendingQueue.Next():
			s.pendingQueue.UnsafeAdvance()
		}
	}
}

func (s *scannerAdaptorImpl) getEventCh(scanner walimpls.ScannerImpls) (<-chan message.ImmutableMessage, chan<- message.ImmutableMessage) {
	if s.pendingQueue.Len() == 0 {
		// If pending queue is empty,
		// no more message can be sent,
		// we always need to recv message from upstream to avoid starve.
		return scanner.Chan(), nil
	}
	// TODO: configurable pending buffer count.
	// If the pending queue is full, we need to wait until it's consumed to avoid scanner overloading.
	if s.pendingQueue.Len() > 16 {
		return nil, s.sendingCh
	}
	return scanner.Chan(), s.sendingCh
}

func (s *scannerAdaptorImpl) handleUpstream(msg message.ImmutableMessage) {
	if msg.MessageType() == message.MessageTypeTimeTick {
		// If the time tick message incoming,
		// the reorder buffer can be consumed into a pending queue with latest timetick.
		s.pendingQueue.Add(s.reorderBuffer.PopUtilTimeTick(msg.TimeTick()))
		return
	}
	// Filtering the message if needed.
	if s.readOption.MessageFilter != nil && !s.readOption.MessageFilter(msg) {
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
