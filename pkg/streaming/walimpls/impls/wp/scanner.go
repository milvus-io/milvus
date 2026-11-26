package wp

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	woodpecker "github.com/zilliztech/woodpecker/woodpecker/log"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
)

var _ walimpls.ScannerImpls = (*scannerImpl)(nil)

type scannerImpl struct {
	*helper.ScannerHelper
	reader     woodpecker.LogReader
	msgChannel chan message.ImmutableMessage
	onClose    func()
	closeOnce  sync.Once
	closeErr   error
}

func newScanner(scannerName string, reader woodpecker.LogReader, onClose func()) *scannerImpl {
	s := &scannerImpl{
		ScannerHelper: helper.NewScannerHelper(scannerName),
		reader:        reader,
		msgChannel:    make(chan message.ImmutableMessage, 1),
		onClose:       onClose,
	}
	go s.executeConsumer()
	return s
}

func (s *scannerImpl) Chan() <-chan message.ImmutableMessage {
	return s.msgChannel
}

func (s *scannerImpl) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.ScannerHelper.Close()
		if s.closeErr != nil {
			mlog.Warn(s.Context(), "failed to close wp scanner", mlog.Err(s.closeErr))
		}
		s.closeErr = s.reader.Close(context.Background())
		if s.closeErr != nil {
			mlog.Warn(s.Context(), "failed to close wp reader", mlog.Err(s.closeErr))
		}
		if s.onClose != nil {
			s.onClose()
		}
	})
	return s.closeErr
}

func (s *scannerImpl) executeConsumer() {
	defer close(s.msgChannel)
	for {
		msg, err := s.reader.ReadNext(s.Context())
		if err != nil {
			// underlying mq may report ctx error, so we need to check the ctx error here to avoid return nil Error() without close.
			if s.Context().Err() != nil {
				s.Finish(nil)
				return
			}
			if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
				s.Finish(errors.Wrap(err, "wp readNext Timeout"))
				return
			}
			mlog.Error(s.Context(), "wp readNext msg exception", mlog.Err(err))
			s.Finish(err)
			return
		}
		newImmutableMessage := message.NewImmutableMesasge(
			wpID{msg.Id},
			msg.Payload,
			msg.Properties,
		)

		select {
		case <-s.Context().Done():
			s.Finish(nil)
			return
		case s.msgChannel <- newImmutableMessage:
		}
	}
}
