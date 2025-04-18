package wp

import (
	"context"

	"github.com/cockroachdb/errors"
	woodpecker "github.com/zilliztech/woodpecker/woodpecker/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/helper"
)

var _ walimpls.ScannerImpls = (*scannerImpl)(nil)

type scannerImpl struct {
	*helper.ScannerHelper
	reader     woodpecker.LogReader
	msgChannel chan message.ImmutableMessage
}

func newScanner(scannerName string, reader woodpecker.LogReader) *scannerImpl {
	s := &scannerImpl{
		ScannerHelper: helper.NewScannerHelper(scannerName),
		reader:        reader,
		msgChannel:    make(chan message.ImmutableMessage, 1),
	}
	go s.executeConsumer()
	return s
}

func (s *scannerImpl) Chan() <-chan message.ImmutableMessage {
	return s.msgChannel
}

func (s *scannerImpl) Close() error {
	err := s.ScannerHelper.Close()
	if err != nil {
		log.Ctx(s.Context()).Warn("failed to close wp scanner", zap.Error(err))
	}
	if s.reader != nil {
		err = s.reader.Close(context.Background())
		if err != nil {
			log.Ctx(s.Context()).Warn("failed to close wp reader", zap.Error(err))
		}
	}
	return err
}

func (s *scannerImpl) executeConsumer() {
	defer close(s.msgChannel)
	for {
		msg, err := s.reader.ReadNext(s.Context())
		if err != nil {
			if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
				s.Finish(nil)
				return
			}
			log.Ctx(s.Context()).Error("wp readNext msg exception", zap.Error(err))
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
