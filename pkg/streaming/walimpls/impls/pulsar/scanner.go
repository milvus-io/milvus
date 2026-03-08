package pulsar

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/helper"
)

var _ walimpls.ScannerImpls = (*scannerImpl)(nil)

func newScanner(
	scannerName string,
	reader pulsar.Reader,
) *scannerImpl {
	s := &scannerImpl{
		ScannerHelper: helper.NewScannerHelper(scannerName),
		reader:        reader,
		msgChannel:    make(chan message.ImmutableMessage, 1),
	}
	go s.executeConsume()
	return s
}

type scannerImpl struct {
	*helper.ScannerHelper
	reader     pulsar.Reader
	msgChannel chan message.ImmutableMessage
}

// Chan returns the channel of message.
func (s *scannerImpl) Chan() <-chan message.ImmutableMessage {
	return s.msgChannel
}

// Close the scanner, release the underlying resources.
// Return the error same with `Error`
func (s *scannerImpl) Close() error {
	err := s.ScannerHelper.Close()
	s.reader.Close()
	return err
}

func (s *scannerImpl) executeConsume() {
	defer close(s.msgChannel)
	for {
		msg, err := s.reader.Next(s.Context())
		if err != nil {
			// underlying mq may report ctx error, so we need to check the ctx error here to avoid return nil Error() without close.
			if s.Context().Err() != nil {
				s.Finish(nil)
				return
			}
			if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
				s.Finish(errors.Wrap(err, "pulsar readNext timeout"))
				return
			}
			s.Finish(err)
			return
		}
		newImmutableMessage := message.NewImmutableMesasge(
			pulsarID{msg.ID()},
			msg.Payload(),
			msg.Properties(),
		)

		select {
		case <-s.Context().Done():
			s.Finish(nil)
			return
		case s.msgChannel <- newImmutableMessage:
		}
	}
}
