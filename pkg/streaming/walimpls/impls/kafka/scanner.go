package kafka

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/helper"
)

var _ walimpls.ScannerImpls = (*scannerImpl)(nil)

// newScanner creates a new scanner.
func newScanner(scannerName string, exclude *kafkaID, consumer *kafka.Consumer) *scannerImpl {
	s := &scannerImpl{
		ScannerHelper: helper.NewScannerHelper(scannerName),
		consumer:      consumer,
		msgChannel:    make(chan message.ImmutableMessage, 1),
		exclude:       exclude,
	}
	go s.executeConsume()
	return s
}

// scannerImpl is the implementation of ScannerImpls for kafka.
type scannerImpl struct {
	*helper.ScannerHelper
	consumer   *kafka.Consumer
	msgChannel chan message.ImmutableMessage
	exclude    *kafkaID
}

// Chan returns the channel of message.
func (s *scannerImpl) Chan() <-chan message.ImmutableMessage {
	return s.msgChannel
}

// Close the scanner, release the underlying resources.
// Return the error same with `Error`
func (s *scannerImpl) Close() error {
	s.consumer.Unassign()
	err := s.ScannerHelper.Close()
	s.consumer.Close()
	return err
}

func (s *scannerImpl) executeConsume() {
	defer close(s.msgChannel)
	for {
		msg, err := s.consumer.ReadMessage(200 * time.Millisecond)
		if err != nil {
			if s.Context().Err() != nil {
				// context canceled, means the the scanner is closed.
				s.Finish(nil)
				return
			}
			if c, ok := err.(kafka.Error); ok && c.Code() == kafka.ErrTimedOut {
				continue
			}
			s.Finish(err)
			return
		}
		messageID := kafkaID(msg.TopicPartition.Offset)
		if s.exclude != nil && messageID.EQ(*s.exclude) {
			// Skip the message that is exclude for StartAfter semantics.
			continue
		}

		properties := make(map[string]string, len(msg.Headers))
		for _, header := range msg.Headers {
			properties[header.Key] = string(header.Value)
		}

		newImmutableMessage := message.NewImmutableMesasge(
			messageID,
			msg.Value,
			properties,
		)
		select {
		case <-s.Context().Done():
			s.Finish(nil)
			return
		case s.msgChannel <- newImmutableMessage:
		}
	}
}
