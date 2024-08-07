package rmq

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/mqimpl/rocksmq/client"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/helper"
)

const defaultReadAheadBufferSize = 1024

var _ walimpls.WALImpls = (*walImpl)(nil)

// walImpl is the implementation of walimpls.WAL interface.
type walImpl struct {
	*helper.WALHelper
	p client.Producer
	c client.Client
}

func (w *walImpl) WALName() string {
	return walName
}

// Append appends a message to the wal.
func (w *walImpl) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	id, err := w.p.Send(&common.ProducerMessage{
		Payload:    msg.Payload(),
		Properties: msg.Properties().ToRawMap(),
	})
	if err != nil {
		w.Log().RatedWarn(1, "send message to rmq failed", zap.Error(err))
		return nil, err
	}
	return rmqID(id), nil
}

// Read create a scanner to read the wal.
func (w *walImpl) Read(ctx context.Context, opt walimpls.ReadOption) (s walimpls.ScannerImpls, err error) {
	scannerName := opt.Name
	if opt.ReadAheadBufferSize == 0 {
		opt.ReadAheadBufferSize = defaultReadAheadBufferSize
	}
	receiveChannel := make(chan common.Message, opt.ReadAheadBufferSize)
	consumerOption := client.ConsumerOptions{
		Topic:                       w.Channel().Name,
		SubscriptionName:            scannerName,
		SubscriptionInitialPosition: common.SubscriptionPositionUnknown,
		MessageChannel:              receiveChannel,
	}
	switch opt.DeliverPolicy.GetPolicy().(type) {
	case *streamingpb.DeliverPolicy_All:
		consumerOption.SubscriptionInitialPosition = common.SubscriptionPositionEarliest
	case *streamingpb.DeliverPolicy_Latest:
		consumerOption.SubscriptionInitialPosition = common.SubscriptionPositionLatest
	}

	// Subscribe the MQ consumer.
	consumer, err := w.c.Subscribe(consumerOption)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			// release the subscriber if following operation is failure.
			// to avoid resource leak.
			consumer.Close()
		}
	}()

	// Seek the MQ consumer.
	var exclude *rmqID
	switch t := opt.DeliverPolicy.GetPolicy().(type) {
	case *streamingpb.DeliverPolicy_StartFrom:
		id, err := unmarshalMessageID(t.StartFrom.GetId())
		if err != nil {
			return nil, err
		}
		// Do a inslusive seek.
		if err = consumer.Seek(int64(id)); err != nil {
			return nil, err
		}
	case *streamingpb.DeliverPolicy_StartAfter:
		id, err := unmarshalMessageID(t.StartAfter.GetId())
		if err != nil {
			return nil, err
		}
		exclude = &id
		if err = consumer.Seek(int64(id)); err != nil {
			return nil, err
		}
	}
	return newScanner(scannerName, exclude, consumer), nil
}

// Close closes the wal.
func (w *walImpl) Close() {
	w.p.Close() // close all producer
}
