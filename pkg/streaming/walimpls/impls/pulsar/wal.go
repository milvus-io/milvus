package pulsar

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/helper"
)

var _ walimpls.WALImpls = (*walImpl)(nil)

type walImpl struct {
	*helper.WALHelper
	c pulsar.Client
	p pulsar.Producer
}

func (w *walImpl) WALName() string {
	return walName
}

func (w *walImpl) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	id, err := w.p.Send(ctx, &pulsar.ProducerMessage{
		Payload:    msg.Payload(),
		Properties: msg.Properties().ToRawMap(),
	})
	if err != nil {
		w.Log().RatedWarn(1, "send message to pulsar failed", zap.Error(err))
		return nil, err
	}
	return pulsarID{id}, nil
}

func (w *walImpl) Read(ctx context.Context, opt walimpls.ReadOption) (s walimpls.ScannerImpls, err error) {
	ch := make(chan pulsar.ReaderMessage, 1)
	readerOpt := pulsar.ReaderOptions{
		Topic:             w.Channel().Name,
		Name:              opt.Name,
		MessageChannel:    ch,
		ReceiverQueueSize: opt.ReadAheadBufferSize,
	}

	switch opt.DeliverPolicy.Policy() {
	case options.DeliverPolicyTypeAll:
		readerOpt.StartMessageID = pulsar.EarliestMessageID()
	case options.DeliverPolicyTypeLatest:
		readerOpt.StartMessageID = pulsar.LatestMessageID()
	case options.DeliverPolicyTypeStartFrom:
		readerOpt.StartMessageID = opt.DeliverPolicy.MessageID().(pulsarID).MessageID
		readerOpt.StartMessageIDInclusive = true
	case options.DeliverPolicyTypeStartAfter:
		readerOpt.StartMessageID = opt.DeliverPolicy.MessageID().(pulsarID).MessageID
		readerOpt.StartMessageIDInclusive = false
	}
	reader, err := w.c.CreateReader(readerOpt)
	if err != nil {
		return nil, err
	}
	return newScanner(opt.Name, reader), nil
}

func (w *walImpl) Close() {
	w.p.Close() // close all producer
}
