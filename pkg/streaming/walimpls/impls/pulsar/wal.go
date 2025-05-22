package pulsar

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/helper"
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
	if w.Channel().AccessMode != types.AccessModeRW {
		panic("write on a wal that is not in read-write mode")
	}
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

	switch t := opt.DeliverPolicy.GetPolicy().(type) {
	case *streamingpb.DeliverPolicy_All:
		readerOpt.StartMessageID = pulsar.EarliestMessageID()
	case *streamingpb.DeliverPolicy_Latest:
		readerOpt.StartMessageID = pulsar.LatestMessageID()
	case *streamingpb.DeliverPolicy_StartFrom:
		id, err := unmarshalMessageID(t.StartFrom.GetId())
		if err != nil {
			return nil, err
		}
		readerOpt.StartMessageID = id
		readerOpt.StartMessageIDInclusive = true
	case *streamingpb.DeliverPolicy_StartAfter:
		id, err := unmarshalMessageID(t.StartAfter.GetId())
		if err != nil {
			return nil, err
		}
		readerOpt.StartMessageID = id
		readerOpt.StartMessageIDInclusive = false
	}
	reader, err := w.c.CreateReader(readerOpt)
	if err != nil {
		return nil, err
	}
	return newScanner(opt.Name, reader), nil
}

func (w *walImpl) Truncate(ctx context.Context, id message.MessageID) error {
	if w.Channel().AccessMode != types.AccessModeRW {
		panic("truncate on a wal that is not in read-write mode")
	}
	cursor, err := w.c.Subscribe(pulsar.ConsumerOptions{
		Topic:                    w.Channel().Name,
		SubscriptionName:         truncateCursorSubscriptionName,
		Type:                     pulsar.Exclusive,
		MaxPendingChunkedMessage: 0,
		StartMessageIDInclusive:  true,
	})
	if err != nil {
		return err
	}
	defer cursor.Close()
	return cursor.Seek(id.(pulsarID).PulsarID())
}

func (w *walImpl) Close() {
	if w.p != nil {
		w.p.Close() // close producer
	}
}
