package pulsar

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var _ walimpls.WALImpls = (*walImpl)(nil)

type walImpl struct {
	*helper.WALHelper
	c                  pulsar.Client
	p                  *syncutil.Future[pulsar.Producer]
	notifier           *syncutil.AsyncTaskNotifier[struct{}]
	backlogClearHelper *backlogClearHelper
}

// initProducerAtBackground initializes the producer at background.
func (w *walImpl) initProducerAtBackground() {
	if w.Channel().AccessMode != types.AccessModeRW {
		w.notifier.Finish(struct{}{})
		return
	}

	defer w.notifier.Finish(struct{}{})
	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 10 * time.Millisecond
	backoff.MaxInterval = 10 * time.Second
	backoff.MaxElapsedTime = 0
	backoff.Reset()

	for {
		if err := w.initProducer(); err == nil {
			return
		}
		select {
		case <-time.After(backoff.NextBackOff()):
			continue
		case <-w.notifier.Context().Done():
			return
		}
	}
}

// initProducer initializes the producer.
func (w *walImpl) initProducer() error {
	p, err := w.c.CreateProducer(pulsar.ProducerOptions{
		Topic: w.Channel().Name,
		// TODO: current go pulsar client does not support fencing, we should enable it after go pulsar client supports it.
		// ProducerAccessMode: pulsar.ProducerAccessModeExclusiveWithFencing,
	})
	if err != nil {
		w.Log().Warn("create producer failed", zap.Error(err))
		return err
	}
	w.Log().Info("pulsar create producer done")
	w.p.Set(p)
	return nil
}

func (w *walImpl) WALName() message.WALName {
	return message.WALNamePulsar
}

func (w *walImpl) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	if w.Channel().AccessMode != types.AccessModeRW {
		panic("write on a wal that is not in read-write mode")
	}
	p, err := w.p.GetWithContext(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get producer from future")
	}
	pb := msg.IntoMessageProto()
	id, err := p.Send(ctx, &pulsar.ProducerMessage{
		Payload:    pb.Payload,
		Properties: pb.Properties,
	})
	if w.backlogClearHelper != nil {
		// Observe the append traffic even if the message is not sent successfully.
		// Because if the write is failed, the message may be already written to the pulsar topic.
		w.backlogClearHelper.ObserveAppend(msg.EstimateSize())
	}
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
	if w.backlogClearHelper != nil {
		// if the backlog clear helper is enabled, the truncate make no sense, skip it.
		return nil
	}
	cursor, err := w.c.Subscribe(pulsar.ConsumerOptions{
		Topic:                    w.Channel().Name,
		SubscriptionName:         truncateCursorSubscriptionName,
		Type:                     pulsar.Exclusive,
		MaxPendingChunkedMessage: 1, // We cannot set it to 0, because the 0 means 100.
		ReceiverQueueSize:        1, // We cannot set it to 0, because the 0 means 1000.
		StartMessageIDInclusive:  true,
	})
	if err != nil {
		return err
	}
	defer cursor.Close()
	return cursor.Seek(id.(pulsarID).PulsarID())
}

func (w *walImpl) Close() {
	w.notifier.Cancel()
	w.notifier.BlockUntilFinish()
	// close producer if it is initialized
	if w.p.Ready() {
		w.p.Get().Close()
	}
	if w.backlogClearHelper != nil {
		w.backlogClearHelper.Close()
	}
}
