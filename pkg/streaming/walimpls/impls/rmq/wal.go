package rmq

import (
	"context"

	"github.com/cockroachdb/errors"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/mqimpl/rocksmq/client"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const defaultReadAheadBufferSize = 1024

var _ walimpls.WALImpls = (*walImpl)(nil)

// walImpl is the implementation of walimpls.WAL interface.
type walImpl struct {
	*helper.WALHelper
	p client.Producer
	c client.Client
}

func (w *walImpl) WALName() message.WALName {
	return message.WALNameRocksmq
}

// Append appends a message to the wal.
func (w *walImpl) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	if w.Channel().AccessMode != types.AccessModeRW {
		panic("write on a wal that is not in read-write mode")
	}
	pb := msg.IntoMessageProto()
	id, err := w.p.SendForStreamingService(&common.ProducerMessage{
		Payload:    pb.Payload,
		Properties: pb.Properties,
	})
	if err != nil {
		w.Log().RatedWarn(ctx, rate.Limit(1), "send message to rmq failed", mlog.Err(err))
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
		if err = w.seek(ctx, consumer, int64(id)); err != nil {
			return nil, err
		}
	case *streamingpb.DeliverPolicy_StartAfter:
		id, err := unmarshalMessageID(t.StartAfter.GetId())
		if err != nil {
			return nil, err
		}
		exclude = &id
		if err = w.seek(ctx, consumer, int64(id)); err != nil {
			return nil, err
		}
	}
	return newScanner(scannerName, exclude, consumer), nil
}

func (w *walImpl) seek(ctx context.Context, consumer client.Consumer, id int64) error {
	err := consumer.Seek(id)
	if err == nil || w.Channel().AccessMode == types.AccessModeRO || !errors.Is(err, merr.ErrMqTopicNotFound) {
		return err
	}

	// A read-write RocksMQ reader keeps the pre-existing recovery behavior when
	// retention has removed its checkpoint. Read-only readers must
	// return the typed error because resetting them could skip the AlterWAL
	// boundary needed to continue the logical stream.
	w.Log().Warn(ctx, "RocksMQ position is unavailable on read-write WAL, reset reader to earliest",
		mlog.Int64("messageID", id),
		mlog.Err(err))
	if resetErr := consumer.Seek(client.EarliestMessageID()); resetErr != nil {
		return merr.Wrap(resetErr, "reset RocksMQ reader to earliest")
	}
	return nil
}

func (w *walImpl) Truncate(ctx context.Context, id message.MessageID) error {
	if w.Channel().AccessMode != types.AccessModeRW {
		panic("truncate on a wal that is not in read-write mode")
	}
	return nil
}

// Close closes the wal.
func (w *walImpl) Close() {
	if w.p != nil {
		w.p.Close() // close all producer
	}
}
