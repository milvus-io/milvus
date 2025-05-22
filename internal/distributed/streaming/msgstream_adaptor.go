package streaming

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

var (
	_ msgstream.Factory   = (*delegatorMsgstreamFactory)(nil)
	_ msgstream.MsgStream = (*delegatorMsgstreamAdaptor)(nil)
)

// NewDelegatorMsgstreamFactory returns a streaming-based msgstream factory for delegator.
func NewDelegatorMsgstreamFactory() msgstream.Factory {
	return &delegatorMsgstreamFactory{}
}

// Only for delegator.
type delegatorMsgstreamFactory struct{}

func (f *delegatorMsgstreamFactory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	panic("should never be called")
}

func (f *delegatorMsgstreamFactory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return &delegatorMsgstreamAdaptor{}, nil
}

func (f *delegatorMsgstreamFactory) NewMsgStreamDisposer(ctx context.Context) func([]string, string) error {
	panic("should never be called")
}

// Only for delegator.
type delegatorMsgstreamAdaptor struct {
	scanner Scanner
	ch      <-chan *msgstream.ConsumeMsgPack
}

func (m *delegatorMsgstreamAdaptor) Close() {
	if m.scanner != nil {
		m.scanner.Close()
	}
}

func (m *delegatorMsgstreamAdaptor) AsProducer(ctx context.Context, channels []string) {
	panic("should never be called")
}

func (m *delegatorMsgstreamAdaptor) Produce(context.Context, *msgstream.MsgPack) error {
	panic("should never be called")
}

func (m *delegatorMsgstreamAdaptor) SetRepackFunc(repackFunc msgstream.RepackFunc) {
	panic("should never be called")
}

func (m *delegatorMsgstreamAdaptor) GetProduceChannels() []string {
	panic("should never be called")
}

func (m *delegatorMsgstreamAdaptor) Broadcast(context.Context, *msgstream.MsgPack) (map[string][]msgstream.MessageID, error) {
	panic("should never be called")
}

func (m *delegatorMsgstreamAdaptor) AsConsumer(ctx context.Context, channels []string, subName string, position common.SubscriptionInitialPosition) error {
	// always ignored.
	if position != common.SubscriptionPositionUnknown {
		panic("should never be called")
	}
	return nil
}

func (m *delegatorMsgstreamAdaptor) Chan() <-chan *msgstream.ConsumeMsgPack {
	if m.ch == nil {
		panic("should never be called if seek is not done")
	}
	return m.ch
}

func (m *delegatorMsgstreamAdaptor) GetUnmarshalDispatcher() msgstream.UnmarshalDispatcher {
	return adaptor.UnmashalerDispatcher
}

func (m *delegatorMsgstreamAdaptor) Seek(ctx context.Context, msgPositions []*msgstream.MsgPosition, includeCurrentMsg bool) error {
	if len(msgPositions) != 1 {
		panic("should never be called if len(msgPositions) is not 1")
	}
	position := msgPositions[0]
	startFrom := adaptor.MustGetMessageIDFromMQWrapperIDBytes(WAL().WALName(), position.MsgID)
	log.Info(
		"delegator msgstream adaptor seeks from position with scanner",
		zap.String("channel", position.GetChannelName()),
		zap.Any("startFromMessageID", startFrom),
		zap.Uint64("timestamp", position.GetTimestamp()),
	)
	handler := adaptor.NewMsgPackAdaptorHandler()
	pchannel := funcutil.ToPhysicalChannel(position.GetChannelName())
	m.scanner = WAL().Read(ctx, ReadOption{
		PChannel:      pchannel,
		DeliverPolicy: options.DeliverPolicyStartFrom(startFrom),
		DeliverFilters: []options.DeliverFilter{
			// only consume messages with timestamp >= position timestamp
			options.DeliverFilterTimeTickGTE(position.GetTimestamp()),
			// only consume insert and delete messages
			options.DeliverFilterMessageType(message.MessageTypeInsert, message.MessageTypeDelete, message.MessageTypeSchemaChange),
		},
		MessageHandler: handler,
	})
	m.ch = handler.Chan()
	return nil
}

func (m *delegatorMsgstreamAdaptor) GetLatestMsgID(channel string) (msgstream.MessageID, error) {
	panic("should never be called")
}

func (m *delegatorMsgstreamAdaptor) CheckTopicValid(channel string) error {
	panic("should never be called")
}

func (m *delegatorMsgstreamAdaptor) ForceEnableProduce(can bool) {
	panic("should never be called")
}
