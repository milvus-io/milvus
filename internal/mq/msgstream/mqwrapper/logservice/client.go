package logservice

import (
	"context"
	"errors"

	"github.com/milvus-io/milvus/internal/logservice"
	"github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/options"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper/kafka"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper/nmq"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper/pulsar"
	"go.uber.org/zap"
)

var _ mqwrapper.Client = (*logServiceClient)(nil)

func NewLogServiceClient(client logservice.Client) mqwrapper.Client {
	return &logServiceClient{
		cli:    client,
		mqType: util.MustSelectMQType(),
	}
}

// logServiceClient wraps the true logservice client to mq.
// TODO: Should be removed in future.
type logServiceClient struct {
	cli    logservice.Client
	mqType string // TODO: remove in future.
}

// CreateProducer creates a producer instance
func (c *logServiceClient) CreateProducer(opt mqwrapper.ProducerOptions) (mqwrapper.Producer, error) {
	if err := c.cli.ChannelManagement().CreatePChannel(context.TODO(), opt.Topic); err != nil {
		if st := status.AsLogError(err); st.Code != logpb.LogCode_LOG_CODE_CHANNEL_EXIST {
			return nil, err
		}
	}

	p := c.cli.CreateProducer(&options.ProducerOptions{
		Channel: opt.Topic,
	})
	return &producer{
		producer: p,
	}, nil
}

// Subscribe creates a consumer instance and subscribe a topic
func (c *logServiceClient) Subscribe(opt mqwrapper.ConsumerOptions) (mqwrapper.Consumer, error) {
	if err := c.cli.ChannelManagement().CreatePChannel(context.TODO(), opt.Topic); err != nil {
		if st := status.AsLogError(err); st.Code != logpb.LogCode_LOG_CODE_CHANNEL_EXIST {
			return nil, err
		}
	}

	if opt.BufSize == 0 {
		err := errors.New("subscription bufSize of log service should never be zero")
		log.Warn("unexpected subscription consumer options", zap.Error(err))
		return nil, err
	}

	// Select start message id.
	deliverPolicy := options.DeliverAll()
	switch opt.SubscriptionInitialPosition {
	case mqwrapper.SubscriptionPositionEarliest:
		deliverPolicy = options.DeliverAll()
	case mqwrapper.SubscriptionPositionLatest:
		deliverPolicy = options.DeliverLatest()
	}

	// TODO: opt.SubscriptionInitialPosition not used.
	consumer := &consumer{
		client:           c.cli,
		channelName:      opt.Topic,
		subscriptionName: opt.SubscriptionName,
		deliverPolicy:    deliverPolicy,
		bufSize:          int(opt.BufSize),
	}
	return consumer, nil
}

// Deserialize MessageId from a byte array
func (c *logServiceClient) BytesToMsgID(data []byte) (mqwrapper.MessageID, error) {
	switch c.mqType {
	case util.MQTypeNatsmq:
		return nmq.BytesToMsgID(data)
	case util.MQTypeRocksmq:
		id := server.DeserializeRmqID(data)
		return &server.RmqID{MessageID: id}, nil
	case util.MQTypeKafka:
		return kafka.BytesToMsgID(data)
	case util.MQTypePulsar:
		return pulsar.BytesToMsgID(data)
	default:
		panic("unreachable mq type")
	}
}

// Close the client and free associated resources
func (c *logServiceClient) Close() {
	// TODO: Bad implementation of outside msgstream ( recreate underlying client every new msgstream is created ).
	// We use a client singleton here, and refactor the outside msgstream in future.
	// However, the client can never be close until the milvus node exit.
	// c.cli.Close()
}
