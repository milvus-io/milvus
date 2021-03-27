package rocksmq

import (
	"strconv"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/client"
	"github.com/zilliztech/milvus-distributed/internal/util/rocksmq/client/rocksmq"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type rmqClient struct {
	client rocksmq.Client
}

func NewRmqClient(opts rocksmq.ClientOptions) (*rmqClient, error) {
	c, err := rocksmq.NewClient(opts)
	if err != nil {
		log.Error("Set rmq client failed, error", zap.Error(err))
		return nil, err
	}
	return &rmqClient{client: c}, nil
}

func (rc *rmqClient) CreateProducer(options client.ProducerOptions) (client.Producer, error) {
	rmqOpts := rocksmq.ProducerOptions{Topic: options.Topic}
	pp, err := rc.client.CreateProducer(rmqOpts)
	if err != nil {
		return nil, err
	}
	rp := rmqProducer{p: pp}
	return &rp, nil
}

func (rc *rmqClient) Subscribe(options client.ConsumerOptions) (client.Consumer, error) {
	receiveChannel := make(chan rocksmq.ConsumerMessage, options.BufSize)

	cli, err := rc.client.Subscribe(rocksmq.ConsumerOptions{
		Topic:            options.Topic,
		SubscriptionName: options.SubscriptionName,
		MessageChannel:   receiveChannel,
	})
	if err != nil {
		return nil, err
	}

	msgChannel := make(chan client.ConsumerMessage, 1)
	rConsumer := &rmqConsumer{c: cli, msgChannel: msgChannel}

	go func() {
		for { //nolint:gosimple
			select {
			case msg, ok := <-rConsumer.c.Chan():
				if !ok {
					close(msgChannel)
					return
				}
				msg.Topic = options.Topic
				msgChannel <- &rmqMessage{msg: msg}
			}
		}
	}()
	return rConsumer, nil
}

func (rc *rmqClient) EarliestMessageID() client.MessageID {
	rID := rocksmq.EarliestMessageID()
	return &rmqID{messageID: rID}
}

func (rc *rmqClient) StringToMsgID(id string) (client.MessageID, error) {
	rID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, err
	}
	return &rmqID{messageID: rID}, nil
}

func (rc *rmqClient) BytesToMsgID(id []byte) (client.MessageID, error) {
	rID, err := typeutil.DeserializeRmqID(id)
	if err != nil {
		return nil, err
	}
	return &rmqID{messageID: rID}, nil
}

func (rc *rmqClient) Close() {
	rc.client.Close()
}
