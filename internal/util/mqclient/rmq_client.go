package mqclient

import (
	"strconv"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/util/rocksmq/client/rocksmq"
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

func (rc *rmqClient) CreateProducer(options ProducerOptions) (Producer, error) {
	rmqOpts := rocksmq.ProducerOptions{Topic: options.Topic}
	pp, err := rc.client.CreateProducer(rmqOpts)
	if err != nil {
		return nil, err
	}
	rp := rmqProducer{p: pp}
	return &rp, nil
}

func (rc *rmqClient) Subscribe(options ConsumerOptions) (Consumer, error) {
	receiveChannel := make(chan rocksmq.ConsumerMessage, options.BufSize)

	cli, err := rc.client.Subscribe(rocksmq.ConsumerOptions{
		Topic:            options.Topic,
		SubscriptionName: options.SubscriptionName,
		MessageChannel:   receiveChannel,
	})
	if err != nil {
		return nil, err
	}

	msgChannel := make(chan ConsumerMessage, 1)
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

func (rc *rmqClient) EarliestMessageID() MessageID {
	rID := rocksmq.EarliestMessageID()
	return &rmqID{messageID: rID}
}

func (rc *rmqClient) StringToMsgID(id string) (MessageID, error) {
	rID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, err
	}
	return &rmqID{messageID: rID}, nil
}

func (rc *rmqClient) BytesToMsgID(id []byte) (MessageID, error) {
	rID, err := DeserializeRmqID(id)
	if err != nil {
		return nil, err
	}
	return &rmqID{messageID: rID}, nil
}

func (rc *rmqClient) Close() {
	rc.client.Close()
}
