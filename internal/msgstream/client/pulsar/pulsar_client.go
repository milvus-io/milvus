package pulsar

import (
	"errors"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/client"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"go.uber.org/zap"
)

type pulsarClient struct {
	client pulsar.Client
}

func NewPulsarClient(opts pulsar.ClientOptions) (*pulsarClient, error) {
	c, err := pulsar.NewClient(opts)
	if err != nil {
		log.Error("Set pulsar client failed, error", zap.Error(err))
		return nil, err
	}
	cli := &pulsarClient{client: c}
	return cli, nil
}

func (pc *pulsarClient) CreateProducer(options client.ProducerOptions) (client.Producer, error) {
	opts := pulsar.ProducerOptions{Topic: options.Topic}
	pp, err := pc.client.CreateProducer(opts)
	if err != nil {
		return nil, err
	}
	if pp == nil {
		return nil, errors.New("pulsar is not ready, producer is nil")
	}
	producer := &pulsarProducer{p: pp}
	return producer, nil
}

func (pc *pulsarClient) Subscribe(options client.ConsumerOptions) (client.Consumer, error) {
	receiveChannel := make(chan pulsar.ConsumerMessage, options.BufSize)
	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       options.Topic,
		SubscriptionName:            options.SubscriptionName,
		Type:                        pulsar.SubscriptionType(options.Type),
		SubscriptionInitialPosition: pulsar.SubscriptionInitialPosition(options.SubscriptionInitialPosition),
		MessageChannel:              receiveChannel,
	})
	if err != nil {
		return nil, err
	}
	msgChannel := make(chan client.ConsumerMessage, 1)
	pConsumer := &pulsarConsumer{c: consumer, msgChannel: msgChannel}

	go func() {
		for { //nolint:gosimple
			select {
			case msg, ok := <-pConsumer.c.Chan():
				if !ok {
					close(msgChannel)
					return
				}
				msgChannel <- &pulsarMessage{msg: msg}
			}
		}
	}()

	return pConsumer, nil
}

func (pc *pulsarClient) EarliestMessageID() client.MessageID {
	msgID := pulsar.EarliestMessageID()
	return &pulsarID{messageID: msgID}
}

func (pc *pulsarClient) StringToMsgID(id string) (client.MessageID, error) {
	pID, err := typeutil.StringToPulsarMsgID(id)
	if err != nil {
		return nil, err
	}
	return &pulsarID{messageID: pID}, nil
}

func (pc *pulsarClient) Close() {
	pc.client.Close()
}
