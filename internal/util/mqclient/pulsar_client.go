package mqclient

import (
	"errors"
	"reflect"
	"unsafe"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/zilliztech/milvus-distributed/internal/log"
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

func (pc *pulsarClient) CreateProducer(options ProducerOptions) (Producer, error) {
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

func (pc *pulsarClient) Subscribe(options ConsumerOptions) (Consumer, error) {
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
	msgChannel := make(chan ConsumerMessage, 1)
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

func (pc *pulsarClient) EarliestMessageID() MessageID {
	msgID := pulsar.EarliestMessageID()
	return &pulsarID{messageID: msgID}
}

func (pc *pulsarClient) StringToMsgID(id string) (MessageID, error) {
	pID, err := StringToPulsarMsgID(id)
	if err != nil {
		return nil, err
	}
	return &pulsarID{messageID: pID}, nil
}

func (pc *pulsarClient) BytesToMsgID(id []byte) (MessageID, error) {
	pID, err := DeserializePulsarMsgID(id)
	if err != nil {
		return nil, err
	}
	return &pulsarID{messageID: pID}, nil
}

func (pc *pulsarClient) Close() {
	pc.client.Close()

	// This is a work around to avoid goroutinue leak of pulsar-client-go
	// https://github.com/apache/pulsar-client-go/issues/493
	// Very much unsafe, need to remove later
	f := reflect.ValueOf(pc.client).Elem().FieldByName("cnxPool")
	f = reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem()
	f.MethodByName("Close").Call(nil)
}
