package informer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/czs007/suvlim/pkg/master/common"
	"github.com/czs007/suvlim/pkg/master/mock"
)

func NewPulsarClient() PulsarClient {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               common.PULSAR_URL,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	return PulsarClient{
		Client: client,
	}
}

type PulsarClient struct {
	Client pulsar.Client
}

func (pc PulsarClient) Listener(ssChan chan mock.SegmentStats) error {
	consumer, err := pc.Client.Subscribe(pulsar.ConsumerOptions{
		Topic:            common.PULSAR_TOPIC,
		SubscriptionName: "my-sub",
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	for {
		msg, err := consumer.Receive(context.TODO())
		if err != nil {
			log.Fatal(err)
		}
		m, _ := mock.SegmentUnMarshal(msg.Payload())
		fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
			msg.ID(), m.SegementID)
		ssChan <- m
		consumer.Ack(msg)
	}

	if err := consumer.Unsubscribe(); err != nil {
		log.Fatal(err)
	}
	return nil
}
