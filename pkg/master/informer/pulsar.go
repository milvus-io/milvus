package informer

import (
	"context"
	"fmt"
	"github.com/czs007/suvlim/conf"
	"log"
	"strconv"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/czs007/suvlim/pkg/master/mock"
)

func NewPulsarClient() PulsarClient {
	pulsarAddr := "pulsar://"
	pulsarAddr += conf.Config.Pulsar.Address
	pulsarAddr += ":"
	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               pulsarAddr,
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
		Topic:            conf.Config.Master.PulsarTopic,
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
		m, err := mock.SegmentUnMarshal(msg.Payload())
		if err != nil {
			log.Println("SegmentUnMarshal Failed")
		}
		//fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
		//	msg.ID(), m.SegementID)
		fmt.Println("Received SegmentStats -- segmentID:", m.SegementID,
			",memSize:", m.MemorySize, ",memRate:", m.MemoryRate, ",numRows:", m.Rows, ",status:", m.Status)
		ssChan <- m
		consumer.Ack(msg)
	}

	if err := consumer.Unsubscribe(); err != nil {
		log.Fatal(err)
	}
	return nil
}
