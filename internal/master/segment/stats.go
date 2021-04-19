package segment

import (
	"context"
	"fmt"
	"log"
	"github.com/golang/protobuf/proto"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/master/informer"
)

type SegmentStats struct {
	SegementID uint64
	MemorySize uint64
	MemoryRate float64
	Rows       int64
}


func Listener(ssChan chan internalpb.SegmentStatistics, pc informer.PulsarClient) error {
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

		var m internalpb.SegmentStatistics
		proto.Unmarshal(msg.Payload(), &m)
		fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
			msg.ID(), m.SegmentId)
		ssChan <- m
		consumer.Ack(msg)
	}

	if err := consumer.Unsubscribe(); err != nil {
		log.Fatal(err)
	}
	return nil
}
