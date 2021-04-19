package test

import (
	"context"
	"github.com/apache/pulsar/pulsar-client-go/pulsar"
	"github.com/czs007/suvlim/writer/pb"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

func TestKey2Seg(t *testing.T) {
	lookupUrl := "pulsar://localhost:6650"
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: lookupUrl,
	})
	if err != nil {
		log.Fatal(err)
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            "Key2Seg",
		SubscriptionName: "sub-1",
	})

	obj := pb.Key2SegMsg{}
	msg, err := consumer.Receive(context.Background())
	proto.Unmarshal(msg.Payload(), &obj)
	assert.Equal(t, obj.Uid, int64(0))
	consumer.Ack(msg)
	msg, err = consumer.Receive(context.Background())
	proto.Unmarshal(msg.Payload(), &obj)
	assert.Equal(t, obj.Uid, int64(0))
	consumer.Ack(msg)
	consumer.Close()
	client.Close()
}
