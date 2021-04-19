package test

import (
	"context"
	"log"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	msgpb "github.com/zilliztech/milvus-distributed/internal/proto/message"
)

func TestKey2Seg(t *testing.T) {
	// TODO: fix test
	return

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

	obj := msgpb.Key2SegMsg{}
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
