package proxy

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/message"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestTimeTick(t *testing.T) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
	assert.Nil(t,err)

	producer, err := client.CreateProducer(pulsar.ProducerOptions{Topic: "timesync"})
	assert.Nil(t,err)

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       "timesync",
		SubscriptionName:            "timesync_group",
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	assert.Nil(t,err)

	ctx, _ := context.WithTimeout(context.Background(), 4*time.Second)

	var curTs Timestamp
	curTs = 0
	tt := timeTick{
		interval:             200,
		pulsarProducer:       producer,
		peer_id:              1,
		ctx:                  ctx,
		areRequestsDelivered: func(ts Timestamp) bool { return true },
		getTimestamp: func() (Timestamp, pb.Status) {
			curTs = curTs + 100
			return curTs, pb.Status{ErrorCode: pb.ErrorCode_SUCCESS}
		},
	}
	tt.Restart()

	ctx2, _ := context.WithTimeout(context.Background(), time.Second*2)
	isbreak := false
	for {
		if isbreak {
			break
		}
		select {
		case <-ctx2.Done():
			isbreak = true
			break
		case cm, ok := <-consumer.Chan():
			if !ok {
				t.Fatalf("consumer closed")
			}
			consumer.AckID(cm.ID())
			break
		}
	}

	var lastTimestamp uint64 = 0
	for {
		select {
		case <-ctx.Done():
			return
		case cm, ok := <-consumer.Chan():
			if ok == false {
				return
			}
			msg := cm.Message
			var tsm pb.TimeSyncMsg
			if err := proto.Unmarshal(msg.Payload(), &tsm); err != nil {
				return
			}
			if tsm.Timestamp <= lastTimestamp {
				t.Fatalf("current = %d, last = %d", uint64(tsm.Timestamp), uint64(lastTimestamp))
			}
			t.Log("current = ", tsm.Timestamp)
			lastTimestamp = tsm.Timestamp
		}
	}
}
