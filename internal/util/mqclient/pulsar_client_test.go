// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package mqclient

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/rand"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/stretchr/testify/assert"
)

func IntToBytes(n int) []byte {
	tmp := int32(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, tmp)
	return bytesBuffer.Bytes()
}

func BytesToInt(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)
	var tmp int32
	binary.Read(bytesBuffer, binary.BigEndian, &tmp)
	return int(tmp)
}

func Produce(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, arr []int) {
	producer, err := pc.CreateProducer(ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	log.Info("Produce start")

	for _, v := range arr {
		msg := &ProducerMessage{
			Payload:    IntToBytes(v),
			Properties: map[string]string{},
		}
		err = producer.Send(ctx, msg)
		assert.Nil(t, err)
		log.Info("Pub", zap.Any("SND", v))
	}

	log.Info("Produce done")
}

// Consume1 will consume random messages and record the last MessageID it received
func Consume1(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, subName string, c chan MessageID) {
	consumer, err := pc.Subscribe(ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		Type:                        KeyShared,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	log.Info("Consume1 start")

	// get random number between 1 ~ 5
	rand.Seed(time.Now().UnixNano())
	cnt := 1 + rand.Int()%5

	var msg ConsumerMessage
	for i := 0; i < cnt; i++ {
		select {
		case <-ctx.Done():
			log.Info("Consume1 channel closed")
			return
		case msg = <-consumer.Chan():
			//consumer.Ack(msg)
			v := BytesToInt(msg.Payload())
			log.Info("RECV", zap.Any("v", v))
		}
	}
	c <- msg.ID()

	log.Info("Consume1 randomly RECV", zap.Any("number", cnt))
	log.Info("Consume1 done")
}

// Consume2 will consume messages from specified MessageID
func Consume2(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, subName string, msgID MessageID) {
	consumer, err := pc.Subscribe(ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		Type:                        KeyShared,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	err = consumer.Seek(msgID)
	assert.Nil(t, err)

	// skip the last received message
	<-consumer.Chan()

	log.Info("Consume2 start")

	for {
		select {
		case <-ctx.Done():
			log.Info("Consume2 channel closed")
			return
		case msg := <-consumer.Chan():
			//consumer.Ack(msg)
			v := BytesToInt(msg.Payload())
			log.Info("RECV", zap.Any("v", v))
		}
	}
}

func TestPulsarClient(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	pc, err := NewPulsarClient(pulsar.ClientOptions{URL: pulsarAddress})
	defer pc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, pc)

	topic := "test"
	subName := "subName"
	arr := []int{111, 222, 333, 444, 555, 666, 777}
	c := make(chan MessageID)

	ctx, cancel := context.WithCancel(context.Background())

	// launch consume1
	go Consume1(ctx, t, pc, topic, subName, c)
	time.Sleep(1 * time.Second)

	// launch produce
	go Produce(ctx, t, pc, topic, arr)
	time.Sleep(1 * time.Second)

	// record the last received message id
	lastMsgID := <-c
	log.Info("msg", zap.Any("lastMsgID", lastMsgID))

	// launch consume2
	go Consume2(ctx, t, pc, topic, subName, lastMsgID)
	time.Sleep(1 * time.Second)

	// stop Consume2
	cancel()

	log.Info("main done")
}
