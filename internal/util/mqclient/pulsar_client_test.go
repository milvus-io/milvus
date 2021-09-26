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
	"fmt"
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
		_, err = producer.Send(ctx, msg)
		assert.Nil(t, err)
		log.Info("Pub", zap.Any("SND", v))
	}

	log.Info("Produce done")
}

func VerifyMessage(t *testing.T, msg ConsumerMessage) {
	pload := BytesToInt(msg.Payload())
	log.Info("RECV", zap.Any("v", pload))
	pm := msg.(*pulsarMessage)
	topic := pm.Topic()
	assert.NotEmpty(t, topic)
	log.Info("RECV", zap.Any("t", topic))
	prop := pm.Properties()
	log.Info("RECV", zap.Any("p", len(prop)))
}

// Consume1 will consume random messages and record the last MessageID it received
func Consume1(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, subName string, c chan MessageID, total *int) {
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
			consumer.Ack(msg)
			VerifyMessage(t, msg)
			(*total)++
			//log.Debug("total", zap.Int("val", *total))
		}
	}
	c <- msg.ID()

	log.Info("Consume1 randomly RECV", zap.Any("number", cnt))
	log.Info("Consume1 done")
}

// Consume2 will consume messages from specified MessageID
func Consume2(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, subName string, msgID MessageID, total *int) {
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
	mm := <-consumer.Chan()
	consumer.Ack(mm)

	log.Info("Consume2 start")

	for {
		select {
		case <-ctx.Done():
			log.Info("Consume2 channel closed")
			return
		case msg := <-consumer.Chan():
			consumer.Ack(msg)
			VerifyMessage(t, msg)
			(*total)++
			//log.Debug("total", zap.Int("val", *total))
		}
	}
}

func Consume3(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, subName string, total *int) {
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

	log.Info("Consume3 start")

	for {
		select {
		case <-ctx.Done():
			log.Info("Consume3 channel closed")
			return
		case msg := <-consumer.Chan():
			consumer.Ack(msg)
			VerifyMessage(t, msg)
			(*total)++
			//log.Debug("total", zap.Int("val", *total))
		}
	}
}

func TestPulsarClient_Consume1(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	pc, err := GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
	defer pc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, pc)
	rand.Seed(time.Now().UnixNano())

	topic := fmt.Sprintf("test-topic-%d", rand.Int())
	subName := fmt.Sprintf("test-subname-%d", rand.Int())
	arr := []int{111, 222, 333, 444, 555, 666, 777}
	c := make(chan MessageID, 1)

	ctx, cancel := context.WithCancel(context.Background())

	var total1 int
	var total2 int
	var total3 int

	// launch produce
	Produce(ctx, t, pc, topic, arr)
	time.Sleep(100 * time.Millisecond)

	// launch consume1
	ctx1, cancel1 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel1()
	Consume1(ctx1, t, pc, topic, subName, c, &total1)

	// record the last received message id
	lastMsgID := <-c
	log.Info("msg", zap.Any("lastMsgID", lastMsgID))

	// launch consume2
	ctx2, cancel2 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel2()
	Consume2(ctx2, t, pc, topic, subName, lastMsgID, &total2)

	// launch consume3
	ctx3, cancel3 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel3()
	Consume3(ctx3, t, pc, topic, subName, &total3)

	// stop Consume2
	cancel()
	assert.Equal(t, len(arr), total1+total2)
	assert.Equal(t, len(arr), total3)

	log.Info("main done")
}

func Consume21(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, subName string, c chan MessageID, total *int) {
	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	log.Info("Consume1 start")

	// get random number between 1 ~ 5
	rand.Seed(time.Now().UnixNano())
	cnt := 1 + rand.Int()%5

	var msg pulsar.ConsumerMessage
	for i := 0; i < cnt; i++ {
		select {
		case <-ctx.Done():
			log.Info("Consume1 channel closed")
			return
		case msg = <-consumer.Chan():
			consumer.Ack(msg)
			v := BytesToInt(msg.Payload())
			log.Info("RECV", zap.Any("v", v))
			(*total)++
			//log.Debug("total", zap.Int("val", *total))
		}
	}
	c <- msg.ID()

	log.Info("Consume1 randomly RECV", zap.Any("number", cnt))
	log.Info("Consume1 done")
}

// Consume2 will consume messages from specified MessageID
func Consume22(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, subName string, msgID MessageID, total *int) {
	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	err = consumer.Seek(msgID)
	assert.Nil(t, err)

	// skip the last received message
	mm := <-consumer.Chan()
	consumer.Ack(mm)

	log.Info("Consume2 start")

	for {
		select {
		case <-ctx.Done():
			log.Info("Consume2 channel closed")
			return
		case msg := <-consumer.Chan():
			consumer.Ack(msg)
			v := BytesToInt(msg.Payload())
			log.Info("RECV", zap.Any("v", v))
			(*total)++
			//log.Debug("total", zap.Int("val", *total))
		}
	}
}

func Consume23(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, subName string, total *int) {
	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	log.Info("Consume3 start")

	for {
		select {
		case <-ctx.Done():
			log.Info("Consume3 channel closed")
			return
		case msg := <-consumer.Chan():
			consumer.Ack(msg)
			v := BytesToInt(msg.Payload())
			log.Info("RECV", zap.Any("v", v))
			(*total)++
			//log.Debug("total", zap.Int("val", *total))
		}
	}
}

func TestPulsarClient_Consume2(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	pc, err := GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
	defer pc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, pc)
	rand.Seed(time.Now().UnixNano())

	topic := fmt.Sprintf("test-topic-%d", rand.Int())
	subName := fmt.Sprintf("test-subname-%d", rand.Int())
	arr := []int{111, 222, 333, 444, 555, 666, 777}
	c := make(chan MessageID, 1)

	ctx, cancel := context.WithCancel(context.Background())

	var total1 int
	var total2 int
	var total3 int

	// launch produce
	Produce(ctx, t, pc, topic, arr)
	time.Sleep(100 * time.Millisecond)

	// launch consume1
	ctx1, cancel1 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel1()
	Consume21(ctx1, t, pc, topic, subName, c, &total1)

	// record the last received message id
	lastMsgID := <-c
	log.Info("msg", zap.Any("lastMsgID", lastMsgID))

	// launch consume2
	ctx2, cancel2 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel2()
	Consume22(ctx2, t, pc, topic, subName, lastMsgID, &total2)

	// launch consume3
	ctx3, cancel3 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel3()
	Consume23(ctx3, t, pc, topic, subName, &total3)

	// stop Consume2
	cancel()
	assert.Equal(t, len(arr), total1+total2)
	assert.Equal(t, 0, total3)

	log.Info("main done")
}

func TestPulsarClient_EarliestMessageID(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	client, _ := GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
	defer client.Close()

	mid := client.EarliestMessageID()
	assert.NotNil(t, mid)
}

func TestPulsarClient_StringToMsgID(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	client, _ := GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
	defer client.Close()

	mid := client.EarliestMessageID()
	str := PulsarMsgIDToString(mid)

	res, err := client.StringToMsgID(str)
	assert.Nil(t, err)
	assert.NotNil(t, res)

	str = "X"
	res, err = client.StringToMsgID(str)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}

func TestPulsarClient_BytesToMsgID(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	client, _ := GetPulsarClientInstance(pulsar.ClientOptions{URL: pulsarAddress})
	defer client.Close()

	mid := pulsar.EarliestMessageID()
	binary := SerializePulsarMsgID(mid)

	res, err := client.BytesToMsgID(binary)
	assert.Nil(t, err)
	assert.NotNil(t, res)

	invalidBin := []byte{0}
	res, err = client.BytesToMsgID(invalidBin)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}
