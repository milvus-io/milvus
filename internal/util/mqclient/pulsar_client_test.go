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
	"github.com/prometheus/common/log"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
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

func Produce(t *testing.T, pc *pulsarClient, topic string, arr []int) {
	ctx := context.Background()
	producer, err := pc.CreateProducer(ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	for _, v := range arr {
		msg := &ProducerMessage{
			Payload:    IntToBytes(v),
			Properties: map[string]string{},
		}
		err = producer.Send(ctx, msg)
		assert.Nil(t, err)
		log.Infof("SEND v = %d", v)
	}

	log.Infof("cydrain produce done")
}

// Consume1 will consume random messages and record the MessageID where it fails
func Consume1(t *testing.T, ctx context.Context, pc *pulsarClient, topic string, subName string, c chan MessageID) {
	consumer, err := pc.Subscribe(ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		Type:                        KeyShared,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)

	log.Infof("cydrain consume1 start")

	i := 0
	cnt := 3
	for {
		select {
		case <-ctx.Done():
			log.Infof("channel closed")
			return
		case msg := <-consumer.Chan():
			if i < cnt {
				//consumer.Ack(msg)
				id := BytesToInt(msg.Payload())
				log.Infof("RECV id = %d", id)
				log.Info(msg.ID())
			} else {
				c <- msg.ID()
				log.Infof("failout")
				log.Info(msg.ID())
				return
			}
			i++
		}
	}

	log.Infof("cydrain consume1 done")
}

// Consume2 will consume messages from specified MessageID
func Consume2(t *testing.T, ctx context.Context, pc *pulsarClient, topic string, subName string, msgID MessageID) {
	consumer, err := pc.Subscribe(ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		Type:                        KeyShared,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)

	err = consumer.Seek(msgID)
	assert.Nil(t, err)

	log.Infof("cydrain consume2 start")

	for {
		select {
		case <-ctx.Done():
			log.Infof("channel closed")
			return
		case msg := <-consumer.Chan():
			//consumer.Ack(msg)
			id := BytesToInt(msg.Payload())
			log.Infof("RECV id = %d", id)
			log.Info(msg.ID())
		}
	}

	log.Infof("cydrain consume2 done")
}

func TestPulsarClient(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	pc, err := NewPulsarClient(pulsar.ClientOptions{URL: pulsarAddress})
	defer pc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, pc)

	topic := "test"
	subName := "subName"
	arr := []int{123, 234, 345, 456, 567, 678, 789}
	c := make(chan MessageID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// launch consume1
	go Consume1(t, ctx, pc, topic, subName, c)
	time.Sleep(1*time.Second)

	// launch produce
	go Produce(t, pc, topic, arr)
	time.Sleep(1*time.Second)

	msgID := <-c
	log.Info(msgID)

	// launch consume
	go Consume2(t, ctx, pc, topic, subName, msgID)
	time.Sleep(1*time.Second)

	log.Infof("cydrain main done")
}
