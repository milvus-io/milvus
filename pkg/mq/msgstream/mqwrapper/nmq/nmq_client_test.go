// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nmq

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

func createNmqClient() (*nmqClient, error) {
	return NewClient(natsServerAddress)
}

func Test_NewNmqClient(t *testing.T) {
	client, err := createNmqClient()
	assert.NoError(t, err)
	assert.NotNil(t, client)
	client.Close()

	tests := []struct {
		description  string
		withTimeout  bool
		ctxTimeouted bool
		expectErr    bool
	}{
		{"without context", false, false, false},
		{"without timeout context, no timeout", true, false, false},
		{"without timeout context, timeout", true, true, true},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			ctx := context.Background()
			var cancel context.CancelFunc
			if test.withTimeout {
				ctx, cancel = context.WithTimeout(ctx, time.Second)
				if test.ctxTimeouted {
					cancel()
				} else {
					defer cancel()
				}
			}

			client, err := NewClientWithDefaultOptions(ctx)

			if test.expectErr {
				assert.Error(t, err)
				assert.Nil(t, client)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, client)
				client.Close()
			}
		})
	}
}

func TestNmqClient_CreateProducer(t *testing.T) {
	client, err := createNmqClient()
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	topic := "TestNmqClient_CreateProducer"
	proOpts := common.ProducerOptions{Topic: topic}
	producer, err := client.CreateProducer(context.TODO(), proOpts)
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	nmqProducer := producer.(*nmqProducer)
	assert.Equal(t, nmqProducer.Topic(), topic)

	msg := &common.ProducerMessage{
		Payload:    []byte{},
		Properties: nil,
	}
	_, err = nmqProducer.Send(context.TODO(), msg)
	assert.NoError(t, err)

	invalidOpts := common.ProducerOptions{Topic: ""}
	producer, e := client.CreateProducer(context.TODO(), invalidOpts)
	assert.Nil(t, producer)
	assert.Error(t, e)
}

func TestNmqClient_GetLatestMsg(t *testing.T) {
	client, err := createNmqClient()
	assert.NoError(t, err)
	defer client.Close()

	topic := fmt.Sprintf("t2GetLatestMsg-%d", rand.Int())
	proOpts := common.ProducerOptions{Topic: topic}
	producer, err := client.CreateProducer(context.TODO(), proOpts)
	assert.NoError(t, err)
	defer producer.Close()

	for i := 0; i < 10; i++ {
		msg := &common.ProducerMessage{
			Payload:    []byte{byte(i)},
			Properties: nil,
		}
		_, err = producer.Send(context.TODO(), msg)
		assert.NoError(t, err)
	}

	subName := "subName"
	consumerOpts := mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		SubscriptionInitialPosition: common.SubscriptionPositionEarliest,
		BufSize:                     1024,
	}

	consumer, err := client.Subscribe(context.TODO(), consumerOpts)
	assert.NoError(t, err)

	expectLastMsg, err := consumer.GetLatestMsgID()
	assert.NoError(t, err)

	var actualLastMsg common.Message
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	for i := 0; i < 10; i++ {
		select {
		case <-ctx.Done():
			fmt.Println(i)
			assert.FailNow(t, "consumer failed to yield message in 100 milliseconds")
		case msg := <-consumer.Chan():
			consumer.Ack(msg)
			actualLastMsg = msg
		}
	}
	require.NotNil(t, actualLastMsg)
	ret, err := expectLastMsg.LessOrEqualThan(actualLastMsg.ID().Serialize())
	assert.NoError(t, err)
	assert.True(t, ret)
}

func TestNmqClient_IllegalSubscribe(t *testing.T) {
	client, err := createNmqClient()
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	sub, err := client.Subscribe(context.TODO(), mqwrapper.ConsumerOptions{
		Topic: "",
	})
	assert.Nil(t, sub)
	assert.Error(t, err)

	sub, err = client.Subscribe(context.TODO(), mqwrapper.ConsumerOptions{
		Topic:            "123",
		SubscriptionName: "",
	})
	assert.Nil(t, sub)
	assert.Error(t, err)
}

func TestNmqClient_Subscribe(t *testing.T) {
	client, err := createNmqClient()
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	topic := "TestNmqClient_Subscribe"
	proOpts := common.ProducerOptions{Topic: topic}
	producer, err := client.CreateProducer(context.TODO(), proOpts)
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	subName := "subName"
	consumerOpts := mqwrapper.ConsumerOptions{
		Topic:                       "",
		SubscriptionName:            subName,
		SubscriptionInitialPosition: common.SubscriptionPositionEarliest,
		BufSize:                     1024,
	}

	consumer, err := client.Subscribe(context.TODO(), consumerOpts)
	assert.Error(t, err)
	assert.Nil(t, consumer)

	consumerOpts.Topic = topic
	consumer, err = client.Subscribe(context.TODO(), consumerOpts)
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()
	assert.Equal(t, consumer.Subscription(), subName)

	msg := &common.ProducerMessage{
		Payload:    []byte{1},
		Properties: nil,
	}
	_, err = producer.Send(context.TODO(), msg)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	select {
	case <-ctx.Done():
		assert.FailNow(t, "consumer failed to yield message in 100 milliseconds")
	case msg := <-consumer.Chan():
		consumer.Ack(msg)
		nmqmsg := msg.(*nmqMessage)
		msgPayload := nmqmsg.Payload()
		assert.NotEmpty(t, msgPayload)
		msgTopic := nmqmsg.Topic()
		assert.Equal(t, msgTopic, topic)
		msgProp := nmqmsg.Properties()
		assert.Empty(t, msgProp)
		msgID := nmqmsg.ID()
		rID := msgID.(*nmqID)
		assert.Equal(t, rID.messageID, MessageIDType(1))
	}
}

func TestNmqClient_EarliestMessageID(t *testing.T) {
	client, _ := createNmqClient()
	defer client.Close()

	mid := client.EarliestMessageID()
	assert.NotNil(t, mid)
	nmqmsg := mid.(*nmqID)
	assert.Equal(t, nmqmsg.messageID, MessageIDType(1))
}

func TestNmqClient_StringToMsgID(t *testing.T) {
	client, _ := createNmqClient()
	defer client.Close()

	str := "5"
	res, err := client.StringToMsgID(str)
	assert.NoError(t, err)
	assert.NotNil(t, res)

	str = "X"
	res, err = client.StringToMsgID(str)
	assert.Nil(t, res)
	assert.Error(t, err)
}

func TestNmqClient_BytesToMsgID(t *testing.T) {
	client, _ := createNmqClient()
	defer client.Close()

	mid := client.EarliestMessageID()
	res, err := client.BytesToMsgID(mid.Serialize())
	assert.NoError(t, err)
	assert.NotNil(t, res)
}
