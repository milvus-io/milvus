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
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/mq/mqimpl/natsmq/server"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

func TestNatsConsumer_Subscription(t *testing.T) {
	client, err := createNmqClient()
	assert.NoError(t, err)
	defer client.Close()

	topic := t.Name()
	proOpts := mqwrapper.ProducerOptions{Topic: topic}
	_, err = client.CreateProducer(proOpts)
	assert.NoError(t, err)

	consumer, err := client.Subscribe(mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            topic,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionEarliest,
		BufSize:                     1024,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	str := consumer.Subscription()
	assert.NotNil(t, str)
}

func Test_GetEarliestMessageID(t *testing.T) {
	client, err := createNmqClient()
	assert.NoError(t, err)
	defer client.Close()
	mid := client.EarliestMessageID()

	assert.NotNil(t, mid)
	assert.Equal(t, mid.(*nmqID).messageID, MessageIDType(1))
}

func Test_BadLatestMessageID(t *testing.T) {
	topic := t.Name()
	client, err := createNmqClient()
	assert.NoError(t, err)
	defer client.Close()

	consumer, err := client.Subscribe(mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            topic,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	consumer.Close()
	id, err := consumer.GetLatestMsgID()
	assert.Nil(t, id)
	assert.Error(t, err)
}

func TestComsumeMessage(t *testing.T) {
	client, err := createNmqClient()
	assert.NoError(t, err)
	defer client.Close()

	topic := t.Name()
	p, err := client.CreateProducer(mqwrapper.ProducerOptions{Topic: topic})
	assert.NoError(t, err)

	c, err := client.Subscribe(mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            topic,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	defer c.Close()

	msg := []byte("test the first message")
	prop := map[string]string{"k1": "v1", "k2": "v2"}
	_, err = p.Send(context.Background(), &mqwrapper.ProducerMessage{
		Payload:    msg,
		Properties: prop,
	})
	assert.NoError(t, err)
	recvMsg, err := c.(*Consumer).sub.NextMsg(1 * time.Second)
	assert.NoError(t, err)
	recvMsg.Ack()
	var data NatsMsgData
	err = json.Unmarshal(recvMsg.Data, &data)
	assert.NoError(t, err)
	assert.Equal(t, msg, data.Payload)
	assert.True(t, reflect.DeepEqual(prop, data.Properties))

	msg2 := []byte("test the second message")
	prop2 := map[string]string{"k1": "v3", "k4": "v4"}
	_, err = p.Send(context.Background(), &mqwrapper.ProducerMessage{
		Payload:    msg2,
		Properties: prop2,
	})
	assert.NoError(t, err)
	recvMsg, err = c.(*Consumer).sub.NextMsg(1 * time.Second)
	assert.NoError(t, err)
	recvMsg.Ack()
	var data2 NatsMsgData
	err = json.Unmarshal(recvMsg.Data, &data2)
	assert.Equal(t, msg2, data2.Payload)
	assert.True(t, reflect.DeepEqual(prop2, data2.Properties))

	assert.NoError(t, err)
	assert.NotNil(t, c)
}

func TestNatsConsumer_Close(t *testing.T) {
	client, err := createNmqClient()
	assert.NoError(t, err)
	defer client.Close()

	topic := t.Name()
	c, err := client.Subscribe(mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            topic,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	str := c.Subscription()
	assert.NotNil(t, str)

	c.Close()

	// Disallow double close.
	assert.Panics(t, func() { c.Close() }, "Should panic on consumer double close")
}

func TestNatsClientErrorOnUnsubscribeTwice(t *testing.T) {
	topic := t.Name()
	client, err := createNmqClient()
	assert.NoError(t, err)
	defer client.Close()

	consumer, err := client.Subscribe(mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            topic,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	defer consumer.Close()

	err = consumer.(*Consumer).sub.Unsubscribe()
	assert.NoError(t, err)
	err = consumer.(*Consumer).sub.Unsubscribe()
	assert.True(t, strings.Contains(err.Error(), "invalid subscription"))
	t.Log(err)
}

func TestCheckTopicValid(t *testing.T) {
	client, err := createNmqClient()
	assert.NoError(t, err)
	defer client.Close()

	topic := t.Name()
	consumer, err := client.Subscribe(mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            topic,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)

	str := consumer.Subscription()
	assert.NotNil(t, str)

	err = consumer.CheckTopicValid(topic)
	assert.NoError(t, err)

	// Not allowed to check other topic's validness.
	err = consumer.CheckTopicValid("BadTopic")
	assert.Error(t, err)

	consumer.Close()
	err = consumer.CheckTopicValid(topic)
	assert.Error(t, err)
}

func newTestConsumer(topic string, fromEarliest bool, fromLatest bool) (mqwrapper.Consumer, error) {
	url := server.Nmq.ClientURL()
	conn, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}
	js, err := conn.JetStream()
	if err != nil {
		return nil, err
	}
	groupName := topic
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     groupName,
		Subjects: []string{topic},
	})
	if err != nil {
		return nil, err
	}
	natsChan := make(chan *nats.Msg, 8192)
	closeChan := make(chan struct{})
	return &Consumer{
		js:        js,
		topic:     topic,
		groupName: groupName,
		natsChan:  natsChan,
		closeChan: closeChan,
		skip:      false,
	}, nil
}

func newProducer(t *testing.T, topic string) (*nmqClient, mqwrapper.Producer) {
	client, err := createNmqClient()
	assert.NoError(t, err)
	producer, err := client.CreateProducer(mqwrapper.ProducerOptions{Topic: topic})
	assert.NoError(t, err)
	return client, producer
}

func process(t *testing.T, msgs []string, p mqwrapper.Producer) {
	for _, msg := range msgs {
		_, err := p.Send(context.Background(), &mqwrapper.ProducerMessage{
			Payload:    []byte(msg),
			Properties: map[string]string{},
		})
		assert.NoError(t, err)
	}
}

func TestKafkaConsumer_GetLatestMsgID(t *testing.T) {
	client, err := createNmqClient()
	assert.NoError(t, err)
	defer client.Close()

	topic := t.Name()
	p, err := client.CreateProducer(mqwrapper.ProducerOptions{Topic: topic})
	assert.NoError(t, err)

	c, err := client.Subscribe(mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            topic,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	defer c.Close()

	latestMsgID, err := c.GetLatestMsgID()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), latestMsgID.(*nmqID).messageID)

	msgs := []string{"111", "222", "333", "444", "555"}
	process(t, msgs, p)
	latestMsgID, err = c.GetLatestMsgID()
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), latestMsgID.(*nmqID).messageID)
}

func TestKafkaConsumer_ConsumeFromLatest(t *testing.T) {
	client, err := createNmqClient()
	assert.NoError(t, err)
	defer client.Close()

	topic := t.Name()
	p, err := client.CreateProducer(mqwrapper.ProducerOptions{Topic: topic})
	assert.NoError(t, err)

	msgs := []string{"111", "222", "333"}
	process(t, msgs, p)

	c, err := client.Subscribe(mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            topic,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionLatest,
	})
	assert.NoError(t, err)
	defer c.Close()

	msgs = []string{"444", "555"}
	process(t, msgs, p)

	msg := <-c.Chan()
	assert.Equal(t, "333", string(msg.Payload()))
	msg = <-c.Chan()
	assert.Equal(t, "444", string(msg.Payload()))
}

func TestKafkaConsumer_ConsumeFromEarliest(t *testing.T) {
	client, err := createNmqClient()
	assert.NoError(t, err)
	defer client.Close()

	topic := t.Name()
	p, err := client.CreateProducer(mqwrapper.ProducerOptions{Topic: topic})
	assert.NoError(t, err)

	msgs := []string{"111", "222"}
	process(t, msgs, p)

	c, err := client.Subscribe(mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            topic,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	defer c.Close()

	msgs = []string{"333", "444", "555"}
	process(t, msgs, p)

	msg := <-c.Chan()
	assert.Equal(t, "111", string(msg.Payload()))
	msg = <-c.Chan()
	assert.Equal(t, "222", string(msg.Payload()))

	c2, err := client.Subscribe(mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            topic,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	defer c2.Close()

	msgs = []string{"777"}
	process(t, msgs, p)

	msg = <-c2.Chan()
	assert.Equal(t, "111", string(msg.Payload()))
	msg = <-c2.Chan()
	assert.Equal(t, "222", string(msg.Payload()))
}

func TestNatsConsumer_SeekExclusive(t *testing.T) {
	topic := t.Name()
	c, p := newProducer(t, topic)
	defer c.Close()
	defer p.Close()

	msgs := []string{"111", "222", "333", "444", "555"}
	process(t, msgs, p)

	msgID := &nmqID{messageID: 2}
	consumer, err := newTestConsumer(topic, false, false)
	assert.NoError(t, err)
	defer consumer.Close()
	err = consumer.Seek(msgID, false)
	assert.NoError(t, err)

	msg := <-consumer.Chan()
	assert.Equal(t, "333", string(msg.Payload()))
	msg = <-consumer.Chan()
	assert.Equal(t, "444", string(msg.Payload()))
}

func TestNatsConsumer_SeekInclusive(t *testing.T) {
	topic := t.Name()
	c, p := newProducer(t, topic)
	defer c.Close()
	defer p.Close()

	msgs := []string{"111", "222", "333", "444", "555"}
	process(t, msgs, p)

	msgID := &nmqID{messageID: 2}
	consumer, err := newTestConsumer(topic, false, false)
	assert.NoError(t, err)
	defer consumer.Close()
	err = consumer.Seek(msgID, true)
	assert.NoError(t, err)

	msg := <-consumer.Chan()
	assert.Equal(t, "222", string(msg.Payload()))
	msg = <-consumer.Chan()
	assert.Equal(t, "333", string(msg.Payload()))
}

func TestNatsConsumer_NoDoubleSeek(t *testing.T) {
	topic := t.Name()
	c, p := newProducer(t, topic)
	defer c.Close()
	defer p.Close()

	msgID := &nmqID{messageID: 2}
	consumer, err := newTestConsumer(topic, false, false)
	assert.NoError(t, err)
	defer consumer.Close()
	err = consumer.Seek(msgID, true)
	assert.NoError(t, err)
	err = consumer.Seek(msgID, true)
	assert.Error(t, err)
}

func TestNatsConsumer_ChanWithNoAssign(t *testing.T) {
	topic := t.Name()
	c, p := newProducer(t, topic)
	defer c.Close()
	defer p.Close()

	msgs := []string{"111", "222", "333", "444", "555"}
	process(t, msgs, p)

	consumer, err := newTestConsumer(topic, false, false)
	assert.NoError(t, err)
	defer consumer.Close()

	assert.Panics(t, func() {
		<-consumer.Chan()
	})
}
