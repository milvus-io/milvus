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

package pulsar

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/streamnative/pulsarctl/pkg/cmdutils"
	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
)

func TestPulsarConsumer_Subscription(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	pc, err := NewClient(pulsar.ClientOptions{URL: pulsarAddress})
	assert.Nil(t, err)
	defer pc.Close()

	receiveChannel := make(chan pulsar.ConsumerMessage, 100)
	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       "Topic",
		SubscriptionName:            "SubName",
		SubscriptionInitialPosition: pulsar.SubscriptionInitialPosition(mqwrapper.SubscriptionPositionEarliest),
		MessageChannel:              receiveChannel,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	str := consumer.Subscription()
	assert.NotNil(t, str)
}

func Test_PatchEarliestMessageID(t *testing.T) {
	mid := pulsar.EarliestMessageID()

	// String() -> ledgerID:entryID:partitionIdx
	assert.Equal(t, "-1:-1:-1", fmt.Sprintf("%v", mid))

	patchEarliestMessageID(&mid)

	assert.Equal(t, "-1:-1:0", fmt.Sprintf("%v", mid))
}

func TestComsumeCompressedMessage(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	pc, err := NewClient(pulsar.ClientOptions{URL: pulsarAddress})
	assert.Nil(t, err)
	defer pc.Close()

	receiveChannel := make(chan pulsar.ConsumerMessage, 100)
	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       "TestTopics",
		SubscriptionName:            "SubName",
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionInitialPosition(mqwrapper.SubscriptionPositionEarliest),
		MessageChannel:              receiveChannel,
	})
	assert.NoError(t, err)
	defer consumer.Close()

	producer, err := pc.CreateProducer(mqwrapper.ProducerOptions{Topic: "TestTopics"})
	assert.NoError(t, err)
	compressProducer, err := pc.CreateProducer(mqwrapper.ProducerOptions{Topic: "TestTopics", EnableCompression: true})
	assert.NoError(t, err)

	msg := []byte("test message")
	compressedMsg := []byte("test compressed message")
	_, err = producer.Send(context.Background(), &mqwrapper.ProducerMessage{
		Payload:    msg,
		Properties: map[string]string{},
	})
	assert.NoError(t, err)
	recvMsg, err := consumer.Receive(context.Background())
	assert.NoError(t, err)
	consumer.Ack(recvMsg)
	assert.Equal(t, msg, recvMsg.Payload())

	_, err = compressProducer.Send(context.Background(), &mqwrapper.ProducerMessage{
		Payload:    compressedMsg,
		Properties: map[string]string{},
	})
	assert.NoError(t, err)
	recvMsg, err = consumer.Receive(context.Background())
	assert.NoError(t, err)
	consumer.Ack(recvMsg)
	assert.Equal(t, compressedMsg, recvMsg.Payload())

	assert.Nil(t, err)
	assert.NotNil(t, consumer)
}

func TestPulsarConsumer_Close(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	pc, err := NewClient(pulsar.ClientOptions{URL: pulsarAddress})
	assert.Nil(t, err)

	receiveChannel := make(chan pulsar.ConsumerMessage, 100)
	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       "Topic-1",
		SubscriptionName:            "SubName-1",
		SubscriptionInitialPosition: pulsar.SubscriptionInitialPosition(mqwrapper.SubscriptionPositionEarliest),
		MessageChannel:              receiveChannel,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)

	str := consumer.Subscription()
	assert.NotNil(t, str)

	pulsarConsumer := &Consumer{c: consumer, closeCh: make(chan struct{})}
	pulsarConsumer.Close()

	// test double close
	pulsarConsumer.Close()
}

func TestPulsarClientCloseUnsubscribeError(t *testing.T) {
	topic := "TestPulsarClientCloseUnsubscribeError"
	subName := "test"
	pulsarAddress := getPulsarAddress()

	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: pulsarAddress})
	defer client.Close()
	assert.NoError(t, err)

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	defer consumer.Close()
	assert.NoError(t, err)

	// subscribe agiain
	_, err = client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	defer consumer.Close()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "ConsumerBusy"))

	topicName, err := utils.GetTopicName(topic)
	assert.NoError(t, err)

	pulsarURL, err := url.ParseRequestURI(pulsarAddress)
	if err != nil {
		panic(err)
	}
	webport := Params.LoadWithDefault("pulsar.webport", "80")
	cmdutils.PulsarCtlConfig.WebServiceURL = "http://" + pulsarURL.Hostname() + ":" + webport
	admin := cmdutils.NewPulsarClient()
	err = admin.Subscriptions().Delete(*topicName, subName, true)
	if err != nil {
		cmdutils.PulsarCtlConfig.WebServiceURL = "http://" + pulsarURL.Hostname() + ":" + "8080"
		admin := cmdutils.NewPulsarClient()
		err = admin.Subscriptions().Delete(*topicName, subName, true)
		assert.NoError(t, err)
	}

	err = consumer.Unsubscribe()
	assert.True(t, strings.Contains(err.Error(), "Consumer not found"))
	fmt.Println(err)
}

func TestPulsarClientUnsubscribeTwice(t *testing.T) {
	topic := "TestPulsarClientUnsubscribeTwice"
	subName := "test"
	pulsarAddress := getPulsarAddress()

	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: pulsarAddress})
	defer client.Close()
	assert.NoError(t, err)

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	defer consumer.Close()
	assert.NoError(t, err)

	err = consumer.Unsubscribe()
	assert.NoError(t, err)
	err = consumer.Unsubscribe()
	assert.True(t, strings.Contains(err.Error(), "Consumer not found"))
	fmt.Println(err)
}
