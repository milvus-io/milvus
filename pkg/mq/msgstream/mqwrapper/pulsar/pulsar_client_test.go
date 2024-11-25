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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"testing"
	"time"
	"unsafe"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	mqcommon "github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

const (
	DefaultPulsarTenant    = "public"
	DefaultPulsarNamespace = "default"
)

var Params = paramtable.Get()

func TestMain(m *testing.M) {
	paramtable.Init()
	exitCode := m.Run()
	os.Exit(exitCode)
}

func getPulsarAddress() string {
	pulsarAddress := Params.PulsarCfg.Address.GetValue()
	log.Info("pulsar address", zap.String("address", pulsarAddress))
	if len(pulsarAddress) != 0 {
		return pulsarAddress
	}
	panic("invalid pulsar address")
}

func IntToBytes(n int) []byte {
	tmp := int32(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, common.Endian, tmp)
	return bytesBuffer.Bytes()
}

func BytesToInt(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)
	var tmp int32
	binary.Read(bytesBuffer, common.Endian, &tmp)
	return int(tmp)
}

func Produce(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, arr []int) {
	producer, err := pc.CreateProducer(ctx, mqcommon.ProducerOptions{Topic: topic})
	assert.NoError(t, err)
	assert.NotNil(t, producer)

	log.Info("Produce start")

	for _, v := range arr {
		msg := &mqcommon.ProducerMessage{
			Payload:    IntToBytes(v),
			Properties: map[string]string{},
		}
		_, err = producer.Send(ctx, msg)
		assert.NoError(t, err)
		log.Info("Pub", zap.Any("SND", v))
	}

	log.Info("Produce done")
}

func VerifyMessage(t *testing.T, msg mqcommon.Message) {
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
func Consume1(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, subName string, c chan mqcommon.MessageID, total *int) {
	consumer, err := pc.Subscribe(ctx, mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		SubscriptionInitialPosition: mqcommon.SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	log.Info("Consume1 start")

	// get random number between 1 ~ 5
	rand.Seed(time.Now().UnixNano())
	cnt := 1 + rand.Int()%5

	var msg mqcommon.Message
	for i := 0; i < cnt; i++ {
		select {
		case <-ctx.Done():
			log.Info("Consume1 channel closed")
			return
		case msg = <-consumer.Chan():
			consumer.Ack(msg)
			VerifyMessage(t, msg)
			(*total)++
			// log.Debug("total", zap.Int("val", *total))
		}
	}
	c <- msg.ID()

	log.Info("Consume1 randomly RECV", zap.Any("number", cnt))
	log.Info("Consume1 done")
}

// Consume2 will consume messages from specified MessageID
func Consume2(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, subName string, msgID mqcommon.MessageID, total *int) {
	consumer, err := pc.Subscribe(ctx, mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		SubscriptionInitialPosition: mqcommon.SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	err = consumer.Seek(msgID, true)
	assert.NoError(t, err)

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
			// log.Debug("total", zap.Int("val", *total))
		}
	}
}

func Consume3(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, subName string, total *int) {
	consumer, err := pc.Subscribe(ctx, mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		SubscriptionInitialPosition: mqcommon.SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
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
			// log.Debug("total", zap.Int("val", *total))
		}
	}
}

func TestPulsarClient_Consume1(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	pc, err := NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	defer pc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, pc)
	rand.Seed(time.Now().UnixNano())

	topic := fmt.Sprintf("test-topic-%d", rand.Int())
	subName := fmt.Sprintf("test-subname-%d", rand.Int())
	arr := []int{111, 222, 333, 444, 555, 666, 777}
	c := make(chan mqcommon.MessageID, 1)

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

func Consume21(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, subName string, c chan mqcommon.MessageID, total *int) {
	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
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
			// log.Debug("total", zap.Int("val", *total))
		}
	}
	c <- &pulsarID{messageID: msg.ID()}

	log.Info("Consume1 randomly RECV", zap.Any("number", cnt))
	log.Info("Consume1 done")
}

// Consume2 will consume messages from specified MessageID
func Consume22(ctx context.Context, t *testing.T, pc *pulsarClient, topic string, subName string, msgID mqcommon.MessageID, total *int) {
	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	err = consumer.Seek(msgID.(*pulsarID).messageID)
	assert.NoError(t, err)

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
			// log.Debug("total", zap.Int("val", *total))
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
	assert.NoError(t, err)
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
			// log.Debug("total", zap.Int("val", *total))
		}
	}
}

func TestPulsarClient_Consume2(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	pc, err := NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	defer pc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, pc)
	rand.Seed(time.Now().UnixNano())

	topic := fmt.Sprintf("test-topic-%d", rand.Int())
	subName := fmt.Sprintf("test-subname-%d", rand.Int())
	arr := []int{111, 222, 333, 444, 555, 666, 777}
	c := make(chan mqcommon.MessageID, 1)

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

func TestPulsarClient_SeekPosition(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	pc, err := NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	defer pc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, pc)
	rand.Seed(time.Now().UnixNano())

	ctx := context.Background()
	topic := fmt.Sprintf("test-topic-%d", rand.Int())
	subName := fmt.Sprintf("test-subname-%d", rand.Int())

	producer, err := pc.CreateProducer(ctx, mqcommon.ProducerOptions{Topic: topic})
	assert.NoError(t, err)
	assert.NotNil(t, producer)

	log.Info("Produce start")
	ids := []mqcommon.MessageID{}
	arr1 := []int{1, 2, 3}
	arr2 := []string{"1", "2", "3"}
	for k, v := range arr1 {
		msg := &mqcommon.ProducerMessage{
			Payload: IntToBytes(v),
			Properties: map[string]string{
				common.TraceIDKey: arr2[k],
			},
		}
		id, err := producer.Send(ctx, msg)
		ids = append(ids, id)
		assert.NoError(t, err)
	}

	log.Info("Produced")

	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()
	seekID := ids[2].(*pulsarID).messageID
	consumer.Seek(seekID)

	msgChan := consumer.Chan()

	select {
	case msg := <-msgChan:
		assert.Equal(t, seekID.BatchIdx(), msg.ID().BatchIdx())
		assert.Equal(t, seekID.LedgerID(), msg.ID().LedgerID())
		assert.Equal(t, seekID.EntryID(), msg.ID().EntryID())
		assert.Equal(t, seekID.PartitionIdx(), msg.ID().PartitionIdx())
		assert.Equal(t, 3, BytesToInt(msg.Payload()))
		assert.Equal(t, "3", msg.Properties()[common.TraceIDKey])
	case <-time.After(2 * time.Second):
		assert.FailNow(t, "should not wait")
	}

	seekID = ids[1].(*pulsarID).messageID
	consumer.Seek(seekID)

	msgChan = consumer.Chan()

	select {
	case msg := <-msgChan:
		assert.Equal(t, seekID.BatchIdx(), msg.ID().BatchIdx())
		assert.Equal(t, seekID.LedgerID(), msg.ID().LedgerID())
		assert.Equal(t, seekID.EntryID(), msg.ID().EntryID())
		assert.Equal(t, seekID.PartitionIdx(), msg.ID().PartitionIdx())
		assert.Equal(t, 2, BytesToInt(msg.Payload()))
		assert.Equal(t, "2", msg.Properties()[common.TraceIDKey])
	case <-time.After(2 * time.Second):
		assert.FailNow(t, "should not wait")
	}
}

func TestPulsarClient_SeekLatest(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	pc, err := NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	defer pc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, pc)
	rand.Seed(time.Now().UnixNano())

	ctx := context.Background()
	topic := fmt.Sprintf("test-topic-%d", rand.Int())
	subName := fmt.Sprintf("test-subname-%d", rand.Int())

	producer, err := pc.CreateProducer(ctx, mqcommon.ProducerOptions{Topic: topic})
	assert.NoError(t, err)
	assert.NotNil(t, producer)

	log.Info("Produce start")

	arr := []int{1, 2, 3}
	for _, v := range arr {
		msg := &mqcommon.ProducerMessage{
			Payload:    IntToBytes(v),
			Properties: map[string]string{},
		}
		_, err = producer.Send(ctx, msg)
		assert.NoError(t, err)
	}

	log.Info("Produced")

	consumer, err := pc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	msgChan := consumer.Chan()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	loop := true
	for loop {
		select {
		case msg := <-msgChan:
			consumer.Ack(msg)
			v := BytesToInt(msg.Payload())
			log.Info("RECV", zap.Any("v", v))
			assert.Equal(t, v, 4)
			loop = false
		case <-ticker.C:
			log.Info("after 2 seconds")
			msg := &mqcommon.ProducerMessage{
				Payload:    IntToBytes(4),
				Properties: map[string]string{},
			}
			_, err = producer.Send(ctx, msg)
			assert.NoError(t, err)
		}
	}
}

func TestPulsarClient_EarliestMessageID(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	client, _ := NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	defer client.Close()

	mid := client.EarliestMessageID()
	assert.NotNil(t, mid)
}

func TestPulsarClient_StringToMsgID(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	client, _ := NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	defer client.Close()

	mid := pulsar.EarliestMessageID()
	str := msgIDToString(mid)

	res, err := client.StringToMsgID(str)
	assert.NoError(t, err)
	assert.NotNil(t, res)

	str = "X"
	res, err = client.StringToMsgID(str)
	assert.Nil(t, res)
	assert.Error(t, err)
}

func TestPulsarClient_BytesToMsgID(t *testing.T) {
	pulsarAddress := getPulsarAddress()
	client, _ := NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	defer client.Close()

	mid := pulsar.EarliestMessageID()
	binary := SerializePulsarMsgID(mid)

	res, err := client.BytesToMsgID(binary)
	assert.NoError(t, err)
	assert.NotNil(t, res)

	invalidBin := []byte{0}
	res, err = client.BytesToMsgID(invalidBin)
	assert.Nil(t, res)
	assert.Error(t, err)
}

type mPulsarError struct {
	msg    string
	result pulsar.Result
}

func hackPulsarError(result pulsar.Result) *pulsar.Error {
	pe := &pulsar.Error{}
	// use unsafe to generate test case
	/* #nosec G103 */
	mpe := (*mPulsarError)(unsafe.Pointer(pe))
	// this what we tested
	if result == pulsar.ConsumerBusy {
		mpe.msg = "server error: ConsumerBusy: Exclusive consumer is already connected"
	}

	if result == pulsar.ConsumerNotFound {
		mpe.msg = "server error: MetadataError: Consumer not found"
	}
	mpe.result = result
	return pe
}

type mockPulsarClient struct{}

// CreateProducer Creates the producer instance
// This method will block until the producer is created successfully
func (c *mockPulsarClient) CreateProducer(_ pulsar.ProducerOptions) (pulsar.Producer, error) {
	return nil, hackPulsarError(pulsar.ConnectError)
}

// Subscribe Creates a `Consumer` by subscribing to a topic.
//
// If the subscription does not exist, a new subscription will be created and all messages published after the
// creation will be retained until acknowledged, even if the consumer is not connected
func (c *mockPulsarClient) Subscribe(_ pulsar.ConsumerOptions) (pulsar.Consumer, error) {
	return nil, hackPulsarError(pulsar.ConsumerBusy)
}

// CreateReader Creates a Reader instance.
// This method will block until the reader is created successfully.
func (c *mockPulsarClient) CreateReader(_ pulsar.ReaderOptions) (pulsar.Reader, error) {
	return nil, hackPulsarError(pulsar.ConnectError)
}

func (c *mockPulsarClient) CreateTableView(pulsar.TableViewOptions) (pulsar.TableView, error) {
	return nil, hackPulsarError(pulsar.ConnectError)
}

// TopicPartitions Fetches the list of partitions for a given topic
//
// If the topic is partitioned, this will return a list of partition names.
// If the topic is not partitioned, the returned list will contain the topic
// name itself.
//
// This can be used to discover the partitions and create {@link Reader},
// {@link Consumer} or {@link Producer} instances directly on a particular partition.
func (c *mockPulsarClient) TopicPartitions(topic string) ([]string, error) {
	return nil, hackPulsarError(pulsar.ConnectError)
}

// Close Closes the Client and free associated resources
func (c *mockPulsarClient) Close() {
}

func (c *mockPulsarClient) NewTransaction(duration time.Duration) (pulsar.Transaction, error) {
	return nil, hackPulsarError(pulsar.ConnectError)
}

func TestPulsarClient_SubscribeExclusiveFail(t *testing.T) {
	t.Run("exclusive pulsar consumer failure", func(t *testing.T) {
		pc := &pulsarClient{
			tenant:    DefaultPulsarTenant,
			namespace: DefaultPulsarNamespace,
			client:    &mockPulsarClient{},
		}

		_, err := pc.Subscribe(context.TODO(), mqwrapper.ConsumerOptions{Topic: "test_topic_name"})
		assert.Error(t, err)
		assert.True(t, retry.IsRecoverable(err))
	})
}

func TestPulsarClient_WithTenantAndNamespace(t *testing.T) {
	tenant := "public"
	namespace := "default"
	topic := "test"
	subName := "hello_world"

	pulsarAddress := getPulsarAddress()
	pc, err := NewClient(tenant, namespace, pulsar.ClientOptions{URL: pulsarAddress})
	assert.NoError(t, err)
	producer, err := pc.CreateProducer(context.TODO(), mqcommon.ProducerOptions{Topic: topic})
	defer producer.Close()
	assert.NoError(t, err)
	assert.NotNil(t, producer)

	fullTopicName, err := GetFullTopicName(tenant, namespace, topic)
	assert.NoError(t, err)
	assert.Equal(t, fullTopicName, producer.(*pulsarProducer).Topic())

	consumer, err := pc.Subscribe(context.TODO(), mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		SubscriptionInitialPosition: mqcommon.SubscriptionPositionEarliest,
	})
	defer consumer.Close()
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
}

func TestPulsarCtl(t *testing.T) {
	topic := "test-pulsar-ctl"
	subName := "hello"

	pulsarAddress := getPulsarAddress()
	pc, err := NewClient(DefaultPulsarTenant, DefaultPulsarNamespace, pulsar.ClientOptions{URL: pulsarAddress})
	assert.NoError(t, err)
	consumer, err := pc.Subscribe(context.TODO(), mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		SubscriptionInitialPosition: mqcommon.SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	_, err = pc.Subscribe(context.TODO(), mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		SubscriptionInitialPosition: mqcommon.SubscriptionPositionEarliest,
	})

	assert.Error(t, err)

	_, err = pc.Subscribe(context.TODO(), mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		SubscriptionInitialPosition: mqcommon.SubscriptionPositionEarliest,
	})
	assert.Error(t, err)

	fullTopicName, err := GetFullTopicName(DefaultPulsarTenant, DefaultPulsarNamespace, topic)
	assert.NoError(t, err)
	topicName, err := utils.GetTopicName(fullTopicName)
	assert.NoError(t, err)

	pulsarURL, err := url.ParseRequestURI(pulsarAddress)
	if err != nil {
		panic(err)
	}
	webport := Params.PulsarCfg.WebPort.GetValue()
	webServiceURL := "http://" + pulsarURL.Hostname() + ":" + webport
	admin, err := NewAdminClient(webServiceURL, "", "")
	assert.NoError(t, err)
	err = admin.Subscriptions().Delete(*topicName, subName, true)
	if err != nil {
		webServiceURL = "http://" + pulsarURL.Hostname() + ":" + "8080"
		admin, err := NewAdminClient(webServiceURL, "", "")
		assert.NoError(t, err)
		err = admin.Subscriptions().Delete(*topicName, subName, true)
		assert.NoError(t, err)
	}

	consumer2, err := pc.Subscribe(context.TODO(), mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		SubscriptionInitialPosition: mqcommon.SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer2)
	defer consumer2.Close()
}

func NewPulsarAdminClient() {
	panic("unimplemented")
}

func TestPulsarClient_GetFullTopicName(t *testing.T) {
	fullTopicName, err := GetFullTopicName("", "", "topic")
	assert.Error(t, err)
	assert.Empty(t, fullTopicName)

	fullTopicName, err = GetFullTopicName("tenant", "", "topic")
	assert.Error(t, err)
	assert.Empty(t, fullTopicName)

	fullTopicName, err = GetFullTopicName("", "namespace", "topic")
	assert.Error(t, err)
	assert.Empty(t, fullTopicName)

	fullTopicName, err = GetFullTopicName("tenant", "namespace", "topic")
	assert.NoError(t, err)
	assert.Equal(t, "tenant/namespace/topic", fullTopicName)
}
