package mqclient

import (
	"os"
	"testing"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/util/rocksmq/client/rocksmq"
	rocksmq1 "github.com/zilliztech/milvus-distributed/internal/util/rocksmq/server/rocksmq"
)

var Params paramtable.BaseTable

func TestMain(m *testing.M) {
	Params.Init()
	rocksdbName := "/tmp/rocksdb_mqclient"
	_ = rocksmq1.InitRocksMQ(rocksdbName)
	exitCode := m.Run()
	defer rocksmq1.CloseRocksMQ()
	os.Exit(exitCode)
}

func TestNewRmqClient(t *testing.T) {
	opts := rocksmq.ClientOptions{}
	client, err := NewRmqClient(opts)
	defer client.Close()
	assert.Nil(t, err)
	assert.NotNil(t, client)
}

func TestRmqCreateProducer(t *testing.T) {
	opts := rocksmq.ClientOptions{}
	client, err := NewRmqClient(opts)
	defer client.Close()
	assert.Nil(t, err)
	assert.NotNil(t, client)

	topic := "test_CreateProducer"
	proOpts := ProducerOptions{Topic: topic}
	producer, err := client.CreateProducer(proOpts)
	assert.Nil(t, err)
	assert.NotNil(t, producer)
}

func TestRmqSubscribe(t *testing.T) {
	opts := rocksmq.ClientOptions{}
	client, err := NewRmqClient(opts)
	defer client.Close()
	assert.Nil(t, err)
	assert.NotNil(t, client)

	topic := "test_Subscribe"
	subName := "subName_1"
	consumerOpts := ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subName,
		BufSize:          1024,
	}
	consumer, err := client.Subscribe(consumerOpts)
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
}
