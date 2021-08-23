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
	"os"
	"testing"

	"github.com/milvus-io/milvus/internal/util/paramtable"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/rocksmq/client/rocksmq"
	rocksmq1 "github.com/milvus-io/milvus/internal/util/rocksmq/server/rocksmq"
)

var Params paramtable.BaseTable

func TestMain(m *testing.M) {
	Params.Init()
	os.Setenv("ROCKSMQ_PATH", "/tmp/milvus/rdb_data")
	_ = rocksmq1.InitRocksMQ()
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
