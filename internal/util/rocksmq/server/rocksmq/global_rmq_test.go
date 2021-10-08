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

package rocksmq

import (
	"log"
	"os"
	"strings"
	"testing"

	"github.com/milvus-io/milvus/internal/allocator"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/stretchr/testify/assert"
)

func Test_InitRmq(t *testing.T) {
	name := "/tmp/rmq_init"
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "localhost:2379"
	}
	etcdEndpoints := strings.Split(endpoints, ",")
	etcdKV, err := etcdkv.NewEtcdKV(etcdEndpoints, "/etcd/test/root")
	if err != nil {
		log.Fatalf("New clientv3 error = %v", err)
	}
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	err = InitRmq(name, idAllocator)
	defer Rmq.stopRetention()
	assert.NoError(t, err)
	defer CloseRocksMQ()
}

func Test_InitRocksMQ(t *testing.T) {
	// Params.Init()
	rmqPath := "/tmp/milvus/rdb_data_global"
	err := os.Setenv("ROCKSMQ_PATH", rmqPath)
	assert.Nil(t, err)
	defer os.RemoveAll(rmqPath)
	err = InitRocksMQ()
	defer Rmq.stopRetention()
	assert.NoError(t, err)
	defer CloseRocksMQ()

	topicName := "topic_register"
	err = Rmq.CreateTopic(topicName)
	assert.NoError(t, err)
	groupName := "group_register"
	_ = Rmq.DestroyConsumerGroup(topicName, groupName)
	err = Rmq.CreateConsumerGroup(topicName, groupName)
	assert.Nil(t, err)

	consumer := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
		MsgMutex:  make(chan struct{}),
	}
	Rmq.RegisterConsumer(consumer)
}
