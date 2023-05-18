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

package server

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/allocator"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/pkg/util/etcd"
)

func Test_InitPmq(t *testing.T) {
	name := "/tmp/mq_init"
	defer os.RemoveAll("/tmp/mq_init")
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "localhost:2379"
	}
	etcdEndpoints := strings.Split(endpoints, ",")
	etcdCli, err := etcd.GetRemoteEtcdClient(etcdEndpoints)
	defer etcdCli.Close()
	if err != nil {
		log.Fatalf("New clientv3 error = %v", err)
	}
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	defer os.RemoveAll(name + kvSuffix)
	defer os.RemoveAll(name)
	err = InitPmq(name, idAllocator)
	defer Pmq.stopRetention()
	assert.NoError(t, err)
	defer ClosePebbleMQ()
}

func Test_InitPebbleMQ(t *testing.T) {
	mqPath := "/tmp/milvus_pdb/data_global"
	defer os.RemoveAll(mqPath)
	err := InitPebbleMQ(mqPath)
	defer Pmq.stopRetention()
	assert.NoError(t, err)
	defer ClosePebbleMQ()

	topicName := "topic_register"
	err = Pmq.CreateTopic(topicName)
	assert.NoError(t, err)
	groupName := "group_register"
	_ = Pmq.DestroyConsumerGroup(topicName, groupName)
	err = Pmq.CreateConsumerGroup(topicName, groupName)
	assert.Nil(t, err)

	consumer := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
		MsgMutex:  make(chan struct{}),
	}
	Pmq.RegisterConsumer(consumer)
}

func Test_InitPebbleMQError(t *testing.T) {
	once = sync.Once{}
	dir := "/tmp/milvus_pdb/"
	dummyPath := dir + "dummy"
	err := os.MkdirAll(dir, os.ModePerm)
	assert.NoError(t, err)
	f, err := os.Create(dummyPath)
	defer f.Close()
	assert.NoError(t, err)
	defer os.RemoveAll(dummyPath)
	err = InitPebbleMQ(dummyPath)
	fmt.Println(err)
	assert.Error(t, err)
}
