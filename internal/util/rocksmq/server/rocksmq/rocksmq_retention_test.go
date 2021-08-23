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
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/allocator"
	rocksdbkv "github.com/milvus-io/milvus/internal/kv/rocksdb"
	"github.com/stretchr/testify/assert"
)

func InitIDAllocator(kvPath string) *allocator.GlobalIDAllocator {
	rocksdbKV, err := rocksdbkv.NewRocksdbKV(kvPath)
	if err != nil {
		panic(err)
	}
	idAllocator := allocator.NewGlobalIDAllocator("rmq_id", rocksdbKV)
	_ = idAllocator.Initialize()
	return idAllocator
}

func TestRmqRetention(t *testing.T) {
	//RocksmqRetentionSizeInMB = 0
	//RocksmqRetentionTimeInMinutes = 0
	kvPath := "/tmp/rocksmq_idAllocator_kv"
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := "/tmp/rocksmq_test"
	defer os.RemoveAll(rocksdbPath)
	metaPath := rocksdbPath + "_meta_kv"
	defer os.RemoveAll(metaPath)

	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.Nil(t, err)

	topicName := "topic_a"
	err = rmq.CreateTopic(topicName)
	assert.Nil(t, err)
	defer rmq.DestroyTopic(topicName)

	msgNum := 100
	pMsgs := make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	err = rmq.Produce(topicName, pMsgs)
	assert.Nil(t, err)

	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(topicName, groupName)
	err = rmq.CreateConsumerGroup(topicName, groupName)

	consumer := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
	}
	rmq.RegisterConsumer(consumer)

	assert.Nil(t, err)
	cMsgs := make([]ConsumerMessage, 0)
	for i := 0; i < msgNum; i++ {
		cMsg, err := rmq.Consume(topicName, groupName, 1)
		assert.Nil(t, err)
		cMsgs = append(cMsgs, cMsg[0])
	}
	assert.Equal(t, len(cMsgs), msgNum)

	time.Sleep(time.Duration(CheckTimeInterval+1) * time.Second)
	// Seek to a previous consumed message, the message should be clean up
	err = rmq.Seek(topicName, groupName, cMsgs[msgNum/2].MsgID)
	assert.Nil(t, err)
	newRes, err := rmq.Consume(topicName, groupName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(newRes), 0)
}
