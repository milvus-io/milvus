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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

var path string = "/tmp/rmq_retention"

func TestRmqRetention(t *testing.T) {
	atomic.StoreInt64(&RocksmqRetentionSizeInMB, 0)
	atomic.StoreInt64(&RocksmqRetentionTimeInMinutes, 0)
	kvPath := path + "_kv"
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := path + "_db"
	defer os.RemoveAll(rocksdbPath)
	metaPath := path + "_meta_kv"
	defer os.RemoveAll(metaPath)

	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.Nil(t, err)
	defer rmq.stopRetention()

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

	checkTimeInterval := 6
	time.Sleep(time.Duration(checkTimeInterval+1) * time.Second)
	// Seek to a previous consumed message, the message should be clean up
	err = rmq.Seek(topicName, groupName, cMsgs[msgNum/2].MsgID)
	assert.Nil(t, err)
	newRes, err := rmq.Consume(topicName, groupName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(newRes), 0)
}

func TestRetentionInfo_LoadRetentionInfo(t *testing.T) {
	atomic.StoreInt64(&RocksmqRetentionTimeInMinutes, 0)
	atomic.StoreInt64(&RocksmqRetentionSizeInMB, 0)
	atomic.StoreInt64(&RocksmqPageSize, 100)
	kvPath := path + "_kv_load"
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := path + "_db_load"
	defer os.RemoveAll(rocksdbPath)
	metaPath := path + "_meta_load"

	defer os.RemoveAll(metaPath)

	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.Nil(t, err)
	defer rmq.stopRetention()

	topicName := "topic_a"
	err = rmq.CreateTopic(topicName)
	assert.Nil(t, err)
	defer rmq.DestroyTopic(topicName)

	rmq.retentionInfo.startRetentionInfo()

	rmq.retentionInfo.ackedInfo.Delete(topicName)

	msgNum := 100
	pMsgs := make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	err = rmq.Produce(topicName, pMsgs)
	assert.Nil(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	rmq.retentionInfo.loadRetentionInfo(topicName, &wg)

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

	wg.Add(1)

	ll, ok := topicMu.Load(topicName)
	assert.Equal(t, ok, true)
	lock, _ := ll.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()
	rmq.retentionInfo.loadRetentionInfo(topicName, &wg)

	initRetentionInfo(rmq.retentionInfo.kv, rmq.store)

	dummyTopic := strings.Repeat(topicName, 100)
	err = DeleteMessages(rmq.store, dummyTopic, 0, 0)
	assert.Error(t, err)

	err = DeleteMessages(rmq.store, topicName, 0, 0)
	assert.NoError(t, err)
}

func TestRmqRetention_Complex(t *testing.T) {
	atomic.StoreInt64(&RocksmqRetentionSizeInMB, 0)
	atomic.StoreInt64(&RocksmqRetentionTimeInMinutes, 1)
	atomic.StoreInt64(&RocksmqPageSize, 10)
	kvPath := path + "_kv_com"
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := path + "_db_com"
	defer os.RemoveAll(rocksdbPath)
	metaPath := path + "_meta_kv_com"
	defer os.RemoveAll(metaPath)

	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.Nil(t, err)
	defer rmq.stopRetention()

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

	checkTimeInterval := atomic.LoadInt64(&RocksmqRetentionTimeInMinutes) * MINUTE / 10
	time.Sleep(time.Duration(checkTimeInterval*2) * time.Second)
	// Seek to a previous consumed message, the message should be clean up
	log.Debug("cMsg", zap.Any("id", cMsgs[10].MsgID))
	err = rmq.Seek(topicName, groupName, cMsgs[10].MsgID)
	assert.Nil(t, err)
	newRes, err := rmq.Consume(topicName, groupName, 1)
	assert.Nil(t, err)
	//TODO(yukun)
	log.Debug("Consume result", zap.Any("result len", len(newRes)))
	// assert.NotEqual(t, newRes[0].MsgID, cMsgs[11].MsgID)
}

func TestRmqRetention_PageTimeExpire(t *testing.T) {
	atomic.StoreInt64(&RocksmqRetentionSizeInMB, 0)
	atomic.StoreInt64(&RocksmqRetentionTimeInMinutes, 0)
	atomic.StoreInt64(&RocksmqPageSize, 10)
	kvPath := path + "_kv_com1"
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := path + "_db_com1"
	defer os.RemoveAll(rocksdbPath)
	metaPath := path + "_meta_kv_com1"
	defer os.RemoveAll(metaPath)

	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.Nil(t, err)
	defer rmq.stopRetention()

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

	checkTimeInterval := 7
	time.Sleep(time.Duration(checkTimeInterval) * time.Second)
	// Seek to a previous consumed message, the message should be clean up
	log.Debug("cMsg", zap.Any("id", cMsgs[10].MsgID))
	err = rmq.Seek(topicName, groupName, cMsgs[len(cMsgs)/2].MsgID)
	assert.Nil(t, err)
	newRes, err := rmq.Consume(topicName, groupName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(newRes), 0)
	// assert.NotEqual(t, newRes[0].MsgID, cMsgs[11].MsgID)
}
