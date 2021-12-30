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
	"sync/atomic"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/log"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

var retentionPath = "/tmp/rmq_retention/"

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

// Test write data and wait for retention
func TestRmqRetention_Basic(t *testing.T) {
	err := os.MkdirAll(retentionPath, os.ModePerm)
	if err != nil {
		log.Error("MkdirAll error for path", zap.Any("path", retentionPath))
		return
	}
	defer os.RemoveAll(retentionPath)
	atomic.StoreInt64(&RocksmqRetentionSizeInMB, 0)
	atomic.StoreInt64(&RocksmqRetentionTimeInSecs, 0)
	atomic.StoreInt64(&RocksmqPageSize, 10)
	atomic.StoreInt64(&TickerTimeInSeconds, 2)

	rocksdbPath := retentionPath
	defer os.RemoveAll(rocksdbPath)
	metaPath := retentionPath + metaPathSuffix
	defer os.RemoveAll(metaPath)

	rmq, err := NewRocksMQ(rocksdbPath, nil)
	defer rmq.Close()
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
	ids, err := rmq.Produce(topicName, pMsgs)
	assert.Nil(t, err)
	assert.Equal(t, len(pMsgs), len(ids))

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

	checkTimeInterval := 2
	time.Sleep(time.Duration(checkTimeInterval+1) * time.Second)
	// Seek to a previous consumed message, the message should be clean up
	err = rmq.Seek(topicName, groupName, cMsgs[msgNum/2].MsgID)
	assert.Nil(t, err)
	newRes, err := rmq.Consume(topicName, groupName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(newRes), 0)

	// test acked size acked ts and other meta are updated as expect
	msgSizeKey := MessageSizeTitle + topicName
	msgSizeVal, err := rmq.kv.Load(msgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, msgSizeVal, "0")

	pageMsgSizeKey := constructKey(PageMsgSizeTitle, topicName)
	keys, values, err := rmq.kv.LoadWithPrefix(pageMsgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)

	pageTsSizeKey := constructKey(PageTsTitle, topicName)
	keys, values, err = rmq.kv.LoadWithPrefix(pageTsSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)

	aclTsSizeKey := constructKey(AckedTsTitle, topicName)
	keys, values, err = rmq.kv.LoadWithPrefix(aclTsSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)
}

// Not acked message should not be purged
func TestRmqRetention_NotConsumed(t *testing.T) {
	err := os.MkdirAll(retentionPath, os.ModePerm)
	if err != nil {
		log.Error("MkdirAll error for path", zap.Any("path", retentionPath))
		return
	}
	defer os.RemoveAll(retentionPath)
	atomic.StoreInt64(&RocksmqRetentionSizeInMB, 0)
	atomic.StoreInt64(&RocksmqRetentionTimeInSecs, 0)
	atomic.StoreInt64(&RocksmqPageSize, 10)
	atomic.StoreInt64(&TickerTimeInSeconds, 2)

	rocksdbPath := retentionPath
	defer os.RemoveAll(rocksdbPath)
	metaPath := retentionPath + metaPathSuffix
	defer os.RemoveAll(metaPath)

	rmq, err := NewRocksMQ(rocksdbPath, nil)
	defer rmq.Close()
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
	ids, err := rmq.Produce(topicName, pMsgs)
	assert.Nil(t, err)
	assert.Equal(t, len(pMsgs), len(ids))

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
	for i := 0; i < 5; i++ {
		cMsg, err := rmq.Consume(topicName, groupName, 1)
		assert.Nil(t, err)
		cMsgs = append(cMsgs, cMsg[0])
	}
	assert.Equal(t, len(cMsgs), 5)
	id := cMsgs[0].MsgID

	aclTsSizeKey := constructKey(AckedTsTitle, topicName)
	keys, values, err := rmq.kv.LoadWithPrefix(aclTsSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 2)
	assert.Equal(t, len(values), 2)

	// wait for retention
	checkTimeInterval := 2
	time.Sleep(time.Duration(checkTimeInterval+1) * time.Second)
	// Seek to a previous consumed message, the message should be clean up
	err = rmq.Seek(topicName, groupName, cMsgs[1].MsgID)
	assert.Nil(t, err)
	newRes, err := rmq.Consume(topicName, groupName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(newRes), 1)
	assert.Equal(t, newRes[0].MsgID, id+4)

	// test acked size acked ts and other meta are updated as expect
	msgSizeKey := MessageSizeTitle + topicName
	msgSizeVal, err := rmq.kv.Load(msgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, msgSizeVal, "0")

	// should only clean 2 pages
	pageMsgSizeKey := constructKey(PageMsgSizeTitle, topicName)
	keys, values, err = rmq.kv.LoadWithPrefix(pageMsgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 48)
	assert.Equal(t, len(values), 48)

	pageTsSizeKey := constructKey(PageTsTitle, topicName)
	keys, values, err = rmq.kv.LoadWithPrefix(pageTsSizeKey)
	assert.NoError(t, err)

	assert.Equal(t, len(keys), 48)
	assert.Equal(t, len(values), 48)

	aclTsSizeKey = constructKey(AckedTsTitle, topicName)
	keys, values, err = rmq.kv.LoadWithPrefix(aclTsSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)
}

// Test multiple topic
func TestRmqRetention_MultipleTopic(t *testing.T) {
	err := os.MkdirAll(retentionPath, os.ModePerm)
	if err != nil {
		log.Error("MkdirALl error for path", zap.Any("path", retentionPath))
		return
	}
	defer os.RemoveAll(retentionPath)
	// no retention by size
	atomic.StoreInt64(&RocksmqRetentionSizeInMB, -1)
	// retention by secs
	atomic.StoreInt64(&RocksmqRetentionTimeInSecs, 1)
	atomic.StoreInt64(&RocksmqPageSize, 10)
	atomic.StoreInt64(&TickerTimeInSeconds, 1)
	kvPath := retentionPath + "kv_multi_topic"
	os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := retentionPath + "db_multi_topic"
	os.RemoveAll(rocksdbPath)
	metaPath := retentionPath + "meta_multi_topic"
	os.RemoveAll(metaPath)

	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.Nil(t, err)
	defer rmq.Close()

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
	ids1, err := rmq.Produce(topicName, pMsgs)
	assert.Nil(t, err)
	assert.Equal(t, len(pMsgs), len(ids1))

	topicName = "topic_b"
	err = rmq.CreateTopic(topicName)
	assert.Nil(t, err)
	defer rmq.DestroyTopic(topicName)
	pMsgs = make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids2, err := rmq.Produce(topicName, pMsgs)
	assert.Nil(t, err)
	assert.Equal(t, len(pMsgs), len(ids2))

	topicName = "topic_a"
	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(topicName, groupName)
	err = rmq.CreateConsumerGroup(topicName, groupName)
	assert.NoError(t, err)

	consumer := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
	}
	rmq.RegisterConsumer(consumer)

	cMsgs := make([]ConsumerMessage, 0)
	for i := 0; i < msgNum; i++ {
		cMsg, err := rmq.Consume(topicName, groupName, 1)
		assert.Nil(t, err)
		cMsgs = append(cMsgs, cMsg[0])
	}
	assert.Equal(t, len(cMsgs), msgNum)
	assert.Equal(t, cMsgs[0].MsgID, ids1[0])

	time.Sleep(time.Duration(3) * time.Second)

	err = rmq.Seek(topicName, groupName, ids1[10])
	assert.Nil(t, err)
	newRes, err := rmq.Consume(topicName, groupName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(newRes), 0)

	// test acked size acked ts and other meta are updated as expect
	msgSizeKey := MessageSizeTitle + topicName
	msgSizeVal, err := rmq.kv.Load(msgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, msgSizeVal, "0")

	// 100 page left, each entity is a page
	pageMsgSizeKey := constructKey(PageMsgSizeTitle, "topic_a")
	keys, values, err := rmq.kv.LoadWithPrefix(pageMsgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)

	pageTsSizeKey := constructKey(PageTsTitle, "topic_a")
	keys, values, err = rmq.kv.LoadWithPrefix(pageTsSizeKey)
	assert.NoError(t, err)

	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)

	aclTsSizeKey := constructKey(AckedTsTitle, "topic_a")
	keys, values, err = rmq.kv.LoadWithPrefix(aclTsSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)

	// for topic B, nothing has been cleadn
	pageMsgSizeKey = constructKey(PageMsgSizeTitle, "topic_b")
	keys, values, err = rmq.kv.LoadWithPrefix(pageMsgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 50)
	assert.Equal(t, len(values), 50)

	pageTsSizeKey = constructKey(PageTsTitle, "topic_b")
	keys, values, err = rmq.kv.LoadWithPrefix(pageTsSizeKey)
	assert.NoError(t, err)

	assert.Equal(t, len(keys), 50)
	assert.Equal(t, len(values), 50)

	aclTsSizeKey = constructKey(AckedTsTitle, "topic_b")
	keys, values, err = rmq.kv.LoadWithPrefix(aclTsSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)

	topicName = "topic_b"
	_ = rmq.DestroyConsumerGroup(topicName, groupName)
	err = rmq.CreateConsumerGroup(topicName, groupName)
	assert.NoError(t, err)

	consumer = &Consumer{
		Topic:     topicName,
		GroupName: groupName,
	}
	rmq.RegisterConsumer(consumer)

	cMsgs = make([]ConsumerMessage, 0)
	for i := 0; i < msgNum; i++ {
		cMsg, err := rmq.Consume(topicName, groupName, 1)
		assert.Nil(t, err)
		cMsgs = append(cMsgs, cMsg[0])
	}
	assert.Equal(t, len(cMsgs), msgNum)
	assert.Equal(t, cMsgs[0].MsgID, ids2[0])

	time.Sleep(time.Duration(3) * time.Second)

	err = rmq.Seek(topicName, groupName, ids2[10])
	assert.Nil(t, err)
	newRes, err = rmq.Consume(topicName, groupName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(newRes), 0)

}

func TestRetentionInfo_InitRetentionInfo(t *testing.T) {
	err := os.MkdirAll(retentionPath, os.ModePerm)
	if err != nil {
		log.Error("MkdirALl error for path", zap.Any("path", retentionPath))
		return
	}
	defer os.RemoveAll(retentionPath)
	suffix := "init"
	kvPath := retentionPath + kvPathSuffix + suffix
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := retentionPath + suffix
	defer os.RemoveAll(rocksdbPath)
	metaPath := retentionPath + metaPathSuffix + suffix

	defer os.RemoveAll(metaPath)

	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.Nil(t, err)
	assert.NotNil(t, rmq)

	topicName := "topic_a"
	err = rmq.CreateTopic(topicName)
	assert.Nil(t, err)

	rmq.Close()

	rmq, err = NewRocksMQ(rocksdbPath, idAllocator)
	assert.Nil(t, err)
	assert.NotNil(t, rmq)

	assert.Equal(t, rmq.isClosed(), false)
	// write some data, restart and check.
	topicName = "topic_a"
	err = rmq.CreateTopic(topicName)
	assert.Nil(t, err)
	topicName = "topic_b"
	err = rmq.CreateTopic(topicName)
	assert.Nil(t, err)

	msgNum := 100
	pMsgs := make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids, err := rmq.Produce(topicName, pMsgs)
	assert.Nil(t, err)
	assert.Equal(t, len(pMsgs), len(ids))

	rmq.Close()
}

func TestRmqRetention_PageTimeExpire(t *testing.T) {
	err := os.MkdirAll(retentionPath, os.ModePerm)
	if err != nil {
		log.Error("MkdirALl error for path", zap.Any("path", retentionPath))
		return
	}
	defer os.RemoveAll(retentionPath)
	// no retention by size
	atomic.StoreInt64(&RocksmqRetentionSizeInMB, -1)
	// retention by secs
	atomic.StoreInt64(&RocksmqRetentionTimeInSecs, 5)
	atomic.StoreInt64(&RocksmqPageSize, 10)
	atomic.StoreInt64(&TickerTimeInSeconds, 1)
	kvPath := retentionPath + "kv_com1"
	os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := retentionPath + "db_com1"
	os.RemoveAll(rocksdbPath)
	metaPath := retentionPath + "meta_kv_com1"
	os.RemoveAll(metaPath)

	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.Nil(t, err)
	defer rmq.Close()

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
	ids, err := rmq.Produce(topicName, pMsgs)
	assert.Nil(t, err)
	assert.Equal(t, len(pMsgs), len(ids))

	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(topicName, groupName)
	err = rmq.CreateConsumerGroup(topicName, groupName)
	assert.NoError(t, err)

	consumer := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
	}
	rmq.RegisterConsumer(consumer)

	cMsgs := make([]ConsumerMessage, 0)
	for i := 0; i < msgNum; i++ {
		cMsg, err := rmq.Consume(topicName, groupName, 1)
		assert.Nil(t, err)
		cMsgs = append(cMsgs, cMsg[0])
	}
	assert.Equal(t, len(cMsgs), msgNum)
	assert.Equal(t, cMsgs[0].MsgID, ids[0])
	time.Sleep(time.Duration(3) * time.Second)

	// insert another 100 messages which should not be cleand up
	pMsgs2 := make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i+100)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs2[i] = pMsg
	}
	ids2, err := rmq.Produce(topicName, pMsgs2)
	assert.Nil(t, err)
	assert.Equal(t, len(pMsgs2), len(ids2))

	assert.Nil(t, err)
	cMsgs = make([]ConsumerMessage, 0)
	for i := 0; i < msgNum; i++ {
		cMsg, err := rmq.Consume(topicName, groupName, 1)
		assert.Nil(t, err)
		cMsgs = append(cMsgs, cMsg[0])
	}
	assert.Equal(t, len(cMsgs), msgNum)
	assert.Equal(t, cMsgs[0].MsgID, ids2[0])

	time.Sleep(time.Duration(3) * time.Second)

	err = rmq.Seek(topicName, groupName, ids[10])
	assert.Nil(t, err)
	newRes, err := rmq.Consume(topicName, groupName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(newRes), 1)
	// point to first not consumed messages
	assert.Equal(t, newRes[0].MsgID, ids2[0])

	// test acked size acked ts and other meta are updated as expect
	msgSizeKey := MessageSizeTitle + topicName
	msgSizeVal, err := rmq.kv.Load(msgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, msgSizeVal, "0")

	// 100 page left, each entity is a page
	pageMsgSizeKey := constructKey(PageMsgSizeTitle, topicName)
	keys, values, err := rmq.kv.LoadWithPrefix(pageMsgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 100)
	assert.Equal(t, len(values), 100)

	pageTsSizeKey := constructKey(PageTsTitle, topicName)
	keys, values, err = rmq.kv.LoadWithPrefix(pageTsSizeKey)
	assert.NoError(t, err)

	assert.Equal(t, len(keys), 100)
	assert.Equal(t, len(values), 100)

	aclTsSizeKey := constructKey(AckedTsTitle, topicName)
	keys, values, err = rmq.kv.LoadWithPrefix(aclTsSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 100)
	assert.Equal(t, len(values), 100)
}

func TestRmqRetention_PageSizeExpire(t *testing.T) {
	err := os.MkdirAll(retentionPath, os.ModePerm)
	if err != nil {
		log.Error("MkdirALl error for path", zap.Any("path", retentionPath))
		return
	}
	defer os.RemoveAll(retentionPath)
	atomic.StoreInt64(&RocksmqRetentionSizeInMB, 1)
	atomic.StoreInt64(&RocksmqRetentionTimeInSecs, -1)
	atomic.StoreInt64(&RocksmqPageSize, 10)
	atomic.StoreInt64(&TickerTimeInSeconds, 1)
	kvPath := retentionPath + "kv_com2"
	os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := retentionPath + "db_com2"
	os.RemoveAll(rocksdbPath)
	metaPath := retentionPath + "meta_kv_com2"
	os.RemoveAll(metaPath)

	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.Nil(t, err)
	defer rmq.Close()

	topicName := "topic_a"
	err = rmq.CreateTopic(topicName)
	assert.Nil(t, err)
	defer rmq.DestroyTopic(topicName)

	// need to be larger than 1M
	msgNum := 100000
	pMsgs := make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids, err := rmq.Produce(topicName, pMsgs)
	assert.Nil(t, err)
	assert.Equal(t, len(pMsgs), len(ids))

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
	log.Debug("Already consumed, wait for message cleaned by retention")
	// wait for enough time for page expiration
	time.Sleep(time.Duration(2) * time.Second)
	err = rmq.Seek(topicName, groupName, ids[0])
	assert.Nil(t, err)
	newRes, err := rmq.Consume(topicName, groupName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(newRes), 1)
	// make sure clean up happens
	assert.True(t, newRes[0].MsgID > ids[0])
}
