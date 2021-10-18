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
	"math/rand"
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

var retentionPath string = "/tmp/rmq_retention/"

func TestMain(m *testing.M) {
	err := os.MkdirAll(retentionPath, os.ModePerm)
	if err != nil {
		log.Error("MkdirALl error for path", zap.Any("path", retentionPath))
		return
	}
	atomic.StoreInt64(&TickerTimeInSeconds, 6)
	code := m.Run()
	os.Exit(code)
}

func genRandonName() string {
	len := 6
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := r.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

func TestRmqRetention(t *testing.T) {
	atomic.StoreInt64(&RocksmqRetentionSizeInMB, 0)
	atomic.StoreInt64(&RocksmqRetentionTimeInMinutes, 0)
	atomic.StoreInt64(&TickerTimeInSeconds, 2)
	defer atomic.StoreInt64(&TickerTimeInSeconds, 6)
	kvPath := retentionPath + kvPathSuffix
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := retentionPath + dbPathSuffix
	defer os.RemoveAll(rocksdbPath)
	metaPath := retentionPath + metaPathSuffix
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

	//////////////////////////////////////////////////
	lastRetTsKey := LastRetTsTitle + topicName
	rmq.kv.Save(lastRetTsKey, "")
	time.Sleep(time.Duration(checkTimeInterval+1) * time.Second)

	//////////////////////////////////////////////////
	rmq.kv.Save(lastRetTsKey, "dummy")
	time.Sleep(time.Duration(checkTimeInterval+1) * time.Second)
}

func TestRetentionInfo_InitRetentionInfo(t *testing.T) {
	suffix := "init"
	kvPath := retentionPath + kvPathSuffix + suffix
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := retentionPath + dbPathSuffix + suffix
	defer os.RemoveAll(rocksdbPath)
	metaPath := retentionPath + metaPathSuffix + suffix

	defer os.RemoveAll(metaPath)

	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.Nil(t, err)
	assert.NotNil(t, rmq)

	rmq.retentionInfo.kv.DB = nil
	_, err = initRetentionInfo(rmq.retentionInfo.kv, rmq.retentionInfo.db)
	assert.Error(t, err)
}

func TestRetentionInfo_LoadRetentionInfo(t *testing.T) {
	atomic.StoreInt64(&RocksmqRetentionTimeInMinutes, 0)
	atomic.StoreInt64(&RocksmqRetentionSizeInMB, 0)
	atomic.StoreInt64(&RocksmqPageSize, 100)
	kvPath := retentionPath + "kv_" + genRandonName()
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := retentionPath + "db_" + genRandonName()
	defer os.RemoveAll(rocksdbPath)
	metaPath := retentionPath + "meta_" + genRandonName()
	defer os.RemoveAll(metaPath)

	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.Nil(t, err)
	defer rmq.Close()

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
	ids, err := rmq.Produce(topicName, pMsgs)
	assert.Nil(t, err)
	assert.Equal(t, len(pMsgs), len(ids))

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

	_, err = initRetentionInfo(rmq.retentionInfo.kv, rmq.store)
	assert.Nil(t, err)

	dummyTopic := strings.Repeat(topicName, 100)
	err = DeleteMessages(rmq.store, dummyTopic, 0, 0)
	assert.Error(t, err)

	err = DeleteMessages(rmq.store, topicName, 0, 0)
	assert.NoError(t, err)

	//////////////////////////////////////////////////
	ackedTsPrefix, _ := constructKey(AckedTsTitle, topicName)
	ackedTsKey0 := ackedTsPrefix + "/1"
	err = rmq.retentionInfo.kv.Save(ackedTsKey0, "dummy")
	assert.Nil(t, err)
	wg.Add(1)
	rmq.retentionInfo.loadRetentionInfo(topicName, &wg)
	err = rmq.retentionInfo.kv.Remove(ackedTsKey0)
	assert.Nil(t, err)

	//////////////////////////////////////////////////
	ackedTsKey1 := ackedTsPrefix + "/dummy"
	err = rmq.retentionInfo.kv.Save(ackedTsKey1, "dummy")
	assert.Nil(t, err)
	wg.Add(1)
	rmq.retentionInfo.loadRetentionInfo(topicName, &wg)
	err = rmq.retentionInfo.kv.Remove(ackedTsKey1)
	assert.Nil(t, err)

	//////////////////////////////////////////////////
	lastRetentionTsKey := LastRetTsTitle + topicName
	err = rmq.retentionInfo.kv.Save(lastRetentionTsKey, strconv.FormatInt(1, 10))
	assert.Nil(t, err)
	wg.Add(1)
	rmq.retentionInfo.loadRetentionInfo(topicName, &wg)
	err = rmq.retentionInfo.kv.Remove(lastRetentionTsKey)
	assert.Nil(t, err)

	//////////////////////////////////////////////////
	err = rmq.retentionInfo.kv.Save(lastRetentionTsKey, "dummy")
	assert.Nil(t, err)
	wg.Add(1)
	rmq.retentionInfo.loadRetentionInfo(topicName, &wg)
	err = rmq.retentionInfo.kv.Remove(lastRetentionTsKey)
	assert.Nil(t, err)

	//////////////////////////////////////////////////
	ackedSizeKey := AckedSizeTitle + topicName
	err = rmq.retentionInfo.kv.Save(ackedSizeKey, strconv.FormatInt(1, 10))
	assert.Nil(t, err)
	wg.Add(1)
	rmq.retentionInfo.loadRetentionInfo(topicName, &wg)
	err = rmq.retentionInfo.kv.Remove(ackedSizeKey)
	assert.Nil(t, err)

	//////////////////////////////////////////////////
	err = rmq.retentionInfo.kv.Save(ackedSizeKey, "")
	assert.Nil(t, err)
	wg.Add(1)
	rmq.retentionInfo.loadRetentionInfo(topicName, &wg)
	err = rmq.retentionInfo.kv.Remove(ackedSizeKey)
	assert.Nil(t, err)

	//////////////////////////////////////////////////
	err = rmq.retentionInfo.kv.Save(ackedSizeKey, "dummy")
	assert.Nil(t, err)
	wg.Add(1)
	rmq.retentionInfo.loadRetentionInfo(topicName, &wg)
	err = rmq.retentionInfo.kv.Remove(ackedSizeKey)
	assert.Nil(t, err)

	//////////////////////////////////////////////////
	topicBeginIDKey := TopicBeginIDTitle + topicName
	err = rmq.retentionInfo.kv.Save(topicBeginIDKey, "dummy")
	assert.Nil(t, err)
	wg.Add(1)
	rmq.retentionInfo.loadRetentionInfo(topicName, &wg)
	err = rmq.retentionInfo.kv.Remove(topicBeginIDKey)
	assert.Nil(t, err)

	////////////////////////////////////////////////////
	fixedPageSizeKey0, _ := constructKey(PageMsgSizeTitle, topicName)
	pageMsgSizeKey0 := fixedPageSizeKey0 + "/" + "1"
	err = rmq.retentionInfo.kv.Save(pageMsgSizeKey0, "dummy")
	assert.Nil(t, err)
	wg.Add(1)
	rmq.retentionInfo.loadRetentionInfo(topicName, &wg)
	err = rmq.retentionInfo.kv.Remove(pageMsgSizeKey0)
	assert.Nil(t, err)

	//////////////////////////////////////////////////
	fixedPageSizeKey1, _ := constructKey(PageMsgSizeTitle, topicName)
	pageMsgSizeKey1 := fixedPageSizeKey1 + "/" + "dummy"
	rmq.retentionInfo.kv.Save(pageMsgSizeKey1, "dummy")
	wg.Add(1)
	rmq.retentionInfo.loadRetentionInfo(topicName, &wg)
	rmq.retentionInfo.kv.Remove(pageMsgSizeKey1)

	//////////////////////////////////////////////////
	pageMsgPrefix, _ := constructKey(PageMsgSizeTitle, topicName)
	pageMsgKey := pageMsgPrefix + "/dummy"
	rmq.kv.Save(pageMsgKey, "0")
	rmq.retentionInfo.newExpiredCleanUp(topicName)

	//////////////////////////////////////////////////
	rmq.retentionInfo.kv.DB = nil
	wg.Add(1)
	rmq.retentionInfo.loadRetentionInfo(topicName, &wg)
	rmq.retentionInfo.kv.Remove(pageMsgSizeKey1)

	//////////////////////////////////////////////////
	longTopic := strings.Repeat("dummy", 100)
	wg.Add(1)
	rmq.retentionInfo.loadRetentionInfo(longTopic, &wg)

	//////////////////////////////////////////////////
	topicMu.Delete(topicName)
	topicMu.Store(topicName, topicName)
	rmq.retentionInfo.newExpiredCleanUp(topicName)

	//////////////////////////////////////////////////
	topicMu.Delete(topicName)
	rmq.retentionInfo.newExpiredCleanUp(topicName)

	//////////////////////////////////////////////////
	rmq.retentionInfo.ackedInfo.Delete(topicName)
	rmq.retentionInfo.newExpiredCleanUp(topicName)
}

func TestRmqRetention_Complex(t *testing.T) {
	atomic.StoreInt64(&RocksmqRetentionSizeInMB, 0)
	atomic.StoreInt64(&RocksmqRetentionTimeInMinutes, 1)
	atomic.StoreInt64(&RocksmqPageSize, 10)
	kvPath := retentionPath + "kv_com"
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := retentionPath + "db_com"
	defer os.RemoveAll(rocksdbPath)
	metaPath := retentionPath + "meta_kv_com"
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
	kvPath := retentionPath + "kv_com1"
	os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := retentionPath + "db_com1"
	os.RemoveAll(rocksdbPath)
	metaPath := retentionPath + "meta_kv_com1"
	os.RemoveAll(metaPath)

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
