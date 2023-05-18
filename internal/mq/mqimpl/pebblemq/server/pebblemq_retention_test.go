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
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var retentionPath = "/tmp/pmq_retention/"

// Test write data and wait for retention
func TestPebblemqRetention_Basic(t *testing.T) {
	err := os.MkdirAll(retentionPath, os.ModePerm)
	if err != nil {
		log.Error("MkdirAll error for path", zap.Any("path", retentionPath))
		return
	}
	defer os.RemoveAll(retentionPath)
	pebbledbPath := retentionPath
	defer os.RemoveAll(pebbledbPath)
	metaPath := retentionPath + metaPathSuffix
	defer os.RemoveAll(metaPath)

	params := paramtable.Get()
	paramtable.Init()

	params.Save(params.PebblemqCfg.PageSize.Key, "10")
	params.Save(params.PebblemqCfg.TickerTimeInSeconds.Key, "2")
	pmq, err := NewPebbleMQ(pebbledbPath, nil)
	assert.NoError(t, err)
	defer pmq.Close()
	params.Save(params.PebblemqCfg.RetentionSizeInMB.Key, "0")
	params.Save(params.PebblemqCfg.RetentionTimeInMinutes.Key, "0")

	topicName := "topic_a"
	err = pmq.CreateTopic(topicName)
	assert.NoError(t, err)
	defer pmq.DestroyTopic(topicName)

	msgNum := 100
	pMsgs := make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids, err := pmq.Produce(topicName, pMsgs)
	assert.NoError(t, err)
	assert.Equal(t, len(pMsgs), len(ids))

	groupName := "test_group"
	_ = pmq.DestroyConsumerGroup(topicName, groupName)
	err = pmq.CreateConsumerGroup(topicName, groupName)

	consumer := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
	}
	pmq.RegisterConsumer(consumer)

	assert.NoError(t, err)
	cMsgs := make([]ConsumerMessage, 0)
	for i := 0; i < msgNum; i++ {
		cMsg, err := pmq.Consume(topicName, groupName, 1)
		assert.NoError(t, err)
		cMsgs = append(cMsgs, cMsg[0])
	}
	assert.Equal(t, len(cMsgs), msgNum)

	pmq.Info()
	time.Sleep(time.Duration(3) * time.Second)

	// Seek to a previous consumed message, the message should be clean up
	err = pmq.ForceSeek(topicName, groupName, cMsgs[msgNum/2].MsgID)
	assert.NoError(t, err)
	newRes, err := pmq.Consume(topicName, groupName, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(newRes), 0)

	// test acked size acked ts and other meta are updated as expect
	msgSizeKey := MessageSizeTitle + topicName
	msgSizeVal, err := pmq.kv.Load(msgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, msgSizeVal, "0")

	pageMsgSizeKey := constructKey(PageMsgSizeTitle, topicName)
	keys, values, err := pmq.kv.LoadWithPrefix(pageMsgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)

	pageTsSizeKey := constructKey(PageTsTitle, topicName)
	keys, values, err = pmq.kv.LoadWithPrefix(pageTsSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)

	aclTsSizeKey := constructKey(AckedTsTitle, topicName)
	keys, values, err = pmq.kv.LoadWithPrefix(aclTsSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)
}

// Not acked message should not be purged
func TestPebblemqRetention_NotConsumed(t *testing.T) {
	err := os.MkdirAll(retentionPath, os.ModePerm)
	if err != nil {
		log.Error("MkdirAll error for path", zap.Any("path", retentionPath))
		return
	}
	defer os.RemoveAll(retentionPath)

	pebbledbPath := retentionPath
	defer os.RemoveAll(pebbledbPath)
	metaPath := retentionPath + metaPathSuffix
	defer os.RemoveAll(metaPath)

	params := paramtable.Get()
	paramtable.Init()

	params.Save(params.PebblemqCfg.PageSize.Key, "10")
	params.Save(params.PebblemqCfg.TickerTimeInSeconds.Key, "2")
	pmq, err := NewPebbleMQ(pebbledbPath, nil)
	assert.NoError(t, err)
	defer pmq.Close()

	params.Save(params.PebblemqCfg.RetentionSizeInMB.Key, "0")
	params.Save(params.PebblemqCfg.RetentionTimeInMinutes.Key, "0")

	topicName := "topic_a"
	err = pmq.CreateTopic(topicName)
	assert.NoError(t, err)
	defer pmq.DestroyTopic(topicName)

	msgNum := 100
	pMsgs := make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids, err := pmq.Produce(topicName, pMsgs)
	assert.NoError(t, err)
	assert.Equal(t, len(pMsgs), len(ids))

	groupName := "test_group"
	_ = pmq.DestroyConsumerGroup(topicName, groupName)
	err = pmq.CreateConsumerGroup(topicName, groupName)

	consumer := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
	}
	pmq.RegisterConsumer(consumer)

	assert.NoError(t, err)
	cMsgs := make([]ConsumerMessage, 0)
	for i := 0; i < 5; i++ {
		cMsg, err := pmq.Consume(topicName, groupName, 1)
		assert.NoError(t, err)
		cMsgs = append(cMsgs, cMsg[0])
	}
	assert.Equal(t, len(cMsgs), 5)
	id := cMsgs[0].MsgID

	aclTsSizeKey := constructKey(AckedTsTitle, topicName)
	keys, values, err := pmq.kv.LoadWithPrefix(aclTsSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 2)
	assert.Equal(t, len(values), 2)

	// wait for retention
	checkTimeInterval := 2
	time.Sleep(time.Duration(checkTimeInterval+1) * time.Second)

	// Seek to a previous consumed message, the message should be clean up
	err = pmq.ForceSeek(topicName, groupName, cMsgs[1].MsgID)
	assert.NoError(t, err)
	newRes, err := pmq.Consume(topicName, groupName, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(newRes), 1)
	assert.Equal(t, newRes[0].MsgID, id+4)

	// test acked size acked ts and other meta are updated as expect
	msgSizeKey := MessageSizeTitle + topicName
	msgSizeVal, err := pmq.kv.Load(msgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, msgSizeVal, "0")

	// should only clean 2 pages
	pageMsgSizeKey := constructKey(PageMsgSizeTitle, topicName)
	keys, values, err = pmq.kv.LoadWithPrefix(pageMsgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 48)
	assert.Equal(t, len(values), 48)

	pageTsSizeKey := constructKey(PageTsTitle, topicName)
	keys, values, err = pmq.kv.LoadWithPrefix(pageTsSizeKey)
	assert.NoError(t, err)

	assert.Equal(t, len(keys), 48)
	assert.Equal(t, len(values), 48)

	aclTsSizeKey = constructKey(AckedTsTitle, topicName)
	keys, values, err = pmq.kv.LoadWithPrefix(aclTsSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)
}

// Test multiple topic
func TestPebblemqRetention_MultipleTopic(t *testing.T) {
	err := os.MkdirAll(retentionPath, os.ModePerm)
	if err != nil {
		log.Error("MkdirALl error for path", zap.Any("path", retentionPath))
		return
	}
	defer os.RemoveAll(retentionPath)
	kvPath := retentionPath + "kv_multi_topic"
	os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	pebbledbPath := retentionPath + "db_multi_topic"
	os.RemoveAll(pebbledbPath)
	metaPath := retentionPath + "meta_multi_topic"
	os.RemoveAll(metaPath)

	params := paramtable.Get()
	paramtable.Init()

	params.Save(params.PebblemqCfg.PageSize.Key, "10")
	params.Save(params.PebblemqCfg.TickerTimeInSeconds.Key, "1")

	pmq, err := NewPebbleMQ(pebbledbPath, idAllocator)
	assert.NoError(t, err)
	defer pmq.Close()

	// no retention by size
	params.Save(params.PebblemqCfg.RetentionSizeInMB.Key, "-1")
	// retention by secs
	params.Save(params.PebblemqCfg.RetentionTimeInMinutes.Key, "0.017")

	topicName := "topic_a"
	err = pmq.CreateTopic(topicName)
	assert.NoError(t, err)
	defer pmq.DestroyTopic(topicName)

	msgNum := 100
	pMsgs := make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids1, err := pmq.Produce(topicName, pMsgs)
	assert.NoError(t, err)
	assert.Equal(t, len(pMsgs), len(ids1))

	topicName = "topic_b"
	err = pmq.CreateTopic(topicName)
	assert.NoError(t, err)
	defer pmq.DestroyTopic(topicName)
	pMsgs = make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids2, err := pmq.Produce(topicName, pMsgs)
	assert.NoError(t, err)
	assert.Equal(t, len(pMsgs), len(ids2))

	topicName = "topic_a"
	groupName := "test_group"
	_ = pmq.DestroyConsumerGroup(topicName, groupName)
	err = pmq.CreateConsumerGroup(topicName, groupName)
	assert.NoError(t, err)

	consumer := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
	}
	pmq.RegisterConsumer(consumer)

	cMsgs := make([]ConsumerMessage, 0)
	for i := 0; i < msgNum; i++ {
		cMsg, err := pmq.Consume(topicName, groupName, 1)
		assert.NoError(t, err)
		cMsgs = append(cMsgs, cMsg[0])
	}
	assert.Equal(t, len(cMsgs), msgNum)
	assert.Equal(t, cMsgs[0].MsgID, ids1[0])

	time.Sleep(time.Duration(3) * time.Second)

	err = pmq.ForceSeek(topicName, groupName, ids1[10])
	assert.NoError(t, err)
	newRes, err := pmq.Consume(topicName, groupName, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(newRes), 0)

	// test acked size acked ts and other meta are updated as expect
	msgSizeKey := MessageSizeTitle + topicName
	msgSizeVal, err := pmq.kv.Load(msgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, msgSizeVal, "0")

	// 100 page left, each entity is a page
	pageMsgSizeKey := constructKey(PageMsgSizeTitle, "topic_a")
	keys, values, err := pmq.kv.LoadWithPrefix(pageMsgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)

	pageTsSizeKey := constructKey(PageTsTitle, "topic_a")
	keys, values, err = pmq.kv.LoadWithPrefix(pageTsSizeKey)
	assert.NoError(t, err)

	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)

	aclTsSizeKey := constructKey(AckedTsTitle, "topic_a")
	keys, values, err = pmq.kv.LoadWithPrefix(aclTsSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)

	// for topic B, nothing has been cleadn
	pageMsgSizeKey = constructKey(PageMsgSizeTitle, "topic_b")
	keys, values, err = pmq.kv.LoadWithPrefix(pageMsgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 50)
	assert.Equal(t, len(values), 50)

	pageTsSizeKey = constructKey(PageTsTitle, "topic_b")
	keys, values, err = pmq.kv.LoadWithPrefix(pageTsSizeKey)
	assert.NoError(t, err)

	assert.Equal(t, len(keys), 50)
	assert.Equal(t, len(values), 50)

	aclTsSizeKey = constructKey(AckedTsTitle, "topic_b")
	keys, values, err = pmq.kv.LoadWithPrefix(aclTsSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)

	topicName = "topic_b"
	_ = pmq.DestroyConsumerGroup(topicName, groupName)
	err = pmq.CreateConsumerGroup(topicName, groupName)
	assert.NoError(t, err)

	consumer = &Consumer{
		Topic:     topicName,
		GroupName: groupName,
	}
	pmq.RegisterConsumer(consumer)

	cMsgs = make([]ConsumerMessage, 0)
	for i := 0; i < msgNum; i++ {
		cMsg, err := pmq.Consume(topicName, groupName, 1)
		assert.NoError(t, err)
		cMsgs = append(cMsgs, cMsg[0])
	}
	assert.Equal(t, len(cMsgs), msgNum)
	assert.Equal(t, cMsgs[0].MsgID, ids2[0])

	time.Sleep(time.Duration(3) * time.Second)

	err = pmq.ForceSeek(topicName, groupName, ids2[10])
	assert.NoError(t, err)
	newRes, err = pmq.Consume(topicName, groupName, 1)
	assert.NoError(t, err)
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

	pebbledbPath := retentionPath + suffix
	defer os.RemoveAll(pebbledbPath)
	metaPath := retentionPath + metaPathSuffix + suffix

	defer os.RemoveAll(metaPath)

	paramtable.Init()
	pmq, err := NewPebbleMQ(pebbledbPath, idAllocator)
	assert.NoError(t, err)
	assert.NotNil(t, pmq)

	topicName := "topic_a"
	err = pmq.CreateTopic(topicName)
	assert.NoError(t, err)

	pmq.Close()
	pmq, err = NewPebbleMQ(pebbledbPath, idAllocator)
	assert.NoError(t, err)
	assert.NotNil(t, pmq)

	assert.Equal(t, pmq.isClosed(), false)
	// write some data, restart and check.
	topicName = "topic_a"
	err = pmq.CreateTopic(topicName)
	assert.NoError(t, err)
	topicName = "topic_b"
	err = pmq.CreateTopic(topicName)
	assert.NoError(t, err)

	msgNum := 100
	pMsgs := make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids, err := pmq.Produce(topicName, pMsgs)
	assert.NoError(t, err)
	assert.Equal(t, len(pMsgs), len(ids))

	pmq.Close()
}

func TestPebblemqRetention_PageTimeExpire(t *testing.T) {
	err := os.MkdirAll(retentionPath, os.ModePerm)
	if err != nil {
		log.Error("MkdirALl error for path", zap.Any("path", retentionPath))
		return
	}
	defer os.RemoveAll(retentionPath)

	kvPath := retentionPath + "kv_com1"
	os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	pebbledbPath := retentionPath + "db_com1"
	os.RemoveAll(pebbledbPath)
	metaPath := retentionPath + "meta_kv_com1"
	os.RemoveAll(metaPath)

	params := paramtable.Get()
	paramtable.Init()

	params.Save(params.PebblemqCfg.PageSize.Key, "10")
	params.Save(params.PebblemqCfg.TickerTimeInSeconds.Key, "1")

	pmq, err := NewPebbleMQ(pebbledbPath, idAllocator)
	assert.NoError(t, err)
	defer pmq.Close()

	// no retention by size
	params.Save(params.PebblemqCfg.RetentionSizeInMB.Key, "-1")
	// retention by secs
	params.Save(params.PebblemqCfg.RetentionTimeInMinutes.Key, "0.084")

	topicName := "topic_a"
	err = pmq.CreateTopic(topicName)
	assert.NoError(t, err)
	defer pmq.DestroyTopic(topicName)

	msgNum := 100
	pMsgs := make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids, err := pmq.Produce(topicName, pMsgs)
	assert.NoError(t, err)
	assert.Equal(t, len(pMsgs), len(ids))

	groupName := "test_group"
	_ = pmq.DestroyConsumerGroup(topicName, groupName)
	err = pmq.CreateConsumerGroup(topicName, groupName)
	assert.NoError(t, err)

	consumer := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
	}
	pmq.RegisterConsumer(consumer)

	cMsgs := make([]ConsumerMessage, 0)
	for i := 0; i < msgNum; i++ {
		cMsg, err := pmq.Consume(topicName, groupName, 1)
		assert.NoError(t, err)
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
	ids2, err := pmq.Produce(topicName, pMsgs2)
	assert.NoError(t, err)
	assert.Equal(t, len(pMsgs2), len(ids2))

	assert.NoError(t, err)
	cMsgs = make([]ConsumerMessage, 0)
	for i := 0; i < msgNum; i++ {
		cMsg, err := pmq.Consume(topicName, groupName, 1)
		assert.NoError(t, err)
		cMsgs = append(cMsgs, cMsg[0])
	}
	assert.Equal(t, len(cMsgs), msgNum)
	assert.Equal(t, cMsgs[0].MsgID, ids2[0])

	assert.Eventually(t, func() bool {
		err = pmq.ForceSeek(topicName, groupName, ids[0])
		assert.NoError(t, err)
		newRes, err := pmq.Consume(topicName, groupName, 1)
		assert.NoError(t, err)
		assert.Equal(t, len(newRes), 1)
		// point to first not consumed messages
		return newRes[0].MsgID == ids2[0]
	}, 5*time.Second, 1*time.Second)

	// test acked size acked ts and other meta are updated as expect
	msgSizeKey := MessageSizeTitle + topicName
	msgSizeVal, err := pmq.kv.Load(msgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, msgSizeVal, "0")

	// 100 page left, each entity is a page
	pageMsgSizeKey := constructKey(PageMsgSizeTitle, topicName)
	keys, values, err := pmq.kv.LoadWithPrefix(pageMsgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 100)
	assert.Equal(t, len(values), 100)

	pageTsSizeKey := constructKey(PageTsTitle, topicName)
	keys, values, err = pmq.kv.LoadWithPrefix(pageTsSizeKey)
	assert.NoError(t, err)

	assert.Equal(t, len(keys), 100)
	assert.Equal(t, len(values), 100)

	aclTsSizeKey := constructKey(AckedTsTitle, topicName)
	keys, values, err = pmq.kv.LoadWithPrefix(aclTsSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 100)
	assert.Equal(t, len(values), 100)
}

func TestPebblemqRetention_PageSizeExpire(t *testing.T) {
	err := os.MkdirAll(retentionPath, os.ModePerm)
	if err != nil {
		log.Error("MkdirALl error for path", zap.Any("path", retentionPath))
		return
	}
	defer os.RemoveAll(retentionPath)
	kvPath := retentionPath + "kv_com2"
	os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	pebbledbPath := retentionPath + "db_com2"
	os.RemoveAll(pebbledbPath)
	metaPath := retentionPath + "meta_kv_com2"
	os.RemoveAll(metaPath)

	params := paramtable.Get()
	paramtable.Init()

	params.Save(params.PebblemqCfg.PageSize.Key, "10")
	params.Save(params.PebblemqCfg.TickerTimeInSeconds.Key, "1")

	pmq, err := NewPebbleMQ(pebbledbPath, idAllocator)
	assert.NoError(t, err)
	defer pmq.Close()

	// no retention by size
	params.Save(params.PebblemqCfg.RetentionSizeInMB.Key, "1")
	// retention by secs
	params.Save(params.PebblemqCfg.RetentionTimeInMinutes.Key, "-1")

	topicName := "topic_a"
	err = pmq.CreateTopic(topicName)
	assert.NoError(t, err)
	defer pmq.DestroyTopic(topicName)

	// need to be larger than 1M
	msgNum := 100000
	pMsgs := make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids, err := pmq.Produce(topicName, pMsgs)
	assert.NoError(t, err)
	assert.Equal(t, len(pMsgs), len(ids))

	groupName := "test_group"
	_ = pmq.DestroyConsumerGroup(topicName, groupName)
	err = pmq.CreateConsumerGroup(topicName, groupName)

	consumer := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
	}
	pmq.RegisterConsumer(consumer)

	assert.NoError(t, err)
	cMsgs := make([]ConsumerMessage, 0)
	for i := 0; i < msgNum; i++ {
		cMsg, err := pmq.Consume(topicName, groupName, 1)
		assert.NoError(t, err)
		cMsgs = append(cMsgs, cMsg[0])
	}
	assert.Equal(t, len(cMsgs), msgNum)
	log.Debug("Already consumed, wait for message cleaned by retention")
	// wait for enough time for page expiration
	time.Sleep(time.Duration(2) * time.Second)
	err = pmq.ForceSeek(topicName, groupName, ids[0])
	assert.NoError(t, err)
	newRes, err := pmq.Consume(topicName, groupName, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(newRes), 1)
	// make sure clean up happens
	assert.True(t, newRes[0].MsgID > ids[0])
}
