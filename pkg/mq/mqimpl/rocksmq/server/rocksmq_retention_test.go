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
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var retentionPath = "/tmp/rmq_retention/"

// Test write data and wait for retention
func TestRmqRetention_Basic(t *testing.T) {
	err := os.MkdirAll(retentionPath, os.ModePerm)
	if err != nil {
		log.Error("MkdirAll error for path", zap.Any("path", retentionPath))
		return
	}
	defer os.RemoveAll(retentionPath)
	rocksdbPath := retentionPath
	defer os.RemoveAll(rocksdbPath)
	metaPath := retentionPath + metaPathSuffix
	defer os.RemoveAll(metaPath)

	params := paramtable.Get()
	paramtable.Init()

	params.Save(params.RocksmqCfg.PageSize.Key, "10")
	params.Save(params.RocksmqCfg.TickerTimeInSeconds.Key, "2")
	rmq, err := NewRocksMQ(rocksdbPath)
	assert.NoError(t, err)
	defer rmq.Close()
	params.Save(params.RocksmqCfg.RetentionSizeInMB.Key, "0")
	params.Save(params.RocksmqCfg.RetentionTimeInMinutes.Key, "0")

	topicName := "topic_a"
	err = rmq.CreateTopic(topicName)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(topicName)

	msgNum := 100
	pMsgs := make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids, err := rmq.Produce(topicName, pMsgs)
	assert.NoError(t, err)
	assert.Equal(t, len(pMsgs), len(ids))

	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(topicName, groupName)
	err = rmq.CreateConsumerGroup(topicName, groupName)

	consumer := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
	}
	rmq.RegisterConsumer(consumer)

	assert.NoError(t, err)
	cMsgs := make([]ConsumerMessage, 0)
	for i := 0; i < msgNum; i++ {
		cMsg, err := rmq.Consume(topicName, groupName, 1)
		assert.NoError(t, err)
		cMsgs = append(cMsgs, cMsg[0])
	}
	assert.Equal(t, len(cMsgs), msgNum)

	rmq.Info()
	time.Sleep(time.Duration(3) * time.Second)

	// Seek to a previous consumed message, the message should be clean up
	err = rmq.ForceSeek(topicName, groupName, cMsgs[msgNum/2].MsgID)
	assert.NoError(t, err)
	newRes, err := rmq.Consume(topicName, groupName, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(newRes), 0)

	// test acked size acked ts and other meta are updated as expect
	msgSizeKey := MessageSizeTitle + topicName
	msgSizeVal, err := rmq.kv.Load(context.TODO(), msgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, msgSizeVal, "0")

	pageMsgSizeKey := constructKey(PageMsgSizeTitle, topicName)
	keys, values, err := rmq.kv.LoadWithPrefix(context.TODO(), pageMsgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)

	pageTsSizeKey := constructKey(PageTsTitle, topicName)
	keys, values, err = rmq.kv.LoadWithPrefix(context.TODO(), pageTsSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)

	aclTsSizeKey := constructKey(AckedTsTitle, topicName)
	keys, values, err = rmq.kv.LoadWithPrefix(context.TODO(), aclTsSizeKey)
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

	rocksdbPath := retentionPath
	defer os.RemoveAll(rocksdbPath)
	metaPath := retentionPath + metaPathSuffix
	defer os.RemoveAll(metaPath)

	params := paramtable.Get()
	paramtable.Init()

	params.Save(params.RocksmqCfg.PageSize.Key, "10")
	params.Save(params.RocksmqCfg.TickerTimeInSeconds.Key, "2")
	rmq, err := NewRocksMQ(rocksdbPath)
	assert.NoError(t, err)
	defer rmq.Close()

	params.Save(params.RocksmqCfg.RetentionSizeInMB.Key, "0")
	params.Save(params.RocksmqCfg.RetentionTimeInMinutes.Key, "0")

	topicName := "topic_a"
	err = rmq.CreateTopic(topicName)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(topicName)

	msgNum := 100
	pMsgs := make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids, err := rmq.Produce(topicName, pMsgs)
	assert.NoError(t, err)
	assert.Equal(t, len(pMsgs), len(ids))

	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(topicName, groupName)
	err = rmq.CreateConsumerGroup(topicName, groupName)

	consumer := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
	}
	rmq.RegisterConsumer(consumer)

	assert.NoError(t, err)
	cMsgs := make([]ConsumerMessage, 0)
	for i := 0; i < 5; i++ {
		cMsg, err := rmq.Consume(topicName, groupName, 1)
		assert.NoError(t, err)
		cMsgs = append(cMsgs, cMsg[0])
	}
	assert.Equal(t, len(cMsgs), 5)
	id := cMsgs[0].MsgID

	aclTsSizeKey := constructKey(AckedTsTitle, topicName)
	keys, values, err := rmq.kv.LoadWithPrefix(context.TODO(), aclTsSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 2)
	assert.Equal(t, len(values), 2)

	// wait for retention
	checkTimeInterval := 2
	time.Sleep(time.Duration(checkTimeInterval+1) * time.Second)

	// Seek to a previous consumed message, the message should be clean up
	err = rmq.ForceSeek(topicName, groupName, cMsgs[1].MsgID)
	assert.NoError(t, err)
	newRes, err := rmq.Consume(topicName, groupName, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(newRes), 1)
	assert.Equal(t, newRes[0].MsgID, id+4)

	// test acked size acked ts and other meta are updated as expect
	msgSizeKey := MessageSizeTitle + topicName
	msgSizeVal, err := rmq.kv.Load(context.TODO(), msgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, msgSizeVal, "0")

	// should only clean 2 pages
	pageMsgSizeKey := constructKey(PageMsgSizeTitle, topicName)
	keys, values, err = rmq.kv.LoadWithPrefix(context.TODO(), pageMsgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 48)
	assert.Equal(t, len(values), 48)

	pageTsSizeKey := constructKey(PageTsTitle, topicName)
	keys, values, err = rmq.kv.LoadWithPrefix(context.TODO(), pageTsSizeKey)
	assert.NoError(t, err)

	assert.Equal(t, len(keys), 48)
	assert.Equal(t, len(values), 48)

	aclTsSizeKey = constructKey(AckedTsTitle, topicName)
	keys, values, err = rmq.kv.LoadWithPrefix(context.TODO(), aclTsSizeKey)
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

	rocksdbPath := retentionPath + "db_multi_topic"
	os.RemoveAll(rocksdbPath)
	metaPath := retentionPath + "meta_multi_topic"
	os.RemoveAll(metaPath)

	params := paramtable.Get()
	paramtable.Init()

	params.Save(params.RocksmqCfg.PageSize.Key, "10")
	params.Save(params.RocksmqCfg.TickerTimeInSeconds.Key, "1")

	rmq, err := NewRocksMQ(rocksdbPath)
	assert.NoError(t, err)
	defer rmq.Close()

	// no retention by size
	params.Save(params.RocksmqCfg.RetentionSizeInMB.Key, "-1")
	// retention by secs
	params.Save(params.RocksmqCfg.RetentionTimeInMinutes.Key, "0.017")

	topicName := "topic_a"
	err = rmq.CreateTopic(topicName)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(topicName)

	msgNum := 100
	pMsgs := make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids1, err := rmq.Produce(topicName, pMsgs)
	assert.NoError(t, err)
	assert.Equal(t, len(pMsgs), len(ids1))

	topicName = "topic_b"
	err = rmq.CreateTopic(topicName)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(topicName)
	pMsgs = make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids2, err := rmq.Produce(topicName, pMsgs)
	assert.NoError(t, err)
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
		assert.NoError(t, err)
		cMsgs = append(cMsgs, cMsg[0])
	}
	assert.Equal(t, len(cMsgs), msgNum)
	assert.Equal(t, cMsgs[0].MsgID, ids1[0])

	time.Sleep(time.Duration(3) * time.Second)

	err = rmq.ForceSeek(topicName, groupName, ids1[10])
	assert.NoError(t, err)
	newRes, err := rmq.Consume(topicName, groupName, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(newRes), 0)

	// test acked size acked ts and other meta are updated as expect
	msgSizeKey := MessageSizeTitle + topicName
	msgSizeVal, err := rmq.kv.Load(context.TODO(), msgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, msgSizeVal, "0")

	// 100 page left, each entity is a page
	pageMsgSizeKey := constructKey(PageMsgSizeTitle, "topic_a")
	keys, values, err := rmq.kv.LoadWithPrefix(context.TODO(), pageMsgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)

	pageTsSizeKey := constructKey(PageTsTitle, "topic_a")
	keys, values, err = rmq.kv.LoadWithPrefix(context.TODO(), pageTsSizeKey)
	assert.NoError(t, err)

	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)

	aclTsSizeKey := constructKey(AckedTsTitle, "topic_a")
	keys, values, err = rmq.kv.LoadWithPrefix(context.TODO(), aclTsSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)

	// for topic B, nothing has been cleadn
	pageMsgSizeKey = constructKey(PageMsgSizeTitle, "topic_b")
	keys, values, err = rmq.kv.LoadWithPrefix(context.TODO(), pageMsgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 50)
	assert.Equal(t, len(values), 50)

	pageTsSizeKey = constructKey(PageTsTitle, "topic_b")
	keys, values, err = rmq.kv.LoadWithPrefix(context.TODO(), pageTsSizeKey)
	assert.NoError(t, err)

	assert.Equal(t, len(keys), 50)
	assert.Equal(t, len(values), 50)

	aclTsSizeKey = constructKey(AckedTsTitle, "topic_b")
	keys, values, err = rmq.kv.LoadWithPrefix(context.TODO(), aclTsSizeKey)
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
		assert.NoError(t, err)
		cMsgs = append(cMsgs, cMsg[0])
	}
	assert.Equal(t, len(cMsgs), msgNum)
	assert.Equal(t, cMsgs[0].MsgID, ids2[0])

	time.Sleep(time.Duration(3) * time.Second)

	err = rmq.ForceSeek(topicName, groupName, ids2[10])
	assert.NoError(t, err)
	newRes, err = rmq.Consume(topicName, groupName, 1)
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
	rocksdbPath := retentionPath + suffix
	defer os.RemoveAll(rocksdbPath)
	metaPath := retentionPath + metaPathSuffix + suffix

	defer os.RemoveAll(metaPath)

	paramtable.Init()
	rmq, err := NewRocksMQ(rocksdbPath)
	assert.NoError(t, err)
	assert.NotNil(t, rmq)

	topicName := "topic_a"
	err = rmq.CreateTopic(topicName)
	assert.NoError(t, err)

	rmq.Close()
	rmq, err = NewRocksMQ(rocksdbPath)
	assert.NoError(t, err)
	assert.NotNil(t, rmq)

	assert.Equal(t, rmq.isClosed(), false)
	// write some data, restart and check.
	topicName = "topic_a"
	err = rmq.CreateTopic(topicName)
	assert.NoError(t, err)
	topicName = "topic_b"
	err = rmq.CreateTopic(topicName)
	assert.NoError(t, err)

	msgNum := 100
	pMsgs := make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids, err := rmq.Produce(topicName, pMsgs)
	assert.NoError(t, err)
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

	rocksdbPath := retentionPath + "db_com1"
	os.RemoveAll(rocksdbPath)
	metaPath := retentionPath + "meta_kv_com1"
	os.RemoveAll(metaPath)

	params := paramtable.Get()
	paramtable.Init()

	params.Save(params.RocksmqCfg.PageSize.Key, "10")
	params.Save(params.RocksmqCfg.TickerTimeInSeconds.Key, "1")

	rmq, err := NewRocksMQ(rocksdbPath)
	assert.NoError(t, err)
	defer rmq.Close()

	// no retention by size
	params.Save(params.RocksmqCfg.RetentionSizeInMB.Key, "-1")
	// retention by secs
	params.Save(params.RocksmqCfg.RetentionTimeInMinutes.Key, "0.084")

	topicName := "topic_a"
	err = rmq.CreateTopic(topicName)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(topicName)

	msgNum := 100
	pMsgs := make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids, err := rmq.Produce(topicName, pMsgs)
	assert.NoError(t, err)
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
	ids2, err := rmq.Produce(topicName, pMsgs2)
	assert.NoError(t, err)
	assert.Equal(t, len(pMsgs2), len(ids2))

	assert.NoError(t, err)
	cMsgs = make([]ConsumerMessage, 0)
	for i := 0; i < msgNum; i++ {
		cMsg, err := rmq.Consume(topicName, groupName, 1)
		assert.NoError(t, err)
		cMsgs = append(cMsgs, cMsg[0])
	}
	assert.Equal(t, len(cMsgs), msgNum)
	assert.Equal(t, cMsgs[0].MsgID, ids2[0])

	assert.Eventually(t, func() bool {
		err = rmq.ForceSeek(topicName, groupName, ids[0])
		assert.NoError(t, err)
		newRes, err := rmq.Consume(topicName, groupName, 1)
		assert.NoError(t, err)
		assert.Equal(t, len(newRes), 1)
		// point to first not consumed messages
		return newRes[0].MsgID == ids2[0]
	}, 5*time.Second, 1*time.Second)

	// test acked size acked ts and other meta are updated as expect
	msgSizeKey := MessageSizeTitle + topicName
	msgSizeVal, err := rmq.kv.Load(context.TODO(), msgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, msgSizeVal, "0")

	// 100 page left, each entity is a page
	pageMsgSizeKey := constructKey(PageMsgSizeTitle, topicName)
	keys, values, err := rmq.kv.LoadWithPrefix(context.TODO(), pageMsgSizeKey)
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 100)
	assert.Equal(t, len(values), 100)

	pageTsSizeKey := constructKey(PageTsTitle, topicName)
	keys, values, err = rmq.kv.LoadWithPrefix(context.TODO(), pageTsSizeKey)
	assert.NoError(t, err)

	assert.Equal(t, len(keys), 100)
	assert.Equal(t, len(values), 100)

	aclTsSizeKey := constructKey(AckedTsTitle, topicName)
	keys, values, err = rmq.kv.LoadWithPrefix(context.TODO(), aclTsSizeKey)
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
	rocksdbPath := retentionPath + "db_com2"
	os.RemoveAll(rocksdbPath)
	metaPath := retentionPath + "meta_kv_com2"
	os.RemoveAll(metaPath)

	params := paramtable.Get()
	paramtable.Init()

	params.Save(params.RocksmqCfg.PageSize.Key, "10")
	params.Save(params.RocksmqCfg.TickerTimeInSeconds.Key, "1")

	rmq, err := NewRocksMQ(rocksdbPath)
	assert.NoError(t, err)
	defer rmq.Close()

	// no retention by size
	params.Save(params.RocksmqCfg.RetentionSizeInMB.Key, "1")
	// retention by secs
	params.Save(params.RocksmqCfg.RetentionTimeInMinutes.Key, "-1")

	topicName := "topic_a"
	err = rmq.CreateTopic(topicName)
	assert.NoError(t, err)
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
	assert.NoError(t, err)
	assert.Equal(t, len(pMsgs), len(ids))

	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(topicName, groupName)
	err = rmq.CreateConsumerGroup(topicName, groupName)

	consumer := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
	}
	rmq.RegisterConsumer(consumer)

	assert.NoError(t, err)
	cMsgs := make([]ConsumerMessage, 0)
	for i := 0; i < msgNum; i++ {
		cMsg, err := rmq.Consume(topicName, groupName, 1)
		assert.NoError(t, err)
		cMsgs = append(cMsgs, cMsg[0])
	}
	assert.Equal(t, len(cMsgs), msgNum)
	log.Debug("Already consumed, wait for message cleaned by retention")
	// wait for enough time for page expiration
	time.Sleep(time.Duration(2) * time.Second)
	err = rmq.ForceSeek(topicName, groupName, ids[0])
	assert.NoError(t, err)
	newRes, err := rmq.Consume(topicName, groupName, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(newRes), 1)
	// make sure clean up happens
	assert.True(t, newRes[0].MsgID > ids[0])
}
