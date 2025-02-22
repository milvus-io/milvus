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
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	rocksdbkv "github.com/milvus-io/milvus/pkg/v2/kv/rocksdb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var rmqPath = "/tmp/rocksmq"

var kvPathSuffix = "_kv"

var metaPathSuffix = "_meta"

func TestMain(m *testing.M) {
	paramtable.Init()
	code := m.Run()
	os.Exit(code)
}

type producerMessageBefore2 struct {
	Payload []byte
}

func newChanName() string {
	return fmt.Sprintf("my-chan-%v", time.Now().Nanosecond())
}

func TestRocksmq_RegisterConsumer(t *testing.T) {
	suffix := "_register"
	rocksdbPath := rmqPath + suffix
	defer os.RemoveAll(rocksdbPath + kvSuffix)
	defer os.RemoveAll(rocksdbPath)

	paramtable.Init()
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	rmq, err := NewRocksMQ(rocksdbPath)
	assert.NoError(t, err)
	defer rmq.Close()

	topicName := "topic_register"
	groupName := "group_register"

	err = rmq.CreateTopic(topicName)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(topicName)

	err = rmq.CreateConsumerGroup(topicName, groupName)
	assert.NoError(t, err)
	defer rmq.DestroyConsumerGroup(topicName, groupName)

	consumer := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
		MsgMutex:  make(chan struct{}),
	}
	rmq.RegisterConsumer(consumer)
	exist, _, _ := rmq.ExistConsumerGroup(topicName, groupName)
	assert.Equal(t, exist, true)
	dummyGrpName := "group_dummy"
	exist, _, _ = rmq.ExistConsumerGroup(topicName, dummyGrpName)
	assert.Equal(t, exist, false)

	msgA := "a_message"
	pMsgs := make([]ProducerMessage, 1)
	pMsgA := ProducerMessage{Payload: []byte(msgA)}
	pMsgs[0] = pMsgA

	_, err = rmq.Produce(topicName, pMsgs)
	assert.NoError(t, err)

	rmq.Notify(topicName, groupName)

	consumer1 := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
		MsgMutex:  make(chan struct{}),
	}
	rmq.RegisterConsumer(consumer1)

	groupName2 := "group_register2"
	consumer2 := &Consumer{
		Topic:     topicName,
		GroupName: groupName2,
		MsgMutex:  make(chan struct{}),
	}
	rmq.RegisterConsumer(consumer2)
}

func TestRocksmq_Basic(t *testing.T) {
	suffix := "_rmq"
	rocksdbPath := rmqPath + suffix
	defer os.RemoveAll(rocksdbPath + kvSuffix)
	defer os.RemoveAll(rocksdbPath)
	paramtable.Init()
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	rmq, err := NewRocksMQ(rocksdbPath)
	assert.NoError(t, err)
	defer rmq.Close()

	channelName := "channel_rocks"
	err = rmq.CreateTopic(channelName)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(channelName)

	msgA := "a_message"
	pMsgs := make([]ProducerMessage, 1)
	pMsgA := ProducerMessage{Payload: []byte(msgA)}
	pMsgs[0] = pMsgA

	_, err = rmq.Produce(channelName, pMsgs)
	assert.NoError(t, err)

	pMsgB := ProducerMessage{Payload: []byte("b_message")}
	pMsgC := ProducerMessage{Payload: []byte("c_message")}

	pMsgs[0] = pMsgB
	pMsgs = append(pMsgs, pMsgC)
	_, err = rmq.Produce(channelName, pMsgs)
	assert.NoError(t, err)

	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(channelName, groupName)
	err = rmq.CreateConsumerGroup(channelName, groupName)
	assert.NoError(t, err)
	// double create consumer group
	err = rmq.CreateConsumerGroup(channelName, groupName)
	assert.Error(t, err)
	cMsgs, err := rmq.Consume(channelName, groupName, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(cMsgs), 1)
	assert.Equal(t, string(cMsgs[0].Payload), "a_message")

	cMsgs, err = rmq.Consume(channelName, groupName, 2)
	assert.NoError(t, err)
	assert.Equal(t, len(cMsgs), 2)
	assert.Equal(t, string(cMsgs[0].Payload), "b_message")
	assert.Equal(t, string(cMsgs[1].Payload), "c_message")
}

func TestRocksmq_MultiConsumer(t *testing.T) {
	suffix := "rmq_multi_consumer"
	rocksdbPath := rmqPath + suffix
	defer os.RemoveAll(rocksdbPath + kvSuffix)
	defer os.RemoveAll(rocksdbPath)

	params := paramtable.Get()
	params.Save(params.RocksmqCfg.PageSize.Key, "10")
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	rmq, err := NewRocksMQ(rocksdbPath)
	assert.NoError(t, err)
	defer rmq.Close()

	channelName := "channel_rocks"
	err = rmq.CreateTopic(channelName)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(channelName)

	msgNum := 10
	pMsgs := make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids, err := rmq.Produce(channelName, pMsgs)
	assert.NoError(t, err)
	assert.Equal(t, len(pMsgs), len(ids))

	for i := 0; i <= 10; i++ {
		groupName := "group_" + strconv.Itoa(i)
		_ = rmq.DestroyConsumerGroup(channelName, groupName)
		err = rmq.CreateConsumerGroup(channelName, groupName)
		assert.NoError(t, err)

		consumer := &Consumer{
			Topic:     channelName,
			GroupName: groupName,
		}
		rmq.RegisterConsumer(consumer)
	}

	for i := 0; i <= 10; i++ {
		groupName := "group_" + strconv.Itoa(i)
		cMsgs, err := rmq.Consume(channelName, groupName, 10)
		assert.NoError(t, err)
		assert.Equal(t, len(cMsgs), 10)
		assert.Equal(t, string(cMsgs[0].Payload), "message_0")
	}
}

func TestRocksmq_Dummy(t *testing.T) {
	suffix := "_dummy"
	rocksdbPath := rmqPath + suffix
	defer os.RemoveAll(rocksdbPath + kvSuffix)
	defer os.RemoveAll(rocksdbPath)
	paramtable.Init()
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	rmq, err := NewRocksMQ(rocksdbPath)
	assert.NoError(t, err)
	defer rmq.Close()

	_, err = NewRocksMQ("")
	assert.Error(t, err)

	channelName := "channel_a"
	err = rmq.CreateTopic(channelName)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(channelName)
	// create topic twice should be ignored
	err = rmq.CreateTopic(channelName)
	assert.NoError(t, err)

	channelName1 := "channel_dummy"
	topicMu.Store(channelName1, new(sync.Mutex))
	err = rmq.DestroyTopic(channelName1)
	assert.NoError(t, err)

	err = rmq.DestroyConsumerGroup(channelName, channelName1)
	assert.NoError(t, err)

	_, err = rmq.Produce(channelName, nil)
	assert.Error(t, err)

	_, err = rmq.Produce(channelName1, nil)
	assert.Error(t, err)

	groupName1 := "group_dummy"
	err = rmq.Seek(channelName1, groupName1, 0)
	assert.Error(t, err)

	channelName2 := strings.Repeat(channelName1, 100)
	err = rmq.CreateTopic(channelName2)
	assert.NoError(t, err)
	_, err = rmq.Produce(channelName2, nil)
	assert.Error(t, err)

	channelName3 := "channel/dummy"
	err = rmq.CreateTopic(channelName3)
	assert.Error(t, err)

	msgA := "a_message"
	pMsgs := make([]ProducerMessage, 1)
	pMsgA := ProducerMessage{Payload: []byte(msgA)}
	pMsgs[0] = pMsgA

	topicMu.Delete(channelName)
	_, err = rmq.Consume(channelName, groupName1, 1)
	assert.Error(t, err)
	topicMu.Store(channelName, channelName)
	_, err = rmq.Produce(channelName, nil)
	assert.Error(t, err)

	_, err = rmq.Consume(channelName, groupName1, 1)
	assert.Error(t, err)
}

func TestRocksmq_Seek(t *testing.T) {
	suffix := "_seek"
	rocksdbPath := rmqPath + suffix
	defer os.RemoveAll(rocksdbPath + kvSuffix)
	defer os.RemoveAll(rocksdbPath)

	paramtable.Init()
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	rmq, err := NewRocksMQ(rocksdbPath)
	assert.NoError(t, err)
	defer rmq.Close()

	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	_, err = NewRocksMQ("")
	assert.Error(t, err)
	defer os.RemoveAll("_meta_kv")

	channelName := "channel_seek"
	err = rmq.CreateTopic(channelName)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(channelName)

	var seekID UniqueID
	var seekID2 UniqueID
	for i := 0; i < 100; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs := make([]ProducerMessage, 1)
		pMsgs[0] = pMsg
		id, err := rmq.Produce(channelName, pMsgs)
		if i == 50 {
			seekID = id[0]
		}
		if i == 51 {
			seekID2 = id[0]
		}
		assert.NoError(t, err)
	}

	groupName1 := "group_dummy"

	err = rmq.CreateConsumerGroup(channelName, groupName1)
	assert.NoError(t, err)
	err = rmq.Seek(channelName, groupName1, seekID)
	assert.NoError(t, err)

	messages, err := rmq.Consume(channelName, groupName1, 1)
	assert.NoError(t, err)
	assert.Equal(t, messages[0].MsgID, seekID)

	messages, err = rmq.Consume(channelName, groupName1, 1)
	assert.NoError(t, err)
	assert.Equal(t, messages[0].MsgID, seekID2)

	_ = rmq.DestroyConsumerGroup(channelName, groupName1)
}

func TestRocksmq_Loop(t *testing.T) {
	name := "/tmp/rocksmq_1"
	_ = os.RemoveAll(name)
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)

	paramtable.Init()
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	rmq, err := NewRocksMQ(name)
	assert.NoError(t, err)
	defer rmq.Close()

	loopNum := 100
	channelName := "channel_test"
	err = rmq.CreateTopic(channelName)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(channelName)

	// Produce one message once
	for i := 0; i < loopNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs := make([]ProducerMessage, 1)
		pMsgs[0] = pMsg
		_, err := rmq.Produce(channelName, pMsgs)
		assert.NoError(t, err)
	}

	// Produce loopNum messages once
	pMsgs := make([]ProducerMessage, loopNum)
	for i := 0; i < loopNum; i++ {
		msg := "message_" + strconv.Itoa(i+loopNum)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	_, err = rmq.Produce(channelName, pMsgs)
	assert.NoError(t, err)

	// Consume loopNum message once
	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(channelName, groupName)
	err = rmq.CreateConsumerGroup(channelName, groupName)
	assert.NoError(t, err)
	cMsgs, err := rmq.Consume(channelName, groupName, loopNum)
	assert.NoError(t, err)
	assert.Equal(t, len(cMsgs), loopNum)
	assert.Equal(t, string(cMsgs[0].Payload), "message_"+strconv.Itoa(0))
	assert.Equal(t, string(cMsgs[loopNum-1].Payload), "message_"+strconv.Itoa(loopNum-1))

	// Consume one message once
	for i := 0; i < loopNum; i++ {
		oneMsgs, err := rmq.Consume(channelName, groupName, 1)
		assert.NoError(t, err)
		assert.Equal(t, len(oneMsgs), 1)
		assert.Equal(t, string(oneMsgs[0].Payload), "message_"+strconv.Itoa(i+loopNum))
	}

	cMsgs, err = rmq.Consume(channelName, groupName, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(cMsgs), 0)
}

func TestRocksmq_Goroutines(t *testing.T) {
	name := "/tmp/rocksmq_goroutines"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)

	paramtable.Init()
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	rmq, err := NewRocksMQ(name)
	assert.NoError(t, err)
	defer rmq.Close()

	loopNum := 100
	channelName := "channel_test"
	err = rmq.CreateTopic(channelName)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(channelName)

	// Produce two message in each goroutine
	msgChan := make(chan string, loopNum)
	var wg sync.WaitGroup
	for i := 0; i < loopNum; i += 2 {
		wg.Add(2)
		go func(i int, mq RocksMQ) {
			msg0 := "message_" + strconv.Itoa(i)
			msg1 := "message_" + strconv.Itoa(i+1)
			pMsg0 := ProducerMessage{Payload: []byte(msg0)}
			pMsg1 := ProducerMessage{Payload: []byte(msg1)}
			pMsgs := make([]ProducerMessage, 2)
			pMsgs[0] = pMsg0
			pMsgs[1] = pMsg1

			ids, err := mq.Produce(channelName, pMsgs)
			assert.NoError(t, err)
			assert.Equal(t, len(pMsgs), len(ids))
			msgChan <- msg0
			msgChan <- msg1
		}(i, rmq)
	}

	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(channelName, groupName)
	err = rmq.CreateConsumerGroup(channelName, groupName)
	assert.NoError(t, err)
	// Consume one message in each goroutine
	for i := 0; i < loopNum; i++ {
		go func(group *sync.WaitGroup, mq RocksMQ) {
			defer group.Done()
			<-msgChan
			cMsgs, err := mq.Consume(channelName, groupName, 1)
			assert.NoError(t, err)
			assert.Equal(t, len(cMsgs), 1)
		}(&wg, rmq)
	}
	wg.Wait()
}

/*
*

		This test is aim to measure RocksMq throughout.
		Hardware:
			CPU   Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz
	        Disk  SSD

	    Test with 1,000,000 message, result is as follow:
		  	Produce: 190000 message / s
			Consume: 90000 message / s
*/
func TestRocksmq_Throughout(t *testing.T) {
	name := "/tmp/rocksmq_3"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)

	paramtable.Init()
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	rmq, err := NewRocksMQ(name)
	assert.NoError(t, err)
	defer rmq.Close()

	channelName := "channel_throughout_test"
	err = rmq.CreateTopic(channelName)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(channelName)

	entityNum := 100000

	pt0 := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < entityNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		ids, err := rmq.Produce(channelName, []ProducerMessage{pMsg})
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(ids))
	}
	pt1 := time.Now().UnixNano() / int64(time.Millisecond)
	pDuration := pt1 - pt0
	log.Info("Rocksmq_Throughout",
		zap.Int("Total produce item number", entityNum),
		zap.Int64("Total cost (ms)", pDuration),
		zap.Int64("Total throughout (s)", int64(entityNum)*1000/pDuration))

	groupName := "test_throughout_group"
	_ = rmq.DestroyConsumerGroup(channelName, groupName)
	err = rmq.CreateConsumerGroup(channelName, groupName)
	assert.NoError(t, err)
	defer rmq.DestroyConsumerGroup(groupName, channelName)

	// Consume one message in each goroutine
	ct0 := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < entityNum; i++ {
		cMsgs, err := rmq.Consume(channelName, groupName, 1)
		assert.NoError(t, err)
		assert.Equal(t, len(cMsgs), 1)
	}
	ct1 := time.Now().UnixNano() / int64(time.Millisecond)
	cDuration := ct1 - ct0
	log.Info("Rocksmq_Throughout",
		zap.Int("Total produce item number", entityNum),
		zap.Int64("Total cost (ms)", cDuration),
		zap.Int64("Total throughout (s)", int64(entityNum)*1000/cDuration))
}

func TestRocksmq_MultiChan(t *testing.T) {
	name := "/tmp/rocksmq_multichan"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)

	paramtable.Init()
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	rmq, err := NewRocksMQ(name)
	assert.NoError(t, err)
	defer rmq.Close()

	channelName0 := "chan01"
	channelName1 := "chan11"
	err = rmq.CreateTopic(channelName0)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(channelName0)
	err = rmq.CreateTopic(channelName1)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(channelName1)
	assert.NoError(t, err)

	loopNum := 10
	for i := 0; i < loopNum; i++ {
		msg0 := "for_chann0_" + strconv.Itoa(i)
		msg1 := "for_chann1_" + strconv.Itoa(i)
		pMsg0 := ProducerMessage{Payload: []byte(msg0)}
		pMsg1 := ProducerMessage{Payload: []byte(msg1)}
		_, err = rmq.Produce(channelName0, []ProducerMessage{pMsg0})
		assert.NoError(t, err)
		_, err = rmq.Produce(channelName1, []ProducerMessage{pMsg1})
		assert.NoError(t, err)
	}

	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(channelName1, groupName)
	err = rmq.CreateConsumerGroup(channelName1, groupName)
	assert.NoError(t, err)
	cMsgs, err := rmq.Consume(channelName1, groupName, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(cMsgs), 1)
	assert.Equal(t, string(cMsgs[0].Payload), "for_chann1_"+strconv.Itoa(0))
}

func TestRocksmq_CopyData(t *testing.T) {
	name := "/tmp/rocksmq_copydata"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)

	paramtable.Init()
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	rmq, err := NewRocksMQ(name)
	assert.NoError(t, err)
	defer rmq.Close()

	channelName0 := "test_chan01"
	channelName1 := "test_chan11"
	err = rmq.CreateTopic(channelName0)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(channelName0)
	err = rmq.CreateTopic(channelName1)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(channelName1)
	assert.NoError(t, err)

	msg0 := "abcde"
	pMsg0 := ProducerMessage{Payload: []byte(msg0)}
	_, err = rmq.Produce(channelName0, []ProducerMessage{pMsg0})
	assert.NoError(t, err)

	pMsg1 := ProducerMessage{Payload: nil}
	_, err = rmq.Produce(channelName1, []ProducerMessage{pMsg1})
	assert.NoError(t, err)

	pMsg2 := ProducerMessage{Payload: []byte{}}
	_, err = rmq.Produce(channelName1, []ProducerMessage{pMsg2})
	assert.NoError(t, err)

	var emptyTargetData []byte
	pMsg3 := ProducerMessage{Payload: emptyTargetData}
	_, err = rmq.Produce(channelName1, []ProducerMessage{pMsg3})
	assert.NoError(t, err)

	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(channelName0, groupName)
	err = rmq.CreateConsumerGroup(channelName0, groupName)
	assert.NoError(t, err)
	cMsgs0, err := rmq.Consume(channelName0, groupName, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(cMsgs0), 1)
	assert.Equal(t, string(cMsgs0[0].Payload), msg0)

	_ = rmq.DestroyConsumerGroup(channelName1, groupName)
	err = rmq.CreateConsumerGroup(channelName1, groupName)
	assert.NoError(t, err)
	cMsgs1, err := rmq.Consume(channelName1, groupName, 3)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(cMsgs1))
	assert.Equal(t, emptyTargetData, cMsgs1[0].Payload)
}

func TestRocksmq_SeekToLatest(t *testing.T) {
	name := "/tmp/rocksmq_seektolatest"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)

	paramtable.Init()
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	rmq, err := NewRocksMQ(name)
	assert.NoError(t, err)
	defer rmq.Close()

	channelName := "channel_test"
	err = rmq.CreateTopic(channelName)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(channelName)
	loopNum := 100

	err = rmq.SeekToLatest(channelName, "dummy_group")
	assert.Error(t, err)

	// Consume loopNum message once
	groupName := "group_test"
	_ = rmq.DestroyConsumerGroup(channelName, groupName)
	err = rmq.CreateConsumerGroup(channelName, groupName)
	assert.NoError(t, err)

	err = rmq.SeekToLatest(channelName, groupName)
	assert.NoError(t, err)

	channelNamePrev := "channel_tes"
	err = rmq.CreateTopic(channelNamePrev)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(channelNamePrev)
	pMsgs := make([]ProducerMessage, loopNum)
	for i := 0; i < loopNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	_, err = rmq.Produce(channelNamePrev, pMsgs)
	assert.NoError(t, err)

	// should hit the case where channel is null
	err = rmq.SeekToLatest(channelName, groupName)
	assert.NoError(t, err)

	ids, err := rmq.Produce(channelName, pMsgs)
	assert.NoError(t, err)

	// able to read out
	cMsgs, err := rmq.Consume(channelName, groupName, loopNum)
	assert.NoError(t, err)
	assert.Equal(t, len(cMsgs), loopNum)
	for i := 0; i < loopNum; i++ {
		assert.Equal(t, cMsgs[i].MsgID, ids[i])
	}

	err = rmq.SeekToLatest(channelName, groupName)
	assert.NoError(t, err)

	cMsgs, err = rmq.Consume(channelName, groupName, loopNum)
	assert.NoError(t, err)
	assert.Equal(t, len(cMsgs), 0)

	pMsgs = make([]ProducerMessage, loopNum)
	for i := 0; i < loopNum; i++ {
		msg := "message_" + strconv.Itoa(i+loopNum)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids, err = rmq.Produce(channelName, pMsgs)
	assert.NoError(t, err)

	// make sure we only consume the latest message
	cMsgs, err = rmq.Consume(channelName, groupName, loopNum)
	assert.NoError(t, err)
	assert.Equal(t, len(cMsgs), loopNum)
	for i := 0; i < loopNum; i++ {
		assert.Equal(t, cMsgs[i].MsgID, ids[i])
	}
}

func TestRocksmq_GetLatestMsg(t *testing.T) {
	name := "/tmp/rocksmq_data"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	rmq, err := NewRocksMQ(name)
	assert.NoError(t, err)

	channelName := newChanName()
	err = rmq.CreateTopic(channelName)
	assert.NoError(t, err)

	// Consume loopNum message once
	groupName := "last_msg_test"
	_ = rmq.DestroyConsumerGroup(channelName, groupName)
	err = rmq.CreateConsumerGroup(channelName, groupName)
	assert.NoError(t, err)

	msgID, err := rmq.GetLatestMsg(channelName)
	assert.Equal(t, msgID, DefaultMessageID)
	assert.NoError(t, err)

	loopNum := 10
	pMsgs1 := make([]ProducerMessage, loopNum)
	pMsgs2 := make([]ProducerMessage, loopNum)
	for i := 0; i < loopNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs1[i] = pMsg

		msg = "2message_" + strconv.Itoa(i)
		pMsg = ProducerMessage{Payload: []byte(msg)}
		pMsgs2[i] = pMsg
	}

	ids, err := rmq.Produce(channelName, pMsgs1)
	assert.NoError(t, err)
	assert.Equal(t, len(ids), loopNum)

	// test latest msg when one topic is created
	msgID, err = rmq.GetLatestMsg(channelName)
	assert.NoError(t, err)
	assert.Equal(t, msgID, ids[loopNum-1])

	// test latest msg when two topics are created
	channelName2 := newChanName()
	err = rmq.CreateTopic(channelName2)
	assert.NoError(t, err)
	ids, err = rmq.Produce(channelName2, pMsgs2)
	assert.NoError(t, err)

	msgID, err = rmq.GetLatestMsg(channelName2)
	assert.NoError(t, err)
	assert.Equal(t, msgID, ids[loopNum-1])

	// test close rmq
	rmq.DestroyTopic(channelName)
	rmq.Close()
	msgID, err = rmq.GetLatestMsg(channelName)
	assert.Equal(t, msgID, DefaultMessageID)
	assert.Error(t, err)
}

func TestRocksmq_CheckPreTopicValid(t *testing.T) {
	suffix := "_topic"
	rocksdbPath := rmqPath + suffix
	defer os.RemoveAll(rocksdbPath + kvSuffix)
	defer os.RemoveAll(rocksdbPath)
	paramtable.Init()
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	rmq, err := NewRocksMQ(rocksdbPath)
	assert.NoError(t, err)
	defer rmq.Close()

	channelName1 := "topic1"
	// topic not exist
	err = rmq.CheckTopicValid(channelName1)
	assert.Equal(t, true, errors.Is(err, merr.ErrMqTopicNotFound))

	channelName2 := "topic2"
	// allow topic is not empty
	err = rmq.CreateTopic(channelName2)
	defer rmq.DestroyTopic(channelName2)
	assert.NoError(t, err)
	topicMu.Store(channelName2, new(sync.Mutex))

	pMsgs := make([]ProducerMessage, 10)
	for i := 0; i < 10; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	_, err = rmq.Produce(channelName2, pMsgs)
	assert.NoError(t, err)

	err = rmq.CheckTopicValid(channelName2)
	assert.NoError(t, err)

	channelName3 := "topic3"
	// pass
	err = rmq.CreateTopic(channelName3)
	defer rmq.DestroyTopic(channelName3)
	assert.NoError(t, err)

	topicMu.Store(channelName3, new(sync.Mutex))
	err = rmq.CheckTopicValid(channelName3)
	assert.NoError(t, err)
}

func TestRocksmq_Close(t *testing.T) {
	name := "/tmp/rocksmq_close"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	rmq, err := NewRocksMQ(name)
	assert.NoError(t, err)
	defer rmq.Close()

	atomic.StoreInt64(&rmq.state, RmqStateStopped)
	assert.Error(t, rmq.CreateTopic(""))
	assert.Error(t, rmq.CreateConsumerGroup("", ""))
	rmq.RegisterConsumer(&Consumer{})
	_, err = rmq.Produce("", nil)
	assert.Error(t, err)
	_, err = rmq.Consume("", "", 0)
	assert.Error(t, err)

	assert.Error(t, rmq.seek("", "", 0))
	assert.Error(t, rmq.ForceSeek("", "", 0))
	assert.Error(t, rmq.SeekToLatest("", ""))
}

func TestRocksmq_SeekWithNoConsumerError(t *testing.T) {
	name := "/tmp/rocksmq_seekerror"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	rmq, err := NewRocksMQ(name)
	assert.NoError(t, err)
	defer rmq.Close()

	rmq.CreateTopic("test")
	err = rmq.Seek("test", "", 0)
	t.Log(err)
	assert.Error(t, err)
	assert.Error(t, rmq.ForceSeek("test", "", 0))
}

func TestRocksmq_SeekTopicNotExistError(t *testing.T) {
	name := "/tmp/rocksmq_seekerror2"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	rmq, err := NewRocksMQ(name)
	assert.NoError(t, err)
	defer rmq.Close()

	assert.Error(t, rmq.Seek("test_topic_not_exist", "", 0))
	assert.Error(t, rmq.ForceSeek("test_topic_not_exist", "", 0))
}

func TestRocksmq_SeekTopicMutexError(t *testing.T) {
	name := "/tmp/rocksmq_seekerror2"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	rmq, err := NewRocksMQ(name)
	assert.NoError(t, err)
	defer rmq.Close()

	topicMu.Store("test_topic_mutix_error", nil)
	assert.Error(t, rmq.Seek("test_topic_mutix_error", "", 0))
	assert.Error(t, rmq.ForceSeek("test_topic_mutix_error", "", 0))
}

func TestRocksmq_moveConsumePosError(t *testing.T) {
	name := "/tmp/rocksmq_moveconsumeposerror"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	rmq, err := NewRocksMQ(name)
	assert.NoError(t, err)
	defer rmq.Close()

	rmq.CreateTopic("test_moveConsumePos")
	assert.Error(t, rmq.moveConsumePos("test_moveConsumePos", "", 0))
}

func TestRocksmq_updateAckedInfoErr(t *testing.T) {
	name := "/tmp/rocksmq_updateackedinfoerror"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	params := paramtable.Get()
	params.Save(params.RocksmqCfg.PageSize.Key, "10")
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	rmq, err := NewRocksMQ(name)
	assert.NoError(t, err)
	defer rmq.Close()

	topicName := "test_updateAckedInfo"
	rmq.CreateTopic(topicName)
	defer rmq.DestroyTopic(topicName)

	// add message, make sure rmq has more than one page
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

	groupName := "test"
	for i := 0; i < 2; i++ {
		consumer := &Consumer{
			Topic:     topicName,
			GroupName: groupName + strconv.Itoa(i),
			MsgMutex:  make(chan struct{}),
		}
		// make sure consumer not in rmq.consumersID
		rmq.DestroyConsumerGroup(topicName, groupName+strconv.Itoa(i))
		// add consumer to rmq.consumers
		rmq.RegisterConsumer(consumer)
	}

	// update acked for all page in rmq but some consumer not in rmq.consumers
	assert.Error(t, rmq.updateAckedInfo(topicName, groupName, 0, ids[len(ids)-1]))

	for i := 0; i < 2; i++ {
		rmq.DestroyConsumerGroup(topicName, groupName+strconv.Itoa(i))
	}
	// update acked for topic without any consumer
	assert.Nil(t, rmq.updateAckedInfo(topicName, groupName, 0, ids[len(ids)-1]))
}

func TestRocksmq_Info(t *testing.T) {
	name := "/tmp/rocksmq_testinfo"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	params := paramtable.Get()
	params.Save(params.RocksmqCfg.PageSize.Key, "10")
	paramtable.Get().Save("rocksmq.compressionTypes", "0,0,0,0,0")
	rmq, err := NewRocksMQ(name)
	assert.NoError(t, err)
	defer rmq.Close()

	topicName := "test_testinfo"
	groupName := "test"
	rmq.CreateTopic(topicName)
	defer rmq.DestroyTopic(topicName)

	consumer := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
	}

	_ = rmq.DestroyConsumerGroup(topicName, groupName)
	err = rmq.CreateConsumerGroup(topicName, groupName)
	assert.NoError(t, err)

	err = rmq.RegisterConsumer(consumer)
	assert.NoError(t, err)

	assert.True(t, rmq.Info())

	// test error
	rmq.kv = &rocksdbkv.RocksdbKV{}
	assert.False(t, rmq.Info())
}

func TestRocksmq_ParseCompressionTypeError(t *testing.T) {
	params := paramtable.Get()
	paramtable.Init()
	params.Save(params.RocksmqCfg.CompressionTypes.Key, "invalid,1")
	_, err := parseCompressionType(params)
	assert.Error(t, err)

	params.Save(params.RocksmqCfg.CompressionTypes.Key, "-1,-1")
	defer params.Save(params.RocksmqCfg.CompressionTypes.Key, "0,0,7")
	_, err = parseCompressionType(params)
	assert.Error(t, err)
}
