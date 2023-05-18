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
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	pebblekv "github.com/milvus-io/milvus/internal/kv/pebble"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var pmqPath = "/tmp/pebblemq"

var kvPathSuffix = "_kv"

var metaPathSuffix = "_meta"

func TestMain(m *testing.M) {
	paramtable.Init()
	code := m.Run()
	os.Exit(code)
}

type producerMessageBefore struct {
	Payload []byte
}

func InitIDAllocator(kvPath string) *allocator.GlobalIDAllocator {
	pebbleKV, err := pebblekv.NewPebbleKV(kvPath)
	if err != nil {
		panic(err)
	}
	idAllocator := allocator.NewGlobalIDAllocator("pmq_id", pebbleKV)
	_ = idAllocator.Initialize()
	return idAllocator
}

func newChanName() string {
	return fmt.Sprintf("my-chan-%v", time.Now().Nanosecond())
}

func newGroupName() string {
	return fmt.Sprintf("my-group-%v", time.Now().Nanosecond())
}

func etcdEndpoints() []string {
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "localhost:2379"
	}
	etcdEndpoints := strings.Split(endpoints, ",")
	return etcdEndpoints
}

// to test compatibility concern
func (pmq *pebblemq) produceBefore(topicName string, messages []producerMessageBefore) ([]UniqueID, error) {
	if pmq.isClosed() {
		return nil, errors.New(mqNotServingErrMsg)
	}
	start := time.Now()
	ll, ok := topicMu.Load(topicName)
	if !ok {
		return []UniqueID{}, fmt.Errorf("topic name = %s not exist", topicName)
	}
	lock, ok := ll.(*sync.Mutex)
	if !ok {
		return []UniqueID{}, fmt.Errorf("get mutex failed, topic name = %s", topicName)
	}
	lock.Lock()
	defer lock.Unlock()

	getLockTime := time.Since(start).Milliseconds()

	msgLen := len(messages)
	idStart, idEnd, err := pmq.idAllocator.Alloc(uint32(msgLen))

	if err != nil {
		return []UniqueID{}, err
	}
	allocTime := time.Since(start).Milliseconds()
	if UniqueID(msgLen) != idEnd-idStart {
		return []UniqueID{}, errors.New("Obtained id length is not equal that of message")
	}

	// Insert data to store system
	writeOpts := pebble.WriteOptions{}
	batch := pmq.store.NewBatch()
	defer batch.Close()
	msgSizes := make(map[UniqueID]int64)
	msgIDs := make([]UniqueID, msgLen)
	for i := 0; i < msgLen && idStart+UniqueID(i) < idEnd; i++ {
		msgID := idStart + UniqueID(i)
		key := path.Join(topicName, strconv.FormatInt(msgID, 10))
		batch.Set([]byte(key), messages[i].Payload, &writeOpts)
		msgIDs[i] = msgID
		msgSizes[msgID] = int64(len(messages[i].Payload))
	}

	err = batch.Commit(&writeOpts)
	if err != nil {
		return []UniqueID{}, err
	}
	writeTime := time.Since(start).Milliseconds()
	if vals, ok := pmq.consumers.Load(topicName); ok {
		for _, v := range vals.([]*Consumer) {
			select {
			case v.MsgMutex <- struct{}{}:
				continue
			default:
				continue
			}
		}
	}

	// Update message page info
	err = pmq.updatePageInfo(topicName, msgIDs, msgSizes)
	if err != nil {
		return []UniqueID{}, err
	}

	getProduceTime := time.Since(start).Milliseconds()
	if getProduceTime > 200 {

		log.Warn("pebblemq produce too slowly", zap.String("topic", topicName),
			zap.Int64("get lock elapse", getLockTime),
			zap.Int64("alloc elapse", allocTime-getLockTime),
			zap.Int64("write elapse", writeTime-allocTime),
			zap.Int64("updatePage elapse", getProduceTime-writeTime),
			zap.Int64("produce total elapse", getProduceTime),
		)
	}
	return msgIDs, nil
}

func TestPebblemq_RegisterConsumer(t *testing.T) {
	suffix := "_register"
	kvPath := pmqPath + kvPathSuffix + suffix
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	pebblePath := pmqPath + suffix
	defer os.RemoveAll(pebblePath + kvSuffix)
	defer os.RemoveAll(pebblePath)

	paramtable.Init()
	pmq, err := NewPebbleMQ(pebblePath, idAllocator)
	assert.NoError(t, err)
	defer pmq.Close()

	topicName := "topic_register"
	groupName := "group_register"

	err = pmq.CreateTopic(topicName)
	assert.NoError(t, err)
	defer pmq.DestroyTopic(topicName)

	err = pmq.CreateConsumerGroup(topicName, groupName)
	assert.Nil(t, err)
	defer pmq.DestroyConsumerGroup(topicName, groupName)

	consumer := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
		MsgMutex:  make(chan struct{}),
	}
	pmq.RegisterConsumer(consumer)
	exist, _, _ := pmq.ExistConsumerGroup(topicName, groupName)
	assert.Equal(t, exist, true)
	dummyGrpName := "group_dummy"
	exist, _, _ = pmq.ExistConsumerGroup(topicName, dummyGrpName)
	assert.Equal(t, exist, false)

	msgA := "a_message"
	pMsgs := make([]ProducerMessage, 1)
	pMsgA := ProducerMessage{Payload: []byte(msgA)}
	pMsgs[0] = pMsgA

	_, err = pmq.Produce(topicName, pMsgs)
	assert.Nil(t, err)

	pmq.Notify(topicName, groupName)

	consumer1 := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
		MsgMutex:  make(chan struct{}),
	}
	pmq.RegisterConsumer(consumer1)

	groupName2 := "group_register2"
	consumer2 := &Consumer{
		Topic:     topicName,
		GroupName: groupName2,
		MsgMutex:  make(chan struct{}),
	}
	pmq.RegisterConsumer(consumer2)
}

func TestPebblemq_Basic(t *testing.T) {
	suffix := "_pmq"

	kvPath := pmqPath + kvPathSuffix + suffix
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	pebblePath := pmqPath + suffix
	defer os.RemoveAll(pebblePath + kvSuffix)
	defer os.RemoveAll(pebblePath)
	paramtable.Init()
	pmq, err := NewPebbleMQ(pebblePath, idAllocator)
	assert.Nil(t, err)
	defer pmq.Close()

	channelName := "channel_rocks"
	err = pmq.CreateTopic(channelName)
	assert.Nil(t, err)
	defer pmq.DestroyTopic(channelName)

	msgA := "a_message"
	pMsgs := make([]ProducerMessage, 1)
	pMsgA := ProducerMessage{Payload: []byte(msgA), Properties: map[string]string{common.TraceIDKey: "a"}}
	pMsgs[0] = pMsgA

	_, err = pmq.Produce(channelName, pMsgs)
	assert.Nil(t, err)

	pMsgB := ProducerMessage{Payload: []byte("b_message"), Properties: map[string]string{common.TraceIDKey: "b"}}
	pMsgC := ProducerMessage{Payload: []byte("c_message"), Properties: map[string]string{common.TraceIDKey: "c"}}

	pMsgs[0] = pMsgB
	pMsgs = append(pMsgs, pMsgC)
	_, err = pmq.Produce(channelName, pMsgs)
	assert.Nil(t, err)

	groupName := "test_group"
	_ = pmq.DestroyConsumerGroup(channelName, groupName)
	err = pmq.CreateConsumerGroup(channelName, groupName)
	assert.Nil(t, err)
	// double create consumer group
	err = pmq.CreateConsumerGroup(channelName, groupName)
	assert.Error(t, err)
	cMsgs, err := pmq.Consume(channelName, groupName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), 1)
	assert.Equal(t, string(cMsgs[0].Payload), "a_message")
	_, ok := cMsgs[0].Properties[common.TraceIDKey]
	assert.True(t, ok)
	assert.Equal(t, cMsgs[0].Properties[common.TraceIDKey], "a")

	cMsgs, err = pmq.Consume(channelName, groupName, 2)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), 2)
	assert.Equal(t, string(cMsgs[0].Payload), "b_message")
	_, ok = cMsgs[0].Properties[common.TraceIDKey]
	assert.True(t, ok)
	assert.Equal(t, cMsgs[0].Properties[common.TraceIDKey], "b")
	assert.Equal(t, string(cMsgs[1].Payload), "c_message")
	_, ok = cMsgs[1].Properties[common.TraceIDKey]
	assert.True(t, ok)
	assert.Equal(t, cMsgs[1].Properties[common.TraceIDKey], "c")

	// before 2.2.0, there have no properties in ProducerMessage and ConsumerMessage in pebblemq
	// it aims to test if produce before 2.2.0, but consume after 2.2.0
	msgD := "d_message"
	tMsgs := make([]producerMessageBefore, 1)
	tMsgD := producerMessageBefore{Payload: []byte(msgD)}
	tMsgs[0] = tMsgD

	_, err = pmq.produceBefore(channelName, tMsgs)
	assert.Nil(t, err)

	cMsgs, err = pmq.Consume(channelName, groupName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), 1)
	assert.Equal(t, string(cMsgs[0].Payload), "d_message")
	_, ok = cMsgs[0].Properties[common.TraceIDKey]
	assert.False(t, ok)
	// it will be set empty map if produce message has no properties field
	expect := make(map[string]string)
	assert.Equal(t, cMsgs[0].Properties, expect)
}

func TestPebblemq_MultiConsumer(t *testing.T) {
	suffix := "pmq_multi_consumer"
	kvPath := pmqPath + kvPathSuffix + suffix
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	pebblePath := pmqPath + suffix
	defer os.RemoveAll(pebblePath + kvSuffix)
	defer os.RemoveAll(pebblePath)

	params := paramtable.Get()
	params.Save(params.PebblemqCfg.PageSize.Key, "10")
	pmq, err := NewPebbleMQ(pebblePath, idAllocator)
	assert.Nil(t, err)
	defer pmq.Close()

	channelName := "channel_rocks"
	err = pmq.CreateTopic(channelName)
	assert.Nil(t, err)
	defer pmq.DestroyTopic(channelName)

	msgNum := 10
	pMsgs := make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids, err := pmq.Produce(channelName, pMsgs)
	assert.Nil(t, err)
	assert.Equal(t, len(pMsgs), len(ids))

	for i := 0; i <= 10; i++ {
		groupName := "group_" + strconv.Itoa(i)
		_ = pmq.DestroyConsumerGroup(channelName, groupName)
		err = pmq.CreateConsumerGroup(channelName, groupName)
		assert.Nil(t, err)

		consumer := &Consumer{
			Topic:     channelName,
			GroupName: groupName,
		}
		pmq.RegisterConsumer(consumer)
	}

	for i := 0; i <= 10; i++ {
		groupName := "group_" + strconv.Itoa(i)
		cMsgs, err := pmq.Consume(channelName, groupName, 10)
		assert.Nil(t, err)
		assert.Equal(t, len(cMsgs), 10)
		assert.Equal(t, string(cMsgs[0].Payload), "message_0")
	}
}

func TestPebblemq_Dummy(t *testing.T) {
	suffix := "_dummy"
	kvPath := pmqPath + kvPathSuffix + suffix
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	pebblePath := pmqPath + suffix
	defer os.RemoveAll(pebblePath + kvSuffix)
	defer os.RemoveAll(pebblePath)
	paramtable.Init()
	pmq, err := NewPebbleMQ(pebblePath, idAllocator)
	assert.Nil(t, err)
	defer pmq.Close()

	_, err = NewPebbleMQ("", idAllocator)
	assert.Error(t, err)

	channelName := "channel_a"
	err = pmq.CreateTopic(channelName)
	assert.Nil(t, err)
	defer pmq.DestroyTopic(channelName)
	// create topic twice should be ignored
	err = pmq.CreateTopic(channelName)
	assert.Nil(t, err)

	channelName1 := "channel_dummy"
	topicMu.Store(channelName1, new(sync.Mutex))
	err = pmq.DestroyTopic(channelName1)
	assert.NoError(t, err)

	err = pmq.DestroyConsumerGroup(channelName, channelName1)
	assert.NoError(t, err)

	_, err = pmq.Produce(channelName, nil)
	assert.Error(t, err)

	_, err = pmq.Produce(channelName1, nil)
	assert.Error(t, err)

	groupName1 := "group_dummy"
	err = pmq.Seek(channelName1, groupName1, 0)
	assert.Error(t, err)

	channelName2 := strings.Repeat(channelName1, 100)
	err = pmq.CreateTopic(channelName2)
	assert.NoError(t, err)
	_, err = pmq.Produce(channelName2, nil)
	assert.Error(t, err)

	channelName3 := "channel/dummy"
	err = pmq.CreateTopic(channelName3)
	assert.Error(t, err)

	msgA := "a_message"
	pMsgs := make([]ProducerMessage, 1)
	pMsgA := ProducerMessage{Payload: []byte(msgA)}
	pMsgs[0] = pMsgA

	topicMu.Delete(channelName)
	_, err = pmq.Consume(channelName, groupName1, 1)
	assert.Error(t, err)
	topicMu.Store(channelName, channelName)
	_, err = pmq.Produce(channelName, nil)
	assert.Error(t, err)

	_, err = pmq.Consume(channelName, groupName1, 1)
	assert.Error(t, err)
}

func TestPebblemq_Seek(t *testing.T) {
	suffix := "_seek"
	kvPath := pmqPath + kvPathSuffix + suffix
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	pebblePath := pmqPath + suffix
	defer os.RemoveAll(pebblePath + kvSuffix)
	defer os.RemoveAll(pebblePath)

	paramtable.Init()
	pmq, err := NewPebbleMQ(pebblePath, idAllocator)
	assert.Nil(t, err)
	defer pmq.Close()

	_, err = NewPebbleMQ("", idAllocator)
	assert.Error(t, err)
	defer os.RemoveAll("_meta_kv")

	channelName := "channel_seek"
	err = pmq.CreateTopic(channelName)
	assert.NoError(t, err)
	defer pmq.DestroyTopic(channelName)

	var seekID UniqueID
	var seekID2 UniqueID
	for i := 0; i < 100; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs := make([]ProducerMessage, 1)
		pMsgs[0] = pMsg
		id, err := pmq.Produce(channelName, pMsgs)
		if i == 50 {
			seekID = id[0]
		}
		if i == 51 {
			seekID2 = id[0]
		}
		assert.Nil(t, err)
	}

	groupName1 := "group_dummy"

	err = pmq.CreateConsumerGroup(channelName, groupName1)
	assert.NoError(t, err)
	err = pmq.Seek(channelName, groupName1, seekID)
	assert.NoError(t, err)

	messages, err := pmq.Consume(channelName, groupName1, 1)
	assert.NoError(t, err)
	assert.Equal(t, messages[0].MsgID, seekID)

	messages, err = pmq.Consume(channelName, groupName1, 1)
	assert.NoError(t, err)
	assert.Equal(t, messages[0].MsgID, seekID2)

	_ = pmq.DestroyConsumerGroup(channelName, groupName1)

}

func TestPebblemq_Loop(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.Nil(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/pebblemq_1"
	_ = os.RemoveAll(name)
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)

	paramtable.Init()
	pmq, err := NewPebbleMQ(name, idAllocator)
	assert.Nil(t, err)
	defer pmq.Close()

	loopNum := 100
	channelName := "channel_test"
	err = pmq.CreateTopic(channelName)
	assert.Nil(t, err)
	defer pmq.DestroyTopic(channelName)

	// Produce one message once
	for i := 0; i < loopNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs := make([]ProducerMessage, 1)
		pMsgs[0] = pMsg
		_, err := pmq.Produce(channelName, pMsgs)
		assert.Nil(t, err)
	}

	// Produce loopNum messages once
	pMsgs := make([]ProducerMessage, loopNum)
	for i := 0; i < loopNum; i++ {
		msg := "message_" + strconv.Itoa(i+loopNum)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	_, err = pmq.Produce(channelName, pMsgs)
	assert.Nil(t, err)

	// Consume loopNum message once
	groupName := "test_group"
	_ = pmq.DestroyConsumerGroup(channelName, groupName)
	err = pmq.CreateConsumerGroup(channelName, groupName)
	assert.Nil(t, err)
	cMsgs, err := pmq.Consume(channelName, groupName, loopNum)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), loopNum)
	assert.Equal(t, string(cMsgs[0].Payload), "message_"+strconv.Itoa(0))
	assert.Equal(t, string(cMsgs[loopNum-1].Payload), "message_"+strconv.Itoa(loopNum-1))

	// Consume one message once
	for i := 0; i < loopNum; i++ {
		oneMsgs, err := pmq.Consume(channelName, groupName, 1)
		assert.Nil(t, err)
		assert.Equal(t, len(oneMsgs), 1)
		assert.Equal(t, string(oneMsgs[0].Payload), "message_"+strconv.Itoa(i+loopNum))
	}

	cMsgs, err = pmq.Consume(channelName, groupName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), 0)
}

func TestPebblemq_Goroutines(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.Nil(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/pebblemq_2"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)

	paramtable.Init()
	pmq, err := NewPebbleMQ(name, idAllocator)
	assert.Nil(t, err)
	defer pmq.Close()

	loopNum := 100
	channelName := "channel_test"
	err = pmq.CreateTopic(channelName)
	assert.Nil(t, err)
	defer pmq.DestroyTopic(channelName)

	// Produce two message in each goroutine
	msgChan := make(chan string, loopNum)
	var wg sync.WaitGroup
	for i := 0; i < loopNum; i += 2 {
		wg.Add(2)
		go func(i int, mq PebbleMQ) {
			msg0 := "message_" + strconv.Itoa(i)
			msg1 := "message_" + strconv.Itoa(i+1)
			pMsg0 := ProducerMessage{Payload: []byte(msg0)}
			pMsg1 := ProducerMessage{Payload: []byte(msg1)}
			pMsgs := make([]ProducerMessage, 2)
			pMsgs[0] = pMsg0
			pMsgs[1] = pMsg1

			ids, err := mq.Produce(channelName, pMsgs)
			assert.Nil(t, err)
			assert.Equal(t, len(pMsgs), len(ids))
			msgChan <- msg0
			msgChan <- msg1
		}(i, pmq)
	}

	groupName := "test_group"
	_ = pmq.DestroyConsumerGroup(channelName, groupName)
	err = pmq.CreateConsumerGroup(channelName, groupName)
	assert.Nil(t, err)
	// Consume one message in each goroutine
	for i := 0; i < loopNum; i++ {
		go func(group *sync.WaitGroup, mq PebbleMQ) {
			defer group.Done()
			<-msgChan
			cMsgs, err := mq.Consume(channelName, groupName, 1)
			assert.Nil(t, err)
			assert.Equal(t, len(cMsgs), 1)
		}(&wg, pmq)
	}
	wg.Wait()
}

/*
*

		This test is aim to measure PebbleMq throughout.
		Hardware:
			CPU   Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz
	        Disk  SSD

	    Test with 1,000,000 message, result is as follow:
		  	Produce: 190000 message / s
			Consume: 90000 message / s
*/
func TestPebblemq_Throughout(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.Nil(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/pebblemq_3"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)

	paramtable.Init()
	pmq, err := NewPebbleMQ(name, idAllocator)
	assert.Nil(t, err)
	defer pmq.Close()

	channelName := "channel_throughout_test"
	err = pmq.CreateTopic(channelName)
	assert.Nil(t, err)
	defer pmq.DestroyTopic(channelName)

	entityNum := 100000

	pt0 := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < entityNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		ids, err := pmq.Produce(channelName, []ProducerMessage{pMsg})
		assert.Nil(t, err)
		assert.EqualValues(t, 1, len(ids))
	}
	pt1 := time.Now().UnixNano() / int64(time.Millisecond)
	pDuration := pt1 - pt0
	log.Info("Pebblemq_Throughout",
		zap.Int("Total produce item number", entityNum),
		zap.Int64("Total cost (ms)", pDuration),
		zap.Int64("Total throughout (s)", int64(entityNum)*1000/pDuration))

	groupName := "test_throughout_group"
	_ = pmq.DestroyConsumerGroup(channelName, groupName)
	err = pmq.CreateConsumerGroup(channelName, groupName)
	assert.Nil(t, err)
	defer pmq.DestroyConsumerGroup(groupName, channelName)

	// Consume one message in each goroutine
	ct0 := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < entityNum; i++ {
		cMsgs, err := pmq.Consume(channelName, groupName, 1)
		assert.Nil(t, err)
		assert.Equal(t, len(cMsgs), 1)
	}
	ct1 := time.Now().UnixNano() / int64(time.Millisecond)
	cDuration := ct1 - ct0
	log.Info("Pebblemq_Throughout",
		zap.Int("Total produce item number", entityNum),
		zap.Int64("Total cost (ms)", cDuration),
		zap.Int64("Total throughout (s)", int64(entityNum)*1000/cDuration))
}

func TestPebblemq_MultiChan(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.Nil(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/pebblemq_multichan"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)

	paramtable.Init()
	pmq, err := NewPebbleMQ(name, idAllocator)
	assert.Nil(t, err)
	defer pmq.Close()

	channelName0 := "chan01"
	channelName1 := "chan11"
	err = pmq.CreateTopic(channelName0)
	assert.Nil(t, err)
	defer pmq.DestroyTopic(channelName0)
	err = pmq.CreateTopic(channelName1)
	assert.Nil(t, err)
	defer pmq.DestroyTopic(channelName1)
	assert.Nil(t, err)

	loopNum := 10
	for i := 0; i < loopNum; i++ {
		msg0 := "for_chann0_" + strconv.Itoa(i)
		msg1 := "for_chann1_" + strconv.Itoa(i)
		pMsg0 := ProducerMessage{Payload: []byte(msg0)}
		pMsg1 := ProducerMessage{Payload: []byte(msg1)}
		_, err = pmq.Produce(channelName0, []ProducerMessage{pMsg0})
		assert.Nil(t, err)
		_, err = pmq.Produce(channelName1, []ProducerMessage{pMsg1})
		assert.Nil(t, err)
	}

	groupName := "test_group"
	_ = pmq.DestroyConsumerGroup(channelName1, groupName)
	err = pmq.CreateConsumerGroup(channelName1, groupName)
	assert.Nil(t, err)
	cMsgs, err := pmq.Consume(channelName1, groupName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), 1)
	assert.Equal(t, string(cMsgs[0].Payload), "for_chann1_"+strconv.Itoa(0))
}

func TestPebblemq_CopyData(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.Nil(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/pebblemq_copydata"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)

	paramtable.Init()
	pmq, err := NewPebbleMQ(name, idAllocator)
	assert.Nil(t, err)
	defer pmq.Close()

	channelName0 := "test_chan01"
	channelName1 := "test_chan11"
	err = pmq.CreateTopic(channelName0)
	assert.Nil(t, err)
	defer pmq.DestroyTopic(channelName0)
	err = pmq.CreateTopic(channelName1)
	assert.Nil(t, err)
	defer pmq.DestroyTopic(channelName1)
	assert.Nil(t, err)

	msg0 := "abcde"
	pMsg0 := ProducerMessage{Payload: []byte(msg0)}
	_, err = pmq.Produce(channelName0, []ProducerMessage{pMsg0})
	assert.Nil(t, err)

	pMsg1 := ProducerMessage{Payload: nil}
	_, err = pmq.Produce(channelName1, []ProducerMessage{pMsg1})
	assert.Nil(t, err)

	pMsg2 := ProducerMessage{Payload: []byte{}}
	_, err = pmq.Produce(channelName1, []ProducerMessage{pMsg2})
	assert.Nil(t, err)

	var emptyTargetData []byte
	pMsg3 := ProducerMessage{Payload: emptyTargetData}
	_, err = pmq.Produce(channelName1, []ProducerMessage{pMsg3})
	assert.Nil(t, err)

	groupName := "test_group"
	_ = pmq.DestroyConsumerGroup(channelName0, groupName)
	err = pmq.CreateConsumerGroup(channelName0, groupName)
	assert.Nil(t, err)
	cMsgs0, err := pmq.Consume(channelName0, groupName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs0), 1)
	assert.Equal(t, string(cMsgs0[0].Payload), msg0)

	_ = pmq.DestroyConsumerGroup(channelName1, groupName)
	err = pmq.CreateConsumerGroup(channelName1, groupName)
	assert.Nil(t, err)
	cMsgs1, err := pmq.Consume(channelName1, groupName, 3)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(cMsgs1))
	assert.Equal(t, emptyTargetData, cMsgs1[0].Payload)
}

func TestPebblemq_SeekToLatest(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.Nil(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/pebblemq_seektolatest"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)

	paramtable.Init()
	pmq, err := NewPebbleMQ(name, idAllocator)
	assert.Nil(t, err)
	defer pmq.Close()

	channelName := "channel_test"
	err = pmq.CreateTopic(channelName)
	assert.Nil(t, err)
	defer pmq.DestroyTopic(channelName)
	loopNum := 100

	err = pmq.SeekToLatest(channelName, "dummy_group")
	assert.Error(t, err)

	// Consume loopNum message once
	groupName := "group_test"
	_ = pmq.DestroyConsumerGroup(channelName, groupName)
	err = pmq.CreateConsumerGroup(channelName, groupName)
	assert.Nil(t, err)

	err = pmq.SeekToLatest(channelName, groupName)
	assert.NoError(t, err)

	channelNamePrev := "channel_tes"
	err = pmq.CreateTopic(channelNamePrev)
	assert.Nil(t, err)
	defer pmq.DestroyTopic(channelNamePrev)
	pMsgs := make([]ProducerMessage, loopNum)
	for i := 0; i < loopNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	_, err = pmq.Produce(channelNamePrev, pMsgs)
	assert.Nil(t, err)

	// should hit the case where channel is null
	err = pmq.SeekToLatest(channelName, groupName)
	assert.NoError(t, err)

	ids, err := pmq.Produce(channelName, pMsgs)
	assert.Nil(t, err)

	// able to read out
	cMsgs, err := pmq.Consume(channelName, groupName, loopNum)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), loopNum)
	for i := 0; i < loopNum; i++ {
		assert.Equal(t, cMsgs[i].MsgID, ids[i])
	}

	err = pmq.SeekToLatest(channelName, groupName)
	assert.NoError(t, err)

	cMsgs, err = pmq.Consume(channelName, groupName, loopNum)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), 0)

	pMsgs = make([]ProducerMessage, loopNum)
	for i := 0; i < loopNum; i++ {
		msg := "message_" + strconv.Itoa(i+loopNum)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids, err = pmq.Produce(channelName, pMsgs)
	assert.Nil(t, err)

	// make sure we only consume the latest message
	cMsgs, err = pmq.Consume(channelName, groupName, loopNum)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), loopNum)
	for i := 0; i < loopNum; i++ {
		assert.Equal(t, cMsgs[i].MsgID, ids[i])
	}
}

func TestPebblemq_GetLatestMsg(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.Nil(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/pebblemq_data"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	pmq, err := NewPebbleMQ(name, idAllocator)
	assert.Nil(t, err)

	channelName := newChanName()
	err = pmq.CreateTopic(channelName)
	assert.Nil(t, err)

	// Consume loopNum message once
	groupName := "last_msg_test"
	_ = pmq.DestroyConsumerGroup(channelName, groupName)
	err = pmq.CreateConsumerGroup(channelName, groupName)
	assert.Nil(t, err)

	msgID, err := pmq.GetLatestMsg(channelName)
	assert.Equal(t, msgID, DefaultMessageID)
	assert.Nil(t, err)

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

	ids, err := pmq.Produce(channelName, pMsgs1)
	assert.Nil(t, err)
	assert.Equal(t, len(ids), loopNum)

	// test latest msg when one topic is created
	msgID, err = pmq.GetLatestMsg(channelName)
	assert.Nil(t, err)
	assert.Equal(t, msgID, ids[loopNum-1])

	// test latest msg when two topics are created
	channelName2 := newChanName()
	err = pmq.CreateTopic(channelName2)
	assert.Nil(t, err)
	ids, err = pmq.Produce(channelName2, pMsgs2)
	assert.Nil(t, err)

	msgID, err = pmq.GetLatestMsg(channelName2)
	assert.Nil(t, err)
	assert.Equal(t, msgID, ids[loopNum-1])

	// test close pmq
	pmq.DestroyTopic(channelName)
	pmq.Close()
	msgID, err = pmq.GetLatestMsg(channelName)
	assert.Equal(t, msgID, DefaultMessageID)
	assert.NotNil(t, err)
}

func TestPebblemq_CheckPreTopicValid(t *testing.T) {
	suffix := "_topic"
	kvPath := pmqPath + kvPathSuffix + suffix
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	pebblePath := pmqPath + suffix
	defer os.RemoveAll(pebblePath + kvSuffix)
	defer os.RemoveAll(pebblePath)
	paramtable.Init()
	pmq, err := NewPebbleMQ(pebblePath, idAllocator)
	assert.Nil(t, err)
	defer pmq.Close()

	channelName1 := "topic1"
	// topic not exist
	err = pmq.CheckTopicValid(channelName1)
	assert.Equal(t, true, errors.Is(err, merr.ErrMqTopicNotFound))

	channelName2 := "topic2"
	// topic is not empty
	err = pmq.CreateTopic(channelName2)
	defer pmq.DestroyTopic(channelName2)
	assert.Nil(t, err)
	topicMu.Store(channelName2, new(sync.Mutex))

	pMsgs := make([]ProducerMessage, 10)
	for i := 0; i < 10; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	_, err = pmq.Produce(channelName2, pMsgs)
	assert.NoError(t, err)

	err = pmq.CheckTopicValid(channelName2)
	assert.Equal(t, true, errors.Is(err, merr.ErrMqTopicNotEmpty))

	channelName3 := "topic3"
	// pass
	err = pmq.CreateTopic(channelName3)
	defer pmq.DestroyTopic(channelName3)
	assert.Nil(t, err)

	topicMu.Store(channelName3, new(sync.Mutex))
	err = pmq.CheckTopicValid(channelName3)
	assert.NoError(t, err)
}

func TestPebblemq_Close(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.Nil(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/pebblemq_close"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	pmq, err := NewPebbleMQ(name, idAllocator)
	assert.Nil(t, err)
	defer pmq.Close()

	atomic.StoreInt64(&pmq.state, mqStateStopped)
	assert.Error(t, pmq.CreateTopic(""))
	assert.Error(t, pmq.CreateConsumerGroup("", ""))
	pmq.RegisterConsumer(&Consumer{})
	_, err = pmq.Produce("", nil)
	assert.Error(t, err)
	_, err = pmq.Consume("", "", 0)
	assert.Error(t, err)

	assert.Error(t, pmq.seek("", "", 0))
	assert.Error(t, pmq.ForceSeek("", "", 0))
	assert.Error(t, pmq.SeekToLatest("", ""))
}

func TestPebblemq_SeekWithNoConsumerError(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/pebblemq_seekerror"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	pmq, err := NewPebbleMQ(name, idAllocator)
	assert.Nil(t, err)
	defer pmq.Close()

	pmq.CreateTopic("test")
	err = pmq.Seek("test", "", 0)
	t.Log(err)
	assert.Error(t, err)
	assert.Error(t, pmq.ForceSeek("test", "", 0))
}

func TestPebblemq_SeekTopicNotExistError(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/pebblemq_seekerror2"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	pmq, err := NewPebbleMQ(name, idAllocator)
	assert.Nil(t, err)
	defer pmq.Close()

	assert.Error(t, pmq.Seek("test_topic_not_exist", "", 0))
	assert.Error(t, pmq.ForceSeek("test_topic_not_exist", "", 0))
}

func TestPebblemq_SeekTopicMutexError(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/pebblemq_seekerror2"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	pmq, err := NewPebbleMQ(name, idAllocator)
	assert.Nil(t, err)
	defer pmq.Close()

	topicMu.Store("test_topic_mutix_error", nil)
	assert.Error(t, pmq.Seek("test_topic_mutix_error", "", 0))
	assert.Error(t, pmq.ForceSeek("test_topic_mutix_error", "", 0))
}

func TestPebblemq_moveConsumePosError(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/pebblemq_moveconsumeposerror"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	pmq, err := NewPebbleMQ(name, idAllocator)
	assert.Nil(t, err)
	defer pmq.Close()

	pmq.CreateTopic("test_moveConsumePos")
	assert.Error(t, pmq.moveConsumePos("test_moveConsumePos", "", 0))
}

func TestPebblemq_updateAckedInfoErr(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/pebblemq_updateackedinfoerror"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	params := paramtable.Get()
	params.Save(params.PebblemqCfg.PageSize.Key, "10")
	pmq, err := NewPebbleMQ(name, idAllocator)
	assert.Nil(t, err)
	defer pmq.Close()

	topicName := "test_updateAckedInfo"
	pmq.CreateTopic(topicName)
	defer pmq.DestroyTopic(topicName)

	//add message, make sure pmq has more than one page
	msgNum := 100
	pMsgs := make([]ProducerMessage, msgNum)
	for i := 0; i < msgNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids, err := pmq.Produce(topicName, pMsgs)
	assert.Nil(t, err)
	assert.Equal(t, len(pMsgs), len(ids))

	groupName := "test"
	for i := 0; i < 2; i++ {
		consumer := &Consumer{
			Topic:     topicName,
			GroupName: groupName + strconv.Itoa(i),
		}
		//make sure consumer not in pmq.consumersID
		_ = pmq.DestroyConsumerGroup(topicName, groupName)
		//add consumer to pmq.consumers
		pmq.RegisterConsumer(consumer)
	}

	// update acked for all page in pmq but some consumer not in pmq.consumers
	assert.Error(t, pmq.updateAckedInfo(topicName, groupName, 0, ids[len(ids)-1]))
}

func TestPebblemq_Info(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/pebblemq_testinfo"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	params := paramtable.Get()
	params.Save(params.PebblemqCfg.PageSize.Key, "10")
	pmq, err := NewPebbleMQ(name, idAllocator)
	assert.Nil(t, err)
	defer pmq.Close()

	topicName := "test_testinfo"
	groupName := "test"
	pmq.CreateTopic(topicName)
	defer pmq.DestroyTopic(topicName)

	consumer := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
	}

	_ = pmq.DestroyConsumerGroup(topicName, groupName)
	err = pmq.CreateConsumerGroup(topicName, groupName)
	assert.Nil(t, err)

	err = pmq.RegisterConsumer(consumer)
	assert.Nil(t, err)

	assert.True(t, pmq.Info())

	//test error
	pmq.kv = &pebblekv.PebbleKV{}
	assert.False(t, pmq.Info())
}
