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
	"github.com/stretchr/testify/assert"
	"github.com/tecbot/gorocksdb"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	rocksdbkv "github.com/milvus-io/milvus/internal/kv/rocksdb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var rmqPath = "/tmp/rocksmq"

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
	rocksdbKV, err := rocksdbkv.NewRocksdbKV(kvPath)
	if err != nil {
		panic(err)
	}
	idAllocator := allocator.NewGlobalIDAllocator("rmq_id", rocksdbKV)
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
func (rmq *rocksmq) produceBefore(topicName string, messages []producerMessageBefore) ([]UniqueID, error) {
	if rmq.isClosed() {
		return nil, errors.New(RmqNotServingErrMsg)
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
	idStart, idEnd, err := rmq.idAllocator.Alloc(uint32(msgLen))

	if err != nil {
		return []UniqueID{}, err
	}
	allocTime := time.Since(start).Milliseconds()
	if UniqueID(msgLen) != idEnd-idStart {
		return []UniqueID{}, errors.New("Obtained id length is not equal that of message")
	}

	// Insert data to store system
	batch := gorocksdb.NewWriteBatch()
	defer batch.Destroy()
	msgSizes := make(map[UniqueID]int64)
	msgIDs := make([]UniqueID, msgLen)
	for i := 0; i < msgLen && idStart+UniqueID(i) < idEnd; i++ {
		msgID := idStart + UniqueID(i)
		key := path.Join(topicName, strconv.FormatInt(msgID, 10))
		batch.Put([]byte(key), messages[i].Payload)
		msgIDs[i] = msgID
		msgSizes[msgID] = int64(len(messages[i].Payload))
	}

	opts := gorocksdb.NewDefaultWriteOptions()
	defer opts.Destroy()
	err = rmq.store.Write(opts, batch)
	if err != nil {
		return []UniqueID{}, err
	}
	writeTime := time.Since(start).Milliseconds()
	if vals, ok := rmq.consumers.Load(topicName); ok {
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
	err = rmq.updatePageInfo(topicName, msgIDs, msgSizes)
	if err != nil {
		return []UniqueID{}, err
	}

	getProduceTime := time.Since(start).Milliseconds()
	if getProduceTime > 200 {

		log.Warn("rocksmq produce too slowly", zap.String("topic", topicName),
			zap.Int64("get lock elapse", getLockTime),
			zap.Int64("alloc elapse", allocTime-getLockTime),
			zap.Int64("write elapse", writeTime-allocTime),
			zap.Int64("updatePage elapse", getProduceTime-writeTime),
			zap.Int64("produce total elapse", getProduceTime),
		)
	}
	return msgIDs, nil
}

func TestRocksmq_RegisterConsumer(t *testing.T) {
	suffix := "_register"
	kvPath := rmqPath + kvPathSuffix + suffix
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := rmqPath + suffix
	defer os.RemoveAll(rocksdbPath + kvSuffix)
	defer os.RemoveAll(rocksdbPath)

	paramtable.Init()
	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
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

	kvPath := rmqPath + kvPathSuffix + suffix
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := rmqPath + suffix
	defer os.RemoveAll(rocksdbPath + kvSuffix)
	defer os.RemoveAll(rocksdbPath)
	paramtable.Init()
	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.NoError(t, err)
	defer rmq.Close()

	channelName := "channel_rocks"
	err = rmq.CreateTopic(channelName)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(channelName)

	msgA := "a_message"
	pMsgs := make([]ProducerMessage, 1)
	pMsgA := ProducerMessage{Payload: []byte(msgA), Properties: map[string]string{common.TraceIDKey: "a"}}
	pMsgs[0] = pMsgA

	_, err = rmq.Produce(channelName, pMsgs)
	assert.NoError(t, err)

	pMsgB := ProducerMessage{Payload: []byte("b_message"), Properties: map[string]string{common.TraceIDKey: "b"}}
	pMsgC := ProducerMessage{Payload: []byte("c_message"), Properties: map[string]string{common.TraceIDKey: "c"}}

	pMsgs[0] = pMsgB
	pMsgs = append(pMsgs, pMsgC)
	_, err = rmq.Produce(channelName, pMsgs)
	assert.NoError(t, err)

	// before 2.2.0, there have no properties in ProducerMessage and ConsumerMessage in rocksmq
	// it aims to test if produce before 2.2.0, but consume after 2.2.0
	msgD := "d_message"
	tMsgs := make([]producerMessageBefore, 1)
	tMsgD := producerMessageBefore{Payload: []byte(msgD)}
	tMsgs[0] = tMsgD

	_, err = rmq.produceBefore(channelName, tMsgs)
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
	_, ok := cMsgs[0].Properties[common.TraceIDKey]
	assert.True(t, ok)
	assert.Equal(t, cMsgs[0].Properties[common.TraceIDKey], "a")

	cMsgs, err = rmq.Consume(channelName, groupName, 2)
	assert.NoError(t, err)
	assert.Equal(t, len(cMsgs), 2)
	assert.Equal(t, string(cMsgs[0].Payload), "b_message")
	_, ok = cMsgs[0].Properties[common.TraceIDKey]
	assert.True(t, ok)
	assert.Equal(t, cMsgs[0].Properties[common.TraceIDKey], "b")
	assert.Equal(t, string(cMsgs[1].Payload), "c_message")
	_, ok = cMsgs[1].Properties[common.TraceIDKey]
	assert.True(t, ok)
	assert.Equal(t, cMsgs[1].Properties[common.TraceIDKey], "c")

	cMsgs, err = rmq.Consume(channelName, groupName, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(cMsgs), 1)
	assert.Equal(t, string(cMsgs[0].Payload), "d_message")
	_, ok = cMsgs[0].Properties[common.TraceIDKey]
	assert.False(t, ok)
	// it will be set empty map if produce message has no properties field
	expect := make(map[string]string)
	assert.Equal(t, cMsgs[0].Properties, expect)
}

func TestRocksmq_MultiConsumer(t *testing.T) {
	suffix := "rmq_multi_consumer"
	kvPath := rmqPath + kvPathSuffix + suffix
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := rmqPath + suffix
	defer os.RemoveAll(rocksdbPath + kvSuffix)
	defer os.RemoveAll(rocksdbPath)

	params := paramtable.Get()
	params.Save(params.RocksmqCfg.PageSize.Key, "10")
	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
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
	kvPath := rmqPath + kvPathSuffix + suffix
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := rmqPath + suffix
	defer os.RemoveAll(rocksdbPath + kvSuffix)
	defer os.RemoveAll(rocksdbPath)
	paramtable.Init()
	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.NoError(t, err)
	defer rmq.Close()

	_, err = NewRocksMQ("", idAllocator)
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
	kvPath := rmqPath + kvPathSuffix + suffix
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := rmqPath + suffix
	defer os.RemoveAll(rocksdbPath + kvSuffix)
	defer os.RemoveAll(rocksdbPath)

	paramtable.Init()
	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.NoError(t, err)
	defer rmq.Close()

	_, err = NewRocksMQ("", idAllocator)
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
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.NoError(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.NoError(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_1"
	_ = os.RemoveAll(name)
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)

	paramtable.Init()
	rmq, err := NewRocksMQ(name, idAllocator)
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
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.NoError(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.NoError(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_2"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)

	paramtable.Init()
	rmq, err := NewRocksMQ(name, idAllocator)
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
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.NoError(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.NoError(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_3"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)

	paramtable.Init()
	rmq, err := NewRocksMQ(name, idAllocator)
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
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.NoError(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.NoError(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_multichan"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)

	paramtable.Init()
	rmq, err := NewRocksMQ(name, idAllocator)
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
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.NoError(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.NoError(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_copydata"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)

	paramtable.Init()
	rmq, err := NewRocksMQ(name, idAllocator)
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
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.NoError(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.NoError(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_seektolatest"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)

	paramtable.Init()
	rmq, err := NewRocksMQ(name, idAllocator)
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
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.NoError(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.NoError(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_data"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	rmq, err := NewRocksMQ(name, idAllocator)
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
	kvPath := rmqPath + kvPathSuffix + suffix
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := rmqPath + suffix
	defer os.RemoveAll(rocksdbPath + kvSuffix)
	defer os.RemoveAll(rocksdbPath)
	paramtable.Init()
	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.NoError(t, err)
	defer rmq.Close()

	channelName1 := "topic1"
	// topic not exist
	err = rmq.CheckTopicValid(channelName1)
	assert.Equal(t, true, errors.Is(err, merr.ErrTopicNotFound))

	channelName2 := "topic2"
	// topic is not empty
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
	assert.Equal(t, true, errors.Is(err, merr.ErrTopicNotEmpty))

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
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.NoError(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.NoError(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_close"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	rmq, err := NewRocksMQ(name, idAllocator)
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
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.NoError(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_seekerror"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.NoError(t, err)
	defer rmq.Close()

	rmq.CreateTopic("test")
	err = rmq.Seek("test", "", 0)
	t.Log(err)
	assert.Error(t, err)
	assert.Error(t, rmq.ForceSeek("test", "", 0))
}

func TestRocksmq_SeekTopicNotExistError(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.NoError(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_seekerror2"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.NoError(t, err)
	defer rmq.Close()

	assert.Error(t, rmq.Seek("test_topic_not_exist", "", 0))
	assert.Error(t, rmq.ForceSeek("test_topic_not_exist", "", 0))
}

func TestRocksmq_SeekTopicMutexError(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.NoError(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_seekerror2"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.NoError(t, err)
	defer rmq.Close()

	topicMu.Store("test_topic_mutix_error", nil)
	assert.Error(t, rmq.Seek("test_topic_mutix_error", "", 0))
	assert.Error(t, rmq.ForceSeek("test_topic_mutix_error", "", 0))
}

func TestRocksmq_moveConsumePosError(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.NoError(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_moveconsumeposerror"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.NoError(t, err)
	defer rmq.Close()

	rmq.CreateTopic("test_moveConsumePos")
	assert.Error(t, rmq.moveConsumePos("test_moveConsumePos", "", 0))
}

func TestRocksmq_updateAckedInfoErr(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.NoError(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_updateackedinfoerror"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	params := paramtable.Get()
	params.Save(params.RocksmqCfg.PageSize.Key, "10")
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.NoError(t, err)
	defer rmq.Close()

	topicName := "test_updateAckedInfo"
	rmq.CreateTopic(topicName)
	defer rmq.DestroyTopic(topicName)

	//add message, make sure rmq has more than one page
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
		//make sure consumer not in rmq.consumersID
		rmq.DestroyConsumerGroup(topicName, groupName+strconv.Itoa(i))
		//add consumer to rmq.consumers
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
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.NoError(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_testinfo"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	params := paramtable.Get()
	params.Save(params.RocksmqCfg.PageSize.Key, "10")
	rmq, err := NewRocksMQ(name, idAllocator)
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

	//test error
	rmq.kv = &rocksdbkv.RocksdbKV{}
	assert.False(t, rmq.Info())
}
