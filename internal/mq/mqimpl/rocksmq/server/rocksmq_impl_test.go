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
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/allocator"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	rocksdbkv "github.com/milvus-io/milvus/internal/kv/rocksdb"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/paramtable"

	"github.com/stretchr/testify/assert"
)

var Params paramtable.BaseTable
var rmqPath = "/tmp/rocksmq"
var kvPathSuffix = "_kv"
var metaPathSuffix = "_meta"

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

func TestRocksmq_RegisterConsumer(t *testing.T) {
	suffix := "_register"
	kvPath := rmqPath + kvPathSuffix + suffix
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := rmqPath + suffix
	defer os.RemoveAll(rocksdbPath + kvSuffix)
	defer os.RemoveAll(rocksdbPath)

	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.NoError(t, err)
	defer rmq.stopRetention()

	topicName := "topic_register"
	groupName := "group_register"

	err = rmq.CreateTopic(topicName)
	assert.NoError(t, err)
	defer rmq.DestroyTopic(topicName)

	err = rmq.CreateConsumerGroup(topicName, groupName)
	assert.Nil(t, err)
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

	_ = idAllocator.UpdateID()
	_, err = rmq.Produce(topicName, pMsgs)
	assert.Nil(t, err)

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
	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.Nil(t, err)
	defer rmq.Close()

	channelName := "channel_rocks"
	err = rmq.CreateTopic(channelName)
	assert.Nil(t, err)
	defer rmq.DestroyTopic(channelName)

	msgA := "a_message"
	pMsgs := make([]ProducerMessage, 1)
	pMsgA := ProducerMessage{Payload: []byte(msgA)}
	pMsgs[0] = pMsgA

	_ = idAllocator.UpdateID()
	_, err = rmq.Produce(channelName, pMsgs)
	assert.Nil(t, err)

	pMsgB := ProducerMessage{Payload: []byte("b_message")}
	pMsgC := ProducerMessage{Payload: []byte("c_message")}

	pMsgs[0] = pMsgB
	pMsgs = append(pMsgs, pMsgC)
	_ = idAllocator.UpdateID()
	_, err = rmq.Produce(channelName, pMsgs)
	assert.Nil(t, err)

	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(channelName, groupName)
	err = rmq.CreateConsumerGroup(channelName, groupName)
	assert.Nil(t, err)
	// double create consumer group
	err = rmq.CreateConsumerGroup(channelName, groupName)
	assert.Error(t, err)
	cMsgs, err := rmq.Consume(channelName, groupName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), 1)
	assert.Equal(t, string(cMsgs[0].Payload), "a_message")

	cMsgs, err = rmq.Consume(channelName, groupName, 2)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), 2)
	assert.Equal(t, string(cMsgs[0].Payload), "b_message")
	assert.Equal(t, string(cMsgs[1].Payload), "c_message")
}

func TestRocksmq_Dummy(t *testing.T) {
	suffix := "_dummy"
	kvPath := rmqPath + kvPathSuffix + suffix
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := rmqPath + suffix
	defer os.RemoveAll(rocksdbPath + kvSuffix)
	defer os.RemoveAll(rocksdbPath)

	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.Nil(t, err)
	defer rmq.Close()

	_, err = NewRocksMQ("", idAllocator)
	assert.Error(t, err)

	channelName := "channel_a"
	err = rmq.CreateTopic(channelName)
	assert.Nil(t, err)
	defer rmq.DestroyTopic(channelName)
	// create topic twice should be ignored
	err = rmq.CreateTopic(channelName)
	assert.Nil(t, err)

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

	rmq.stopRetention()
	channelName2 := strings.Repeat(channelName1, 100)
	err = rmq.CreateTopic(string(channelName2))
	assert.NoError(t, err)
	_, err = rmq.Produce(string(channelName2), nil)
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

	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.Nil(t, err)
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
		assert.Nil(t, err)
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
	assert.Nil(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.Nil(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_1"
	_ = os.RemoveAll(name)
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.Nil(t, err)
	defer rmq.Close()

	loopNum := 100
	channelName := "channel_test"
	err = rmq.CreateTopic(channelName)
	assert.Nil(t, err)
	defer rmq.DestroyTopic(channelName)

	// Produce one message once
	for i := 0; i < loopNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs := make([]ProducerMessage, 1)
		pMsgs[0] = pMsg
		_, err := rmq.Produce(channelName, pMsgs)
		assert.Nil(t, err)
	}

	// Produce loopNum messages once
	pMsgs := make([]ProducerMessage, loopNum)
	for i := 0; i < loopNum; i++ {
		msg := "message_" + strconv.Itoa(i+loopNum)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	_, err = rmq.Produce(channelName, pMsgs)
	assert.Nil(t, err)

	// Consume loopNum message once
	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(channelName, groupName)
	err = rmq.CreateConsumerGroup(channelName, groupName)
	assert.Nil(t, err)
	cMsgs, err := rmq.Consume(channelName, groupName, loopNum)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), loopNum)
	assert.Equal(t, string(cMsgs[0].Payload), "message_"+strconv.Itoa(0))
	assert.Equal(t, string(cMsgs[loopNum-1].Payload), "message_"+strconv.Itoa(loopNum-1))

	// Consume one message once
	for i := 0; i < loopNum; i++ {
		oneMsgs, err := rmq.Consume(channelName, groupName, 1)
		assert.Nil(t, err)
		assert.Equal(t, len(oneMsgs), 1)
		assert.Equal(t, string(oneMsgs[0].Payload), "message_"+strconv.Itoa(i+loopNum))
	}

	cMsgs, err = rmq.Consume(channelName, groupName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), 0)
}

func TestRocksmq_Goroutines(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.Nil(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_2"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.Nil(t, err)
	defer rmq.Close()

	loopNum := 100
	channelName := "channel_test"
	err = rmq.CreateTopic(channelName)
	assert.Nil(t, err)
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
			assert.Nil(t, err)
			assert.Equal(t, len(pMsgs), len(ids))
			msgChan <- msg0
			msgChan <- msg1
		}(i, rmq)
	}

	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(channelName, groupName)
	err = rmq.CreateConsumerGroup(channelName, groupName)
	assert.Nil(t, err)
	// Consume one message in each goroutine
	for i := 0; i < loopNum; i++ {
		go func(group *sync.WaitGroup, mq RocksMQ) {
			defer group.Done()
			<-msgChan
			cMsgs, err := mq.Consume(channelName, groupName, 1)
			assert.Nil(t, err)
			assert.Equal(t, len(cMsgs), 1)
		}(&wg, rmq)
	}
	wg.Wait()
}

/**
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
	assert.Nil(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.Nil(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_3"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.Nil(t, err)
	defer rmq.Close()

	channelName := "channel_throughout_test"
	err = rmq.CreateTopic(channelName)
	assert.Nil(t, err)
	defer rmq.DestroyTopic(channelName)

	entityNum := 100000

	pt0 := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < entityNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		assert.Nil(t, idAllocator.UpdateID())
		ids, err := rmq.Produce(channelName, []ProducerMessage{pMsg})
		assert.Nil(t, err)
		assert.EqualValues(t, 1, len(ids))
	}
	pt1 := time.Now().UnixNano() / int64(time.Millisecond)
	pDuration := pt1 - pt0
	log.Printf("Total produce %d item, cost %v ms, throughout %v / s", entityNum, pDuration, int64(entityNum)*1000/pDuration)

	groupName := "test_throughout_group"
	_ = rmq.DestroyConsumerGroup(channelName, groupName)
	err = rmq.CreateConsumerGroup(channelName, groupName)
	assert.Nil(t, err)
	defer rmq.DestroyConsumerGroup(groupName, channelName)

	// Consume one message in each goroutine
	ct0 := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < entityNum; i++ {
		cMsgs, err := rmq.Consume(channelName, groupName, 1)
		assert.Nil(t, err)
		assert.Equal(t, len(cMsgs), 1)
	}
	ct1 := time.Now().UnixNano() / int64(time.Millisecond)
	cDuration := ct1 - ct0
	log.Printf("Total consume %d item, cost %v ms, throughout %v / s", entityNum, cDuration, int64(entityNum)*1000/cDuration)
}

func TestRocksmq_MultiChan(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.Nil(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_multichan"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.Nil(t, err)
	defer rmq.Close()

	channelName0 := "chan01"
	channelName1 := "chan11"
	err = rmq.CreateTopic(channelName0)
	assert.Nil(t, err)
	defer rmq.DestroyTopic(channelName0)
	err = rmq.CreateTopic(channelName1)
	assert.Nil(t, err)
	defer rmq.DestroyTopic(channelName1)
	assert.Nil(t, err)

	loopNum := 10
	for i := 0; i < loopNum; i++ {
		msg0 := "for_chann0_" + strconv.Itoa(i)
		msg1 := "for_chann1_" + strconv.Itoa(i)
		pMsg0 := ProducerMessage{Payload: []byte(msg0)}
		pMsg1 := ProducerMessage{Payload: []byte(msg1)}
		_, err = rmq.Produce(channelName0, []ProducerMessage{pMsg0})
		assert.Nil(t, err)
		_, err = rmq.Produce(channelName1, []ProducerMessage{pMsg1})
		assert.Nil(t, err)
	}

	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(channelName1, groupName)
	err = rmq.CreateConsumerGroup(channelName1, groupName)
	assert.Nil(t, err)
	cMsgs, err := rmq.Consume(channelName1, groupName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), 1)
	assert.Equal(t, string(cMsgs[0].Payload), "for_chann1_"+strconv.Itoa(0))
}

func TestRocksmq_CopyData(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.Nil(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_copydata"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.Nil(t, err)
	defer rmq.Close()

	channelName0 := "test_chan01"
	channelName1 := "test_chan11"
	err = rmq.CreateTopic(channelName0)
	assert.Nil(t, err)
	defer rmq.DestroyTopic(channelName0)
	err = rmq.CreateTopic(channelName1)
	assert.Nil(t, err)
	defer rmq.DestroyTopic(channelName1)
	assert.Nil(t, err)

	msg0 := "abcde"
	pMsg0 := ProducerMessage{Payload: []byte(msg0)}
	_, err = rmq.Produce(channelName0, []ProducerMessage{pMsg0})
	assert.Nil(t, err)

	pMsg1 := ProducerMessage{Payload: nil}
	_, err = rmq.Produce(channelName1, []ProducerMessage{pMsg1})
	assert.Nil(t, err)

	pMsg2 := ProducerMessage{Payload: []byte{}}
	_, err = rmq.Produce(channelName1, []ProducerMessage{pMsg2})
	assert.Nil(t, err)

	var emptyTargetData []byte
	pMsg3 := ProducerMessage{Payload: emptyTargetData}
	_, err = rmq.Produce(channelName1, []ProducerMessage{pMsg3})
	assert.Nil(t, err)

	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(channelName0, groupName)
	err = rmq.CreateConsumerGroup(channelName0, groupName)
	assert.Nil(t, err)
	cMsgs0, err := rmq.Consume(channelName0, groupName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs0), 1)
	assert.Equal(t, string(cMsgs0[0].Payload), msg0)

	_ = rmq.DestroyConsumerGroup(channelName1, groupName)
	err = rmq.CreateConsumerGroup(channelName1, groupName)
	assert.Nil(t, err)
	cMsgs1, err := rmq.Consume(channelName1, groupName, 3)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(cMsgs1))
	assert.Equal(t, emptyTargetData, cMsgs1[0].Payload)
}

func TestRocksmq_SeekToLatest(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.Nil(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_seektolatest"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.Nil(t, err)
	defer rmq.Close()

	channelName := "channel_test"
	err = rmq.CreateTopic(channelName)
	assert.Nil(t, err)
	defer rmq.DestroyTopic(channelName)
	loopNum := 100

	err = rmq.SeekToLatest(channelName, "dummy_group")
	assert.Error(t, err)

	// Consume loopNum message once
	groupName := "group_test"
	_ = rmq.DestroyConsumerGroup(channelName, groupName)
	err = rmq.CreateConsumerGroup(channelName, groupName)
	assert.Nil(t, err)

	err = rmq.SeekToLatest(channelName, groupName)
	assert.NoError(t, err)

	channelNamePrev := "channel_tes"
	err = rmq.CreateTopic(channelNamePrev)
	assert.Nil(t, err)
	defer rmq.DestroyTopic(channelNamePrev)
	pMsgs := make([]ProducerMessage, loopNum)
	for i := 0; i < loopNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	_, err = rmq.Produce(channelNamePrev, pMsgs)
	assert.Nil(t, err)

	// should hit the case where channel is null
	err = rmq.SeekToLatest(channelName, groupName)
	assert.NoError(t, err)

	ids, err := rmq.Produce(channelName, pMsgs)
	assert.Nil(t, err)

	// able to read out
	cMsgs, err := rmq.Consume(channelName, groupName, loopNum)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), loopNum)
	for i := 0; i < loopNum; i++ {
		assert.Equal(t, cMsgs[i].MsgID, ids[i])
	}

	err = rmq.SeekToLatest(channelName, groupName)
	assert.NoError(t, err)

	cMsgs, err = rmq.Consume(channelName, groupName, loopNum)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), 0)

	pMsgs = make([]ProducerMessage, loopNum)
	for i := 0; i < loopNum; i++ {
		msg := "message_" + strconv.Itoa(i+loopNum)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids, err = rmq.Produce(channelName, pMsgs)
	assert.Nil(t, err)

	// make sure we only consume the latest message
	cMsgs, err = rmq.Consume(channelName, groupName, loopNum)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), loopNum)
	for i := 0; i < loopNum; i++ {
		assert.Equal(t, cMsgs[i].MsgID, ids[i])
	}
}

func TestRocksmq_Reader(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.Nil(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_reader"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.Nil(t, err)
	defer rmq.Close()

	channelName := newChanName()
	_, err = rmq.CreateReader(channelName, 0, true, "")
	assert.Error(t, err)
	err = rmq.CreateTopic(channelName)
	assert.Nil(t, err)
	defer rmq.DestroyTopic(channelName)
	loopNum := 100

	pMsgs := make([]ProducerMessage, loopNum)
	for i := 0; i < loopNum; i++ {
		msg := "message_" + strconv.Itoa(i+loopNum)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids, err := rmq.Produce(channelName, pMsgs)
	assert.Nil(t, err)
	assert.Equal(t, len(ids), loopNum)

	readerName1, err := rmq.CreateReader(channelName, ids[0], true, "test-reader-true")
	assert.NoError(t, err)
	rmq.ReaderSeek(channelName, readerName1, ids[0])
	ctx := context.Background()
	for i := 0; i < loopNum; i++ {
		assert.Equal(t, true, rmq.HasNext(channelName, readerName1))
		msg, err := rmq.Next(ctx, channelName, readerName1)
		assert.NoError(t, err)
		assert.Equal(t, msg.MsgID, ids[i])
	}
	assert.False(t, rmq.HasNext(channelName, readerName1))

	readerName2, err := rmq.CreateReader(channelName, ids[0], false, "test-reader-false")
	assert.NoError(t, err)

	rmq.ReaderSeek(channelName, readerName2, ids[0])
	for i := 0; i < loopNum-1; i++ {
		assert.Equal(t, true, rmq.HasNext(channelName, readerName2))
		msg, err := rmq.Next(ctx, channelName, readerName2)
		assert.NoError(t, err)
		assert.Equal(t, msg.MsgID, ids[i+1])
	}
	assert.False(t, rmq.HasNext(channelName, readerName2))
}

func TestReader_CornerCase(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_reader_cornercase"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.Nil(t, err)
	defer rmq.Close()

	channelName := newChanName()
	err = rmq.CreateTopic(channelName)
	assert.Nil(t, err)
	defer rmq.DestroyTopic(channelName)
	loopNum := 10

	pMsgs := make([]ProducerMessage, loopNum)
	for i := 0; i < loopNum; i++ {
		msg := "message_" + strconv.Itoa(i+loopNum)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	ids, err := rmq.Produce(channelName, pMsgs)
	assert.Nil(t, err)
	assert.Equal(t, len(ids), loopNum)

	readerName, err := rmq.CreateReader(channelName, ids[loopNum-1], true, "cornercase")
	assert.NoError(t, err)

	ctx := context.Background()
	msg, err := rmq.Next(ctx, channelName, readerName)
	assert.NoError(t, err)
	assert.Equal(t, msg.MsgID, ids[loopNum-1])

	var extraIds []UniqueID
	go func() {
		time.Sleep(1 * time.Second)
		extraMsgs := make([]ProducerMessage, 1)
		msg := "extra_message"
		extraMsgs[0] = ProducerMessage{Payload: []byte(msg)}
		extraIds, _ = rmq.Produce(channelName, extraMsgs)
		// assert.NoError(t, er)
		assert.Equal(t, 1, len(extraIds))
	}()

	msg, err = rmq.Next(ctx, channelName, readerName)
	assert.NoError(t, err)
	assert.Equal(t, string(msg.Payload), "extra_message")
}

func TestRocksmq_Close(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	assert.Nil(t, err)
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_close"
	defer os.RemoveAll(name)
	kvName := name + "_meta_kv"
	_ = os.RemoveAll(kvName)
	defer os.RemoveAll(kvName)
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.Nil(t, err)
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
	assert.Error(t, rmq.SeekToLatest("", ""))
	_, err = rmq.CreateReader("", 0, false, "")
	assert.Error(t, err)
	rmq.ReaderSeek("", "", 0)
	_, err = rmq.Next(nil, "", "")
	assert.Error(t, err)
	rmq.HasNext("", "")
	rmq.CloseReader("", "")
}

func TestRocksmq_SeekWithNoConsumerError(t *testing.T) {
	ep := etcdEndpoints()
	etcdCli, err := etcd.GetRemoteEtcdClient(ep)
	assert.Nil(t, err)
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
	assert.Nil(t, err)
	defer rmq.Close()

	rmq.CreateTopic("test")
	err = rmq.Seek("test", "", 0)
	fmt.Println(err)
	assert.Error(t, err)
}
