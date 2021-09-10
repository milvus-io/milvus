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
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/util/paramtable"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	rocksdbkv "github.com/milvus-io/milvus/internal/kv/rocksdb"
	"github.com/stretchr/testify/assert"
)

var Params paramtable.BaseTable
var rmqPath string = "/tmp/rocksmq"

func InitIDAllocator(kvPath string) *allocator.GlobalIDAllocator {
	rocksdbKV, err := rocksdbkv.NewRocksdbKV(kvPath)
	if err != nil {
		panic(err)
	}
	idAllocator := allocator.NewGlobalIDAllocator("rmq_id", rocksdbKV)
	_ = idAllocator.Initialize()
	return idAllocator
}

func TestFixChannelName(t *testing.T) {
	name := "abcd"
	fixName, err := fixChannelName(name)
	assert.Nil(t, err)
	assert.Equal(t, len(fixName), FixedChannelNameLen)
}

func etcdEndpoints() []string {
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "localhost:2379"
	}
	etcdEndpoints := strings.Split(endpoints, ",")
	return etcdEndpoints
}

func TestInitRmq(t *testing.T) {
	name := "/tmp/rmq_init"
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "localhost:2379"
	}
	etcdEndpoints := strings.Split(endpoints, ",")
	etcdKV, err := etcdkv.NewEtcdKV(etcdEndpoints, "/etcd/test/root")
	if err != nil {
		log.Fatalf("New clientv3 error = %v", err)
	}
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	err = InitRmq(name, idAllocator)
	defer Rmq.stopRetention()
	assert.NoError(t, err)
	defer CloseRocksMQ()
}

func TestGlobalRmq(t *testing.T) {
	// Params.Init()
	rmqPath := "/tmp/milvus/rdb_data_global"
	os.Setenv("ROCKSMQ_PATH", rmqPath)
	defer os.RemoveAll(rmqPath)
	err := InitRocksMQ()
	defer Rmq.stopRetention()
	assert.NoError(t, err)
	defer CloseRocksMQ()
}

func TestRegisterConsumer(t *testing.T) {
	kvPath := rmqPath + "_kv_register"
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := path + "_db_register"
	defer os.RemoveAll(rocksdbPath)
	metaPath := path + "_meta_kv_register"
	defer os.RemoveAll(metaPath)

	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.NoError(t, err)
	defer rmq.stopRetention()

	topicName := "topic_register"
	groupName := "group_register"
	_ = rmq.DestroyConsumerGroup(topicName, groupName)
	err = rmq.CreateConsumerGroup(topicName, groupName)
	assert.Nil(t, err)

	consumer := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
		MsgMutex:  make(chan struct{}),
	}
	rmq.RegisterConsumer(consumer)
	exist, _ := rmq.ExistConsumerGroup(topicName, groupName)
	assert.Equal(t, exist, true)
	dummyGrpName := "group_dummy"
	exist, _ = rmq.ExistConsumerGroup(topicName, dummyGrpName)
	assert.Equal(t, exist, false)

	msgA := "a_message"
	pMsgs := make([]ProducerMessage, 1)
	pMsgA := ProducerMessage{Payload: []byte(msgA)}
	pMsgs[0] = pMsgA

	_ = idAllocator.UpdateID()
	err = rmq.Produce(topicName, pMsgs)
	assert.Error(t, err)

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

	err = rmq.DestroyConsumerGroup(topicName, groupName)
	assert.Error(t, err)
}

func TestRocksMQ(t *testing.T) {
	kvPath := rmqPath + "_kv_rmq"
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := path + "_db_rmq"
	defer os.RemoveAll(rocksdbPath)
	metaPath := path + "_meta_kv_rmq"
	defer os.RemoveAll(metaPath)

	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.Nil(t, err)
	defer rmq.stopRetention()

	channelName := "channel_a"
	err = rmq.CreateTopic(channelName)
	assert.Nil(t, err)
	defer rmq.DestroyTopic(channelName)

	msgA := "a_message"
	pMsgs := make([]ProducerMessage, 1)
	pMsgA := ProducerMessage{Payload: []byte(msgA)}
	pMsgs[0] = pMsgA

	_ = idAllocator.UpdateID()
	err = rmq.Produce(channelName, pMsgs)
	assert.Nil(t, err)

	pMsgB := ProducerMessage{Payload: []byte("b_message")}
	pMsgC := ProducerMessage{Payload: []byte("c_message")}

	pMsgs[0] = pMsgB
	pMsgs = append(pMsgs, pMsgC)
	_ = idAllocator.UpdateID()
	err = rmq.Produce(channelName, pMsgs)
	assert.Nil(t, err)

	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(channelName, groupName)
	err = rmq.CreateConsumerGroup(channelName, groupName)
	assert.Nil(t, err)
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

func TestRocksMQDummy(t *testing.T) {
	kvPath := rmqPath + "_kv_dummy"
	defer os.RemoveAll(kvPath)
	idAllocator := InitIDAllocator(kvPath)

	rocksdbPath := path + "_db_dummy"
	defer os.RemoveAll(rocksdbPath)
	metaPath := path + "_meta_kv_dummy"
	defer os.RemoveAll(metaPath)

	rmq, err := NewRocksMQ(rocksdbPath, idAllocator)
	assert.Nil(t, err)

	channelName := "channel_a"
	err = rmq.CreateTopic(channelName)
	assert.Nil(t, err)
	defer rmq.DestroyTopic(channelName)
	err = rmq.CreateTopic(channelName)
	assert.NoError(t, err)

	channelName1 := "channel_dummy"
	err = rmq.DestroyTopic(channelName1)
	assert.NoError(t, err)

	err = rmq.DestroyConsumerGroup(channelName, channelName1)
	assert.NoError(t, err)

	err = rmq.Produce(channelName, nil)
	assert.Error(t, err)

	err = rmq.Produce(channelName1, nil)
	assert.Error(t, err)

	groupName1 := "group_dummy"
	err = rmq.Seek(channelName1, groupName1, 0)
	assert.Error(t, err)

	rmq.stopRetention()
	channelName2 := strings.Repeat(channelName1, 100)
	err = rmq.CreateTopic(string(channelName2))
	assert.NoError(t, err)
	err = rmq.Produce(string(channelName2), nil)
	assert.Error(t, err)

	msgA := "a_message"
	pMsgs := make([]ProducerMessage, 1)
	pMsgA := ProducerMessage{Payload: []byte(msgA)}
	pMsgs[0] = pMsgA
}

func TestRocksMQ_Loop(t *testing.T) {
	ep := etcdEndpoints()
	etcdKV, err := etcdkv.NewEtcdKV(ep, "/etcd/test/root")
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
	defer rmq.stopRetention()

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
		err := rmq.Produce(channelName, pMsgs)
		assert.Nil(t, err)
	}

	// Produce loopNum messages once
	pMsgs := make([]ProducerMessage, loopNum)
	for i := 0; i < loopNum; i++ {
		msg := "message_" + strconv.Itoa(i+loopNum)
		pMsg := ProducerMessage{Payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	err = rmq.Produce(channelName, pMsgs)
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

func TestRocksMQ_Goroutines(t *testing.T) {
	ep := etcdEndpoints()
	etcdKV, err := etcdkv.NewEtcdKV(ep, "/etcd/test/root")
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
	defer rmq.stopRetention()

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

			err := mq.Produce(channelName, pMsgs)
			assert.Nil(t, err)
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
func TestRocksMQ_Throughout(t *testing.T) {
	ep := etcdEndpoints()
	etcdKV, err := etcdkv.NewEtcdKV(ep, "/etcd/test/root")
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
	defer rmq.stopRetention()

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
		err := rmq.Produce(channelName, []ProducerMessage{pMsg})
		assert.Nil(t, err)
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

func TestRocksMQ_MultiChan(t *testing.T) {
	ep := etcdEndpoints()
	etcdKV, err := etcdkv.NewEtcdKV(ep, "/etcd/test/root")
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
	defer rmq.stopRetention()

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
		err = rmq.Produce(channelName0, []ProducerMessage{pMsg0})
		assert.Nil(t, err)
		err = rmq.Produce(channelName1, []ProducerMessage{pMsg1})
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

func TestRocksMQ_CopyData(t *testing.T) {
	ep := etcdEndpoints()
	etcdKV, err := etcdkv.NewEtcdKV(ep, "/etcd/test/root")
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
	defer rmq.stopRetention()

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
	err = rmq.Produce(channelName0, []ProducerMessage{pMsg0})
	assert.Nil(t, err)

	pMsg1 := ProducerMessage{Payload: nil}
	err = rmq.Produce(channelName1, []ProducerMessage{pMsg1})
	assert.Nil(t, err)

	pMsg2 := ProducerMessage{Payload: []byte{}}
	err = rmq.Produce(channelName1, []ProducerMessage{pMsg2})
	assert.Nil(t, err)

	var emptyTargetData []byte
	pMsg3 := ProducerMessage{Payload: emptyTargetData}
	err = rmq.Produce(channelName1, []ProducerMessage{pMsg3})
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
