package rocksmq

import (
	"log"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/allocator"

	"github.com/stretchr/testify/assert"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"go.etcd.io/etcd/clientv3"
)

func TestFixChannelName(t *testing.T) {
	name := "abcd"
	fixName, err := fixChannelName(name)
	assert.Nil(t, err)
	assert.Equal(t, len(fixName), FixedChannelNameLen)
}

func TestRocksMQ(t *testing.T) {
	etcdAddr := os.Getenv("ETCD_ADDRESS")
	if etcdAddr == "" {
		etcdAddr = "localhost:2379"
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(cli, "/etcd/test/root")
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq"
	_ = os.RemoveAll(name)
	defer os.RemoveAll(name)
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.Nil(t, err)

	channelName := "channel_a"
	err = rmq.CreateChannel(channelName)
	assert.Nil(t, err)
	defer rmq.DestroyChannel(channelName)

	msgA := "a_message"
	pMsgs := make([]ProducerMessage, 1)
	pMsgA := ProducerMessage{payload: []byte(msgA)}
	pMsgs[0] = pMsgA

	_ = idAllocator.UpdateID()
	err = rmq.Produce(channelName, pMsgs)
	assert.Nil(t, err)

	pMsgB := ProducerMessage{payload: []byte("b_message")}
	pMsgC := ProducerMessage{payload: []byte("c_message")}

	pMsgs[0] = pMsgB
	pMsgs = append(pMsgs, pMsgC)
	_ = idAllocator.UpdateID()
	err = rmq.Produce(channelName, pMsgs)
	assert.Nil(t, err)

	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(groupName, channelName)
	_, err = rmq.CreateConsumerGroup(groupName, channelName)
	assert.Nil(t, err)
	cMsgs, err := rmq.Consume(groupName, channelName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), 1)
	assert.Equal(t, string(cMsgs[0].Payload), "a_message")

	cMsgs, err = rmq.Consume(groupName, channelName, 2)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), 2)
	assert.Equal(t, string(cMsgs[0].Payload), "b_message")
	assert.Equal(t, string(cMsgs[1].Payload), "c_message")
}

func TestRocksMQ_Loop(t *testing.T) {
	etcdAddr := os.Getenv("ETCD_ADDRESS")
	if etcdAddr == "" {
		etcdAddr = "localhost:2379"
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(cli, "/etcd/test/root")
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_1"
	_ = os.RemoveAll(name)
	defer os.RemoveAll(name)
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.Nil(t, err)

	loopNum := 100
	channelName := "channel_test"
	err = rmq.CreateChannel(channelName)
	assert.Nil(t, err)
	defer rmq.DestroyChannel(channelName)

	// Produce one message once
	for i := 0; i < loopNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{payload: []byte(msg)}
		pMsgs := make([]ProducerMessage, 1)
		pMsgs[0] = pMsg
		err := rmq.Produce(channelName, pMsgs)
		assert.Nil(t, err)
	}

	// Produce loopNum messages once
	pMsgs := make([]ProducerMessage, loopNum)
	for i := 0; i < loopNum; i++ {
		msg := "message_" + strconv.Itoa(i+loopNum)
		pMsg := ProducerMessage{payload: []byte(msg)}
		pMsgs[i] = pMsg
	}
	err = rmq.Produce(channelName, pMsgs)
	assert.Nil(t, err)

	// Consume loopNum message once
	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(groupName, channelName)
	_, err = rmq.CreateConsumerGroup(groupName, channelName)
	assert.Nil(t, err)
	cMsgs, err := rmq.Consume(groupName, channelName, loopNum)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), loopNum)
	assert.Equal(t, string(cMsgs[0].Payload), "message_"+strconv.Itoa(0))
	assert.Equal(t, string(cMsgs[loopNum-1].Payload), "message_"+strconv.Itoa(loopNum-1))

	// Consume one message once
	for i := 0; i < loopNum; i++ {
		oneMsgs, err := rmq.Consume(groupName, channelName, 1)
		assert.Nil(t, err)
		assert.Equal(t, len(oneMsgs), 1)
		assert.Equal(t, string(oneMsgs[0].Payload), "message_"+strconv.Itoa(i+loopNum))
	}

	cMsgs, err = rmq.Consume(groupName, channelName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), 0)
}

func TestRocksMQ_Goroutines(t *testing.T) {
	etcdAddr := os.Getenv("ETCD_ADDRESS")
	if etcdAddr == "" {
		etcdAddr = "localhost:2379"
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(cli, "/etcd/test/root")
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_2"
	defer os.RemoveAll(name)
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.Nil(t, err)

	loopNum := 100
	channelName := "channel_test"
	err = rmq.CreateChannel(channelName)
	assert.Nil(t, err)
	defer rmq.DestroyChannel(channelName)

	// Produce two message in each goroutine
	msgChan := make(chan string, loopNum)
	var wg sync.WaitGroup
	for i := 0; i < loopNum; i += 2 {
		go func(i int, group *sync.WaitGroup, mq *RocksMQ) {
			group.Add(2)
			msg0 := "message_" + strconv.Itoa(i)
			msg1 := "message_" + strconv.Itoa(i+1)
			pMsg0 := ProducerMessage{payload: []byte(msg0)}
			pMsg1 := ProducerMessage{payload: []byte(msg1)}
			pMsgs := make([]ProducerMessage, 2)
			pMsgs[0] = pMsg0
			pMsgs[1] = pMsg1

			err := mq.Produce(channelName, pMsgs)
			assert.Nil(t, err)
			msgChan <- msg0
			msgChan <- msg1
		}(i, &wg, rmq)
	}

	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(groupName, channelName)
	_, err = rmq.CreateConsumerGroup(groupName, channelName)
	assert.Nil(t, err)
	// Consume one message in each goroutine
	for i := 0; i < loopNum; i++ {
		go func(group *sync.WaitGroup, mq *RocksMQ) {
			defer group.Done()
			<-msgChan
			cMsgs, err := mq.Consume(groupName, channelName, 1)
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
	etcdAddr := os.Getenv("ETCD_ADDRESS")
	if etcdAddr == "" {
		etcdAddr = "localhost:2379"
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(cli, "/etcd/test/root")
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_3"
	defer os.RemoveAll(name)
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.Nil(t, err)

	channelName := "channel_throughout_test"
	err = rmq.CreateChannel(channelName)
	assert.Nil(t, err)
	defer rmq.DestroyChannel(channelName)

	entityNum := 1000000

	pt0 := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < entityNum; i++ {
		msg := "message_" + strconv.Itoa(i)
		pMsg := ProducerMessage{payload: []byte(msg)}
		assert.Nil(t, idAllocator.UpdateID())
		err := rmq.Produce(channelName, []ProducerMessage{pMsg})
		assert.Nil(t, err)
	}
	pt1 := time.Now().UnixNano() / int64(time.Millisecond)
	pDuration := pt1 - pt0
	log.Printf("Total produce %d item, cost %v ms, throughout %v / s", entityNum, pDuration, int64(entityNum)*1000/pDuration)

	groupName := "test_throughout_group"
	_ = rmq.DestroyConsumerGroup(groupName, channelName)
	_, err = rmq.CreateConsumerGroup(groupName, channelName)
	assert.Nil(t, err)
	defer rmq.DestroyConsumerGroup(groupName, channelName)

	// Consume one message in each goroutine
	ct0 := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < entityNum; i++ {
		cMsgs, err := rmq.Consume(groupName, channelName, 1)
		assert.Nil(t, err)
		assert.Equal(t, len(cMsgs), 1)
	}
	ct1 := time.Now().UnixNano() / int64(time.Millisecond)
	cDuration := ct1 - ct0
	log.Printf("Total consume %d item, cost %v ms, throughout %v / s", entityNum, cDuration, int64(entityNum)*1000/cDuration)
}

func TestRocksMQ_MultiChan(t *testing.T) {
	etcdAddr := os.Getenv("ETCD_ADDRESS")
	if etcdAddr == "" {
		etcdAddr = "localhost:2379"
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(cli, "/etcd/test/root")
	defer etcdKV.Close()
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_multichan"
	defer os.RemoveAll(name)
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.Nil(t, err)

	channelName0 := "chan01"
	channelName1 := "chan11"
	err = rmq.CreateChannel(channelName0)
	assert.Nil(t, err)
	defer rmq.DestroyChannel(channelName0)
	err = rmq.CreateChannel(channelName1)
	assert.Nil(t, err)
	defer rmq.DestroyChannel(channelName1)
	assert.Nil(t, err)

	loopNum := 10
	for i := 0; i < loopNum; i++ {
		msg0 := "for_chann0_" + strconv.Itoa(i)
		msg1 := "for_chann1_" + strconv.Itoa(i)
		pMsg0 := ProducerMessage{payload: []byte(msg0)}
		pMsg1 := ProducerMessage{payload: []byte(msg1)}
		err = rmq.Produce(channelName0, []ProducerMessage{pMsg0})
		assert.Nil(t, err)
		err = rmq.Produce(channelName1, []ProducerMessage{pMsg1})
		assert.Nil(t, err)
	}

	groupName := "test_group"
	_ = rmq.DestroyConsumerGroup(groupName, channelName1)
	_, err = rmq.CreateConsumerGroup(groupName, channelName1)
	assert.Nil(t, err)
	cMsgs, err := rmq.Consume(groupName, channelName1, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), 1)
	assert.Equal(t, string(cMsgs[0].Payload), "for_chann1_"+strconv.Itoa(0))
}
