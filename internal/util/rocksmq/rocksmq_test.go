package rocksmq

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	master "github.com/zilliztech/milvus-distributed/internal/master"
	"go.etcd.io/etcd/clientv3"
)

func TestFixChannelName(t *testing.T) {
	name := "abcd"
	fixName, err := fixChannelName(name)
	assert.Nil(t, err)
	assert.Equal(t, len(fixName), FixedChannelNameLen)
}

func TestRocksMQ(t *testing.T) {
	master.Init()

	etcdAddr := master.Params.EtcdAddress
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(cli, "/etcd/test/root")
	defer etcdKV.Close()
	idAllocator := master.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq"
	_ = os.RemoveAll(name)
	defer os.RemoveAll(name)
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.Nil(t, err)

	channelName := "channel_a"
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

	groupName := "query_node"
	_ = rmq.DestroyConsumerGroup(groupName, channelName)
	err = rmq.CreateConsumerGroup(groupName, channelName)
	assert.Nil(t, err)
	cMsgs, err := rmq.Consume(groupName, channelName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), 1)
	assert.Equal(t, string(cMsgs[0].payload), "a_message")

	cMsgs, err = rmq.Consume(groupName, channelName, 2)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), 2)
	assert.Equal(t, string(cMsgs[0].payload), "b_message")
	assert.Equal(t, string(cMsgs[1].payload), "c_message")
}

func TestRocksMQ_Loop(t *testing.T) {
	master.Init()

	etcdAddr := master.Params.EtcdAddress
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(cli, "/etcd/test/root")
	defer etcdKV.Close()
	idAllocator := master.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq_1"
	_ = os.RemoveAll(name)
	defer os.RemoveAll(name)
	rmq, err := NewRocksMQ(name, idAllocator)
	assert.Nil(t, err)

	loopNum := 100
	channelName := "channel_test"
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
	err = rmq.CreateConsumerGroup(groupName, channelName)
	assert.Nil(t, err)
	cMsgs, err := rmq.Consume(groupName, channelName, loopNum)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), loopNum)
	assert.Equal(t, string(cMsgs[0].payload), "message_"+strconv.Itoa(0))
	assert.Equal(t, string(cMsgs[loopNum-1].payload), "message_"+strconv.Itoa(loopNum-1))

	// Consume one message once
	for i := 0; i < loopNum; i++ {
		oneMsgs, err := rmq.Consume(groupName, channelName, 1)
		assert.Nil(t, err)
		assert.Equal(t, len(oneMsgs), 1)
		assert.Equal(t, string(oneMsgs[0].payload), "message_"+strconv.Itoa(i+loopNum))
	}

	cMsgs, err = rmq.Consume(groupName, channelName, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(cMsgs), 0)
}

//func TestRocksMQ_Goroutines(t *testing.T) {
//	master.Init()
//
//	etcdAddr := master.Params.EtcdAddress
//	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
//	assert.Nil(t, err)
//	etcdKV := etcdkv.NewEtcdKV(cli, "/etcd/test/root")
//	defer etcdKV.Close()
//	idAllocator := master.NewGlobalIDAllocator("dummy", etcdKV)
//	_ = idAllocator.Initialize()
//
//	name := "/tmp/rocksmq"
//	defer os.RemoveAll(name)
//	rmq, err := NewRocksMQ(name, idAllocator)
//	assert.Nil(t, err)
//
//	loopNum := 100
//	channelName := "channel_test"
//	// Produce two message in each goroutine
//	var wg sync.WaitGroup
//	wg.Add(1)
//	for i := 0; i < loopNum/2; i++ {
//		go func() {
//			wg.Add(2)
//			msg_0 := "message_" + strconv.Itoa(i)
//			msg_1 := "message_" + strconv.Itoa(i+1)
//			pMsg_0 := ProducerMessage{payload: []byte(msg_0)}
//			pMsg_1 := ProducerMessage{payload: []byte(msg_1)}
//			pMsgs := make([]ProducerMessage, 2)
//			pMsgs[0] = pMsg_0
//			pMsgs[1] = pMsg_1
//
//			err := rmq.Produce(channelName, pMsgs)
//			assert.Nil(t, err)
//		}()
//	}
//
//	groupName := "test_group"
//	_ = rmq.DestroyConsumerGroup(groupName, channelName)
//	err = rmq.CreateConsumerGroup(groupName, channelName)
//	assert.Nil(t, err)
//	// Consume one message in each goroutine
//	for i := 0; i < loopNum; i++ {
//		go func() {
//			wg.Done()
//			cMsgs, err := rmq.Consume(groupName, channelName, 1)
//			fmt.Println(string(cMsgs[0].payload))
//			assert.Nil(t, err)
//			assert.Equal(t, len(cMsgs), 1)
//		}()
//	}
//	wg.Done()
//	wg.Wait()
//}
