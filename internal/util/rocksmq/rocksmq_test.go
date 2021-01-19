package rocksmq

import (
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
	idAllocator := master.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	name := "/tmp/rocksmq"
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
