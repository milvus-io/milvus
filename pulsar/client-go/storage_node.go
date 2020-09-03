package client_go

import (
	"fmt"
	"github.com/czs007/suvlim/pulsar/client-go/schema"
	"sync"
	"time"
)

var (
	consumerWSchema = "{\"type\":\"record\",\"name\":\"suvlim\",\"namespace\":\"pulsar\",\"fields\":[" +
		"{\"name\":\"MsgType\",\"type\":\"OpType\"}," +
		"]}"
)

type WriteNode struct {
	mc MessageClient
}

func (wn *WriteNode) doWriteNode(wg sync.WaitGroup) {
	wg.Add(2)
	go wn.insert_write(wn.mc.InsertMsg, wg)
	go wn.delete_write(wn.mc.DeleteMsg, wg)
	wg.Wait()
}

func (wn *WriteNode) PrepareBatchMsg() {
	wn.mc.PrepareBatchMsg(JobType(1))
}
func storageNodeMain() {

	mc := MessageClient{}
	topics := []string{"insert", "delete"}
	mc.InitClient("pulsar://localhost:6650", topics, consumerWSchema)

	go mc.ReceiveMessage()
	wg := sync.WaitGroup{}
	wn := WriteNode{mc}

	for {
		time.Sleep(200 * time.Millisecond)
		wn.PrepareBatchMsg()
		wn.doWriteNode(wg)
		fmt.Println("do a batch in 200ms")
	}
}

func (wn *WriteNode) insert_write(data []*schema.InsertMsg, wg sync.WaitGroup) schema.Status {
	wg.Done()
	return schema.Status{schema.ErrorCode_SUCCESS, ""}
}

func (wn *WriteNode) delete_write(data []*schema.DeleteMsg, wg sync.WaitGroup) schema.Status {
	wg.Done()
	return schema.Status{schema.ErrorCode_SUCCESS, ""}
}
