package pulsar

import (
	"fmt"
	"suvlim/pulsar/schema"
	"sync"
	"time"
)

type QueryNode struct {
	mc MessageClient
}

func (qn *QueryNode)doQueryNode(wg sync.WaitGroup) {
	wg.Add(3)
	go qn.insert_query(qn.mc.InsertMsg, wg)
	go qn.delete_query(qn.mc.DeleteMsg, wg)
	go qn.search_query(qn.mc.SearchMsg, wg)
	wg.Wait()
}


func (qn *QueryNode) PrepareBatchMsg() {
	qn.mc.PrepareBatchMsg(JobType(0))
}
func main() {

	mc := MessageClient{}
	topics := []string{"insert", "delete"}
	mc.InitClient("pulsar://localhost:6650", topics)

	go mc.ReceiveMessage()

	qn := QueryNode{mc}

	for {
		time.Sleep(200 * time.Millisecond)
		qn.PrepareBatchMsg()
		qn.doQueryNode(wg)
		fmt.Println("do a batch in 200ms")
	}
}

func (qn *QueryNode) insert_query(data []*schema.InsertMsg, wg sync.WaitGroup) schema.Status{
	wg.Done()
	return schema.Status{schema.ErrorCode_SUCCESS, ""}
}

func (qn *QueryNode) delete_query(data []*schema.DeleteMsg, wg sync.WaitGroup) schema.Status{
	wg.Done()
	return schema.Status{schema.ErrorCode_SUCCESS, ""}
}

func (qn *QueryNode) search_query(data []*schema.SearchMsg, wg sync.WaitGroup) schema.Status{
	wg.Done()
	return schema.Status{schema.ErrorCode_SUCCESS, ""}
}



