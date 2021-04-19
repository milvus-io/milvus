package main

import (
	"container/list"
	"fmt"
	"github.com/czs007/suvlim/pulsar/schema"
)

func GetInsertMsg(entityId int64) *schema.InsertMsg {
	return &schema.InsertMsg{
		CollectionName: "collection",
		PartitionTag:   "tag01",
		EntityId:       entityId,
		Timestamp:      uint64(entityId),
		ClientId:       0,
	}
}

func GetDeleteMsg(entityId int64) *schema.DeleteMsg {
	return &schema.DeleteMsg{
		CollectionName: "collection",
		EntityId:       entityId,
		Timestamp:      uint64(entityId + 100),
	}
}

//type example struct {
//	id int
//}
//
//type data struct {
//	buffer *list.List
//}

//func GetExample(num int) []*example {
//	var examples []*example
//	i := 0
//	for i = 0; i < num; i++ {
//		examples = append(examples, &example{id: i})
//	}
//	return examples
//}
//
//func GetValue(data *list.List, value []int) []int {
//	for e := data.Front(); e != nil; e = e.Next() {
//		value = append(value, e.Value.(*example).id)
//	}
//	return value
//}

func main() {
	//ctx := context.Background()
	deleteBuffer := list.New()
	//insertBuffer := list.New()
	deleteBuffer.PushBack(1)
	deleteBuffer.PushBack(2)
	var data []*list.Element
	for e := deleteBuffer.Front(); e != nil; e = e.Next() {
		if e.Value.(int) == 1 {
			data = append(data, e)
		}
	}
	fmt.Println(data[0].Value.(int))
	//writeNode := writer.NewWriteNode(
	//	ctx,
	//	"",
	//	)
	//a := make(map[string]in)
}
