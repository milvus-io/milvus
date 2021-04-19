package main

import (
	"context"
	"fmt"
	"github.com/czs007/suvlim/pulsar/schema"
	"github.com/czs007/suvlim/writer"
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

func main() {
	ctx := context.Background()
	writer, err := writer.NewWriteNode(ctx,
		"collection_tag01_seg01",
		100,
		"collection_tag01_seg02",
		200,
		0)
	if err != nil {
		fmt.Println("Can't create write node")
	}
	var data1 []*schema.InsertMsg
	var i int64
	for i = 0; i < 100; i++ {
		data1 = append(data1, GetInsertMsg(i))
	}
	writer.InsertBatchData(ctx, data1, 99)
	var data2 []*schema.InsertMsg
	for i = 100; i < 200; i++ {
		data2 = append(data2, GetInsertMsg(i))
	}
	writer.InsertBatchData(ctx, data2, 199)
	var deleteData []*schema.DeleteMsg
	for i = 0; i < 99; i++ {
		deleteData = append(deleteData, GetDeleteMsg(i))
	}
	for i = 100; i < 110; i++ {
		deleteData = append(deleteData, GetDeleteMsg(i))
	}
	writer.DeleteBatchData(ctx, deleteData, 110)
	kvMap := (*writer.KvStore).GetData(ctx)

	for k, v := range kvMap {
		fmt.Println(k + ":" + string(v))
	}

}
