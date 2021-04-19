package main

import (
	"context"
	"github.com/czs007/suvlim/pulsar/schema"
	"github.com/czs007/suvlim/writer"
)

func GetInsertMsg(collectionName string, partitionTag string, entityId int64) *schema.InsertMsg {
	return &schema.InsertMsg{
		CollectionName: collectionName,
		PartitionTag:   partitionTag,
		SegmentId:      uint64(entityId / 100),
		EntityId:       int64(entityId),
		Timestamp:      uint64(entityId),
		ClientId:       0,
	}
}

func GetDeleteMsg(collectionName string, entityId int64) *schema.DeleteMsg {
	return &schema.DeleteMsg{
		CollectionName: collectionName,
		EntityId:       entityId,
		Timestamp:      uint64(entityId + 100),
	}
}

func main() {
	ctx := context.Background()
	var topics []string
	topics = append(topics, "test")
	topics = append(topics, "test1")
	writerNode, _ := writer.NewWriteNode(ctx, "null", topics, 0)
	var insertMsgs []*schema.InsertMsg
	for i := 0; i < 120; i++ {
		insertMsgs = append(insertMsgs, GetInsertMsg("collection0", "tag01", int64(i)))
	}
	//var wg sync.WaitGroup
	writerNode.InsertBatchData(ctx, insertMsgs, 100)
	data1 := writerNode.KvStore.GetData(ctx)
	gtInsertBuffer := writerNode.GetInsertBuffer()
	println(len(data1))
	println(gtInsertBuffer.Len())
	var insertMsgs2 []*schema.InsertMsg
	for i := 120; i < 200; i++ {
		insertMsgs2 = append(insertMsgs2, GetInsertMsg("collection0", "tag02", int64(i)))
	}
	writerNode.InsertBatchData(ctx, insertMsgs2, 200)
	data2 := writerNode.KvStore.GetData(ctx)
	println(len(data2))
	var deleteMsgs []*schema.DeleteMsg
	deleteMsgs = append(deleteMsgs, GetDeleteMsg("collection0", 2))
	deleteMsgs = append(deleteMsgs, GetDeleteMsg("collection0", 120))
	writerNode.DeleteBatchData(ctx, deleteMsgs, 200)
	data3 := writerNode.KvStore.GetData(ctx)
	println(len(data3))
}
