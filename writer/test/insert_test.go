package test

import (
	"context"
	"sync"
	"testing"
	"writer/pb"
	"writer/write_node"
)

func GetInsertMsg(collectionName string, partitionTag string, entityId int64) *pb.InsertOrDeleteMsg {
	return &pb.InsertOrDeleteMsg{
		CollectionName: collectionName,
		PartitionTag:   partitionTag,
		SegmentId:      int64(entityId / 100),
		Uid:       		int64(entityId),
		Timestamp:      int64(entityId),
		ClientId:       0,
	}
}

func GetDeleteMsg(collectionName string, entityId int64) *pb.InsertOrDeleteMsg {
	return &pb.InsertOrDeleteMsg{
		CollectionName: collectionName,
		Uid:       		entityId,
		Timestamp:      int64(entityId + 100),
	}
}

func TestInsert(t *testing.T) {
	ctx := context.Background()
	var topics []string
	topics = append(topics, "test")
	topics = append(topics, "test1")
	writerNode, _ := write_node.NewWriteNode(ctx, "null", topics, 0)
	var insertMsgs []*pb.InsertOrDeleteMsg
	for i := 0; i < 120; i++ {
		insertMsgs = append(insertMsgs, GetInsertMsg("collection0", "tag01", int64(i)))
	}
	wg := sync.WaitGroup{}
	wg.Add(3)
	//var wg sync.WaitGroup
	writerNode.InsertBatchData(ctx, insertMsgs, wg)
	data1 := writerNode.KvStore.GetData(ctx)
	//gtInsertBuffer := writerNode.GetInsertBuffer()
	println(len(data1))

	var insertMsgs2 []*pb.InsertOrDeleteMsg
	for i := 120; i < 200; i++ {
		insertMsgs2 = append(insertMsgs2, GetInsertMsg("collection0", "tag02", int64(i)))
	}
	writerNode.InsertBatchData(ctx, insertMsgs2, wg)
	data2 := writerNode.KvStore.GetData(ctx)
	println(len(data2))
	var deleteMsgs []*pb.InsertOrDeleteMsg
	deleteMsgs = append(deleteMsgs, GetDeleteMsg("collection0", 2))
	deleteMsgs = append(deleteMsgs, GetDeleteMsg("collection0", 120))
	writerNode.DeleteBatchData(ctx, deleteMsgs, wg)
	data3 := writerNode.KvStore.GetData(ctx)
	println(len(data3))
}
