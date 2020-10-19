package test

import (
	"context"
	msgpb "github.com/zilliztech/milvus-distributed/internal/proto/message"
	"github.com/zilliztech/milvus-distributed/internal/writer"
	"sync"
	"testing"
)

func GetInsertMsg(collectionName string, partitionTag string, entityId int64) *msgpb.InsertOrDeleteMsg {
	return &msgpb.InsertOrDeleteMsg{
		CollectionName: collectionName,
		PartitionTag:   partitionTag,
		SegmentId:      int64(entityId / 100),
		Uid:       		int64(entityId),
		Timestamp:      uint64(entityId),
		ClientId:       0,
	}
}

func GetDeleteMsg(collectionName string, entityId int64) *msgpb.InsertOrDeleteMsg {
	return &msgpb.InsertOrDeleteMsg{
		CollectionName: collectionName,
		Uid:       		entityId,
		Timestamp:      uint64(entityId + 100),
	}
}

func TestInsert(t *testing.T) {
	// TODO: fix test
	return
	ctx := context.Background()
	var topics []string
	topics = append(topics, "test")
	topics = append(topics, "test1")
	writerNode, _ := writer.NewWriteNode(ctx, "null", topics, 0)
	var insertMsgs []*msgpb.InsertOrDeleteMsg
	for i := 0; i < 120; i++ {
		insertMsgs = append(insertMsgs, GetInsertMsg("collection0", "tag01", int64(i)))
	}
	wg := sync.WaitGroup{}
	wg.Add(3)
	//var wg sync.WaitGroup
	writerNode.InsertBatchData(ctx, insertMsgs, &wg)
	var insertMsgs2 []*msgpb.InsertOrDeleteMsg
	for i := 120; i < 200; i++ {
		insertMsgs2 = append(insertMsgs2, GetInsertMsg("collection0", "tag02", int64(i)))
	}
	writerNode.InsertBatchData(ctx, insertMsgs2, &wg)
	var deleteMsgs []*msgpb.InsertOrDeleteMsg
	deleteMsgs = append(deleteMsgs, GetDeleteMsg("collection0", 2))
	deleteMsgs = append(deleteMsgs, GetDeleteMsg("collection0", 120))
	writerNode.DeleteBatchData(ctx, deleteMsgs)
}
