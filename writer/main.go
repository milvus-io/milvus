package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
	"writer/message_client"
	"writer/mock"
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

func main() {

	mc := message_client.MessageClient{}
	mc.InitClient("pulsar://localhost:6650")
	//TODO::close client / consumer/ producer
	//mc.Close()

	go mc.ReceiveMessage()
	wg := sync.WaitGroup{}

	kv, err := mock.NewTikvStore()
	if err != nil {
		log.Fatal(err)
	}

	wn := write_node.WriteNode{
		KvStore:       kv,
		MessageClient: &mc,
		TimeSync:      100,
	}

	ctx := context.Background()
	for {
		time.Sleep(200 * time.Millisecond)
		msgLength := wn.MessageClient.PrepareBatchMsg()
		readyDo := true
		for _, len := range msgLength {
			if len <= 0 { readyDo = false }
		}
		if readyDo {
			wn.DoWriteNode(ctx, 100, wg)
		}
		fmt.Println("do a batch in 200ms")
	}
}
