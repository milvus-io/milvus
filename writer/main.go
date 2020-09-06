package main

import (
	"context"
	"fmt"
	"github.com/czs007/suvlim/storage/pkg"
	"github.com/czs007/suvlim/storage/pkg/types"
	"github.com/czs007/suvlim/writer/message_client"
	"github.com/czs007/suvlim/writer/write_node"
	"log"
	"sync"
	"time"
)

func main() {

	mc := message_client.MessageClient{}
	mc.InitClient("pulsar://localhost:6650")
	//mc.InitClient("pulsar://192.168.2.18:6650")
	//TODO::close client / consumer/ producer
	//mc.Close()

	go mc.ReceiveMessage()
	wg := sync.WaitGroup{}

	kv, err := storage.NewStore(context.Background(), types.MinIODriver)
	if err != nil {
		log.Fatal(err)
	}

	wn := write_node.WriteNode{
		KvStore:       &kv,
		MessageClient: &mc,
		TimeSync:      100,
	}

	ctx := context.Background()
	for {
		time.Sleep(200 * time.Millisecond)
		msgLength := wn.MessageClient.PrepareBatchMsg()
		readyDo := false
		for _, len := range msgLength {
			if len > 0 { readyDo = true }
		}
		if readyDo {
			wn.DoWriteNode(ctx, 100, &wg)
			fmt.Println("write node do a batch message, storage len: ")
		}
		fmt.Println("do a batch in 200ms")
	}
}
