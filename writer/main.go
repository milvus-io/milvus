package main

import (
	"context"
	"fmt"
	"github.com/czs007/suvlim/conf"
	"github.com/czs007/suvlim/storage/pkg"
	"github.com/czs007/suvlim/writer/message_client"
	"github.com/czs007/suvlim/writer/write_node"
	"log"
	"sync"
	"time"
)

func main() {

	mc := message_client.MessageClient{}
	mc.InitClient("pulsar://localhost:6650")
	//TODO::close client / consumer/ producer
	//mc.Close()

	go mc.ReceiveMessage()
	wg := sync.WaitGroup{}
	ctx := context.Background()
	kv, err := storage.NewStore(ctx, conf.Config.Storage.Driver)
	// if err != nil, should retry link
	if err != nil {
		log.Fatal(err)
	}

	wn := write_node.WriteNode{
		KvStore:       &kv,
		MessageClient: &mc,
		TimeSync:      100,
	}

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
