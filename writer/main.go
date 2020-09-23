package main

import (
	"context"
	"fmt"
	"github.com/czs007/suvlim/conf"
	storage "github.com/czs007/suvlim/storage/pkg"
	"github.com/czs007/suvlim/writer/message_client"
	"github.com/czs007/suvlim/writer/write_node"
	"log"
	"strconv"
	"sync"
	"time"
)

func main() {
	pulsarAddr := "pulsar://"
	pulsarAddr += conf.Config.Pulsar.Address
	pulsarAddr += ":"
	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
	mc := message_client.MessageClient{}

	mc.InitClient(pulsarAddr)
	//TODO::close client / consumer/ producer

	mc.ReceiveMessage()
	wg := sync.WaitGroup{}
	ctx := context.Background()
	kv, err := storage.NewStore(ctx, conf.Config.Storage.Driver)
	// TODO:: if err != nil, should retry link
	if err != nil {
		log.Fatal(err)
	}

	wn := write_node.WriteNode{
		KvStore:       &kv,
		MessageClient: &mc,
		TimeSync:      100,
	}

	const Debug = true
	const CountMsgNum = 1000 * 1000

	if Debug {
		var start = time.Now()
		var printFlag = true

		for {
			// Test insert time
			if printFlag && wn.MsgCounter.InsertCounter >= CountMsgNum {
				printFlag = false
				timeSince := time.Since(start)
				fmt.Println("============> Do", wn.MsgCounter.InsertCounter, "Insert in", timeSince, "<============")
			}

			if ctx.Err() != nil {
				break
			}
			msgLength := wn.MessageClient.PrepareBatchMsg()
			if msgLength > 0 {
				wn.DoWriteNode(ctx, &wg)
				fmt.Println("write node do a batch message, storage len: ", msgLength)
			}
		}
	}

	//TODO:: start a gorouter for searchById
	for {
		if ctx.Err() != nil {
			break
		}
		msgLength := wn.MessageClient.PrepareBatchMsg()
		if msgLength > 0 {
			wn.DoWriteNode(ctx, &wg)
			fmt.Println("write node do a batch message, storage len: ", msgLength)
		}
	}
	wn.Close()
}
