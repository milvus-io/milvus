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

	msgCounter := write_node.MsgCounter{
		InsertCounter: 0,
		DeleteCounter: 0,
	}

	wn := write_node.WriteNode{
		KvStore:       &kv,
		MessageClient: &mc,
		TimeSync:      100,
		MsgCounter:    &msgCounter,
	}

	const Debug = true
	const CountMsgNum = 1000 * 1000

	if Debug {
		var printFlag = true
		var startTime = true
		var start time.Time

		for {
			if ctx.Err() != nil {
				break
			}
			msgLength := wn.MessageClient.PrepareBatchMsg()
			if msgLength > 0 {
				if startTime {
					fmt.Println("============> Start Test <============")
					startTime = false
					start = time.Now()
				}

				wn.DoWriteNode(ctx, &wg)
				fmt.Println("write node do a batch message, storage len: ", msgLength)
			}

			// Test insert time
			if printFlag && wn.MsgCounter.InsertCounter >= CountMsgNum {
				printFlag = false
				timeSince := time.Since(start)
				fmt.Println("============> Do", wn.MsgCounter.InsertCounter, "Insert in", timeSince, "<============")
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
