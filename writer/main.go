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
	const MB = 1024 * 1024
	const timeInterval = time.Second * 5
	const CountMsgNum = 10000 * 10

	if Debug {
		var shouldBenchmark = false
		var start time.Time

		for {
			if ctx.Err() != nil {
				break
			}
			msgLength := wn.MessageClient.PrepareBatchMsg()
			// wait until first 100,000 rows are successfully wrote
			if wn.MsgCounter.InsertCounter >= CountMsgNum {
				shouldBenchmark = true
				start = time.Now()
			}
			if msgLength > 0 {
				wn.DoWriteNode(ctx, &wg)
				fmt.Println("write node do a batch message, storage len: ", msgLength)
			}
			// Test insert time
			// ignore if less than 1000 records per time interval
			if shouldBenchmark && wn.MsgCounter.InsertCounter > 1000{
				timeSince := time.Since(start)
				if timeSince >= timeInterval {
					speed := wn.MsgCounter.InsertedRecordSize / timeInterval.Seconds() / MB
					fmt.Println("============> Insert", wn.MsgCounter.InsertCounter,"records, cost:" , timeSince, "speed:", speed, "M/s", "<============")
					wn.MsgCounter.InsertCounter = 0
					start = time.Now()
				}
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
