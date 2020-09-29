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

	if Debug {
		const CountInsertMsgBaseline = 1000 * 1000
		var BaselineCounter int64 = 0

		for {
			if ctx.Err() != nil {
				break
			}
			msgLength := wn.MessageClient.PrepareBatchMsg()

			if wn.MsgCounter.InsertCounter/CountInsertMsgBaseline != BaselineCounter {
				wn.WriteWriterLog()
				BaselineCounter = wn.MsgCounter.InsertCounter/CountInsertMsgBaseline
			}

			if msgLength > 0 {
				wn.DoWriteNode(ctx)
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
			wn.DoWriteNode(ctx)
			fmt.Println("write node do a batch message, storage len: ", msgLength)
		}
	}
	wn.Close()
}
