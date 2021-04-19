package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/czs007/suvlim/internal/conf"
	"github.com/czs007/suvlim/internal/storage"
	"github.com/czs007/suvlim/internal/msgclient"
	"github.com/czs007/suvlim/internal/writer"
	"log"
	"strconv"
)

func main() {
    var yamlFile string
	flag.StringVar(&yamlFile, "yaml", "", "yaml file")
	flag.Parse()
    // flag.Usage()
	fmt.Println("yaml file: ", yamlFile)
	conf.LoadConfig(yamlFile)

	pulsarAddr := "pulsar://"
	pulsarAddr += conf.Config.Pulsar.Address
	pulsarAddr += ":"
	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
	mc := msgclient.WriterMessageClient{}

	mc.InitClient(pulsarAddr)
	//TODO::close client / consumer/ producer

	mc.ReceiveMessage()
	ctx := context.Background()
	kv, err := storage.NewStore(ctx, conf.Config.Storage.Driver)
	// TODO:: if err != nil, should retry link
	if err != nil {
		log.Fatal(err)
	}

	msgCounter := writer.MsgCounter{
		InsertCounter: 0,
		DeleteCounter: 0,
	}

	wn := writer.WriteNode{
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
