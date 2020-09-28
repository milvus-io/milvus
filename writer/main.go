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
		//var shouldBenchmark = false
		//var start time.Time
		//var LogRecords int64
		//var logFlag int64
		//var logString = ""
		//logFile, err := os.OpenFile("writenode.benchmark", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
		//defer logFile.Close()
		//if err != nil {
		//	log.Fatalf("Prepare log file error, " + err.Error())
		//}

		const CountInsertMsgBaseline = 1000 * 1000
		var BaselineCounter int64 = 0

		for {
			if ctx.Err() != nil {
				break
			}
			msgLength := wn.MessageClient.PrepareBatchMsg()
			// wait until first 100,000 rows are successfully wrote
			//if wn.MsgCounter.InsertCounter >= CountMsgNum && shouldBenchmark == false {
			//	shouldBenchmark = true
			//	wn.MsgCounter.InsertCounter = 0
			//	wn.MsgCounter.InsertedRecordSize = 0
			//	start = time.Now()
			//}

			if wn.MsgCounter.InsertCounter/CountInsertMsgBaseline != BaselineCounter {
				wn.WriteWriterLog()
				BaselineCounter = wn.MsgCounter.InsertCounter/CountInsertMsgBaseline
			}

			if msgLength > 0 {
				wn.DoWriteNode(ctx)
				fmt.Println("write node do a batch message, storage len: ", msgLength)
			}

			// Test insert time
			// ignore if less than 1000 records per time interval
			//if shouldBenchmark && wn.MsgCounter.InsertCounter > 1000 {
			//	LogRecords += msgCounter.InsertCounter
			//	timeSince := time.Since(start)
			//	if timeSince >= timeInterval {
			//		speed := wn.MsgCounter.InsertedRecordSize / timeInterval.Seconds() / MB
			//		logString = fmt.Sprintln("============> Insert", wn.MsgCounter.InsertCounter, "records, cost:", timeSince, "speed:", speed, "M/s", "<============")
			//		newFlag := LogRecords / (10000 * 100)
			//		if newFlag != logFlag {
			//			logFlag = newFlag
			//			fmt.Fprintln(logFile, logString)
			//			logString = ""
			//		}
			//		wn.MsgCounter.InsertCounter = 0
			//		wn.MsgCounter.InsertedRecordSize = 0
			//		start = time.Now()
			//	}
			//}
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
