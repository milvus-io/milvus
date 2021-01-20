package pulsarms

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/util"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

var Params paramtable.BaseTable

func TestMain(m *testing.M) {
	Params.Init()
	exitCode := m.Run()
	os.Exit(exitCode)
}

func initPulsarStream(pulsarAddress string,
	producerChannels []string,
	consumerChannels []string,
	consumerSubName string,
	opts ...msgstream.RepackFunc) (*msgstream.MsgStream, *msgstream.MsgStream) {

	// set input stream
	inputStream := NewPulsarMsgStream(context.Background(), 100)
	inputStream.SetPulsarClient(pulsarAddress)
	inputStream.CreatePulsarProducers(producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	inputStream.Start()
	var input msgstream.MsgStream = inputStream

	// set output stream
	outputStream := NewPulsarMsgStream(context.Background(), 100)
	outputStream.SetPulsarClient(pulsarAddress)
	unmarshalDispatcher := util.NewUnmarshalDispatcher()
	outputStream.CreatePulsarConsumers(consumerChannels, consumerSubName, unmarshalDispatcher, 100)
	outputStream.Start()
	var output msgstream.MsgStream = outputStream

	return &input, &output
}

func initPulsarTtStream(pulsarAddress string,
	producerChannels []string,
	consumerChannels []string,
	consumerSubName string,
	opts ...msgstream.RepackFunc) (*msgstream.MsgStream, *msgstream.MsgStream) {

	// set input stream
	inputStream := NewPulsarMsgStream(context.Background(), 100)
	inputStream.SetPulsarClient(pulsarAddress)
	inputStream.CreatePulsarProducers(producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	inputStream.Start()
	var input msgstream.MsgStream = inputStream

	// set output stream
	outputStream := NewPulsarTtMsgStream(context.Background(), 100)
	outputStream.SetPulsarClient(pulsarAddress)
	unmarshalDispatcher := util.NewUnmarshalDispatcher()
	outputStream.CreatePulsarConsumers(consumerChannels, consumerSubName, unmarshalDispatcher, 100)
	outputStream.Start()
	var output msgstream.MsgStream = outputStream

	return &input, &output
}

func receiveMsg(outputStream *msgstream.MsgStream, msgCount int) {
	receiveCount := 0
	for {
		result := (*outputStream).Consume()
		if len(result.Msgs) > 0 {
			msgs := result.Msgs
			for _, v := range msgs {
				receiveCount++
				fmt.Println("msg type: ", v.Type(), ", msg value: ", v)
			}
		}
		if receiveCount >= msgCount {
			break
		}
	}
}

func TestStream_PulsarMsgStream_Insert(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, util.GetTsMsg(commonpb.MsgType_kInsert, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, util.GetTsMsg(commonpb.MsgType_kInsert, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
	err := (*inputStream).Produce(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}

	receiveMsg(outputStream, len(msgPack.Msgs))
	(*inputStream).Close()
	(*outputStream).Close()

}

func TestStream_PulsarMsgStream_Delete(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	producerChannels := []string{"delete"}
	consumerChannels := []string{"delete"}
	consumerSubName := "subDelete"

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, util.GetTsMsg(commonpb.MsgType_kDelete, 1, 1))
	//msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kDelete, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
	err := (*inputStream).Produce(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack.Msgs))
	(*inputStream).Close()
	(*outputStream).Close()
}

func TestStream_PulsarMsgStream_Search(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	producerChannels := []string{"search"}
	consumerChannels := []string{"search"}
	consumerSubName := "subSearch"

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, util.GetTsMsg(commonpb.MsgType_kSearch, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, util.GetTsMsg(commonpb.MsgType_kSearch, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
	err := (*inputStream).Produce(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack.Msgs))
	(*inputStream).Close()
	(*outputStream).Close()
}

func TestStream_PulsarMsgStream_SearchResult(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	producerChannels := []string{"searchResult"}
	consumerChannels := []string{"searchResult"}
	consumerSubName := "subSearchResult"

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, util.GetTsMsg(commonpb.MsgType_kSearchResult, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, util.GetTsMsg(commonpb.MsgType_kSearchResult, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
	err := (*inputStream).Produce(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack.Msgs))
	(*inputStream).Close()
	(*outputStream).Close()
}

func TestStream_PulsarMsgStream_TimeTick(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	producerChannels := []string{"timeTick"}
	consumerChannels := []string{"timeTick"}
	consumerSubName := "subTimeTick"

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, util.GetTsMsg(commonpb.MsgType_kTimeTick, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, util.GetTsMsg(commonpb.MsgType_kTimeTick, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
	err := (*inputStream).Produce(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack.Msgs))
	(*inputStream).Close()
	(*outputStream).Close()
}

func TestStream_PulsarMsgStream_BroadCast(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, util.GetTsMsg(commonpb.MsgType_kTimeTick, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, util.GetTsMsg(commonpb.MsgType_kTimeTick, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
	err := (*inputStream).Broadcast(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(consumerChannels)*len(msgPack.Msgs))
	(*inputStream).Close()
	(*outputStream).Close()
}

func TestStream_PulsarMsgStream_RepackFunc(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, util.GetTsMsg(commonpb.MsgType_kInsert, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, util.GetTsMsg(commonpb.MsgType_kInsert, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, util.RepackFunc)
	err := (*inputStream).Produce(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack.Msgs))
	(*inputStream).Close()
	(*outputStream).Close()
}

func TestStream_PulsarMsgStream_InsertRepackFunc(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	baseMsg := msgstream.BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{1, 3},
	}

	insertRequest := internalpb2.InsertRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kInsert,
			MsgID:     1,
			Timestamp: 1,
			SourceID:  1,
		},
		CollectionName: "Collection",
		PartitionName:  "Partition",
		SegmentID:      1,
		ChannelID:      "1",
		Timestamps:     []msgstream.Timestamp{1, 1},
		RowIDs:         []int64{1, 3},
		RowData:        []*commonpb.Blob{{}, {}},
	}
	insertMsg := &msgstream.InsertMsg{
		BaseMsg:       baseMsg,
		InsertRequest: insertRequest,
	}

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, insertMsg)

	inputStream := NewPulsarMsgStream(context.Background(), 100)
	inputStream.SetPulsarClient(pulsarAddress)
	inputStream.CreatePulsarProducers(producerChannels)
	inputStream.Start()

	outputStream := NewPulsarMsgStream(context.Background(), 100)
	outputStream.SetPulsarClient(pulsarAddress)
	unmarshalDispatcher := util.NewUnmarshalDispatcher()
	outputStream.CreatePulsarConsumers(consumerChannels, consumerSubName, unmarshalDispatcher, 100)
	outputStream.Start()
	var output msgstream.MsgStream = outputStream

	err := (*inputStream).Produce(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(&output, len(msgPack.Msgs)*2)
	(*inputStream).Close()
	(*outputStream).Close()
}

func TestStream_PulsarMsgStream_DeleteRepackFunc(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	baseMsg := msgstream.BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{1, 3},
	}

	deleteRequest := internalpb2.DeleteRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kDelete,
			MsgID:     1,
			Timestamp: 1,
			SourceID:  1,
		},
		CollectionName: "Collection",
		ChannelID:      "1",
		Timestamps:     []msgstream.Timestamp{1, 1},
		PrimaryKeys:    []int64{1, 3},
	}
	deleteMsg := &msgstream.DeleteMsg{
		BaseMsg:       baseMsg,
		DeleteRequest: deleteRequest,
	}

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, deleteMsg)

	inputStream := NewPulsarMsgStream(context.Background(), 100)
	inputStream.SetPulsarClient(pulsarAddress)
	inputStream.CreatePulsarProducers(producerChannels)
	inputStream.Start()

	outputStream := NewPulsarMsgStream(context.Background(), 100)
	outputStream.SetPulsarClient(pulsarAddress)
	unmarshalDispatcher := util.NewUnmarshalDispatcher()
	outputStream.CreatePulsarConsumers(consumerChannels, consumerSubName, unmarshalDispatcher, 100)
	outputStream.Start()
	var output msgstream.MsgStream = outputStream

	err := (*inputStream).Produce(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(&output, len(msgPack.Msgs)*2)
	(*inputStream).Close()
	(*outputStream).Close()
}

func TestStream_PulsarMsgStream_DefaultRepackFunc(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, util.GetTsMsg(commonpb.MsgType_kTimeTick, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, util.GetTsMsg(commonpb.MsgType_kSearch, 2, 2))
	msgPack.Msgs = append(msgPack.Msgs, util.GetTsMsg(commonpb.MsgType_kSearchResult, 3, 3))
	msgPack.Msgs = append(msgPack.Msgs, util.GetTsMsg(commonpb.MsgType_kQueryNodeStats, 4, 4))

	inputStream := NewPulsarMsgStream(context.Background(), 100)
	inputStream.SetPulsarClient(pulsarAddress)
	inputStream.CreatePulsarProducers(producerChannels)
	inputStream.Start()

	outputStream := NewPulsarMsgStream(context.Background(), 100)
	outputStream.SetPulsarClient(pulsarAddress)
	unmarshalDispatcher := util.NewUnmarshalDispatcher()
	outputStream.CreatePulsarConsumers(consumerChannels, consumerSubName, unmarshalDispatcher, 100)
	outputStream.Start()
	var output msgstream.MsgStream = outputStream

	err := (*inputStream).Produce(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(&output, len(msgPack.Msgs))
	(*inputStream).Close()
	(*outputStream).Close()
}

func TestStream_PulsarTtMsgStream_Insert(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack0 := msgstream.MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, util.GetTimeTickMsg(0, 0, 0))

	msgPack1 := msgstream.MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, util.GetTsMsg(commonpb.MsgType_kInsert, 1, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, util.GetTsMsg(commonpb.MsgType_kInsert, 3, 3))

	msgPack2 := msgstream.MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, util.GetTimeTickMsg(5, 5, 5))

	inputStream, outputStream := initPulsarTtStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
	err := (*inputStream).Broadcast(&msgPack0)
	if err != nil {
		log.Fatalf("broadcast error = %v", err)
	}
	err = (*inputStream).Produce(&msgPack1)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	err = (*inputStream).Broadcast(&msgPack2)
	if err != nil {
		log.Fatalf("broadcast error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack1.Msgs))
	(*inputStream).Close()
	(*outputStream).Close()
}

func TestStream_PulsarTtMsgStream_UnMarshalHeader(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack0 := msgstream.MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, util.GetTimeTickMsg(0, 0, 0))

	msgPack1 := msgstream.MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, util.GetTsMsg(commonpb.MsgType_kInsert, 1, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, util.GetTsMsg(commonpb.MsgType_kInsert, 3, 3))

	msgPack2 := msgstream.MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, util.GetTimeTickMsg(5, 5, 5))

	inputStream, outputStream := initPulsarTtStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
	err := (*inputStream).Broadcast(&msgPack0)
	if err != nil {
		log.Fatalf("broadcast error = %v", err)
	}
	err = (*inputStream).Produce(&msgPack1)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	err = (*inputStream).Broadcast(&msgPack2)
	if err != nil {
		log.Fatalf("broadcast error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack1.Msgs))
	(*inputStream).Close()
	(*outputStream).Close()
}
