package msgstream

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

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

func repackFunc(msgs []TsMsg, hashKeys [][]int32) (map[int32]*MsgPack, error) {
	result := make(map[int32]*MsgPack)
	for i, request := range msgs {
		keys := hashKeys[i]
		for _, channelID := range keys {
			_, ok := result[channelID]
			if ok == false {
				msgPack := MsgPack{}
				result[channelID] = &msgPack
			}
			result[channelID].Msgs = append(result[channelID].Msgs, request)
		}
	}
	return result, nil
}

func getTsMsg(msgType MsgType, reqID UniqueID, hashValue uint32) TsMsg {
	baseMsg := BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{hashValue},
	}
	switch msgType {
	case commonpb.MsgType_kInsert:
		insertRequest := internalpb2.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kInsert,
				MsgID:     reqID,
				Timestamp: 11,
				SourceID:  reqID,
			},
			CollectionName: "Collection",
			PartitionName:  "Partition",
			SegmentID:      1,
			ChannelID:      "0",
			Timestamps:     []Timestamp{1},
			RowIDs:         []int64{1},
			RowData:        []*commonpb.Blob{{}},
		}
		insertMsg := &InsertMsg{
			BaseMsg:       baseMsg,
			InsertRequest: insertRequest,
		}
		return insertMsg
	case commonpb.MsgType_kDelete:
		deleteRequest := internalpb2.DeleteRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kDelete,
				MsgID:     reqID,
				Timestamp: 11,
				SourceID:  reqID,
			},
			CollectionName: "Collection",
			ChannelID:      "1",
			Timestamps:     []Timestamp{1},
			PrimaryKeys:    []IntPrimaryKey{1},
		}
		deleteMsg := &DeleteMsg{
			BaseMsg:       baseMsg,
			DeleteRequest: deleteRequest,
		}
		return deleteMsg
	case commonpb.MsgType_kSearch:
		searchRequest := internalpb2.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kSearch,
				MsgID:     reqID,
				Timestamp: 11,
				SourceID:  reqID,
			},
			Query:           nil,
			ResultChannelID: "0",
		}
		searchMsg := &SearchMsg{
			BaseMsg:       baseMsg,
			SearchRequest: searchRequest,
		}
		return searchMsg
	case commonpb.MsgType_kSearchResult:
		searchResult := internalpb2.SearchResults{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kSearchResult,
				MsgID:     reqID,
				Timestamp: 1,
				SourceID:  reqID,
			},
			Status:          &commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS},
			ResultChannelID: "0",
		}
		searchResultMsg := &SearchResultMsg{
			BaseMsg:       baseMsg,
			SearchResults: searchResult,
		}
		return searchResultMsg
	case commonpb.MsgType_kTimeTick:
		timeTickResult := internalpb2.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kTimeTick,
				MsgID:     reqID,
				Timestamp: 1,
				SourceID:  reqID,
			},
		}
		timeTickMsg := &TimeTickMsg{
			BaseMsg:     baseMsg,
			TimeTickMsg: timeTickResult,
		}
		return timeTickMsg
	case commonpb.MsgType_kQueryNodeStats:
		queryNodeSegStats := internalpb2.QueryNodeStats{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_kQueryNodeStats,
				SourceID: reqID,
			},
		}
		queryNodeSegStatsMsg := &QueryNodeStatsMsg{
			BaseMsg:        baseMsg,
			QueryNodeStats: queryNodeSegStats,
		}
		return queryNodeSegStatsMsg
	}
	return nil
}

func getTimeTickMsg(reqID UniqueID, hashValue uint32, time uint64) TsMsg {
	baseMsg := BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{hashValue},
	}
	timeTickResult := internalpb2.TimeTickMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kTimeTick,
			MsgID:     reqID,
			Timestamp: time,
			SourceID:  reqID,
		},
	}
	timeTickMsg := &TimeTickMsg{
		BaseMsg:     baseMsg,
		TimeTickMsg: timeTickResult,
	}
	return timeTickMsg
}

func initPulsarStream(pulsarAddress string,
	producerChannels []string,
	consumerChannels []string,
	consumerSubName string,
	opts ...RepackFunc) (*MsgStream, *MsgStream) {

	// set input stream
	inputStream := NewPulsarMsgStream(context.Background(), 100)
	inputStream.SetPulsarClient(pulsarAddress)
	inputStream.CreatePulsarProducers(producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	inputStream.Start()
	var input MsgStream = inputStream

	// set output stream
	outputStream := NewPulsarMsgStream(context.Background(), 100)
	outputStream.SetPulsarClient(pulsarAddress)
	unmarshalDispatcher := NewUnmarshalDispatcher()
	outputStream.CreatePulsarConsumers(consumerChannels, consumerSubName, unmarshalDispatcher, 100)
	outputStream.Start()
	var output MsgStream = outputStream

	return &input, &output
}

func initPulsarTtStream(pulsarAddress string,
	producerChannels []string,
	consumerChannels []string,
	consumerSubName string,
	opts ...RepackFunc) (*MsgStream, *MsgStream) {

	// set input stream
	inputStream := NewPulsarMsgStream(context.Background(), 100)
	inputStream.SetPulsarClient(pulsarAddress)
	inputStream.CreatePulsarProducers(producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	inputStream.Start()
	var input MsgStream = inputStream

	// set output stream
	outputStream := NewPulsarTtMsgStream(context.Background(), 100)
	outputStream.SetPulsarClient(pulsarAddress)
	unmarshalDispatcher := NewUnmarshalDispatcher()
	outputStream.CreatePulsarConsumers(consumerChannels, consumerSubName, unmarshalDispatcher, 100)
	outputStream.Start()
	var output MsgStream = outputStream

	return &input, &output
}

func receiveMsg(outputStream *MsgStream, msgCount int) {
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

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kInsert, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kInsert, 3, 3))

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

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kDelete, 1, 1))
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

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kSearch, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kSearch, 3, 3))

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

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kSearchResult, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kSearchResult, 3, 3))

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

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kTimeTick, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kTimeTick, 3, 3))

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

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kTimeTick, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kTimeTick, 3, 3))

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

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kInsert, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kInsert, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, repackFunc)
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

	baseMsg := BaseMsg{
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
		Timestamps:     []Timestamp{1, 1},
		RowIDs:         []int64{1, 3},
		RowData:        []*commonpb.Blob{{}, {}},
	}
	insertMsg := &InsertMsg{
		BaseMsg:       baseMsg,
		InsertRequest: insertRequest,
	}

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, insertMsg)

	inputStream := NewPulsarMsgStream(context.Background(), 100)
	inputStream.SetPulsarClient(pulsarAddress)
	inputStream.CreatePulsarProducers(producerChannels)
	inputStream.Start()

	outputStream := NewPulsarMsgStream(context.Background(), 100)
	outputStream.SetPulsarClient(pulsarAddress)
	unmarshalDispatcher := NewUnmarshalDispatcher()
	outputStream.CreatePulsarConsumers(consumerChannels, consumerSubName, unmarshalDispatcher, 100)
	outputStream.Start()
	var output MsgStream = outputStream

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

	baseMsg := BaseMsg{
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
		Timestamps:     []Timestamp{1, 1},
		PrimaryKeys:    []int64{1, 3},
	}
	deleteMsg := &DeleteMsg{
		BaseMsg:       baseMsg,
		DeleteRequest: deleteRequest,
	}

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, deleteMsg)

	inputStream := NewPulsarMsgStream(context.Background(), 100)
	inputStream.SetPulsarClient(pulsarAddress)
	inputStream.CreatePulsarProducers(producerChannels)
	inputStream.Start()

	outputStream := NewPulsarMsgStream(context.Background(), 100)
	outputStream.SetPulsarClient(pulsarAddress)
	unmarshalDispatcher := NewUnmarshalDispatcher()
	outputStream.CreatePulsarConsumers(consumerChannels, consumerSubName, unmarshalDispatcher, 100)
	outputStream.Start()
	var output MsgStream = outputStream

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

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kTimeTick, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kSearch, 2, 2))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kSearchResult, 3, 3))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kQueryNodeStats, 4, 4))

	inputStream := NewPulsarMsgStream(context.Background(), 100)
	inputStream.SetPulsarClient(pulsarAddress)
	inputStream.CreatePulsarProducers(producerChannels)
	inputStream.Start()

	outputStream := NewPulsarMsgStream(context.Background(), 100)
	outputStream.SetPulsarClient(pulsarAddress)
	unmarshalDispatcher := NewUnmarshalDispatcher()
	outputStream.CreatePulsarConsumers(consumerChannels, consumerSubName, unmarshalDispatcher, 100)
	outputStream.Start()
	var output MsgStream = outputStream

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

	msgPack0 := MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0, 0, 0))

	msgPack1 := MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_kInsert, 1, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_kInsert, 3, 3))

	msgPack2 := MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(5, 5, 5))

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

	msgPack0 := MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0, 0, 0))

	msgPack1 := MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_kInsert, 1, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_kInsert, 3, 3))

	msgPack2 := MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(5, 5, 5))

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
