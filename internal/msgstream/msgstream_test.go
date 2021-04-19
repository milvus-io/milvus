package msgstream

import (
	"context"
	"fmt"
	"log"
	"testing"

	commonPb "github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

func repackFunc(msgs []*TsMsg, hashKeys [][]int32) (map[int32]*MsgPack, error) {
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

func getTsMsg(msgType MsgType, reqID UniqueID, hashValue int32) *TsMsg {
	var tsMsg TsMsg
	baseMsg := BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []int32{hashValue},
	}
	switch msgType {
	case internalPb.MsgType_kInsert:
		insertRequest := internalPb.InsertRequest{
			MsgType:        internalPb.MsgType_kInsert,
			ReqID:          reqID,
			CollectionName: "Collection",
			PartitionTag:   "Partition",
			SegmentID:      1,
			ChannelID:      1,
			ProxyID:        1,
			Timestamps:     []Timestamp{1},
			RowIDs:         []int64{1},
			RowData:        []*commonPb.Blob{{}},
		}
		insertMsg := &InsertMsg{
			BaseMsg:       baseMsg,
			InsertRequest: insertRequest,
		}
		tsMsg = insertMsg
	case internalPb.MsgType_kDelete:
		deleteRequest := internalPb.DeleteRequest{
			MsgType:        internalPb.MsgType_kDelete,
			ReqID:          reqID,
			CollectionName: "Collection",
			ChannelID:      1,
			ProxyID:        1,
			Timestamps:     []Timestamp{1},
			PrimaryKeys:    []IntPrimaryKey{1},
		}
		deleteMsg := &DeleteMsg{
			BaseMsg:       baseMsg,
			DeleteRequest: deleteRequest,
		}
		tsMsg = deleteMsg
	case internalPb.MsgType_kSearch:
		searchRequest := internalPb.SearchRequest{
			MsgType:         internalPb.MsgType_kSearch,
			ReqID:           reqID,
			ProxyID:         1,
			Timestamp:       1,
			ResultChannelID: 1,
		}
		searchMsg := &SearchMsg{
			BaseMsg:       baseMsg,
			SearchRequest: searchRequest,
		}
		tsMsg = searchMsg
	case internalPb.MsgType_kSearchResult:
		searchResult := internalPb.SearchResult{
			MsgType:         internalPb.MsgType_kSearchResult,
			Status:          &commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS},
			ReqID:           reqID,
			ProxyID:         1,
			QueryNodeID:     1,
			Timestamp:       1,
			ResultChannelID: 1,
		}
		searchResultMsg := &SearchResultMsg{
			BaseMsg:      baseMsg,
			SearchResult: searchResult,
		}
		tsMsg = searchResultMsg
	case internalPb.MsgType_kTimeTick:
		timeTickResult := internalPb.TimeTickMsg{
			MsgType:   internalPb.MsgType_kTimeTick,
			PeerID:    reqID,
			Timestamp: 1,
		}
		timeTickMsg := &TimeTickMsg{
			BaseMsg:     baseMsg,
			TimeTickMsg: timeTickResult,
		}
		tsMsg = timeTickMsg
	case internalPb.MsgType_kQueryNodeSegStats:
		queryNodeSegStats := internalPb.QueryNodeSegStats{
			MsgType: internalPb.MsgType_kQueryNodeSegStats,
			PeerID:  reqID,
		}
		queryNodeSegStatsMsg := &QueryNodeSegStatsMsg{
			BaseMsg:           baseMsg,
			QueryNodeSegStats: queryNodeSegStats,
		}
		tsMsg = queryNodeSegStatsMsg
	}
	return &tsMsg
}

func getTimeTickMsg(reqID UniqueID, hashValue int32, time uint64) *TsMsg {
	var tsMsg TsMsg
	baseMsg := BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []int32{hashValue},
	}
	timeTickResult := internalPb.TimeTickMsg{
		MsgType:   internalPb.MsgType_kTimeTick,
		PeerID:    reqID,
		Timestamp: time,
	}
	timeTickMsg := &TimeTickMsg{
		BaseMsg:     baseMsg,
		TimeTickMsg: timeTickResult,
	}
	tsMsg = timeTickMsg
	return &tsMsg
}

func initPulsarStream(pulsarAddress string,
	producerChannels []string,
	consumerChannels []string,
	consumerSubName string,
	opts ...RepackFunc) (*MsgStream, *MsgStream) {

	// set input stream
	inputStream := NewPulsarMsgStream(context.Background(), 100)
	inputStream.SetPulsarCient(pulsarAddress)
	inputStream.CreatePulsarProducers(producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	inputStream.Start()
	var input MsgStream = inputStream

	// set output stream
	outputStream := NewPulsarMsgStream(context.Background(), 100)
	outputStream.SetPulsarCient(pulsarAddress)
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
	inputStream.SetPulsarCient(pulsarAddress)
	inputStream.CreatePulsarProducers(producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	inputStream.Start()
	var input MsgStream = inputStream

	// set output stream
	outputStream := NewPulsarTtMsgStream(context.Background(), 100)
	outputStream.SetPulsarCient(pulsarAddress)
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
				fmt.Println("msg type: ", (*v).Type(), ", msg value: ", *v)
			}
		}
		if receiveCount >= msgCount {
			break
		}
	}
}

func TestStream_PulsarMsgStream_Insert(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kInsert, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kInsert, 3, 3))

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
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"delete"}
	consumerChannels := []string{"delete"}
	consumerSubName := "subDelete"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kDelete, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kDelete, 3, 3))

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
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"search"}
	consumerChannels := []string{"search"}
	consumerSubName := "subSearch"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kSearch, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kSearch, 3, 3))

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
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"searchResult"}
	consumerChannels := []string{"searchResult"}
	consumerSubName := "subSearchResult"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kSearchResult, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kSearchResult, 3, 3))

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
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"timeTick"}
	consumerChannels := []string{"timeTick"}
	consumerSubName := "subTimeTick"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kTimeTick, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kTimeTick, 3, 3))

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
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kTimeTick, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kTimeTick, 3, 3))

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
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kInsert, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kInsert, 3, 3))

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
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	baseMsg := BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []int32{1, 3},
	}

	insertRequest := internalPb.InsertRequest{
		MsgType:        internalPb.MsgType_kInsert,
		ReqID:          1,
		CollectionName: "Collection",
		PartitionTag:   "Partition",
		SegmentID:      1,
		ChannelID:      1,
		ProxyID:        1,
		Timestamps:     []Timestamp{1, 1},
		RowIDs:         []int64{1, 3},
		RowData:        []*commonPb.Blob{{}, {}},
	}
	insertMsg := &InsertMsg{
		BaseMsg:       baseMsg,
		InsertRequest: insertRequest,
	}
	var tsMsg TsMsg = insertMsg
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, &tsMsg)

	inputStream := NewPulsarMsgStream(context.Background(), 100)
	inputStream.SetPulsarCient(pulsarAddress)
	inputStream.CreatePulsarProducers(producerChannels)
	inputStream.Start()

	outputStream := NewPulsarMsgStream(context.Background(), 100)
	outputStream.SetPulsarCient(pulsarAddress)
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
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	baseMsg := BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []int32{1, 3},
	}

	deleteRequest := internalPb.DeleteRequest{
		MsgType:        internalPb.MsgType_kDelete,
		ReqID:          1,
		CollectionName: "Collection",
		ChannelID:      1,
		ProxyID:        1,
		Timestamps:     []Timestamp{1, 1},
		PrimaryKeys:    []int64{1, 3},
	}
	deleteMsg := &DeleteMsg{
		BaseMsg:       baseMsg,
		DeleteRequest: deleteRequest,
	}
	var tsMsg TsMsg = deleteMsg
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, &tsMsg)

	inputStream := NewPulsarMsgStream(context.Background(), 100)
	inputStream.SetPulsarCient(pulsarAddress)
	inputStream.CreatePulsarProducers(producerChannels)
	inputStream.Start()

	outputStream := NewPulsarMsgStream(context.Background(), 100)
	outputStream.SetPulsarCient(pulsarAddress)
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
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kTimeTick, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kSearch, 2, 2))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kSearchResult, 3, 3))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kQueryNodeSegStats, 4, 4))

	inputStream := NewPulsarMsgStream(context.Background(), 100)
	inputStream.SetPulsarCient(pulsarAddress)
	inputStream.CreatePulsarProducers(producerChannels)
	inputStream.Start()

	outputStream := NewPulsarMsgStream(context.Background(), 100)
	outputStream.SetPulsarCient(pulsarAddress)
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
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack0 := MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0, 0, 0))

	msgPack1 := MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(internalPb.MsgType_kInsert, 1, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(internalPb.MsgType_kInsert, 3, 3))

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
	outputTtStream := (*outputStream).(*PulsarTtMsgStream)
	fmt.Printf("timestamp = %v", outputTtStream.lastTimeStamp)
	(*inputStream).Close()
	(*outputStream).Close()
}
