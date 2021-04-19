package msgstream

import (
	"context"
	"fmt"
	"testing"

	commonPb "github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

func repackFunc(msgs []*TsMsg, hashKeys [][]int32) map[int32]*MsgPack {
	result := make(map[int32]*MsgPack)
	for i, request := range msgs {
		keys := hashKeys[i]
		for _, channelId := range keys {
			_, ok := result[channelId]
			if ok == false {
				msgPack := MsgPack{}
				result[channelId] = &msgPack
			}
			result[channelId].Msgs = append(result[channelId].Msgs, request)
		}
	}
	return result
}

func getTsMsg(msgType MsgType, reqId UniqueID, hashValue int32) *TsMsg {
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
			ReqId:          reqId,
			CollectionName: "Collection",
			PartitionTag:   "Partition",
			SegmentId:      1,
			ChannelId:      1,
			ProxyId:        1,
			Timestamps:     []Timestamp{1},
		}
		insertMsg := &InsertMsg{
			BaseMsg:       baseMsg,
			InsertRequest: insertRequest,
		}
		tsMsg = insertMsg
	case internalPb.MsgType_kDelete:
		deleteRequest := internalPb.DeleteRequest{
			MsgType:        internalPb.MsgType_kDelete,
			ReqId:          reqId,
			CollectionName: "Collection",
			ChannelId:      1,
			ProxyId:        1,
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
			ReqId:           reqId,
			ProxyId:         1,
			Timestamp:       1,
			ResultChannelId: 1,
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
			ReqId:           reqId,
			ProxyId:         1,
			QueryNodeId:     1,
			Timestamp:       1,
			ResultChannelId: 1,
		}
		searchResultMsg := &SearchResultMsg{
			BaseMsg:      baseMsg,
			SearchResult: searchResult,
		}
		tsMsg = searchResultMsg
	case internalPb.MsgType_kTimeTick:
		timeTickResult := internalPb.TimeTickMsg{
			MsgType:   internalPb.MsgType_kTimeTick,
			PeerId:    reqId,
			Timestamp: 1,
		}
		timeTickMsg := &TimeTickMsg{
			BaseMsg:     baseMsg,
			TimeTickMsg: timeTickResult,
		}
		tsMsg = timeTickMsg
	}
	return &tsMsg
}

func getTimeTickMsg(msgType MsgType, reqId UniqueID, hashValue int32, time uint64) *TsMsg {
	var tsMsg TsMsg
	baseMsg := BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []int32{hashValue},
	}
	timeTickResult := internalPb.TimeTickMsg{
		MsgType:   internalPb.MsgType_kTimeTick,
		PeerId:    reqId,
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
	(*inputStream).Produce(&msgPack)
	receiveMsg(outputStream, len(msgPack.Msgs))
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
	(*inputStream).Produce(&msgPack)
	receiveMsg(outputStream, len(msgPack.Msgs))
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
	(*inputStream).Produce(&msgPack)
	receiveMsg(outputStream, len(msgPack.Msgs))
}

func TestStream_PulsarMsgStream_SearchResult(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"search"}
	consumerChannels := []string{"search"}
	consumerSubName := "subSearch"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kSearchResult, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kSearchResult, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
	(*inputStream).Produce(&msgPack)
	receiveMsg(outputStream, len(msgPack.Msgs))
}

func TestStream_PulsarMsgStream_TimeTick(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"search"}
	consumerChannels := []string{"search"}
	consumerSubName := "subSearch"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kTimeTick, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kTimeTick, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
	(*inputStream).Produce(&msgPack)
	receiveMsg(outputStream, len(msgPack.Msgs))
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
	(*inputStream).Broadcast(&msgPack)
	receiveMsg(outputStream, len(consumerChannels)*len(msgPack.Msgs))
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
	(*inputStream).Produce(&msgPack)
	receiveMsg(outputStream, len(msgPack.Msgs))
}

func TestStream_PulsarTtMsgStream_Insert(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack0 := MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(internalPb.MsgType_kTimeTick, 0, 0, 0))

	msgPack1 := MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(internalPb.MsgType_kInsert, 1, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(internalPb.MsgType_kInsert, 3, 3))

	msgPack2 := MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(internalPb.MsgType_kTimeTick, 5, 5, 5))

	inputStream, outputStream := initPulsarTtStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
	(*inputStream).Broadcast(&msgPack0)
	(*inputStream).Produce(&msgPack1)
	(*inputStream).Broadcast(&msgPack2)
	receiveMsg(outputStream, len(msgPack1.Msgs))
	outputTtStream := (*outputStream).(*PulsarTtMsgStream)
	fmt.Printf("timestamp = %v", outputTtStream.lastTimeStamp)
}
