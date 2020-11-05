package msgstream

import (
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
		beginTs:    0,
		endTs:      0,
		HashValues: []int32{hashValue},
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
			MsgType: internalPb.MsgType_kSearchResult,
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
			MsgType: internalPb.MsgType_kTimeTick,
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

func initStream(pulsarAddress string,
	producerChannels []string,
	consumerChannels []string,
	consumerSubName string,
	msgPack *MsgPack,
	inputMsgType MsgType,
	outputMsgType MsgType,
	broadCast bool) {

	// set input stream
	inputStream := PulsarMsgStream{}
	inputStream.SetPulsarCient(pulsarAddress)
	inputStream.SetMsgMarshaler(GetMarshaler(inputMsgType), nil)
	inputStream.SetProducers(producerChannels)
	inputStream.SetRepackFunc(repackFunc)

	// set output stream
	outputStream := PulsarMsgStream{}
	outputStream.SetPulsarCient(pulsarAddress)
	outputStream.SetMsgMarshaler(nil, GetMarshaler(outputMsgType))
	outputStream.SetConsumers(consumerChannels, consumerSubName, 100)
	outputStream.InitMsgPackBuf(100)
	outputStream.Start()

	//send msgPack
	if broadCast {
		inputStream.BroadCast(msgPack)
	} else {
		inputStream.Produce(msgPack)
		//outputStream.Start()
	}

	// receive msg
	receiveCount := 0
	for {
		result := outputStream.Consume()
		if len(result.Msgs) > 0 {
			msgs := result.Msgs
			for _, v := range msgs {
				receiveCount++
				fmt.Println("msg type: ", (*v).Type(), ", msg value: ", *v)
			}
		}
		if broadCast {
			if receiveCount >= len(msgPack.Msgs)*len(producerChannels) {
				break
			}
		} else {
			if receiveCount >= len(msgPack.Msgs) {
				break
			}
		}
	}
}

func TestStream_Insert(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kInsert, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kInsert, 3, 3))

	//run stream
	initStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, internalPb.MsgType_kInsert, internalPb.MsgType_kInsert, false)
}

func
TestStream_Delete(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"delete"}
	consumerChannels := []string{"delete"}
	consumerSubName := "subDelete"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kDelete, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kDelete, 3, 3))

	//run stream
	initStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, internalPb.MsgType_kDelete, internalPb.MsgType_kDelete, false)
}

func TestStream_Search(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"search"}
	consumerChannels := []string{"search"}
	consumerSubName := "subSearch"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kSearch, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kSearch, 3, 3))

	//run stream
	initStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, internalPb.MsgType_kSearch, internalPb.MsgType_kSearch, false)
}

func
TestStream_SearchResult(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"search"}
	consumerChannels := []string{"search"}
	consumerSubName := "subSearch"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kSearchResult, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kSearchResult, 3, 3))

	//run stream
	initStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, internalPb.MsgType_kSearchResult, internalPb.MsgType_kSearchResult, false)
}

func TestStream_TimeTick(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"search"}
	consumerChannels := []string{"search"}
	consumerSubName := "subSearch"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kTimeTick, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kTimeTick, 3, 3))

	//run stream
	initStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, internalPb.MsgType_kTimeTick, internalPb.MsgType_kTimeTick, false)
}

func TestStream_BroadCast(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kTimeTick, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kTimeTick, 3, 3))

	//run stream
	initStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, internalPb.MsgType_kTimeTick, internalPb.MsgType_kTimeTick, true)
}
