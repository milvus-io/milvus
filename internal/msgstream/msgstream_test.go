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

func getTsMsg(msgType MsgType, reqId int64, hashValue int32) *TsMsg {
	var tsMsg TsMsg
	switch msgType {
	case KInsert:
		insertRequest := internalPb.InsertRequest{
			MsgType:        internalPb.MsgType_kInsert,
			ReqId:          reqId,
			CollectionName: "Collection",
			PartitionTag:   "Partition",
			SegmentId:      1,
			ChannelId:      1,
			ProxyId:        1,
			Timestamps:     []uint64{1},
		}
		insertMsg := InsertTask{
			HashValues:    []int32{hashValue},
			InsertRequest: insertRequest,
		}
		tsMsg = insertMsg
	case KDelete:
		deleteRequest := internalPb.DeleteRequest{
			MsgType:        internalPb.MsgType_kDelete,
			ReqId:          reqId,
			CollectionName: "Collection",
			ChannelId:      1,
			ProxyId:        1,
			Timestamps:     []uint64{1},
			PrimaryKeys:    []int64{1},
		}
		deleteMsg := DeleteTask{
			HashValues:    []int32{hashValue},
			DeleteRequest: deleteRequest,
		}
		tsMsg = deleteMsg
	case KSearch:
		searchRequest := internalPb.SearchRequest{
			MsgType:         internalPb.MsgType_kSearch,
			ReqId:           reqId,
			ProxyId:         1,
			Timestamp:       1,
			ResultChannelId: 1,
		}
		searchMsg := SearchTask{
			HashValues:    []int32{hashValue},
			SearchRequest: searchRequest,
		}
		tsMsg = searchMsg
	case KSearchResult:
		searchResult := internalPb.SearchResult{
			Status:          &commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS},
			ReqId:           reqId,
			ProxyId:         1,
			QueryNodeId:     1,
			Timestamp:       1,
			ResultChannelId: 1,
		}
		searchResultMsg := SearchResultTask{
			HashValues:   []int32{hashValue},
			SearchResult: searchResult,
		}
		tsMsg = searchResultMsg
	case KTimeTick:
		timeTickResult := internalPb.TimeTickMsg{
			PeerId:    reqId,
			Timestamp: 1,
		}
		timeTickMsg := TimeTickTask{
			HashValues:  []int32{hashValue},
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
			if receiveCount >= len(msgPack.Msgs) * len(producerChannels) {
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
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(KInsert, 0, 0))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(KInsert, 1, 1))

	//run stream
	initStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, KInsert, KInsert, false)
}

func TestStream_Delete(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"delete"}
	consumerChannels := []string{"delete"}
	consumerSubName := "subDelete"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(KDelete, 0, 0))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(KDelete, 3, 3))

	//run stream
	initStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, KDelete, KDelete, false)
}

func TestStream_Search(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"search"}
	consumerChannels := []string{"search"}
	consumerSubName := "subSearch"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(KSearch, 0, 0))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(KSearch, 3, 3))

	//run stream
	initStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, KSearch, KSearch, false)
}

func TestStream_SearchResult(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"search"}
	consumerChannels := []string{"search"}
	consumerSubName := "subSearch"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(KSearchResult, 0, 0))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(KSearchResult, 3, 3))

	//run stream
	initStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, KSearchResult, KSearchResult, false)
}

func TestStream_TimeTick(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"search"}
	consumerChannels := []string{"search"}
	consumerSubName := "subSearch"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(KTimeTick, 0, 0))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(KTimeTick, 3, 3))

	//run stream
	initStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, KTimeTick, KTimeTick, false)
}


func TestStream_BroadCast(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert2", "insert2"}
	consumerSubName := "subInsert"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(KTimeTick, 0, 0))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(KTimeTick, 3, 3))

	//run stream
	initStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, KTimeTick, KTimeTick, true)
}
