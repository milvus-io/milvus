package msgstream

import (
	"fmt"
	commonPb "github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"testing"
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
	case kInsert:
		insertRequest := internalPb.InsertRequest{
			ReqType:        internalPb.ReqType_kInsert,
			ReqId:          reqId,
			CollectionName: "Collection",
			PartitionTag:   "Partition",
			SegmentId:      1,
			ChannelId:      1,
			ProxyId:        1,
			Timestamps:     []uint64{1},
		}
		insertMsg := InsertTask{
			HashValues: []int32{hashValue},
			InsertRequest: insertRequest,
		}
		tsMsg = insertMsg
	case kDelete:
		deleteRequest := internalPb.DeleteRequest{
			ReqType:        internalPb.ReqType_kDelete,
			ReqId:          reqId,
			CollectionName: "Collection",
			ChannelId:      1,
			ProxyId:        1,
			Timestamps:      []uint64{1},
			PrimaryKeys:    []int64{1},
		}
		deleteMsg := DeleteTask{
			HashValues: []int32{hashValue},
			DeleteRequest: deleteRequest,
		}
		tsMsg = deleteMsg
	case kSearch:
		searchRequest := internalPb.SearchRequest{
			ReqType:         internalPb.ReqType_kSearch,
			ReqId:           reqId,
			ProxyId:         1,
			Timestamp:       1,
			ResultChannelId: 1,
		}
		searchMsg := SearchTask{
			HashValues: []int32{hashValue},
			SearchRequest: searchRequest,
		}
		tsMsg = searchMsg
	case kSearchResult:
		searchResult := internalPb.SearchResult{
			Status:          &commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS},
			ReqId:           reqId,
			ProxyId:         1,
			QueryNodeId:     1,
			Timestamp:       1,
			ResultChannelId: 1,
		}
		searchResultMsg := SearchResultTask{
			HashValues: []int32{hashValue},
			SearchResult: searchResult,
		}
		tsMsg = searchResultMsg
	case kTimeSync:
		timeSyncResult := internalPb.TimeSyncMsg{
			PeerId:    reqId,
			Timestamp: 1,
		}
		timeSyncMsg := TimeSyncTask{
			HashValues: []int32{hashValue},
			TimeSyncMsg: timeSyncResult,
		}
		tsMsg = timeSyncMsg
	}
	return &tsMsg
}

func initStream(pulsarAddress string,
	producerChannels []string,
	consumerChannels []string,
	consumerSubName string,
	msgPack *MsgPack,
	inputMsgType MsgType,
	outputMsgType MsgType) {

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
	inputStream.Produce(msgPack)
	//outputStream.Start()

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
		if receiveCount >= len(msgPack.Msgs) {
			break
		}
	}
}

func TestStream_Insert(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kInsert, 0, 0))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kInsert, 1, 1))

	//run stream
	initStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, kInsert, kInsert)
}

func TestStream_Delete(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"delete"}
	consumerChannels := []string{"delete"}
	consumerSubName := "subDelete"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kDelete, 0, 0))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kDelete, 3, 3))

	//run stream
	initStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, kDelete, kDelete)
}

func TestStream_Search(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"search"}
	consumerChannels := []string{"search"}
	consumerSubName := "subSearch"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kSearch, 0, 0))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kSearch, 3, 3))

	//run stream
	initStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, kSearch, kSearch)
}

func TestStream_SearchResult(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"search"}
	consumerChannels := []string{"search"}
	consumerSubName := "subSearch"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kSearchResult, 0, 0))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kSearchResult, 3, 3))

	//run stream
	initStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, kSearchResult, kSearchResult)
}

func TestStream_TimeSync(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"search"}
	consumerChannels := []string{"search"}
	consumerSubName := "subSearch"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kTimeSync, 0, 0))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kTimeSync, 3, 3))

	//run stream
	initStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, kTimeSync, kTimeSync)
}
