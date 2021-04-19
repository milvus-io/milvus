package msgstream

import (
	"fmt"
	commonPb "github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"log"
	"testing"
)

func produceHashTopic(input *MsgPack) map[uint32]*MsgPack {
	msgs := input.Msgs
	result := make(map[uint32]*MsgPack)
	count := len(msgs)
	for i := 0; i < count; i++ {
		var key uint32
		var err error
		switch (*msgs[i]).Type() {
		case kInsert:
			var insertMsg InsertTask = (*msgs[i]).(InsertTask)
			key, err = typeutil.Hash32Int64(insertMsg.ReqId)
		case kDelete:
			var deleteMsg DeleteTask = (*msgs[i]).(DeleteTask)
			key, err = typeutil.Hash32Int64(deleteMsg.ReqId)
		case kSearch:
			var searchMsg SearchTask = (*msgs[i]).(SearchTask)
			key, err = typeutil.Hash32Int64(searchMsg.ReqId)
		case kSearchResult:
			var searchResultMsg SearchResultTask = (*msgs[i]).(SearchResultTask)
			key, err = typeutil.Hash32Int64(searchResultMsg.ReqId)
		case kTimeSync:
			var timeSyncMsg TimeSyncTask = (*msgs[i]).(TimeSyncTask)
			key, err = typeutil.Hash32Int64(timeSyncMsg.PeerId)
		default:
			log.Fatal("con't find msgType")
		}

		if err != nil {
			log.Fatal(err)
		}
		_, ok := result[key]
		if ok == false {
			msgPack := MsgPack{}
			result[key] = &msgPack
		}
		result[key].Msgs = append(result[key].Msgs, msgs[i])
	}
	return result
}

func baseTest(pulsarAddress string,
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
	inputStream.SetHashFunc(produceHashTopic)

	// set output stream
	outputStream := PulsarMsgStream{}
	outputStream.SetPulsarCient(pulsarAddress)
	outputStream.SetMsgMarshaler(nil, GetMarshaler(outputMsgType))
	outputStream.SetConsumers(consumerChannels, consumerSubName)

	//send msgPack
	inputStream.Produce(msgPack)

	// receive msg
	for {
		result := outputStream.Consume()
		if len(result.Msgs) > 0 {
			msgs := result.Msgs
			for _, v := range msgs {
				fmt.Println("msg type: ", (*v).Type(), ", msg value: ", *v)
			}
			break
		}
	}
}

func TestStream_Insert(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"insert"}
	consumerChannels := []string{"insert"}
	consumerSubName := "subInsert"

	//pack tsmsg
	insertRequest := internalPb.InsertRequest{
		ReqType:        internalPb.ReqType_kInsert,
		ReqId:          1,
		CollectionName: "Collection",
		PartitionTag:   "Partition",
		SegmentId:      1,
		ChannelId:      1,
		ProxyId:        1,
		Timestamp:      1,
	}
	insertMsg := InsertTask{
		insertRequest,
	}
	var tsMsg TsMsg = insertMsg
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, &tsMsg)

	//run stream
	baseTest(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, kInsert, kInsert)
}

func TestStream_Delete(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"delete"}
	consumerChannels := []string{"delete"}
	consumerSubName := "subDelete"

	//pack tsmsg
	deleteRequest := internalPb.DeleteRequest{
		ReqType:        internalPb.ReqType_kInsert,
		ReqId:          1,
		CollectionName: "Collection",
		ChannelId:      1,
		ProxyId:        1,
		Timestamp:      1,
		PrimaryKeys:    []int64{1},
	}
	deleteMsg := DeleteTask{
		deleteRequest,
	}
	var tsMsg TsMsg = deleteMsg
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, &tsMsg)

	//run stream
	baseTest(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, kDelete, kDelete)
}

func TestStream_Search(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"search"}
	consumerChannels := []string{"search"}
	consumerSubName := "subSearch"

	//pack tsmsg
	searchRequest := internalPb.SearchRequest{
		ReqType:         internalPb.ReqType_kSearch,
		ReqId:           1,
		ProxyId:         1,
		Timestamp:       1,
		ResultChannelId: 1,
	}
	searchMsg := SearchTask{
		searchRequest,
	}
	var tsMsg TsMsg = searchMsg
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, &tsMsg)

	//run stream
	baseTest(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, kSearch, kSearch)
}

func TestStream_SearchResult(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"search"}
	consumerChannels := []string{"search"}
	consumerSubName := "subSearch"

	//pack tsmsg
	searchResult := internalPb.SearchResult{
		Status:          &commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS},
		ReqId:           1,
		ProxyId:         1,
		QueryNodeId:     1,
		Timestamp:       1,
		ResultChannelId: 1,
	}
	searchResultMsg := SearchResultTask{
		searchResult,
	}
	var tsMsg TsMsg = searchResultMsg
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, &tsMsg)

	//run stream
	baseTest(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, kSearchResult, kSearchResult)
}

func TestStream_TimeSync(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"search"}
	consumerChannels := []string{"search"}
	consumerSubName := "subSearch"

	//pack tsmsg
	timeSyncResult := internalPb.TimeSyncMsg{
		PeerId:    1,
		Timestamp: 1,
	}
	timeSyncMsg := TimeSyncTask{
		timeSyncResult,
	}
	var tsMsg TsMsg = timeSyncMsg
	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, &tsMsg)

	//run stream
	baseTest(pulsarAddress, producerChannels, consumerChannels, consumerSubName, &msgPack, kTimeSync, kTimeSync)
}
