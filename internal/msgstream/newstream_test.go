package msgstream

import (
	"fmt"
	"testing"

	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

func TestNewStream_Insert(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kInsert, 0, 0))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kInsert, 1, 1))
	inputStream := NewInputStream(pulsarAddress, producerChannels, false)
	outputStream := NewOutputStream(pulsarAddress, 100, 100, consumerChannels, consumerSubName, false)

	(*inputStream).SetMsgMarshaler(GetMarshaler(kInsert), nil)
	(*inputStream).SetRepackFunc(repackFunc)
	(*outputStream).SetMsgMarshaler(nil, GetMarshaler(kInsert))
	(*outputStream).Start()

	//send msgPack
	(*inputStream).Produce(&msgPack)
	//(*outputStream).Start()

	// receive msg
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
		if receiveCount >= len(msgPack.Msgs) {
			break
		}
	}
}

func TestNewStream_Delete(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"delete1", "delete2"}
	consumerChannels := []string{"delete1", "delete2"}
	consumerSubName := "subDelete"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kDelete, 0, 0))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kDelete, 1, 1))
	inputStream := NewInputStream(pulsarAddress, producerChannels, false)
	outputStream := NewOutputStream(pulsarAddress, 100, 100, consumerChannels, consumerSubName, false)

	(*inputStream).SetMsgMarshaler(GetMarshaler(kDelete), nil)
	(*inputStream).SetRepackFunc(repackFunc)
	(*outputStream).SetMsgMarshaler(nil, GetMarshaler(kDelete))
	(*outputStream).Start()

	//send msgPack
	(*inputStream).Produce(&msgPack)
	//(*outputStream).Start()

	// receive msg
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
		if receiveCount >= len(msgPack.Msgs) {
			break
		}
	}
}

func TestNewStream_Search(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"search1", "search2"}
	consumerChannels := []string{"search1", "search2"}
	consumerSubName := "subSearch"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kSearch, 0, 0))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kSearch, 1, 1))
	inputStream := NewInputStream(pulsarAddress, producerChannels, false)
	outputStream := NewOutputStream(pulsarAddress, 100, 100, consumerChannels, consumerSubName, false)

	(*inputStream).SetMsgMarshaler(GetMarshaler(kSearch), nil)
	(*inputStream).SetRepackFunc(repackFunc)
	(*outputStream).SetMsgMarshaler(nil, GetMarshaler(kSearch))
	(*outputStream).Start()

	//send msgPack
	(*inputStream).Produce(&msgPack)
	//(*outputStream).Start()

	// receive msg
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
		if receiveCount >= len(msgPack.Msgs) {
			break
		}
	}
}

func TestNewStream_SearchResult(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"searchResult1", "searchResult2"}
	consumerChannels := []string{"searchResult1", "searchResult2"}
	consumerSubName := "subInsert"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kSearchResult, 0, 0))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kSearchResult, 1, 1))
	inputStream := NewInputStream(pulsarAddress, producerChannels, false)
	outputStream := NewOutputStream(pulsarAddress, 100, 100, consumerChannels, consumerSubName, false)

	(*inputStream).SetMsgMarshaler(GetMarshaler(kSearchResult), nil)
	(*inputStream).SetRepackFunc(repackFunc)
	(*outputStream).SetMsgMarshaler(nil, GetMarshaler(kSearchResult))
	(*outputStream).Start()

	//send msgPack
	(*inputStream).Produce(&msgPack)
	//(*outputStream).Start()

	// receive msg
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
		if receiveCount >= len(msgPack.Msgs) {
			break
		}
	}
}

func TestNewStream_TimeSync(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"timeSync1", "timeSync2"}
	consumerChannels := []string{"timeSync1", "timeSync2"}
	consumerSubName := "subInsert"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kTimeSync, 0, 0))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kTimeSync, 1, 1))
	inputStream := NewInputStream(pulsarAddress, producerChannels, false)
	outputStream := NewOutputStream(pulsarAddress, 100, 100, consumerChannels, consumerSubName, false)

	(*inputStream).SetMsgMarshaler(GetMarshaler(kTimeSync), nil)
	(*inputStream).SetRepackFunc(repackFunc)
	(*outputStream).SetMsgMarshaler(nil, GetMarshaler(kTimeSync))
	(*outputStream).Start()

	//send msgPack
	(*inputStream).Produce(&msgPack)

	// receive msg
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
		if receiveCount >= len(msgPack.Msgs) {
			break
		}
	}
}

func TestNewStream_Insert_TimeTick(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"insert"}
	consumerChannels := []string{"insert"}
	consumerSubName := "subInsert"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kInsert, 0, 0))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(kInsert, 1, 1))

	insertRequest := internalPb.InsertRequest{
		ReqType:        internalPb.ReqType_kTimeTick,
		ReqId:          2,
		CollectionName: "Collection",
		PartitionTag:   "Partition",
		SegmentId:      1,
		ChannelId:      1,
		ProxyId:        1,
		Timestamps:     []uint64{1},
	}
	insertMsg := InsertTask{
		HashValues:    []int32{2},
		InsertRequest: insertRequest,
	}
	var tsMsg TsMsg = insertMsg
	msgPack.Msgs = append(msgPack.Msgs, &tsMsg)

	inputStream := NewInputStream(pulsarAddress, producerChannels, false)
	outputStream := NewOutputStream(pulsarAddress, 100, 100, consumerChannels, consumerSubName, true)

	(*inputStream).SetMsgMarshaler(GetMarshaler(kInsert), nil)
	(*inputStream).SetRepackFunc(repackFunc)
	(*outputStream).SetMsgMarshaler(nil, GetMarshaler(kInsert))
	(*outputStream).Start()

	//send msgPack
	(*inputStream).Produce(&msgPack)

	// receive msg
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
		if receiveCount+1 >= len(msgPack.Msgs) {
			break
		}
	}
}
