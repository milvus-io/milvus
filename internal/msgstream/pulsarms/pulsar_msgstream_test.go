package pulsarms

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
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
			Timestamps:     []Timestamp{uint64(reqID)},
			RowIDs:         []int64{1},
			RowData:        []*commonpb.Blob{{}},
		}
		insertMsg := &msgstream.InsertMsg{
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
		deleteMsg := &msgstream.DeleteMsg{
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
		searchMsg := &msgstream.SearchMsg{
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
		searchResultMsg := &msgstream.SearchResultMsg{
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
	opts ...RepackFunc) (msgstream.MsgStream, msgstream.MsgStream) {
	factory := msgstream.ProtoUDFactory{}

	// set input stream
	inputStream, _ := newPulsarMsgStream(context.Background(), pulsarAddress, 100, 100, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	inputStream.Start()
	var input msgstream.MsgStream = inputStream

	// set output stream
	outputStream, _ := newPulsarMsgStream(context.Background(), pulsarAddress, 100, 100, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerSubName)
	outputStream.Start()
	var output msgstream.MsgStream = outputStream

	return input, output
}

func initPulsarTtStream(pulsarAddress string,
	producerChannels []string,
	consumerChannels []string,
	consumerSubName string,
	opts ...RepackFunc) (msgstream.MsgStream, msgstream.MsgStream) {
	factory := msgstream.ProtoUDFactory{}

	// set input stream
	inputStream, _ := newPulsarMsgStream(context.Background(), pulsarAddress, 100, 100, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	for _, opt := range opts {
		inputStream.SetRepackFunc(opt)
	}
	inputStream.Start()
	var input msgstream.MsgStream = inputStream

	// set output stream
	outputStream, _ := newPulsarTtMsgStream(context.Background(), pulsarAddress, 100, 100, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerSubName)
	outputStream.Start()
	var output msgstream.MsgStream = outputStream

	return input, output
}

func receiveMsg(outputStream msgstream.MsgStream, msgCount int) {
	receiveCount := 0
	for {
		result, _ := outputStream.Consume()
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
	ctx := context.Background()
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kInsert, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kInsert, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
	err := inputStream.Produce(ctx, &msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}

	receiveMsg(outputStream, len(msgPack.Msgs))
	inputStream.Close()
	outputStream.Close()

}

func TestStream_PulsarMsgStream_Delete(t *testing.T) {
	ctx := context.Background()
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c := funcutil.RandomString(8)
	producerChannels := []string{c}
	consumerChannels := []string{c}
	consumerSubName := funcutil.RandomString(8)
	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kDelete, 1, 1))
	//msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kDelete, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
	err := inputStream.Produce(ctx, &msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarMsgStream_Search(t *testing.T) {
	ctx := context.Background()
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c := funcutil.RandomString(8)
	producerChannels := []string{c}
	consumerChannels := []string{c}
	consumerSubName := funcutil.RandomString(8)

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kSearch, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kSearch, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
	err := inputStream.Produce(ctx, &msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarMsgStream_SearchResult(t *testing.T) {
	ctx := context.Background()
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c := funcutil.RandomString(8)
	producerChannels := []string{c}
	consumerChannels := []string{c}
	consumerSubName := funcutil.RandomString(8)
	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kSearchResult, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kSearchResult, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
	err := inputStream.Produce(ctx, &msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarMsgStream_TimeTick(t *testing.T) {
	ctx := context.Background()
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c := funcutil.RandomString(8)
	producerChannels := []string{c}
	consumerChannels := []string{c}
	consumerSubName := funcutil.RandomString(8)
	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kTimeTick, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kTimeTick, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
	err := inputStream.Produce(ctx, &msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarMsgStream_BroadCast(t *testing.T) {
	ctx := context.Background()
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kTimeTick, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kTimeTick, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
	err := inputStream.Broadcast(ctx, &msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(consumerChannels)*len(msgPack.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarMsgStream_RepackFunc(t *testing.T) {
	ctx := context.Background()
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kInsert, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kInsert, 3, 3))

	inputStream, outputStream := initPulsarStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName, repackFunc)
	err := inputStream.Produce(ctx, &msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarMsgStream_InsertRepackFunc(t *testing.T) {
	ctx := context.Background()
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)
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

	factory := msgstream.ProtoUDFactory{}
	inputStream, _ := newPulsarMsgStream(context.Background(), pulsarAddress, 100, 100, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	inputStream.Start()

	outputStream, _ := newPulsarMsgStream(context.Background(), pulsarAddress, 100, 100, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerSubName)
	outputStream.Start()
	var output msgstream.MsgStream = outputStream

	err := (*inputStream).Produce(ctx, &msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(output, len(msgPack.Msgs)*2)
	(*inputStream).Close()
	(*outputStream).Close()
}

func TestStream_PulsarMsgStream_DeleteRepackFunc(t *testing.T) {
	ctx := context.Background()
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

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

	factory := msgstream.ProtoUDFactory{}
	inputStream, _ := newPulsarMsgStream(context.Background(), pulsarAddress, 100, 100, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	inputStream.Start()

	outputStream, _ := newPulsarMsgStream(context.Background(), pulsarAddress, 100, 100, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerSubName)
	outputStream.Start()
	var output msgstream.MsgStream = outputStream

	err := (*inputStream).Produce(ctx, &msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(output, len(msgPack.Msgs)*2)
	(*inputStream).Close()
	(*outputStream).Close()
}

func TestStream_PulsarMsgStream_DefaultRepackFunc(t *testing.T) {
	ctx := context.Background()
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kTimeTick, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kSearch, 2, 2))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kSearchResult, 3, 3))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(commonpb.MsgType_kQueryNodeStats, 4, 4))

	factory := msgstream.ProtoUDFactory{}
	inputStream, _ := newPulsarMsgStream(context.Background(), pulsarAddress, 100, 100, factory.NewUnmarshalDispatcher())
	inputStream.AsProducer(producerChannels)
	inputStream.Start()

	outputStream, _ := newPulsarMsgStream(context.Background(), pulsarAddress, 100, 100, factory.NewUnmarshalDispatcher())
	outputStream.AsConsumer(consumerChannels, consumerSubName)
	outputStream.Start()
	var output msgstream.MsgStream = outputStream

	err := (*inputStream).Produce(ctx, &msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveMsg(output, len(msgPack.Msgs))
	(*inputStream).Close()
	(*outputStream).Close()
}

func TestStream_PulsarTtMsgStream_Insert(t *testing.T) {
	ctx := context.Background()
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)
	msgPack0 := msgstream.MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0, 0, 0))

	msgPack1 := msgstream.MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_kInsert, 1, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_kInsert, 3, 3))

	msgPack2 := msgstream.MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(5, 5, 5))

	inputStream, outputStream := initPulsarTtStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
	err := inputStream.Broadcast(ctx, &msgPack0)
	if err != nil {
		log.Fatalf("broadcast error = %v", err)
	}
	err = inputStream.Produce(ctx, &msgPack1)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	err = inputStream.Broadcast(ctx, &msgPack2)
	if err != nil {
		log.Fatalf("broadcast error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack1.Msgs))
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarTtMsgStream_Seek(t *testing.T) {
	ctx := context.Background()
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	msgPack0 := MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0, 0, 0))

	msgPack1 := MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_kInsert, 1, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_kInsert, 19, 19))

	msgPack2 := MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(5, 5, 5))

	msgPack3 := MsgPack{}
	msgPack3.Msgs = append(msgPack3.Msgs, getTsMsg(commonpb.MsgType_kInsert, 14, 14))
	msgPack3.Msgs = append(msgPack3.Msgs, getTsMsg(commonpb.MsgType_kInsert, 9, 9))

	msgPack4 := MsgPack{}
	msgPack4.Msgs = append(msgPack4.Msgs, getTimeTickMsg(11, 11, 11))

	msgPack5 := MsgPack{}
	msgPack5.Msgs = append(msgPack5.Msgs, getTimeTickMsg(15, 15, 15))

	inputStream, outputStream := initPulsarTtStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
	err := inputStream.Broadcast(ctx, &msgPack0)
	assert.Nil(t, err)
	err = inputStream.Produce(ctx, &msgPack1)
	assert.Nil(t, err)
	err = inputStream.Broadcast(ctx, &msgPack2)
	assert.Nil(t, err)
	err = inputStream.Produce(ctx, &msgPack3)
	assert.Nil(t, err)
	err = inputStream.Broadcast(ctx, &msgPack4)
	assert.Nil(t, err)

	outputStream.Consume()
	receivedMsg, _ := outputStream.Consume()
	for _, position := range receivedMsg.StartPositions {
		outputStream.Seek(position)
	}
	err = inputStream.Broadcast(ctx, &msgPack5)
	assert.Nil(t, err)
	seekMsg, _ := outputStream.Consume()
	for _, msg := range seekMsg.Msgs {
		assert.Equal(t, msg.BeginTs(), uint64(14))
	}
	inputStream.Close()
	outputStream.Close()
}

func TestStream_PulsarTtMsgStream_UnMarshalHeader(t *testing.T) {
	ctx := context.Background()
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	c1, c2 := funcutil.RandomString(8), funcutil.RandomString(8)
	producerChannels := []string{c1, c2}
	consumerChannels := []string{c1, c2}
	consumerSubName := funcutil.RandomString(8)

	msgPack0 := msgstream.MsgPack{}
	msgPack0.Msgs = append(msgPack0.Msgs, getTimeTickMsg(0, 0, 0))

	msgPack1 := msgstream.MsgPack{}
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_kInsert, 1, 1))
	msgPack1.Msgs = append(msgPack1.Msgs, getTsMsg(commonpb.MsgType_kInsert, 3, 3))

	msgPack2 := msgstream.MsgPack{}
	msgPack2.Msgs = append(msgPack2.Msgs, getTimeTickMsg(5, 5, 5))

	inputStream, outputStream := initPulsarTtStream(pulsarAddress, producerChannels, consumerChannels, consumerSubName)
	err := inputStream.Broadcast(ctx, &msgPack0)
	if err != nil {
		log.Fatalf("broadcast error = %v", err)
	}
	err = inputStream.Produce(ctx, &msgPack1)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	err = inputStream.Broadcast(ctx, &msgPack2)
	if err != nil {
		log.Fatalf("broadcast error = %v", err)
	}
	receiveMsg(outputStream, len(msgPack1.Msgs))
	inputStream.Close()
	outputStream.Close()
}
