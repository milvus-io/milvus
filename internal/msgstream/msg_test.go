package msgstream

import (
	"context"
	"errors"
	"fmt"
	"log"
	"testing"

	"github.com/golang/protobuf/proto"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type InsertTask struct {
	Tag string
	InsertMsg
}

func (tt *InsertTask) Marshal(input TsMsg) ([]byte, error) {
	testMsg := input.(*InsertTask)
	insertRequest := &testMsg.InsertRequest
	mb, err := proto.Marshal(insertRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (tt *InsertTask) Unmarshal(input []byte) (TsMsg, error) {
	insertRequest := internalpb2.InsertRequest{}
	err := proto.Unmarshal(input, &insertRequest)
	testMsg := &InsertTask{InsertMsg: InsertMsg{InsertRequest: insertRequest}}
	testMsg.Tag = testMsg.InsertRequest.PartitionName
	if err != nil {
		return nil, err
	}

	return testMsg, nil
}

func newRepackFunc(tsMsgs []TsMsg, hashKeys [][]int32) (map[int32]*MsgPack, error) {
	result := make(map[int32]*MsgPack)
	for i, request := range tsMsgs {
		if request.Type() != commonpb.MsgType_kInsert {
			return nil, errors.New(string("msg's must be Insert"))
		}
		insertRequest := request.(*InsertTask).InsertRequest
		keys := hashKeys[i]

		timestampLen := len(insertRequest.Timestamps)
		rowIDLen := len(insertRequest.RowIDs)
		rowDataLen := len(insertRequest.RowData)
		keysLen := len(keys)

		if keysLen != timestampLen || keysLen != rowIDLen || keysLen != rowDataLen {
			return nil, errors.New(string("the length of hashValue, timestamps, rowIDs, RowData are not equal"))
		}
		for index, key := range keys {
			_, ok := result[key]
			if !ok {
				msgPack := MsgPack{}
				result[key] = &msgPack
			}

			sliceRequest := internalpb2.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_kInsert,
					MsgID:     insertRequest.Base.MsgID,
					Timestamp: insertRequest.Timestamps[index],
					SourceID:  insertRequest.Base.SourceID,
				},
				CollectionName: insertRequest.CollectionName,
				PartitionName:  insertRequest.PartitionName,
				SegmentID:      insertRequest.SegmentID,
				ChannelID:      insertRequest.ChannelID,
				Timestamps:     []Timestamp{insertRequest.Timestamps[index]},
				RowIDs:         []int64{insertRequest.RowIDs[index]},
				RowData:        []*commonpb.Blob{insertRequest.RowData[index]},
			}

			insertMsg := &InsertTask{
				InsertMsg: InsertMsg{InsertRequest: sliceRequest},
			}

			result[key].Msgs = append(result[key].Msgs, insertMsg)
		}
	}
	return result, nil
}

func getInsertTask(reqID UniqueID, hashValue uint32) TsMsg {
	baseMsg := BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{hashValue},
	}
	insertRequest := internalpb2.InsertRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kInsert,
			MsgID:     reqID,
			Timestamp: 1,
			SourceID:  1,
		},
		CollectionName: "Collection",
		PartitionName:  "Partition",
		SegmentID:      1,
		ChannelID:      "1",
		Timestamps:     []Timestamp{1},
		RowIDs:         []int64{1},
		RowData:        []*commonpb.Blob{{}},
	}
	insertMsg := InsertMsg{
		BaseMsg:       baseMsg,
		InsertRequest: insertRequest,
	}

	testTask := &InsertTask{
		InsertMsg: insertMsg,
	}

	return testTask
}

func TestStream_task_Insert(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getInsertTask(1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getInsertTask(3, 3))

	inputStream := NewPulsarMsgStream(context.Background(), 100)
	inputStream.SetPulsarClient(pulsarAddress)
	inputStream.CreatePulsarProducers(producerChannels)
	inputStream.SetRepackFunc(newRepackFunc)
	inputStream.Start()

	outputStream := NewPulsarMsgStream(context.Background(), 100)
	outputStream.SetPulsarClient(pulsarAddress)
	unmarshalDispatcher := NewUnmarshalDispatcher()
	testTask := InsertTask{}
	unmarshalDispatcher.AddMsgTemplate(commonpb.MsgType_kInsert, testTask.Unmarshal)
	outputStream.CreatePulsarConsumers(consumerChannels, consumerSubName, unmarshalDispatcher, 100)
	outputStream.Start()

	err := inputStream.Produce(&msgPack)
	if err != nil {
		log.Fatalf("produce error = %v", err)
	}
	receiveCount := 0
	for {
		result := (*outputStream).Consume()
		if len(result.Msgs) > 0 {
			msgs := result.Msgs
			for _, v := range msgs {
				receiveCount++
				fmt.Println("msg type: ", v.Type(), ", msg value: ", v, "msg tag: ", v.(*InsertTask).Tag)
			}
		}
		if receiveCount >= len(msgPack.Msgs) {
			break
		}
	}
	inputStream.Close()
	outputStream.Close()
}
