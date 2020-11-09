package msgstream

import (
	"context"
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type InsertTask struct {
	Tag string
	InsertMsg
}

func (tt *InsertTask) Marshal(input *TsMsg) ([]byte, error) {
	testMsg := (*input).(*InsertTask)
	insertRequest := &testMsg.InsertRequest
	mb, err := proto.Marshal(insertRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (tt *InsertTask) Unmarshal(input []byte) (*TsMsg, error) {
	insertRequest := internalPb.InsertRequest{}
	err := proto.Unmarshal(input, &insertRequest)
	testMsg := &InsertTask{InsertMsg: InsertMsg{InsertRequest: insertRequest}}
	testMsg.Tag = testMsg.PartitionTag

	if err != nil {
		return nil, err
	}
	var tsMsg TsMsg = testMsg
	return &tsMsg, nil
}

func getMsg(reqID UniqueID, hashValue int32) *TsMsg {
	var tsMsg TsMsg
	baseMsg := BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []int32{hashValue},
	}
	insertRequest := internalPb.InsertRequest{
		MsgType:        internalPb.MsgType_kInsert,
		ReqId:          reqID,
		CollectionName: "Collection",
		PartitionTag:   "Partition",
		SegmentId:      1,
		ChannelId:      1,
		ProxyId:        1,
		Timestamps:     []Timestamp{1},
	}
	insertMsg := InsertMsg{
		BaseMsg:       baseMsg,
		InsertRequest: insertRequest,
	}

	testTask := InsertTask{
		InsertMsg: insertMsg,
	}
	tsMsg = &testTask
	return &tsMsg
}

func TestStream_task_Insert(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getMsg(1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getMsg(3, 3))

	inputStream := NewPulsarMsgStream(context.Background(), 100)
	inputStream.SetPulsarCient(pulsarAddress)
	inputStream.CreatePulsarProducers(producerChannels)
	inputStream.Start()

	outputStream := NewPulsarMsgStream(context.Background(), 100)
	outputStream.SetPulsarCient(pulsarAddress)
	unmarshalDispatcher := NewUnmarshalDispatcher()
	testTask := InsertTask{}
	unmarshalDispatcher.AddMsgTemplate(internalPb.MsgType_kInsert, testTask.Unmarshal)
	outputStream.CreatePulsarConsumers(consumerChannels, consumerSubName, unmarshalDispatcher, 100)
	outputStream.Start()

	inputStream.Produce(&msgPack)
	receiveCount := 0
	for {
		result := (*outputStream).Consume()
		if len(result.Msgs) > 0 {
			msgs := result.Msgs
			for _, v := range msgs {
				receiveCount++
				fmt.Println("msg type: ", (*v).Type(), ", msg value: ", *v, "msg tag: ", (*v).(*InsertTask).Tag)
			}
		}
		if receiveCount >= len(msgPack.Msgs) {
			break
		}
	}
	inputStream.Close()
	outputStream.Close()
}
