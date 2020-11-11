package msgstream

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

func newInsertMsgUnmarshal(input []byte) (*TsMsg, error) {
	insertRequest := internalPb.InsertRequest{}
	err := proto.Unmarshal(input, &insertRequest)
	insertMsg := &InsertMsg{InsertRequest: insertRequest}
	fmt.Println("use func newInsertMsgUnmarshal unmarshal")

	if err != nil {
		return nil, err
	}
	var tsMsg TsMsg = insertMsg
	return &tsMsg, nil
}

func TestStream_unmarshal_Insert(t *testing.T) {
	pulsarAddress := "pulsar://localhost:6650"
	producerChannels := []string{"insert1", "insert2"}
	consumerChannels := []string{"insert1", "insert2"}
	consumerSubName := "subInsert"

	msgPack := MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kInsert, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, getTsMsg(internalPb.MsgType_kInsert, 3, 3))

	inputStream := NewPulsarMsgStream(context.Background(), 100)
	inputStream.SetPulsarCient(pulsarAddress)
	inputStream.CreatePulsarProducers(producerChannels)
	inputStream.Start()

	outputStream := NewPulsarMsgStream(context.Background(), 100)
	outputStream.SetPulsarCient(pulsarAddress)
	unmarshalDispatcher := NewUnmarshalDispatcher()

	//add a new unmarshall func for msgType kInsert
	unmarshalDispatcher.AddMsgTemplate(internalPb.MsgType_kInsert, newInsertMsgUnmarshal)

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
				fmt.Println("msg type: ", (*v).Type(), ", msg value: ", *v, "msg tag: ")
			}
		}
		if receiveCount >= len(msgPack.Msgs) {
			break
		}
	}
	inputStream.Close()
	outputStream.Close()
}
