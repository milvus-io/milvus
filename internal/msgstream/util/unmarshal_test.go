package util

import (
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

var Params paramtable.BaseTable

func newInsertMsgUnmarshal(input []byte) (msgstream.TsMsg, error) {
	insertRequest := internalpb2.InsertRequest{}
	err := proto.Unmarshal(input, &insertRequest)
	insertMsg := &msgstream.InsertMsg{InsertRequest: insertRequest}
	fmt.Println("use func newInsertMsgUnmarshal unmarshal")
	if err != nil {
		return nil, err
	}

	return insertMsg, nil
}

func TestStream_unmarshal_Insert(t *testing.T) {
	//pulsarAddress, _ := Params.Load("_PulsarAddress")
	//producerChannels := []string{"insert1", "insert2"}
	//consumerChannels := []string{"insert1", "insert2"}
	//consumerSubName := "subInsert"

	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, GetTsMsg(commonpb.MsgType_kInsert, 1, 1))
	msgPack.Msgs = append(msgPack.Msgs, GetTsMsg(commonpb.MsgType_kInsert, 3, 3))

	//inputStream := pulsarms.NewPulsarMsgStream(context.Background(), 100)
	//inputStream.SetPulsarClient(pulsarAddress)
	//inputStream.CreatePulsarProducers(producerChannels)
	//inputStream.Start()

	//outputStream := pulsarms.NewPulsarMsgStream(context.Background(), 100)
	//outputStream.SetPulsarClient(pulsarAddress)
	unmarshalDispatcher := NewUnmarshalDispatcher()

	//add a new unmarshall func for msgType kInsert
	unmarshalDispatcher.AddMsgTemplate(commonpb.MsgType_kInsert, newInsertMsgUnmarshal)

	for _, v := range msgPack.Msgs {
		headerMsg := commonpb.MsgHeader{}
		payload, err := v.Marshal(v)
		assert.Nil(t, err)
		err = proto.Unmarshal(payload, &headerMsg)
		assert.Nil(t, err)
		msg, err := unmarshalDispatcher.Unmarshal(payload, headerMsg.Base.MsgType)
		assert.Nil(t, err)
		fmt.Println("msg type: ", msg.Type(), ", msg value: ", msg, "msg tag: ")
	}

	//outputStream.CreatePulsarConsumers(consumerChannels, consumerSubName, unmarshalDispatcher, 100)
	//outputStream.Start()

	//err := inputStream.Produce(&msgPack)
	//if err != nil {
	//	log.Fatalf("produce error = %v", err)
	//}
	//receiveCount := 0
	//for {
	//	result := (*outputStream).Consume()
	//	if len(result.Msgs) > 0 {
	//		msgs := result.Msgs
	//		for _, v := range msgs {
	//			receiveCount++
	//			fmt.Println("msg type: ", v.Type(), ", msg value: ", v, "msg tag: ")
	//		}
	//	}
	//	if receiveCount >= len(msgPack.Msgs) {
	//		break
	//	}
	//}
	//inputStream.Close()
	//outputStream.Close()
}
