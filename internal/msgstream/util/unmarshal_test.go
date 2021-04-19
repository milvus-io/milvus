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

type Timestamp = msgstream.Timestamp

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
	msgPack := msgstream.MsgPack{}
	insertMsg := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     []uint32{1},
		},
		InsertRequest: internalpb2.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kInsert,
				MsgID:     1,
				Timestamp: 11,
				SourceID:  1,
			},
			CollectionName: "Collection",
			PartitionName:  "Partition",
			SegmentID:      1,
			ChannelID:      "0",
			Timestamps:     []Timestamp{uint64(1)},
			RowIDs:         []int64{1},
			RowData:        []*commonpb.Blob{{}},
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, insertMsg)

	unmarshalDispatcher := NewUnmarshalDispatcher()
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
}
