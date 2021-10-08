// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package msgstream

import (
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

func newInsertMsgUnmarshal(input []byte) (TsMsg, error) {
	insertRequest := internalpb.InsertRequest{}
	err := proto.Unmarshal(input, &insertRequest)
	insertMsg := &InsertMsg{InsertRequest: insertRequest}
	fmt.Println("use func newInsertMsgUnmarshal unmarshal")
	if err != nil {
		return nil, err
	}

	return insertMsg, nil
}

func Test_ProtoUnmarshalDispatcher(t *testing.T) {
	msgPack := MsgPack{}
	insertMsg := &InsertMsg{
		BaseMsg: BaseMsg{
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     []uint32{1},
		},
		InsertRequest: internalpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				MsgID:     1,
				Timestamp: 11,
				SourceID:  1,
			},
			CollectionName: "Collection",
			PartitionName:  "Partition",
			SegmentID:      1,
			ShardName:      "0",
			Timestamps:     []Timestamp{uint64(1)},
			RowIDs:         []int64{1},
			RowData:        []*commonpb.Blob{{}},
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, insertMsg)

	factory := &ProtoUDFactory{}
	unmarshalDispatcher := factory.NewUnmarshalDispatcher()

	// FIXME(wxyu): Maybe we dont need this interface
	//unmarshalDispatcher.AddMsgTemplate(commonpb.MsgType_kInsert, newInsertMsgUnmarshal)

	for _, v := range msgPack.Msgs {
		headerMsg := commonpb.MsgHeader{}
		payload, err := v.Marshal(v)
		assert.Nil(t, err)
		p, err := convertToByteArray(payload)
		assert.Nil(t, err)
		err = proto.Unmarshal(p, &headerMsg)
		assert.Nil(t, err)
		msg, err := unmarshalDispatcher.Unmarshal(p, headerMsg.Base.MsgType)
		assert.Nil(t, err)
		fmt.Println("msg type: ", msg.Type(), ", msg value: ", msg, "msg tag: ")
	}
}
