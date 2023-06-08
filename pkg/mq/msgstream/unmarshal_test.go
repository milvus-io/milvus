// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package msgstream

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/stretchr/testify/assert"
)

func Test_ProtoUnmarshalDispatcher(t *testing.T) {
	msgPack := MsgPack{}
	insertMsg := &InsertMsg{
		BaseMsg: BaseMsg{
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     []uint32{1},
		},
		InsertRequest: msgpb.InsertRequest{
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

	for _, v := range msgPack.Msgs {
		headerMsg := commonpb.MsgHeader{}
		payload, err := v.Marshal(v)
		assert.NoError(t, err)
		p, err := convertToByteArray(payload)
		assert.NoError(t, err)
		err = proto.Unmarshal(p, &headerMsg)
		assert.NoError(t, err)
		msg, err := unmarshalDispatcher.Unmarshal(p, headerMsg.Base.MsgType)
		assert.NoError(t, err)
		t.Log("msg type: ", msg.Type(), ", msg value: ", msg, "msg tag: ")
	}
}
