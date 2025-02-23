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

package msgdispatcher

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	dim = 128
)

var Params = paramtable.Get()

func TestMain(m *testing.M) {
	paramtable.Init()
	Params.Save(Params.ServiceParam.MQCfg.EnablePursuitMode.Key, "false")
	exitCode := m.Run()
	os.Exit(exitCode)
}

func newMockFactory() msgstream.Factory {
	return msgstream.NewPmsFactory(&Params.ServiceParam)
}

func newMockProducer(factory msgstream.Factory, pchannel string) (msgstream.MsgStream, error) {
	stream, err := factory.NewMsgStream(context.Background())
	if err != nil {
		return nil, err
	}
	stream.AsProducer(context.TODO(), []string{pchannel})
	stream.SetRepackFunc(defaultInsertRepackFunc)
	return stream, nil
}

func getSeekPositions(factory msgstream.Factory, pchannel string, maxNum int) ([]*msgstream.MsgPosition, error) {
	stream, err := factory.NewTtMsgStream(context.Background())
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	stream.AsConsumer(context.TODO(), []string{pchannel}, fmt.Sprintf("%d", rand.Int()), common.SubscriptionPositionEarliest)
	positions := make([]*msgstream.MsgPosition, 0)
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		select {
		case <-timeoutCtx.Done(): // no message to consume
			return positions, nil
		case pack := <-stream.Chan():
			positions = append(positions, pack.EndPositions[0])
			if len(positions) >= maxNum {
				return positions, nil
			}
		}
	}
}

func genPKs(numRows int) []typeutil.IntPrimaryKey {
	ids := make([]typeutil.IntPrimaryKey, numRows)
	for i := 0; i < numRows; i++ {
		ids[i] = typeutil.IntPrimaryKey(i)
	}
	return ids
}

func genTimestamps(numRows int) []typeutil.Timestamp {
	ts := make([]typeutil.Timestamp, numRows)
	for i := 0; i < numRows; i++ {
		ts[i] = typeutil.Timestamp(i + 1)
	}
	return ts
}

func genInsertMsg(numRows int, vchannel string, msgID typeutil.UniqueID) *msgstream.InsertMsg {
	floatVec := make([]float32, numRows*dim)
	for i := 0; i < numRows*dim; i++ {
		floatVec[i] = rand.Float32()
	}
	hashValues := make([]uint32, numRows)
	for i := 0; i < numRows; i++ {
		hashValues[i] = uint32(1)
	}
	return &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{HashValues: hashValues},
		InsertRequest: &msgpb.InsertRequest{
			Base:       &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert, MsgID: msgID},
			ShardName:  vchannel,
			Timestamps: genTimestamps(numRows),
			RowIDs:     genPKs(numRows),
			FieldsData: []*schemapb.FieldData{{
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim:  dim,
						Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: floatVec}},
					},
				},
			}},
			NumRows: uint64(numRows),
			Version: msgpb.InsertDataVersion_ColumnBased,
		},
	}
}

func genDeleteMsg(numRows int, vchannel string, msgID typeutil.UniqueID) *msgstream.DeleteMsg {
	return &msgstream.DeleteMsg{
		BaseMsg: msgstream.BaseMsg{HashValues: make([]uint32, numRows)},
		DeleteRequest: &msgpb.DeleteRequest{
			Base:      &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete, MsgID: msgID},
			ShardName: vchannel,
			PrimaryKeys: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: genPKs(numRows),
					},
				},
			},
			Timestamps: genTimestamps(numRows),
			NumRows:    int64(numRows),
		},
	}
}

func genDDLMsg(msgType commonpb.MsgType, collectionID int64) msgstream.TsMsg {
	switch msgType {
	case commonpb.MsgType_CreateCollection:
		return &msgstream.CreateCollectionMsg{
			BaseMsg: msgstream.BaseMsg{HashValues: []uint32{0}},
			CreateCollectionRequest: &msgpb.CreateCollectionRequest{
				Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionID: collectionID,
			},
		}
	case commonpb.MsgType_DropCollection:
		return &msgstream.DropCollectionMsg{
			BaseMsg: msgstream.BaseMsg{HashValues: []uint32{0}},
			DropCollectionRequest: &msgpb.DropCollectionRequest{
				Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionID: collectionID,
			},
		}
	case commonpb.MsgType_CreatePartition:
		return &msgstream.CreatePartitionMsg{
			BaseMsg: msgstream.BaseMsg{HashValues: []uint32{0}},
			CreatePartitionRequest: &msgpb.CreatePartitionRequest{
				Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_CreatePartition},
				CollectionID: collectionID,
			},
		}
	case commonpb.MsgType_DropPartition:
		return &msgstream.DropPartitionMsg{
			BaseMsg: msgstream.BaseMsg{HashValues: []uint32{0}},
			DropPartitionRequest: &msgpb.DropPartitionRequest{
				Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_DropPartition},
				CollectionID: collectionID,
			},
		}
	}
	return nil
}

func genTimeTickMsg(ts typeutil.Timestamp) *msgstream.TimeTickMsg {
	return &msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{HashValues: []uint32{0}},
		TimeTickMsg: &msgpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				Timestamp: ts,
			},
		},
	}
}

// defaultInsertRepackFunc repacks the dml messages.
func defaultInsertRepackFunc(
	tsMsgs []msgstream.TsMsg,
	hashKeys [][]int32,
) (map[int32]*msgstream.MsgPack, error) {
	if len(hashKeys) < len(tsMsgs) {
		return nil, fmt.Errorf(
			"the length of hash keys (%d) is less than the length of messages (%d)",
			len(hashKeys),
			len(tsMsgs),
		)
	}

	// after assigning segment id to msg, tsMsgs was already re-bucketed
	pack := make(map[int32]*msgstream.MsgPack)
	for idx, msg := range tsMsgs {
		if len(hashKeys[idx]) <= 0 {
			return nil, fmt.Errorf("no hash key for %dth message", idx)
		}
		key := hashKeys[idx][0]
		_, ok := pack[key]
		if !ok {
			pack[key] = &msgstream.MsgPack{}
		}
		pack[key].Msgs = append(pack[key].Msgs, msg)
	}
	return pack, nil
}
