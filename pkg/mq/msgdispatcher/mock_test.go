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
	"sync"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
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
	stream.AsProducer(context.Background(), []string{pchannel})
	stream.SetRepackFunc(defaultInsertRepackFunc)
	return stream, nil
}

func genPKs(numRows int) []typeutil.IntPrimaryKey {
	ids := make([]typeutil.IntPrimaryKey, numRows)
	for i := 0; i < numRows; i++ {
		ids[i] = typeutil.IntPrimaryKey(i)
	}
	return ids
}

func genTimestamps(numRows int, ts typeutil.Timestamp) []typeutil.Timestamp {
	tss := make([]typeutil.Timestamp, numRows)
	for i := 0; i < numRows; i++ {
		tss[i] = ts
	}
	return tss
}

func genInsertMsg(numRows int, vchannel string, msgID typeutil.UniqueID, ts typeutil.Timestamp) *msgstream.InsertMsg {
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
			Base:       &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert, MsgID: msgID, Timestamp: ts},
			ShardName:  vchannel,
			Timestamps: genTimestamps(numRows, ts),
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

func genDeleteMsg(numRows int, vchannel string, msgID typeutil.UniqueID, ts typeutil.Timestamp) *msgstream.DeleteMsg {
	return &msgstream.DeleteMsg{
		BaseMsg: msgstream.BaseMsg{HashValues: make([]uint32, numRows)},
		DeleteRequest: &msgpb.DeleteRequest{
			Base:      &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete, MsgID: msgID, Timestamp: ts},
			ShardName: vchannel,
			PrimaryKeys: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: genPKs(numRows),
					},
				},
			},
			Timestamps: genTimestamps(numRows, ts),
			NumRows:    int64(numRows),
		},
	}
}

func genDDLMsg(msgType commonpb.MsgType, collectionID int64, ts typeutil.Timestamp) msgstream.TsMsg {
	switch msgType {
	case commonpb.MsgType_CreateCollection:
		return &msgstream.CreateCollectionMsg{
			BaseMsg: msgstream.BaseMsg{HashValues: []uint32{0}},
			CreateCollectionRequest: &msgpb.CreateCollectionRequest{
				Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection, Timestamp: ts},
				CollectionID: collectionID,
			},
		}
	case commonpb.MsgType_DropCollection:
		return &msgstream.DropCollectionMsg{
			BaseMsg: msgstream.BaseMsg{HashValues: []uint32{0}},
			DropCollectionRequest: &msgpb.DropCollectionRequest{
				Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection, Timestamp: ts},
				CollectionID: collectionID,
			},
		}
	case commonpb.MsgType_CreatePartition:
		return &msgstream.CreatePartitionMsg{
			BaseMsg: msgstream.BaseMsg{HashValues: []uint32{0}},
			CreatePartitionRequest: &msgpb.CreatePartitionRequest{
				Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_CreatePartition, Timestamp: ts},
				CollectionID: collectionID,
			},
		}
	case commonpb.MsgType_DropPartition:
		return &msgstream.DropPartitionMsg{
			BaseMsg: msgstream.BaseMsg{HashValues: []uint32{0}},
			DropPartitionRequest: &msgpb.DropPartitionRequest{
				Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_DropPartition, Timestamp: ts},
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

type vchannelHelper struct {
	output <-chan *msgstream.MsgPack

	pubInsMsgNum atomic.Int32
	pubDelMsgNum atomic.Int32
	pubDDLMsgNum atomic.Int32
	pubPackNum   atomic.Int32

	subInsMsgNum atomic.Int32
	subDelMsgNum atomic.Int32
	subDDLMsgNum atomic.Int32
	subPackNum   atomic.Int32

	seekPos          *Pos
	skippedInsMsgNum int32
	skippedDelMsgNum int32
	skippedDDLMsgNum int32
	skippedPackNum   int32
}

func produceMsgs(t *testing.T, ctx context.Context, wg *sync.WaitGroup, producer msgstream.MsgStream, vchannels map[string]*vchannelHelper) {
	defer wg.Done()

	uniqueMsgID := int64(0)
	vchannelNames := lo.Keys(vchannels)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	i := 1
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ts := uint64(i * 100)
			// produce random insert
			insNum := rand.Intn(10)
			for j := 0; j < insNum; j++ {
				vchannel := vchannelNames[rand.Intn(len(vchannels))]
				err := producer.Produce(context.Background(), &msgstream.MsgPack{
					Msgs: []msgstream.TsMsg{genInsertMsg(rand.Intn(20)+1, vchannel, uniqueMsgID, ts)},
				})
				assert.NoError(t, err)
				uniqueMsgID++
				vchannels[vchannel].pubInsMsgNum.Inc()
			}
			// produce random delete
			delNum := rand.Intn(2)
			for j := 0; j < delNum; j++ {
				vchannel := vchannelNames[rand.Intn(len(vchannels))]
				err := producer.Produce(context.Background(), &msgstream.MsgPack{
					Msgs: []msgstream.TsMsg{genDeleteMsg(rand.Intn(10)+1, vchannel, uniqueMsgID, ts)},
				})
				assert.NoError(t, err)
				uniqueMsgID++
				vchannels[vchannel].pubDelMsgNum.Inc()
			}
			// produce random ddl
			ddlNum := rand.Intn(2)
			for j := 0; j < ddlNum; j++ {
				vchannel := vchannelNames[rand.Intn(len(vchannels))]
				collectionID := funcutil.GetCollectionIDFromVChannel(vchannel)
				err := producer.Produce(context.Background(), &msgstream.MsgPack{
					Msgs: []msgstream.TsMsg{genDDLMsg(commonpb.MsgType_DropCollection, collectionID, ts)},
				})
				assert.NoError(t, err)
				uniqueMsgID++
				vchannels[vchannel].pubDDLMsgNum.Inc()
			}
			// produce time tick
			err := producer.Produce(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{genTimeTickMsg(ts)},
			})
			assert.NoError(t, err)
			for k := range vchannels {
				vchannels[k].pubPackNum.Inc()
			}
			i++
		}
	}
}

func consumeMsgsFromTargets(t *testing.T, ctx context.Context, wg *sync.WaitGroup, vchannel string, helper *vchannelHelper) {
	defer wg.Done()

	var lastTs typeutil.Timestamp
	for {
		select {
		case <-ctx.Done():
			return
		case pack := <-helper.output:
			if pack == nil || pack.EndTs == 0 {
				continue
			}
			assert.Greater(t, pack.EndTs, lastTs, fmt.Sprintf("vchannel=%s", vchannel))
			lastTs = pack.EndTs
			helper.subPackNum.Inc()
			for _, msg := range pack.Msgs {
				switch msg.Type() {
				case commonpb.MsgType_Insert:
					helper.subInsMsgNum.Inc()
				case commonpb.MsgType_Delete:
					helper.subDelMsgNum.Inc()
				case commonpb.MsgType_CreateCollection, commonpb.MsgType_DropCollection,
					commonpb.MsgType_CreatePartition, commonpb.MsgType_DropPartition:
					helper.subDDLMsgNum.Inc()
				}
			}
		}
	}
}

func produceTimeTick(t *testing.T, ctx context.Context, producer msgstream.MsgStream) {
	tt := 1
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ts := uint64(tt * 1000)
			err := producer.Produce(ctx, &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{genTimeTickMsg(ts)},
			})
			assert.NoError(t, err)
			tt++
		}
	}
}

func getRandomSeekPositions(t *testing.T, ctx context.Context, factory msgstream.Factory, pchannel string, vchannels map[string]*vchannelHelper) {
	stream, err := factory.NewTtMsgStream(context.Background())
	assert.NoError(t, err)
	defer stream.Close()

	err = stream.AsConsumer(context.Background(), []string{pchannel}, fmt.Sprintf("%d", rand.Int()), common.SubscriptionPositionEarliest)
	assert.NoError(t, err)

	for {
		select {
		case <-ctx.Done():
			return
		case pack := <-stream.Chan():
			for _, msg := range pack.Msgs {
				switch msg.Type() {
				case commonpb.MsgType_Insert:
					vchannel := msg.(*msgstream.InsertMsg).GetShardName()
					if vchannels[vchannel].seekPos == nil {
						vchannels[vchannel].skippedInsMsgNum++
					}
				case commonpb.MsgType_Delete:
					vchannel := msg.(*msgstream.DeleteMsg).GetShardName()
					if vchannels[vchannel].seekPos == nil {
						vchannels[vchannel].skippedDelMsgNum++
					}
				case commonpb.MsgType_DropCollection:
					collectionID := msg.(*msgstream.DropCollectionMsg).GetCollectionID()
					for vchannel := range vchannels {
						if vchannels[vchannel].seekPos == nil &&
							funcutil.GetCollectionIDFromVChannel(vchannel) == collectionID {
							vchannels[vchannel].skippedDDLMsgNum++
						}
					}
				}
			}
			for _, helper := range vchannels {
				if helper.seekPos == nil {
					helper.skippedPackNum++
				}
			}
			if rand.Intn(5) == 0 { // assign random seek position
				for _, helper := range vchannels {
					if helper.seekPos == nil {
						helper.seekPos = pack.EndPositions[0]
						break
					}
				}
			}
			allAssigned := true
			for _, helper := range vchannels {
				if helper.seekPos == nil {
					allAssigned = false
					break
				}
			}
			if allAssigned {
				return // all seek positions have been assigned
			}
		}
	}
}
