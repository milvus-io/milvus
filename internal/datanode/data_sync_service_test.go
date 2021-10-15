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

package datanode

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

func getVchanInfo(info *testInfo) *datapb.VchannelInfo {
	var ufs []*datapb.SegmentInfo
	var fs []*datapb.SegmentInfo
	if info.isValidCase {
		ufs = []*datapb.SegmentInfo{{
			CollectionID:  info.ufCollID,
			PartitionID:   1,
			InsertChannel: info.ufchanName,
			ID:            info.ufSegID,
			NumOfRows:     info.ufNor,
			DmlPosition:   &internalpb.MsgPosition{},
		}}
		fs = []*datapb.SegmentInfo{{
			CollectionID:  info.fCollID,
			PartitionID:   1,
			InsertChannel: info.fchanName,
			ID:            info.fSegID,
			NumOfRows:     info.fNor,
			DmlPosition:   &internalpb.MsgPosition{},
		}}
	} else {
		ufs = []*datapb.SegmentInfo{}
	}

	vi := &datapb.VchannelInfo{
		CollectionID:      info.collID,
		ChannelName:       info.chanName,
		SeekPosition:      &internalpb.MsgPosition{},
		UnflushedSegments: ufs,
		FlushedSegments:   fs,
	}
	return vi
}

type testInfo struct {
	isValidCase  bool
	replicaNil   bool
	inMsgFactory msgstream.Factory

	collID   UniqueID
	chanName string

	ufCollID   UniqueID
	ufSegID    UniqueID
	ufchanName string
	ufNor      int64

	fCollID   UniqueID
	fSegID    UniqueID
	fchanName string
	fNor      int64

	description string
}

func TestDataSyncService_newDataSyncService(te *testing.T) {

	ctx := context.Background()

	tests := []*testInfo{
		{false, false, &mockMsgStreamFactory{false, true},
			0, "",
			0, 0, "", 0,
			0, 0, "", 0,
			"SetParamsReturnError"},
		{true, false, &mockMsgStreamFactory{true, true},
			0, "",
			1, 0, "", 0,
			1, 1, "", 0,
			"CollID 0 mismach with seginfo collID 1"},
		{true, false, &mockMsgStreamFactory{true, true},
			1, "c1",
			1, 0, "c2", 0,
			1, 1, "c3", 0,
			"chanName c1 mismach with seginfo chanName c2"},
		{true, false, &mockMsgStreamFactory{true, true},
			1, "c1",
			1, 0, "c1", 0,
			1, 1, "c2", 0,
			"add normal segments"},
		{false, false, &mockMsgStreamFactory{true, false},
			0, "",
			0, 0, "", 0,
			0, 0, "", 0,
			"error when newinsertbufernode"},
		{false, true, &mockMsgStreamFactory{true, false},
			0, "",
			0, 0, "", 0,
			0, 0, "", 0,
			"replica nil"},
	}

	for _, test := range tests {
		te.Run(test.description, func(t *testing.T) {
			df := &DataCoordFactory{}

			replica, err := newReplica(context.Background(), &RootCoordFactory{}, test.collID)
			assert.Nil(t, err)
			if test.replicaNil {
				replica = nil
			}

			ds, err := newDataSyncService(ctx,
				&flushChans{make(chan *flushMsg), make(chan *flushMsg)},
				replica,
				NewAllocatorFactory(),
				test.inMsgFactory,
				getVchanInfo(test),
				make(chan UniqueID),
				df,
				newCache(),
			)

			if !test.isValidCase {
				assert.Error(t, err)
				assert.Nil(t, ds)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, ds)

				// save binlog
				fu := &segmentFlushUnit{
					collID:     1,
					segID:      100,
					field2Path: map[UniqueID]string{100: "path1"},
					checkPoint: map[UniqueID]segmentCheckPoint{100: {100, internalpb.MsgPosition{}}},
				}

				df.SaveBinlogPathError = true
				err := ds.saveBinlog(fu)
				assert.Error(t, err)

				df.SaveBinlogPathError = false
				df.SaveBinlogPathNotSucess = true
				err = ds.saveBinlog(fu)
				assert.Error(t, err)

				df.SaveBinlogPathError = false
				df.SaveBinlogPathNotSucess = false
				err = ds.saveBinlog(fu)
				assert.NoError(t, err)

				// start
				ds.fg = nil
				ds.start()
			}
		})
	}

}

// NOTE: start pulsar before test
func TestDataSyncService_Start(t *testing.T) {
	t.Skip()
	const ctxTimeInMillisecond = 2000

	delay := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), delay)
	defer cancel()

	// init data node
	pulsarURL := Params.PulsarAddress

	Factory := &MetaFactory{}
	collMeta := Factory.CollectionMetaFactory(UniqueID(0), "coll1")
	mockRootCoord := &RootCoordFactory{}
	collectionID := UniqueID(1)

	flushChan := &flushChans{make(chan *flushMsg, 100), make(chan *flushMsg, 100)}
	replica, err := newReplica(context.Background(), mockRootCoord, collectionID)
	assert.Nil(t, err)

	allocFactory := NewAllocatorFactory(1)
	msFactory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"pulsarAddress":  pulsarURL,
		"receiveBufSize": 1024,
		"pulsarBufSize":  1024}
	err = msFactory.SetParams(m)
	assert.Nil(t, err)

	insertChannelName := "data_sync_service_test_dml"
	ddlChannelName := "data_sync_service_test_ddl"
	Params.FlushInsertBufferSize = 1

	ufs := []*datapb.SegmentInfo{{
		CollectionID:  collMeta.ID,
		InsertChannel: insertChannelName,
		ID:            0,
		NumOfRows:     0,
		DmlPosition:   &internalpb.MsgPosition{},
	}}
	fs := []*datapb.SegmentInfo{{
		CollectionID:  collMeta.ID,
		PartitionID:   1,
		InsertChannel: insertChannelName,
		ID:            1,
		NumOfRows:     0,
		DmlPosition:   &internalpb.MsgPosition{},
	}}

	vchan := &datapb.VchannelInfo{
		CollectionID:      collMeta.ID,
		ChannelName:       insertChannelName,
		UnflushedSegments: ufs,
		FlushedSegments:   fs,
	}

	signalCh := make(chan UniqueID, 100)
	sync, err := newDataSyncService(ctx, flushChan, replica, allocFactory, msFactory, vchan, signalCh, &DataCoordFactory{}, newCache())

	assert.Nil(t, err)
	// sync.replica.addCollection(collMeta.ID, collMeta.Schema)
	sync.start()

	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}
	dataFactory := NewDataFactory()
	insertMessages := dataFactory.GetMsgStreamTsInsertMsgs(2, insertChannelName)

	msgPack := msgstream.MsgPack{
		BeginTs: timeRange.timestampMin,
		EndTs:   timeRange.timestampMax,
		Msgs:    insertMessages,
		StartPositions: []*internalpb.MsgPosition{{
			ChannelName: insertChannelName,
		}},
		EndPositions: []*internalpb.MsgPosition{{
			ChannelName: insertChannelName,
		}},
	}

	// generate timeTick
	timeTickMsgPack := msgstream.MsgPack{}

	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: Timestamp(0),
			EndTimestamp:   Timestamp(0),
			HashValues:     []uint32{0},
		},
		TimeTickMsg: internalpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				MsgID:     UniqueID(0),
				Timestamp: math.MaxUint64,
				SourceID:  0,
			},
		},
	}
	timeTickMsgPack.Msgs = append(timeTickMsgPack.Msgs, timeTickMsg)

	// pulsar produce
	assert.NoError(t, err)
	insertStream, _ := msFactory.NewMsgStream(ctx)
	insertStream.AsProducer([]string{insertChannelName})

	ddStream, _ := msFactory.NewMsgStream(ctx)
	ddStream.AsProducer([]string{ddlChannelName})

	var insertMsgStream msgstream.MsgStream = insertStream
	insertMsgStream.Start()

	var ddMsgStream msgstream.MsgStream = ddStream
	ddMsgStream.Start()

	err = insertMsgStream.Produce(&msgPack)
	assert.NoError(t, err)

	err = insertMsgStream.Broadcast(&timeTickMsgPack)
	assert.NoError(t, err)
	err = ddMsgStream.Broadcast(&timeTickMsgPack)
	assert.NoError(t, err)

	// dataSync
	<-sync.ctx.Done()

	sync.close()
}

func genBytes() (rawData []byte) {
	const DIM = 2
	const N = 1

	// Float vector
	var fvector = [DIM]float32{1, 2}
	for _, ele := range fvector {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}

	// Binary vector
	// Dimension of binary vector is 32
	// size := 4,  = 32 / 8
	var bvector = []byte{255, 255, 255, 0}
	rawData = append(rawData, bvector...)

	// Bool
	var fieldBool = true
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, fieldBool); err != nil {
		panic(err)
	}

	rawData = append(rawData, buf.Bytes()...)

	// int8
	var dataInt8 int8 = 100
	bint8 := new(bytes.Buffer)
	if err := binary.Write(bint8, binary.LittleEndian, dataInt8); err != nil {
		panic(err)
	}
	rawData = append(rawData, bint8.Bytes()...)
	log.Debug("Rawdata length:", zap.Int("Length of rawData", len(rawData)))
	return
}

func TestBytesReader(t *testing.T) {
	rawData := genBytes()

	// Bytes Reader is able to recording the position
	rawDataReader := bytes.NewReader(rawData)

	var fvector []float32 = make([]float32, 2)
	err := binary.Read(rawDataReader, binary.LittleEndian, &fvector)
	assert.Nil(t, err)
	assert.ElementsMatch(t, fvector, []float32{1, 2})

	var bvector []byte = make([]byte, 4)
	err = binary.Read(rawDataReader, binary.LittleEndian, &bvector)
	assert.Nil(t, err)
	assert.ElementsMatch(t, bvector, []byte{255, 255, 255, 0})

	var fieldBool bool
	err = binary.Read(rawDataReader, binary.LittleEndian, &fieldBool)
	assert.Nil(t, err)
	assert.Equal(t, true, fieldBool)

	var dataInt8 int8
	err = binary.Read(rawDataReader, binary.LittleEndian, &dataInt8)
	assert.Nil(t, err)
	assert.Equal(t, int8(100), dataInt8)
}
