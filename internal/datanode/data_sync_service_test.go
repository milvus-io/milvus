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

package datanode

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

func getVchanInfo(cp bool, collID, ufCollID, ufSegID UniqueID, chanName, ufchanName string, ufNor int64) *datapb.VchannelInfo {
	var ufs []*datapb.SegmentInfo
	if cp {
		ufs = []*datapb.SegmentInfo{{
			CollectionID:  ufCollID,
			PartitionID:   1,
			InsertChannel: ufchanName,
			ID:            ufSegID,
			NumOfRows:     ufNor,
			DmlPosition:   &internalpb.MsgPosition{},
		}}
	} else {
		ufs = []*datapb.SegmentInfo{}
	}

	vi := &datapb.VchannelInfo{
		CollectionID:      collID,
		ChannelName:       chanName,
		SeekPosition:      &internalpb.MsgPosition{},
		UnflushedSegments: ufs,
		FlushedSegments:   []int64{},
	}
	return vi
}

func TestDataSyncService_newDataSyncService(te *testing.T) {

	ctx := context.Background()

	tests := []struct {
		isValidCase  bool
		replicaNil   bool
		inMsgFactory msgstream.Factory

		collID     UniqueID
		ufCollID   UniqueID
		ufSegID    UniqueID
		chanName   string
		ufchanName string
		ufNor      int64

		description string
	}{
		{false, false, &mockMsgStreamFactory{false, true},
			0, 0, 0, "", "", 0,
			"SetParamsReturnError"},
		{true, false, &mockMsgStreamFactory{true, true},
			0, 1, 0, "", "", 0,
			"CollID 0 mismach with seginfo collID 1"},
		{true, false, &mockMsgStreamFactory{true, true},
			1, 1, 0, "c1", "c2", 0,
			"chanName c1 mismach with seginfo chanName c2"},
		{true, false, &mockMsgStreamFactory{true, true},
			1, 1, 0, "c1", "c1", 0,
			"add normal segments"},
		{false, false, &mockMsgStreamFactory{true, false},
			0, 0, 0, "", "", 0,
			"error when newinsertbufernode"},
		{false, true, &mockMsgStreamFactory{true, false},
			0, 0, 0, "", "", 0,
			"replica nil"},
	}

	for _, test := range tests {
		te.Run(test.description, func(t *testing.T) {
			df := &DataCoordFactory{}

			replica := newReplica(&RootCoordFactory{}, test.collID)
			if test.replicaNil {
				replica = nil
			}

			ds, err := newDataSyncService(ctx,
				make(chan *flushMsg),
				replica,
				NewAllocatorFactory(),
				test.inMsgFactory,
				getVchanInfo(test.isValidCase, test.collID, test.ufCollID, test.ufSegID, test.chanName, test.ufchanName, test.ufNor),
				make(chan UniqueID),
				df,
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

	flushChan := make(chan *flushMsg, 100)
	replica := newReplica(mockRootCoord, collectionID)

	allocFactory := NewAllocatorFactory(1)
	msFactory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"pulsarAddress":  pulsarURL,
		"receiveBufSize": 1024,
		"pulsarBufSize":  1024}
	err := msFactory.SetParams(m)
	assert.Nil(t, err)

	insertChannelName := "data_sync_service_test_dml"
	ddlChannelName := "data_sync_service_test_ddl"
	Params.FlushInsertBufferSize = 1

	vchan := &datapb.VchannelInfo{
		CollectionID:      collMeta.GetID(),
		ChannelName:       insertChannelName,
		UnflushedSegments: []*datapb.SegmentInfo{},
		FlushedSegments:   []int64{},
	}

	signalCh := make(chan UniqueID, 100)
	sync, err := newDataSyncService(ctx, flushChan, replica, allocFactory, msFactory, vchan, signalCh, &DataCoordFactory{})

	assert.Nil(t, err)
	// sync.replica.addCollection(collMeta.ID, collMeta.Schema)
	go sync.start()

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
