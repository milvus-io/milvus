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

package querynode

import (
	"context"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

// NOTE: start pulsar before test
func TestDataSyncService_Start(t *testing.T) {
	collectionID := UniqueID(0)

	node := newQueryNodeMock()
	initTestMeta(t, node, 0, 0)
	// test data generate
	const msgLength = 10
	const DIM = 16
	const N = 10

	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var rawData []byte
	for _, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 1)
	rawData = append(rawData, bs...)
	var records []*commonpb.Blob
	for i := 0; i < N; i++ {
		blob := &commonpb.Blob{
			Value: rawData,
		}
		records = append(records, blob)
	}
	// messages generate
	insertMessages := make([]msgstream.TsMsg, 0)
	for i := 0; i < msgLength; i++ {
		var msg msgstream.TsMsg = &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{
					uint32(i), uint32(i),
				},
			},
			InsertRequest: internalpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_Insert,
					MsgID:     0,
					Timestamp: Timestamp(i + 1000),
					SourceID:  0,
				},
				CollectionID: collectionID,
				PartitionID:  defaultPartitionID,
				SegmentID:    UniqueID(0),
				ChannelID:    "0",
				Timestamps:   []Timestamp{Timestamp(i + 1000), Timestamp(i + 1000)},
				RowIDs:       []int64{int64(i), int64(i)},
				RowData: []*commonpb.Blob{
					{Value: rawData},
					{Value: rawData},
				},
			},
		}
		insertMessages = append(insertMessages, msg)
	}

	// generate timeTick
	timeTickMsgPack := msgstream.MsgPack{}
	baseMsg := msgstream.BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{0},
	}
	timeTickResult := internalpb.TimeTickMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_TimeTick,
			MsgID:     0,
			Timestamp: math.MaxUint64,
			SourceID:  0,
		},
	}
	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg:     baseMsg,
		TimeTickMsg: timeTickResult,
	}
	timeTickMsgPack.Msgs = append(timeTickMsgPack.Msgs, timeTickMsg)

	// pulsar produce
	const receiveBufSize = 1024
	pulsarURL := Params.PulsarAddress

	msFactory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"receiveBufSize": receiveBufSize,
		"pulsarAddress":  pulsarURL,
		"pulsarBufSize":  1024}
	err := msFactory.SetParams(m)
	assert.Nil(t, err)

	channels := []Channel{"0"}
	node.streaming.dataSyncService.addCollectionFlowGraph(collectionID, channels)
	err = node.streaming.dataSyncService.startCollectionFlowGraph(collectionID, channels)
	assert.NoError(t, err)

	<-node.queryNodeLoopCtx.Done()
	node.streaming.dataSyncService.close()

	err = node.Stop()
	assert.NoError(t, err)
}

func TestDataSyncService_collectionFlowGraphs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streaming, err := genSimpleStreaming(ctx)
	assert.NoError(t, err)

	fac, err := genFactory()
	assert.NoError(t, err)

	dataSyncService := newDataSyncService(ctx, streaming.replica, streaming.tSafeReplica, fac)
	assert.NotNil(t, dataSyncService)

	dataSyncService.addCollectionFlowGraph(defaultCollectionID, []Channel{defaultVChannel})

	fg, err := dataSyncService.getCollectionFlowGraphs(defaultCollectionID, []Channel{defaultVChannel})
	assert.NotNil(t, fg)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(fg))

	fg, err = dataSyncService.getCollectionFlowGraphs(UniqueID(1000), []Channel{defaultVChannel})
	assert.Nil(t, fg)
	assert.Error(t, err)

	fg, err = dataSyncService.getCollectionFlowGraphs(defaultCollectionID, []Channel{"invalid-vChannel"})
	assert.NotNil(t, fg)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(fg))

	fg, err = dataSyncService.getCollectionFlowGraphs(UniqueID(1000), []Channel{"invalid-vChannel"})
	assert.Nil(t, fg)
	assert.Error(t, err)

	err = dataSyncService.startCollectionFlowGraph(defaultCollectionID, []Channel{defaultVChannel})
	assert.NoError(t, err)

	dataSyncService.removeCollectionFlowGraph(defaultCollectionID)

	fg, err = dataSyncService.getCollectionFlowGraphs(defaultCollectionID, []Channel{defaultVChannel})
	assert.Nil(t, fg)
	assert.Error(t, err)
}

func TestDataSyncService_partitionFlowGraphs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streaming, err := genSimpleStreaming(ctx)
	assert.NoError(t, err)

	fac, err := genFactory()
	assert.NoError(t, err)

	dataSyncService := newDataSyncService(ctx, streaming.replica, streaming.tSafeReplica, fac)
	assert.NotNil(t, dataSyncService)

	dataSyncService.addPartitionFlowGraph(defaultPartitionID, defaultPartitionID, []Channel{defaultVChannel})

	fg, err := dataSyncService.getPartitionFlowGraphs(defaultPartitionID, []Channel{defaultVChannel})
	assert.NotNil(t, fg)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(fg))

	fg, err = dataSyncService.getPartitionFlowGraphs(UniqueID(1000), []Channel{defaultVChannel})
	assert.Nil(t, fg)
	assert.Error(t, err)

	fg, err = dataSyncService.getPartitionFlowGraphs(defaultPartitionID, []Channel{"invalid-vChannel"})
	assert.NotNil(t, fg)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(fg))

	fg, err = dataSyncService.getPartitionFlowGraphs(UniqueID(1000), []Channel{"invalid-vChannel"})
	assert.Nil(t, fg)
	assert.Error(t, err)

	err = dataSyncService.startPartitionFlowGraph(defaultPartitionID, []Channel{defaultVChannel})
	assert.NoError(t, err)

	dataSyncService.removePartitionFlowGraph(defaultPartitionID)

	fg, err = dataSyncService.getPartitionFlowGraphs(defaultPartitionID, []Channel{defaultVChannel})
	assert.Nil(t, fg)
	assert.Error(t, err)
}

func TestDataSyncService_removePartitionFlowGraphs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("test no tSafe", func(t *testing.T) {
		streaming, err := genSimpleStreaming(ctx)
		assert.NoError(t, err)

		fac, err := genFactory()
		assert.NoError(t, err)

		dataSyncService := newDataSyncService(ctx, streaming.replica, streaming.tSafeReplica, fac)
		assert.NotNil(t, dataSyncService)

		dataSyncService.addPartitionFlowGraph(defaultPartitionID, defaultPartitionID, []Channel{defaultVChannel})

		err = dataSyncService.tSafeReplica.removeTSafe(defaultVChannel)
		assert.NoError(t, err)
		dataSyncService.removePartitionFlowGraph(defaultPartitionID)
	})
}
