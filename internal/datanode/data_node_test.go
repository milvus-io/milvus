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
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

func TestMain(t *testing.M) {
	Params.Init()
	refreshChannelNames()
	code := t.Run()
	os.Exit(code)
}

func TestDataNode(t *testing.T) {
	node := newIDLEDataNodeMock()
	node.Start()

	t.Run("Test WatchDmChannels", func(t *testing.T) {
		node1 := newIDLEDataNodeMock()
		node1.Start()
		vchannels := []*datapb.VchannelPair{}
		for _, ch := range Params.InsertChannelNames {
			log.Debug("InsertChannels", zap.String("name", ch))
			vpair := &datapb.VchannelPair{
				CollectionID:    1,
				DmlVchannelName: ch,
				DdlVchannelName: Params.DDChannelNames[0],
				DdlPosition:     &datapb.PositionPair{},
				DmlPosition:     &datapb.PositionPair{},
			}
			vchannels = append(vchannels, vpair)
		}
		req := &datapb.WatchDmChannelsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  Params.NodeID,
			},
			Vchannels: vchannels,
		}

		_, err := node1.WatchDmChannels(node.ctx, req)
		assert.NoError(t, err)
		assert.NotNil(t, node1.vchan2FlushCh)
		assert.NotNil(t, node1.vchan2SyncService)
		sync, ok := node1.vchan2SyncService[Params.InsertChannelNames[0]]
		assert.True(t, ok)
		assert.NotNil(t, sync)
		assert.Equal(t, UniqueID(1), sync.collectionID)
		assert.Equal(t, 2, len(node1.vchan2SyncService))
		assert.Equal(t, len(node1.vchan2FlushCh), len(node1.vchan2SyncService))

		_, err = node1.WatchDmChannels(node1.ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(node1.vchan2SyncService))

		<-node1.ctx.Done()
		node1.Stop()
	})

	t.Run("Test GetComponentStates", func(t *testing.T) {
		stat, err := node.GetComponentStates(node.ctx)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, stat.Status.ErrorCode)
	})

	t.Run("Test NewDataSyncService", func(t *testing.T) {
		node2 := newIDLEDataNodeMock()
		node2.Start()
		dmChannelName := "fake-dm-channel-test-NewDataSyncService"
		ddChannelName := "fake-dd-channel-test-NewDataSyncService"
		vpair := &datapb.VchannelPair{
			CollectionID:    1,
			DmlVchannelName: dmChannelName,
			DdlVchannelName: ddChannelName,
			DdlPosition:     &datapb.PositionPair{},
			DmlPosition:     &datapb.PositionPair{},
		}

		require.Equal(t, 0, len(node2.vchan2FlushCh))
		require.Equal(t, 0, len(node2.vchan2SyncService))

		err := node2.NewDataSyncService(vpair)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(node2.vchan2FlushCh))
		assert.Equal(t, 1, len(node2.vchan2SyncService))

		err = node2.NewDataSyncService(vpair)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(node2.vchan2FlushCh))
		assert.Equal(t, 1, len(node2.vchan2SyncService))
		<-node2.ctx.Done()
		node2.Stop()
	})

	t.Run("Test FlushSegments", func(t *testing.T) {
		dmChannelName := "fake-dm-channel-test-HEALTHDataNodeMock"
		ddChannelName := "fake-dd-channel-test-HEALTHDataNodeMock"

		node1 := newHEALTHDataNodeMock(dmChannelName, ddChannelName)

		sync, ok := node1.vchan2SyncService[dmChannelName]
		assert.True(t, ok)
		sync.replica.addSegment(0, 1, 1, dmChannelName)
		sync.replica.addSegment(1, 1, 1, dmChannelName)

		req := &datapb.FlushSegmentsRequest{
			Base:         &commonpb.MsgBase{},
			DbID:         0,
			CollectionID: 1,
			SegmentIDs:   []int64{0, 1},
		}
		status, err := node1.FlushSegments(node.ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)

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
		msFactory := msgstream.NewPmsFactory()
		m := map[string]interface{}{
			"pulsarAddress":  Params.PulsarAddress,
			"receiveBufSize": 1024,
			"pulsarBufSize":  1024}
		err = msFactory.SetParams(m)
		assert.NoError(t, err)
		insertStream, _ := msFactory.NewMsgStream(node1.ctx)
		insertStream.AsProducer([]string{dmChannelName})

		ddStream, _ := msFactory.NewMsgStream(node1.ctx)
		ddStream.AsProducer([]string{ddChannelName})

		var insertMsgStream msgstream.MsgStream = insertStream
		insertMsgStream.Start()

		var ddMsgStream msgstream.MsgStream = ddStream
		ddMsgStream.Start()

		err = insertMsgStream.Broadcast(&timeTickMsgPack)
		assert.NoError(t, err)
		err = ddMsgStream.Broadcast(&timeTickMsgPack)
		assert.NoError(t, err)

		<-node1.ctx.Done()
		node1.Stop()
	})

	t.Run("Test GetTimeTickChannel", func(t *testing.T) {
		_, err := node.GetTimeTickChannel(node.ctx)
		assert.NoError(t, err)
	})

	t.Run("Test GetStatisticsChannel", func(t *testing.T) {
		_, err := node.GetStatisticsChannel(node.ctx)
		assert.NoError(t, err)
	})

	<-node.ctx.Done()
	node.Stop()
}
