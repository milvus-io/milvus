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
	"os"
	"testing"
	"time"

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
	Params.InitAlias("datanode-alias-1")
	Params.Init()
	refreshChannelNames()
	code := t.Run()
	os.Exit(code)
}

func TestDataNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	node := newIDLEDataNodeMock(ctx)
	node.Init()
	node.Start()
	node.Register()

	t.Run("Test WatchDmChannels", func(t *testing.T) {
		t.Skip()
		ctx, cancel := context.WithCancel(context.Background())
		node1 := newIDLEDataNodeMock(ctx)
		node1.Start()
		vchannels := []*datapb.VchannelInfo{}
		for _, ch := range []string{"datanode-01-test-WatchDmChannel",
			"datanode-02-test-WatchDmChannels"} {
			log.Debug("InsertChannels", zap.String("name", ch))
			vchan := &datapb.VchannelInfo{
				CollectionID:      1,
				ChannelName:       ch,
				UnflushedSegments: []*datapb.SegmentInfo{},
			}
			vchannels = append(vchannels, vchan)
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
		sync, ok := node1.vchan2SyncService["datanode-01-test-WatchDmChannel"]
		assert.True(t, ok)
		assert.NotNil(t, sync)
		assert.Equal(t, UniqueID(1), sync.collectionID)
		assert.Equal(t, 2, len(node1.vchan2SyncService))
		assert.Equal(t, len(node1.vchan2FlushCh), len(node1.vchan2SyncService))

		_, err = node1.WatchDmChannels(node1.ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(node1.vchan2SyncService))

		cancel()
		<-node1.ctx.Done()
		node1.Stop()
	})

	t.Run("Test GetComponentStates", func(t *testing.T) {
		stat, err := node.GetComponentStates(node.ctx)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, stat.Status.ErrorCode)
	})

	t.Run("Test NewDataSyncService", func(t *testing.T) {
		t.Skip()
		ctx, cancel := context.WithCancel(context.Background())
		node2 := newIDLEDataNodeMock(ctx)
		node2.Start()
		dmChannelName := "fake-dm-channel-test-NewDataSyncService"

		vchan := &datapb.VchannelInfo{
			CollectionID:      1,
			ChannelName:       dmChannelName,
			UnflushedSegments: []*datapb.SegmentInfo{},
		}

		require.Equal(t, 0, len(node2.vchan2FlushCh))
		require.Equal(t, 0, len(node2.vchan2SyncService))

		err := node2.NewDataSyncService(vchan)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(node2.vchan2FlushCh))
		assert.Equal(t, 1, len(node2.vchan2SyncService))

		err = node2.NewDataSyncService(vchan)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(node2.vchan2FlushCh))
		assert.Equal(t, 1, len(node2.vchan2SyncService))

		cancel()
		<-node2.ctx.Done()
		node2.Stop()
	})

	t.Run("Test FlushSegments", func(t *testing.T) {
		t.Skipf("Fix latter")
		dmChannelName := "fake-dm-channel-test-HEALTHDataNodeMock"

		node1 := newHEALTHDataNodeMock(dmChannelName)

		sync, ok := node1.vchan2SyncService[dmChannelName]
		assert.True(t, ok)
		sync.replica.addNewSegment(0, 1, 1, dmChannelName, nil, nil)

		req := &datapb.FlushSegmentsRequest{
			Base:         &commonpb.MsgBase{},
			DbID:         0,
			CollectionID: 1,
			SegmentIDs:   []int64{0},
		}

		status, err := node1.FlushSegments(node1.ctx, req)
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

		var insertMsgStream msgstream.MsgStream = insertStream
		insertMsgStream.Start()

		err = insertMsgStream.Broadcast(&timeTickMsgPack)
		assert.NoError(t, err)

		err = insertMsgStream.Broadcast(&timeTickMsgPack)
		assert.NoError(t, err)

		defer func() {
			<-node1.ctx.Done()
			node1.Stop()
		}()
	})

	t.Run("Test GetTimeTickChannel", func(t *testing.T) {
		_, err := node.GetTimeTickChannel(node.ctx)
		assert.NoError(t, err)
	})

	t.Run("Test GetStatisticsChannel", func(t *testing.T) {
		_, err := node.GetStatisticsChannel(node.ctx)
		assert.NoError(t, err)
	})

	t.Run("Test BackGroundGC", func(te *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		node := newIDLEDataNodeMock(ctx)

		collIDCh := make(chan UniqueID)
		go node.BackGroundGC(collIDCh)
		node.clearSignal = collIDCh

		testDataSyncs := []struct {
			collID        UniqueID
			dmChannelName string
		}{
			{1, "fake-dm-backgroundgc-1"},
			{2, "fake-dm-backgroundgc-2"},
			{3, "fake-dm-backgroundgc-3"},
			{4, ""},
			{1, ""},
		}

		for i, t := range testDataSyncs {
			if i <= 2 {
				node.NewDataSyncService(&datapb.VchannelInfo{CollectionID: t.collID, ChannelName: t.dmChannelName})

				msFactory := msgstream.NewPmsFactory()
				insertStream, _ := msFactory.NewMsgStream(ctx)
				var insertMsgStream msgstream.MsgStream = insertStream
				insertMsgStream.Start()
			}

			collIDCh <- t.collID
		}

		assert.Eventually(t, func() bool {
			node.chanMut.Lock()
			defer node.chanMut.Unlock()
			return len(node.vchan2FlushCh) == 0
		}, time.Second, time.Millisecond)

		cancel()
	})

	t.Run("Test ReleaseDataSyncService", func(t *testing.T) {
		dmChannelName := "fake-dm-channel-test-NewDataSyncService"

		vchan := &datapb.VchannelInfo{
			CollectionID:      1,
			ChannelName:       dmChannelName,
			UnflushedSegments: []*datapb.SegmentInfo{},
		}

		err := node.NewDataSyncService(vchan)
		require.NoError(t, err)
		require.Equal(t, 1, len(node.vchan2FlushCh))
		require.Equal(t, 1, len(node.vchan2SyncService))
		time.Sleep(time.Second)

		node.ReleaseDataSyncService(dmChannelName)
		assert.Equal(t, 0, len(node.vchan2FlushCh))
		assert.Equal(t, 0, len(node.vchan2SyncService))

		s, ok := node.vchan2SyncService[dmChannelName]
		assert.False(t, ok)
		assert.Nil(t, s)

	})

	t.Run("Test GetChannelName", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		node := newIDLEDataNodeMock(ctx)

		testCollIDs := []UniqueID{0, 1, 2, 1}
		testSegIDs := []UniqueID{10, 11, 12, 13}
		testchanNames := []string{"a", "b", "c", "d"}

		node.chanMut.Lock()
		for i, name := range testchanNames {
			replica := &SegmentReplica{
				collectionID: testCollIDs[i],
				newSegments:  make(map[UniqueID]*Segment),
			}

			replica.addNewSegment(testSegIDs[i], testCollIDs[i], 0, name, &internalpb.MsgPosition{}, nil)
			node.vchan2SyncService[name] = &dataSyncService{collectionID: testCollIDs[i], replica: replica}
		}
		node.chanMut.Unlock()

		type Test struct {
			inCollID         UniqueID
			expectedChannels []string

			inSegID         UniqueID
			expectedChannel string
		}
		tests := []Test{
			{0, []string{"a"}, 10, "a"},
			{1, []string{"b", "d"}, 11, "b"},
			{2, []string{"c"}, 12, "c"},
			{3, []string{}, 13, "d"},
			{3, []string{}, 100, ""},
		}

		for _, test := range tests {
			actualChannels := node.getChannelNamesbyCollectionID(test.inCollID)
			assert.ElementsMatch(t, test.expectedChannels, actualChannels)

			actualChannel := node.getChannelNamebySegmentID(test.inSegID)
			assert.Equal(t, test.expectedChannel, actualChannel)
		}

		cancel()
	})

	cancel()
	<-node.ctx.Done()
	node.Stop()
}
