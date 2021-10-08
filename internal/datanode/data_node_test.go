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
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

func TestMain(t *testing.M) {
	rand.Seed(time.Now().Unix())
	Params.InitAlias("datanode-alias-1")
	Params.Init()
	// change to specific channel for test
	Params.TimeTickChannelName = Params.TimeTickChannelName + strconv.Itoa(rand.Int())
	Params.SegmentStatisticsChannelName = Params.SegmentStatisticsChannelName + strconv.Itoa(rand.Int())
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
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		node1 := newIDLEDataNodeMock(ctx)
		node1.Init()
		node1.Start()
		node1.Register()
		defer func() {
			err := node1.Stop()
			assert.Nil(t, err)
		}()

		cases := []struct {
			desc       string
			channels   []string
			expect     bool
			failReason string
		}{
			{"test watch channel normally", []string{"datanode-01-test-WatchDmChannel", "datanode-02-test-WatchDmChannels"}, true, ""},
			{"test send empty request", []string{}, false, illegalRequestErrStr},
		}

		for _, testcase := range cases {
			vchannels := []*datapb.VchannelInfo{}
			for _, ch := range testcase.channels {
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

			resp, err := node1.WatchDmChannels(context.TODO(), req)
			assert.NoError(t, err)
			if testcase.expect {
				assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
				assert.NotNil(t, node1.vchan2FlushChs)
				assert.NotNil(t, node1.vchan2SyncService)
				sync, ok := node1.vchan2SyncService[testcase.channels[0]]
				assert.True(t, ok)
				assert.NotNil(t, sync)
				assert.Equal(t, UniqueID(1), sync.collectionID)
				assert.Equal(t, len(testcase.channels), len(node1.vchan2SyncService))
				assert.Equal(t, len(node1.vchan2FlushChs), len(node1.vchan2SyncService))
			} else {
				assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
				assert.Equal(t, testcase.failReason, resp.Reason)
			}
		}
	})

	t.Run("Test WatchDmChannels fails", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		node := newIDLEDataNodeMock(ctx)

		// before healthy
		status, err := node.WatchDmChannels(ctx, &datapb.WatchDmChannelsRequest{})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)

		node.Init()
		node.Start()
		node.Register()
		defer func() {
			err := node.Stop()
			assert.Nil(t, err)
		}()

		node.msFactory = &FailMessageStreamFactory{
			Factory: node.msFactory,
		}

		status, err = node.WatchDmChannels(ctx, &datapb.WatchDmChannelsRequest{
			Vchannels: []*datapb.VchannelInfo{
				{
					CollectionID: collectionID0,
					ChannelName:  "test_channel_name",
				},
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
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

		require.Equal(t, 0, len(node2.vchan2FlushChs))
		require.Equal(t, 0, len(node2.vchan2SyncService))

		err := node2.NewDataSyncService(vchan)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(node2.vchan2FlushChs))
		assert.Equal(t, 1, len(node2.vchan2SyncService))

		err = node2.NewDataSyncService(vchan)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(node2.vchan2FlushChs))
		assert.Equal(t, 1, len(node2.vchan2SyncService))

		cancel()
		<-node2.ctx.Done()
		node2.Stop()
	})

	t.Run("Test FlushSegments", func(t *testing.T) {
		dmChannelName := "fake-dm-channel-test-HEALTHDataNodeMock"

		node1 := newIDLEDataNodeMock(context.TODO())
		node1.Init()
		node1.Start()
		node1.Register()
		defer func() {
			err := node1.Stop()
			assert.Nil(t, err)
		}()

		vchan := &datapb.VchannelInfo{
			CollectionID:      1,
			ChannelName:       dmChannelName,
			UnflushedSegments: []*datapb.SegmentInfo{},
			FlushedSegments:   []*datapb.SegmentInfo{},
		}
		err := node1.NewDataSyncService(vchan)
		assert.Nil(t, err)

		service, ok := node1.vchan2SyncService[dmChannelName]
		assert.True(t, ok)
		service.replica.addNewSegment(0, 1, 1, dmChannelName, &internalpb.MsgPosition{}, &internalpb.MsgPosition{})

		req := &datapb.FlushSegmentsRequest{
			Base:         &commonpb.MsgBase{},
			DbID:         0,
			CollectionID: 1,
			SegmentIDs:   []int64{0},
		}

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			defer wg.Done()

			status, err := node1.FlushSegments(node1.ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, status.ErrorCode)
		}()

		go func() {
			defer wg.Done()

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
			insertStream, err := msFactory.NewMsgStream(node1.ctx)
			assert.NoError(t, err)
			insertStream.AsProducer([]string{dmChannelName})
			insertStream.Start()
			defer insertStream.Close()

			err = insertStream.Broadcast(&timeTickMsgPack)
			assert.NoError(t, err)

			err = insertStream.Broadcast(&timeTickMsgPack)
			assert.NoError(t, err)
		}()

		wg.Wait()
	})

	t.Run("Test GetTimeTickChannel", func(t *testing.T) {
		_, err := node.GetTimeTickChannel(node.ctx)
		assert.NoError(t, err)
	})

	t.Run("Test GetStatisticsChannel", func(t *testing.T) {
		_, err := node.GetStatisticsChannel(node.ctx)
		assert.NoError(t, err)
	})

	t.Run("Test getSystemInfoMetrics", func(t *testing.T) {
		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.NoError(t, err)
		resp, err := node.getSystemInfoMetrics(node.ctx, req)
		assert.NoError(t, err)
		log.Info("Test DataNode.getSystemInfoMetrics",
			zap.String("name", resp.ComponentName),
			zap.String("response", resp.Response))
	})

	t.Run("Test GetMetrics", func(t *testing.T) {
		// server is closed
		stateSave := node.State.Load().(internalpb.StateCode)
		node.State.Store(internalpb.StateCode_Abnormal)
		resp, err := node.GetMetrics(ctx, &milvuspb.GetMetricsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		node.State.Store(stateSave)

		// failed to parse metric type
		invalidRequest := "invalid request"
		resp, err = node.GetMetrics(ctx, &milvuspb.GetMetricsRequest{
			Request: invalidRequest,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// unsupported metric type
		unsupportedMetricType := "unsupported"
		req, err := metricsinfo.ConstructRequestByMetricType(unsupportedMetricType)
		assert.NoError(t, err)
		resp, err = node.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// normal case
		req, err = metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.NoError(t, err)
		resp, err = node.GetMetrics(node.ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		log.Info("Test DataNode.GetMetrics",
			zap.String("name", resp.ComponentName),
			zap.String("response", resp.Response))
	})

	t.Run("Test BackGroundGC", func(te *testing.T) {
		te.Skipf("issue #6574")
		ctx, cancel := context.WithCancel(context.Background())
		node := newIDLEDataNodeMock(ctx)

		collIDCh := make(chan UniqueID)
		node.clearSignal = collIDCh
		go node.BackGroundGC(collIDCh)

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
			}

			collIDCh <- t.collID
		}

		assert.Eventually(t, func() bool {
			node.chanMut.Lock()
			defer node.chanMut.Unlock()
			return len(node.vchan2FlushChs) == 0
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
		require.Equal(t, 1, len(node.vchan2FlushChs))
		require.Equal(t, 1, len(node.vchan2SyncService))
		time.Sleep(100 * time.Millisecond)

		node.ReleaseDataSyncService(dmChannelName)
		assert.Equal(t, 0, len(node.vchan2FlushChs))
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

func TestWatchChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	node := newIDLEDataNodeMock(ctx)
	node.Init()
	node.Start()
	node.Register()

	defer cancel()

	t.Run("test watch channel", func(t *testing.T) {

		kv, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
		require.NoError(t, err)
		ch := fmt.Sprintf("datanode-etcd-test-channel_%d", rand.Int31())
		path := fmt.Sprintf("channel/%d/%s", node.NodeID, ch)
		c := make(chan struct{})
		go func() {
			ec := kv.WatchWithPrefix(fmt.Sprintf("channel/%d", node.NodeID))
			c <- struct{}{}
			cnt := 0
			for {
				evt := <-ec
				for _, event := range evt.Events {
					if strings.Contains(string(event.Kv.Key), ch) {
						cnt++
					}
				}
				if cnt >= 2 {
					break
				}
			}
			c <- struct{}{}
		}()
		// wait for check goroutine start Watch
		<-c

		vchan := &datapb.VchannelInfo{
			CollectionID:      1,
			ChannelName:       ch,
			UnflushedSegments: []*datapb.SegmentInfo{},
		}
		info := &datapb.ChannelWatchInfo{
			State: datapb.ChannelWatchState_Uncomplete,
			Vchan: vchan,
		}
		val, err := proto.Marshal(info)
		assert.Nil(t, err)
		err = kv.Save(path, string(val))
		assert.Nil(t, err)

		// wait for check goroutine received 2 events
		<-c
		node.chanMut.RLock()
		_, has := node.vchan2SyncService[ch]
		node.chanMut.RUnlock()
		assert.True(t, has)

		kv.RemoveWithPrefix(fmt.Sprintf("channel/%d", node.NodeID))
		//TODO there is not way to sync Release done, use sleep for now
		time.Sleep(100 * time.Millisecond)

		node.chanMut.RLock()
		_, has = node.vchan2SyncService[ch]
		node.chanMut.RUnlock()
		assert.False(t, has)
	})

	t.Run("watch dm channel fails", func(t *testing.T) {
		node.WatchDmChannels(context.Background(), &datapb.WatchDmChannelsRequest{})
	})
}
