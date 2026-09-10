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

package dist

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type DistHandlerSuite struct {
	suite.Suite

	ctx    context.Context
	meta   *meta.Meta
	broker *meta.MockBroker

	nodeID           int64
	client           *session.MockCluster
	nodeManager      *session.NodeManager
	scheduler        *task.MockScheduler
	dispatchMockCall *mock.Call
	dist             *meta.DistributionManager
	target           *meta.MockTargetManager
	handler          *distHandler
}

func (suite *DistHandlerSuite) SetupSuite() {
	paramtable.Init()
	suite.nodeID = 1
	suite.client = session.NewMockCluster(suite.T())
	suite.nodeManager = session.NewNodeManager()
	suite.scheduler = task.NewMockScheduler(suite.T())
	suite.dist = meta.NewDistributionManager(suite.nodeManager)

	suite.target = meta.NewMockTargetManager(suite.T())
	suite.ctx = context.Background()

	suite.target.EXPECT().GetSealedSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	suite.target.EXPECT().GetDmChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	suite.target.EXPECT().GetCollectionTargetVersion(mock.Anything, mock.Anything, mock.Anything).Return(1011).Maybe()
}

func (suite *DistHandlerSuite) TestBasic() {
	if suite.dispatchMockCall != nil {
		suite.dispatchMockCall.Unset()
		suite.dispatchMockCall = nil
	}

	suite.target.EXPECT().GetSealedSegmentsByChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(map[int64]*datapb.SegmentInfo{})
	suite.dispatchMockCall = suite.scheduler.EXPECT().Dispatch(mock.Anything).Maybe()
	suite.nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.client.EXPECT().GetDataDistribution(mock.Anything, mock.Anything, mock.Anything).Return(&querypb.GetDataDistributionResponse{
		Status: merr.Success(),
		NodeID: 1,
		Channels: []*querypb.ChannelVersionInfo{
			{
				Channel:    "test-channel-1",
				Collection: 1,
				Version:    1,
			},
		},
		Segments: []*querypb.SegmentVersionInfo{
			{
				ID:         1,
				Collection: 1,
				Partition:  1,
				Channel:    "test-channel-1",
				Version:    1,
			},
		},

		LeaderViews: []*querypb.LeaderView{
			{
				Collection:    1,
				Channel:       "test-channel-1",
				TargetVersion: 1011,
			},
		},
		LastModifyTs: 1,
	}, nil)

	suite.handler = newDistHandler(suite.ctx, suite.nodeID, suite.client, suite.nodeManager, suite.scheduler, suite.dist, suite.target, func(collectionID ...int64) {})
	defer suite.handler.stop()

	time.Sleep(3 * time.Second)
}

func (suite *DistHandlerSuite) TestGetDistributionFailed() {
	if suite.dispatchMockCall != nil {
		suite.dispatchMockCall.Unset()
		suite.dispatchMockCall = nil
	}
	suite.target.EXPECT().GetSealedSegmentsByChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(map[int64]*datapb.SegmentInfo{}).Maybe()
	suite.dispatchMockCall = suite.scheduler.EXPECT().Dispatch(mock.Anything).Maybe()
	suite.nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.client.EXPECT().GetDataDistribution(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("fake error"))

	suite.handler = newDistHandler(suite.ctx, suite.nodeID, suite.client, suite.nodeManager, suite.scheduler, suite.dist, suite.target, func(collectionID ...int64) {})
	defer suite.handler.stop()

	time.Sleep(3 * time.Second)
}

func TestGetDistributionDisableDelta(t *testing.T) {
	params := paramtable.Get()
	params.Save("queryCoord.enableDataDistributionDelta", "false")
	defer params.Reset("queryCoord.enableDataDistributionDelta")

	client := session.NewMockCluster(t)
	handler := &distHandler{
		nodeID:       1,
		client:       client,
		lastUpdateTs: 100,
	}

	var gotReq *querypb.GetDataDistributionRequest
	client.EXPECT().GetDataDistribution(mock.Anything, int64(1), mock.Anything).RunAndReturn(
		func(ctx context.Context, nodeID int64, req *querypb.GetDataDistributionRequest) (*querypb.GetDataDistributionResponse, error) {
			gotReq = req
			return &querypb.GetDataDistributionResponse{
				Status: merr.Success(),
				NodeID: nodeID,
			}, nil
		},
	)

	_, err := handler.getDistribution(context.Background())
	assert.NoError(t, err)
	assert.False(t, gotReq.GetSupportDelta())
	assert.EqualValues(t, 100, gotReq.GetLastUpdateTs())
	assert.EqualValues(t, 100, handler.lastUpdateTs)
}

func TestUnserviceableDelegatorDistributionPullFallback(t *testing.T) {
	tests := []struct {
		name                 string
		isDelta              bool
		expectedLastUpdateTs int64
	}{
		{
			name:                 "full report keeps polling fallback",
			expectedLastUpdateTs: 0,
		},
		{
			name:                 "delta report keeps incremental base",
			isDelta:              true,
			expectedLastUpdateTs: 100,
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nodeID := int64(i + 1)
			nodeManager := session.NewNodeManager()
			nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
				NodeID:   nodeID,
				Address:  "localhost:19530",
				Hostname: "localhost",
			}))
			target := meta.NewMockTargetManager(t)
			target.EXPECT().GetDmChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
			handler := &distHandler{
				nodeID:       nodeID,
				nodeManager:  nodeManager,
				dist:         meta.NewDistributionManager(nodeManager),
				target:       target,
				lastUpdateTs: 99,
			}
			defer metrics.QueryCoordLastHeartbeatTimeStamp.DeleteLabelValues(fmt.Sprint(nodeID))

			handler.handleDistResp(context.Background(), &querypb.GetDataDistributionResponse{
				Status:       merr.Success(),
				NodeID:       nodeID,
				LastModifyTs: 100,
				IsDelta:      test.isDelta,
				Channels: []*querypb.ChannelVersionInfo{
					{Channel: "test-channel", Collection: 10, Version: 1},
				},
				LeaderViews: []*querypb.LeaderView{
					{
						Collection: 10,
						Channel:    "test-channel",
						Status:     &querypb.LeaderViewStatus{Serviceable: false},
					},
				},
			})

			assert.Equal(t, test.expectedLastUpdateTs, handler.lastUpdateTs)
		})
	}
}

func (suite *DistHandlerSuite) TestHandlerWithSyncDelegatorChanges() {
	if suite.dispatchMockCall != nil {
		suite.dispatchMockCall.Unset()
		suite.dispatchMockCall = nil
	}

	suite.target.EXPECT().GetSealedSegmentsByChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(map[int64]*datapb.SegmentInfo{}).Maybe()
	suite.dispatchMockCall = suite.scheduler.EXPECT().Dispatch(mock.Anything).Maybe()
	suite.nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))

	// Test scenario: update segments and channels distribution without replicaMgr
	suite.client.EXPECT().GetDataDistribution(mock.Anything, mock.Anything, mock.Anything).Return(&querypb.GetDataDistributionResponse{
		Status: merr.Success(),
		NodeID: 1,
		Channels: []*querypb.ChannelVersionInfo{
			{
				Channel:    "test-channel-1",
				Collection: 1,
				Version:    1,
			},
		},
		Segments: []*querypb.SegmentVersionInfo{
			{
				ID:         1,
				Collection: 1,
				Partition:  1,
				Channel:    "test-channel-1",
				Version:    1,
			},
		},
		LeaderViews: []*querypb.LeaderView{
			{
				Collection:    1,
				Channel:       "test-channel-1",
				TargetVersion: 1011,
				Status: &querypb.LeaderViewStatus{
					Serviceable: true,
				},
			},
		},
		LastModifyTs: 2, // Different from previous test to ensure update happens
	}, nil)

	notifyCounter := atomic.NewInt32(0)
	notifyFunc := func(collectionID ...int64) {
		suite.Require().Equal(1, len(collectionID))
		suite.Require().Equal(int64(1), collectionID[0])
		notifyCounter.Inc()
	}

	suite.handler = newDistHandler(suite.ctx, suite.nodeID, suite.client, suite.nodeManager, suite.scheduler, suite.dist, suite.target, notifyFunc)
	defer suite.handler.stop()

	// Wait for distribution to be processed
	time.Sleep(1000 * time.Millisecond)

	// Verify that the distributions were updated correctly
	segments := suite.dist.SegmentDistManager.GetByFilter(meta.WithNodeID(1))
	suite.Require().Equal(1, len(segments))
	suite.Require().Equal(int64(1), segments[0].ID)

	channels := suite.dist.ChannelDistManager.GetByFilter(meta.WithNodeID2Channel(1))
	suite.Require().Equal(1, len(channels))
	suite.Require().Equal("test-channel-1", channels[0].ChannelName)

	// Verify that the notification was called
	suite.Require().Greater(notifyCounter.Load(), int32(0))
}

// TestHeartbeatMetricsRecording tests that heartbeat metrics are properly recorded
func TestHeartbeatMetricsRecording(t *testing.T) {
	// Arrange: Create test response with a unique nodeID to avoid test interference
	nodeID := time.Now().UnixNano() % 1000000 // Use timestamp-based unique ID
	resp := &querypb.GetDataDistributionResponse{
		Status:       merr.Success(),
		NodeID:       nodeID,
		LastModifyTs: 1,
	}

	// Create mock node
	nodeManager := session.NewNodeManager()
	nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   nodeID,
		Address:  "localhost:19530",
		Hostname: "localhost",
	})
	nodeManager.Add(nodeInfo)

	// Mock time.Now() to get predictable timestamp
	expectedTimestamp := time.Unix(1640995200, 0) // 2022-01-01 00:00:00 UTC
	mockTimeNow := mockey.Mock(time.Now).Return(expectedTimestamp).Build()
	defer mockTimeNow.UnPatch()

	// Record the initial state of the metric for our specific nodeID
	initialMetricValue := getMetricValueForNode(fmt.Sprint(nodeID))

	// Create dist handler
	ctx := context.Background()
	handler := &distHandler{
		nodeID:      nodeID,
		nodeManager: nodeManager,
		dist:        meta.NewDistributionManager(nodeManager),
		target:      meta.NewTargetManager(nil, nil),
		scheduler:   task.NewScheduler(ctx, nil, nil, nil, nil, nil, nil),
	}

	// Act: Handle distribution response
	handler.handleDistResp(ctx, resp)

	// Assert: Verify our specific metric was recorded with the expected value
	finalMetricValue := getMetricValueForNode(fmt.Sprint(nodeID))

	// Check that the metric value changed and matches our expected timestamp
	assert.NotEqual(t, initialMetricValue, finalMetricValue, "Metric value should have changed")
	assert.Equal(t, float64(expectedTimestamp.UnixNano()), finalMetricValue, "Metric should record the expected timestamp")

	// Clean up: Remove the test metric to avoid affecting other tests
	metrics.QueryCoordLastHeartbeatTimeStamp.DeleteLabelValues(fmt.Sprint(nodeID))
}

func TestDeltaDistributionPatch(t *testing.T) {
	ctx := context.Background()
	nodeID := time.Now().UnixNano() % 1000000
	nodeManager := session.NewNodeManager()
	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   nodeID,
		Address:  "localhost:19530",
		Hostname: "localhost",
	}))
	target := meta.NewMockTargetManager(t)
	target.EXPECT().GetSealedSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	target.EXPECT().GetDmChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	target.EXPECT().GetCollectionTargetVersion(mock.Anything, mock.Anything, mock.Anything).Return(int64(101)).Maybe()
	dist := meta.NewDistributionManager(nodeManager)
	handler := &distHandler{
		nodeID:      nodeID,
		nodeManager: nodeManager,
		dist:        dist,
		target:      target,
	}
	defer metrics.QueryCoordLastHeartbeatTimeStamp.DeleteLabelValues(fmt.Sprint(nodeID))

	channel := "test-channel-delta"
	releasedChannel := "test-channel-delta-released"
	fullResp := &querypb.GetDataDistributionResponse{
		Status:       merr.Success(),
		NodeID:       nodeID,
		LastModifyTs: 1,
		Segments: []*querypb.SegmentVersionInfo{
			{ID: 1, Collection: 10, Partition: 100, Channel: channel, Version: 1},
		},
		Channels: []*querypb.ChannelVersionInfo{
			{Channel: channel, Collection: 10, Version: 1},
			{Channel: releasedChannel, Collection: 11, Version: 1},
		},
		LeaderViews: []*querypb.LeaderView{
			{
				Collection:    10,
				Channel:       channel,
				TargetVersion: 101,
				SegmentDist:   map[int64]*querypb.SegmentDist{1: {NodeID: nodeID, Version: 1}},
				Status:        &querypb.LeaderViewStatus{Serviceable: true},
			},
			{
				Collection:    11,
				Channel:       releasedChannel,
				TargetVersion: 101,
				Status:        &querypb.LeaderViewStatus{Serviceable: true},
			},
		},
	}
	handler.handleDistResp(ctx, fullResp)
	channels := dist.ChannelDistManager.GetByFilter(meta.WithNodeID2Channel(nodeID))
	assert.Len(t, channels, 2)
	findChannel := func(channelName string) *meta.DmChannel {
		channels := dist.ChannelDistManager.GetByFilter(meta.WithNodeID2Channel(nodeID))
		for _, channel := range channels {
			if channel.GetChannelName() == channelName {
				return channel
			}
		}
		return nil
	}

	addResp := &querypb.GetDataDistributionResponse{
		Status:            merr.Success(),
		NodeID:            nodeID,
		LastModifyTs:      2,
		IsDelta:           true,
		TotalSegmentCount: 2,
		TotalChannelCount: 2,
		Segments: []*querypb.SegmentVersionInfo{
			{ID: 2, Collection: 10, Partition: 100, Channel: channel, Version: 2},
		},
		Channels: []*querypb.ChannelVersionInfo{
			{Channel: channel, Collection: 10, Version: 1},
		},
		LeaderViews: []*querypb.LeaderView{
			{
				Collection:    10,
				Channel:       channel,
				TargetVersion: 101,
				SegmentDist:   map[int64]*querypb.SegmentDist{1: {NodeID: nodeID, Version: 1}, 2: {NodeID: nodeID, Version: 2}},
				Status:        &querypb.LeaderViewStatus{Serviceable: true},
			},
		},
	}
	handler.handleDistResp(ctx, addResp)

	segments := dist.SegmentDistManager.GetByFilter(meta.WithNodeID(nodeID))
	assert.Len(t, segments, 2)
	channels = dist.ChannelDistManager.GetByFilter(meta.WithNodeID2Channel(nodeID))
	assert.Len(t, channels, 2)
	patchedChannel := findChannel(channel)
	assert.NotNil(t, patchedChannel)
	assert.Len(t, patchedChannel.View.Segments, 2)
	assert.Contains(t, patchedChannel.View.Segments, int64(1))
	assert.Contains(t, patchedChannel.View.Segments, int64(2))
	assert.NotNil(t, findChannel(releasedChannel))

	growingOnlyResp := &querypb.GetDataDistributionResponse{
		Status:            merr.Success(),
		NodeID:            nodeID,
		LastModifyTs:      3,
		IsDelta:           true,
		TotalSegmentCount: 2,
		TotalChannelCount: 2,
		Channels: []*querypb.ChannelVersionInfo{
			{Channel: channel, Collection: 10, Version: 1},
		},
		LeaderViews: []*querypb.LeaderView{
			{
				Collection:      10,
				Channel:         channel,
				TargetVersion:   101,
				SegmentDist:     map[int64]*querypb.SegmentDist{1: {NodeID: nodeID, Version: 1}, 2: {NodeID: nodeID, Version: 2}},
				GrowingSegments: map[int64]*msgpb.MsgPosition{3: {}},
				Status:          &querypb.LeaderViewStatus{Serviceable: true},
			},
		},
	}
	handler.handleDistResp(ctx, growingOnlyResp)
	patchedChannel = findChannel(channel)
	assert.Len(t, patchedChannel.View.Segments, 2)
	assert.Contains(t, patchedChannel.View.GrowingSegments, int64(3))

	removeResp := &querypb.GetDataDistributionResponse{
		Status:            merr.Success(),
		NodeID:            nodeID,
		LastModifyTs:      4,
		IsDelta:           true,
		RemovedSegmentIds: []int64{1},
		TotalSegmentCount: 1,
		TotalChannelCount: 2,
		Channels: []*querypb.ChannelVersionInfo{
			{Channel: channel, Collection: 10, Version: 1},
		},
		LeaderViews: []*querypb.LeaderView{
			{
				Collection:    10,
				Channel:       channel,
				TargetVersion: 101,
				SegmentDist:   map[int64]*querypb.SegmentDist{2: {NodeID: nodeID, Version: 2}},
				Status:        &querypb.LeaderViewStatus{Serviceable: true},
			},
		},
	}
	handler.handleDistResp(ctx, removeResp)

	segments = dist.SegmentDistManager.GetByFilter(meta.WithNodeID(nodeID))
	assert.Len(t, segments, 1)
	assert.Equal(t, int64(2), segments[0].GetID())
	patchedChannel = findChannel(channel)
	assert.Len(t, patchedChannel.View.Segments, 1)
	assert.NotContains(t, patchedChannel.View.Segments, int64(1))
	assert.Contains(t, patchedChannel.View.Segments, int64(2))

	removeChannelResp := &querypb.GetDataDistributionResponse{
		Status:              merr.Success(),
		NodeID:              nodeID,
		LastModifyTs:        5,
		IsDelta:             true,
		TotalSegmentCount:   1,
		TotalChannelCount:   1,
		RemovedChannelNames: []string{releasedChannel},
	}
	handler.handleDistResp(ctx, removeChannelResp)
	channels = dist.ChannelDistManager.GetByFilter(meta.WithNodeID2Channel(nodeID))
	assert.Len(t, channels, 1)
	assert.Equal(t, channel, channels[0].GetChannelName())
}

func TestDeltaDistributionPatchNotifiesNewServiceableChannel(t *testing.T) {
	ctx := context.Background()
	nodeID := time.Now().UnixNano() % 1000000
	collectionID := int64(10)
	channel := "test-channel-delta-notify"
	nodeManager := session.NewNodeManager()
	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   nodeID,
		Address:  "localhost:19530",
		Hostname: "localhost",
	}))
	target := meta.NewMockTargetManager(t)
	target.EXPECT().GetDmChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	dist := meta.NewDistributionManager(nodeManager)
	handler := &distHandler{
		nodeID:      nodeID,
		nodeManager: nodeManager,
		dist:        dist,
		target:      target,
	}
	defer metrics.QueryCoordLastHeartbeatTimeStamp.DeleteLabelValues(fmt.Sprint(nodeID))

	fullResp := &querypb.GetDataDistributionResponse{
		Status:       merr.Success(),
		NodeID:       nodeID,
		LastModifyTs: 1,
		Channels: []*querypb.ChannelVersionInfo{
			{Channel: channel, Collection: collectionID, Version: 1},
		},
		LeaderViews: []*querypb.LeaderView{
			{
				Collection: collectionID,
				Channel:    channel,
				Status:     &querypb.LeaderViewStatus{Serviceable: false},
			},
		},
	}
	handler.handleDistResp(ctx, fullResp)

	var notified []int64
	handler.SetNotifyFunc(func(collectionID ...int64) {
		notified = append(notified, collectionID...)
	})
	deltaResp := &querypb.GetDataDistributionResponse{
		Status:            merr.Success(),
		NodeID:            nodeID,
		LastModifyTs:      2,
		IsDelta:           true,
		TotalChannelCount: 1,
		Channels: []*querypb.ChannelVersionInfo{
			{Channel: channel, Collection: collectionID, Version: 2},
		},
		LeaderViews: []*querypb.LeaderView{
			{
				Collection: collectionID,
				Channel:    channel,
				Status:     &querypb.LeaderViewStatus{Serviceable: true},
			},
		},
	}

	handler.handleDistResp(ctx, deltaResp)

	assert.Equal(t, []int64{collectionID}, notified)
}

// Helper function to get the current metric value for a specific nodeID
func getMetricValueForNode(nodeID string) float64 {
	// Create a temporary registry to capture the current state
	registry := prometheus.NewRegistry()
	registry.MustRegister(metrics.QueryCoordLastHeartbeatTimeStamp)

	metricFamilies, err := registry.Gather()
	if err != nil {
		return -1 // Return -1 if we can't gather metrics
	}

	for _, mf := range metricFamilies {
		if mf.GetName() == "milvus_querycoord_last_heartbeat_timestamp" {
			for _, metric := range mf.GetMetric() {
				for _, label := range metric.GetLabel() {
					if label.GetName() == "node_id" && label.GetValue() == nodeID {
						return metric.GetGauge().GetValue()
					}
				}
			}
		}
	}
	return 0 // Return 0 if metric not found (default value)
}

func TestDispatchLoopUsesDispatchInterval(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.QueryCoordCfg.DistPullInterval.Key, "60000")
	params.Save(params.QueryCoordCfg.DispatchInterval.Key, "10")
	defer params.Reset(params.QueryCoordCfg.DistPullInterval.Key)
	defer params.Reset(params.QueryCoordCfg.DispatchInterval.Key)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dispatched := atomic.NewBool(false)
	scheduler := task.NewMockScheduler(t)
	scheduler.EXPECT().Dispatch(int64(1)).Run(func(nodeID int64) {
		dispatched.Store(true)
		cancel()
	})

	handler := &distHandler{
		nodeID:    1,
		c:         make(chan struct{}),
		scheduler: scheduler,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.startDispatchLoop(ctx)
	}()

	assert.Eventually(t, dispatched.Load, time.Second, 10*time.Millisecond)
	<-done
}

func TestDistHandlerSuite(t *testing.T) {
	suite.Run(t, new(DistHandlerSuite))
}
