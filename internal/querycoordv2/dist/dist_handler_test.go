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
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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
	executedFlagChan chan struct{}
	dist             *meta.DistributionManager
	target           *meta.MockTargetManager

	handler *distHandler
}

func (suite *DistHandlerSuite) SetupSuite() {
	paramtable.Init()
	suite.nodeID = 1
	suite.client = session.NewMockCluster(suite.T())
	suite.nodeManager = session.NewNodeManager()
	suite.scheduler = task.NewMockScheduler(suite.T())
	suite.dist = meta.NewDistributionManager()

	suite.target = meta.NewMockTargetManager(suite.T())
	suite.ctx = context.Background()

	suite.executedFlagChan = make(chan struct{}, 1)
	suite.scheduler.EXPECT().GetExecutedFlag(mock.Anything).Return(suite.executedFlagChan).Maybe()
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

	syncTargetVersionFn := func(collectionID int64) {}
	suite.handler = newDistHandler(suite.ctx, suite.nodeID, suite.client, suite.nodeManager, suite.scheduler, suite.dist, suite.target, syncTargetVersionFn)
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

	syncTargetVersionFn := func(collectionID int64) {}
	suite.handler = newDistHandler(suite.ctx, suite.nodeID, suite.client, suite.nodeManager, suite.scheduler, suite.dist, suite.target, syncTargetVersionFn)
	defer suite.handler.stop()

	time.Sleep(3 * time.Second)
}

func (suite *DistHandlerSuite) TestForcePullDist() {
	if suite.dispatchMockCall != nil {
		suite.dispatchMockCall.Unset()
		suite.dispatchMockCall = nil
	}

	suite.target.EXPECT().GetSealedSegmentsByChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(map[int64]*datapb.SegmentInfo{}).Maybe()

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
				Collection: 1,
				Channel:    "test-channel-1",
			},
		},
		LastModifyTs: 1,
	}, nil)
	suite.executedFlagChan <- struct{}{}
	syncTargetVersionFn := func(collectionID int64) {}
	suite.handler = newDistHandler(suite.ctx, suite.nodeID, suite.client, suite.nodeManager, suite.scheduler, suite.dist, suite.target, syncTargetVersionFn)
	defer suite.handler.stop()

	time.Sleep(300 * time.Millisecond)
}

func TestDistHandlerSuite(t *testing.T) {
	suite.Run(t, new(DistHandlerSuite))
}
