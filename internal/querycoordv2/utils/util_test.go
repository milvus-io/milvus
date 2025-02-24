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

package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

type UtilTestSuite struct {
	suite.Suite
	nodeMgr *session.NodeManager
}

func (suite *UtilTestSuite) SetupTest() {
	suite.nodeMgr = session.NewNodeManager()
}

func (suite *UtilTestSuite) setNodeAvailable(nodes ...int64) {
	for _, node := range nodes {
		nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   node,
			Address:  "",
			Hostname: "localhost",
		})
		nodeInfo.SetLastHeartbeat(time.Now())
		suite.nodeMgr.Add(nodeInfo)
	}
}

func (suite *UtilTestSuite) TestCheckLeaderAvaliable() {
	leadview := &meta.LeaderView{
		ID:            1,
		Channel:       "test",
		Segments:      map[int64]*querypb.SegmentDist{2: {NodeID: 2}},
		TargetVersion: 1011,
	}

	mockTargetManager := meta.NewMockTargetManager(suite.T())
	mockTargetManager.EXPECT().GetSealedSegmentsByChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(map[int64]*datapb.SegmentInfo{
		2: {
			ID:            2,
			InsertChannel: "test",
		},
	}).Maybe()
	mockTargetManager.EXPECT().GetCollectionTargetVersion(mock.Anything, mock.Anything, mock.Anything).Return(1011).Maybe()

	suite.setNodeAvailable(1, 2)
	err := CheckDelegatorDataReady(suite.nodeMgr, mockTargetManager, leadview, meta.CurrentTarget)
	suite.NoError(err)
}

func (suite *UtilTestSuite) TestCheckLeaderAvaliableFailed() {
	suite.Run("leader not available", func() {
		leadview := &meta.LeaderView{
			ID:            1,
			Channel:       "test",
			Segments:      map[int64]*querypb.SegmentDist{2: {NodeID: 2}},
			TargetVersion: 1011,
		}
		mockTargetManager := meta.NewMockTargetManager(suite.T())
		mockTargetManager.EXPECT().GetSealedSegmentsByChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(map[int64]*datapb.SegmentInfo{
			2: {
				ID:            2,
				InsertChannel: "test",
			},
		}).Maybe()
		mockTargetManager.EXPECT().GetCollectionTargetVersion(mock.Anything, mock.Anything, mock.Anything).Return(1011).Maybe()
		// leader nodeID=1 not available
		suite.setNodeAvailable(2)
		err := CheckDelegatorDataReady(suite.nodeMgr, mockTargetManager, leadview, meta.CurrentTarget)
		suite.Error(err)
	})

	suite.Run("shard worker not available", func() {
		leadview := &meta.LeaderView{
			ID:            11111,
			Channel:       "test",
			Segments:      map[int64]*querypb.SegmentDist{2: {NodeID: 2}},
			TargetVersion: 1011,
		}

		mockTargetManager := meta.NewMockTargetManager(suite.T())
		mockTargetManager.EXPECT().GetSealedSegmentsByChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(map[int64]*datapb.SegmentInfo{
			2: {
				ID:            2,
				InsertChannel: "test",
			},
		}).Maybe()
		mockTargetManager.EXPECT().GetCollectionTargetVersion(mock.Anything, mock.Anything, mock.Anything).Return(1011).Maybe()
		// leader nodeID=2 not available
		suite.setNodeAvailable(1)
		err := CheckDelegatorDataReady(suite.nodeMgr, mockTargetManager, leadview, meta.CurrentTarget)
		suite.Error(err)
	})

	suite.Run("segment lacks", func() {
		leadview := &meta.LeaderView{
			ID:            1,
			Channel:       "test",
			Segments:      map[int64]*querypb.SegmentDist{2: {NodeID: 2}},
			TargetVersion: 1011,
		}
		mockTargetManager := meta.NewMockTargetManager(suite.T())
		mockTargetManager.EXPECT().GetSealedSegmentsByChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(map[int64]*datapb.SegmentInfo{
			// target segmentID=1 not in leadView
			1: {
				ID:            1,
				InsertChannel: "test",
			},
		}).Maybe()
		mockTargetManager.EXPECT().GetCollectionTargetVersion(mock.Anything, mock.Anything, mock.Anything).Return(1011).Maybe()
		suite.setNodeAvailable(1, 2)
		err := CheckDelegatorDataReady(suite.nodeMgr, mockTargetManager, leadview, meta.CurrentTarget)
		suite.Error(err)
	})

	suite.Run("target version not synced", func() {
		leadview := &meta.LeaderView{
			ID:       1,
			Channel:  "test",
			Segments: map[int64]*querypb.SegmentDist{2: {NodeID: 2}},
		}
		mockTargetManager := meta.NewMockTargetManager(suite.T())
		mockTargetManager.EXPECT().GetSealedSegmentsByChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(map[int64]*datapb.SegmentInfo{
			// target segmentID=1 not in leadView
			1: {
				ID:            1,
				InsertChannel: "test",
			},
		}).Maybe()
		mockTargetManager.EXPECT().GetCollectionTargetVersion(mock.Anything, mock.Anything, mock.Anything).Return(1011).Maybe()
		suite.setNodeAvailable(1, 2)
		err := CheckDelegatorDataReady(suite.nodeMgr, mockTargetManager, leadview, meta.CurrentTarget)
		suite.Error(err)
	})
}

func TestUtilSuite(t *testing.T) {
	suite.Run(t, new(UtilTestSuite))
}
