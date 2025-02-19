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

package datacoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/testutils"
)

func TestCluster(t *testing.T) {
	suite.Run(t, new(ClusterSuite))
}

func getWatchKV(t *testing.T) kv.WatchKV {
	rootPath := "/etcd/test/root/" + t.Name()
	kv, err := etcdkv.NewWatchKVFactory(rootPath, &Params.EtcdCfg)
	require.NoError(t, err)

	return kv
}

type ClusterSuite struct {
	testutils.PromMetricsSuite

	mockKv        *mocks.WatchKV
	mockChManager *MockChannelManager
	mockSession   *session.MockDataNodeManager
}

func (suite *ClusterSuite) SetupTest() {
	suite.mockKv = mocks.NewWatchKV(suite.T())
	suite.mockChManager = NewMockChannelManager(suite.T())
	suite.mockSession = session.NewMockDataNodeManager(suite.T())
}

func (suite *ClusterSuite) TearDownTest() {}

func (suite *ClusterSuite) TestStartup() {
	nodes := []*session.NodeInfo{
		{NodeID: 1, Address: "addr1"},
		{NodeID: 2, Address: "addr2"},
		{NodeID: 3, Address: "addr3"},
		{NodeID: 4, Address: "addr4"},
	}
	suite.mockSession.EXPECT().AddSession(mock.Anything).Return().Times(len(nodes))
	suite.mockChManager.EXPECT().Startup(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, legacys []int64, nodeIDs []int64) error {
			suite.ElementsMatch(lo.Map(nodes, func(info *session.NodeInfo, _ int) int64 { return info.NodeID }), nodeIDs)
			return nil
		}).Once()

	cluster := NewClusterImpl(suite.mockSession, suite.mockChManager)
	err := cluster.Startup(context.Background(), nodes)
	suite.NoError(err)
}

func (suite *ClusterSuite) TestRegister() {
	info := &session.NodeInfo{NodeID: 1, Address: "addr1"}

	suite.mockSession.EXPECT().AddSession(mock.Anything).Return().Once()
	suite.mockChManager.EXPECT().AddNode(mock.Anything).
		RunAndReturn(func(nodeID int64) error {
			suite.EqualValues(info.NodeID, nodeID)
			return nil
		}).Once()

	cluster := NewClusterImpl(suite.mockSession, suite.mockChManager)
	err := cluster.Register(info)
	suite.NoError(err)
}

func (suite *ClusterSuite) TestUnregister() {
	info := &session.NodeInfo{NodeID: 1, Address: "addr1"}

	suite.mockSession.EXPECT().DeleteSession(mock.Anything).Return().Once()
	suite.mockChManager.EXPECT().DeleteNode(mock.Anything).
		RunAndReturn(func(nodeID int64) error {
			suite.EqualValues(info.NodeID, nodeID)
			return nil
		}).Once()

	cluster := NewClusterImpl(suite.mockSession, suite.mockChManager)
	err := cluster.UnRegister(info)
	suite.NoError(err)
}

func (suite *ClusterSuite) TestWatch() {
	var (
		ch           string   = "ch-1"
		collectionID UniqueID = 1
	)

	suite.mockChManager.EXPECT().Watch(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, channel RWChannel) error {
			suite.EqualValues(ch, channel.GetName())
			suite.EqualValues(collectionID, channel.GetCollectionID())
			return nil
		}).Once()

	cluster := NewClusterImpl(suite.mockSession, suite.mockChManager)
	err := cluster.Watch(context.Background(), getChannel(ch, collectionID))
	suite.NoError(err)
}

func (suite *ClusterSuite) TestFlush() {
	suite.mockChManager.EXPECT().GetChannel(mock.Anything, mock.Anything).
		RunAndReturn(func(nodeID int64, channel string) (RWChannel, bool) {
			if nodeID == 1 {
				return nil, false
			}
			return getChannel("ch-1", 2), true
		}).Twice()

	suite.mockSession.EXPECT().Flush(mock.Anything, mock.Anything, mock.Anything).Once()

	cluster := NewClusterImpl(suite.mockSession, suite.mockChManager)

	err := cluster.Flush(context.Background(), 1, "ch-1", nil)
	suite.Error(err)

	err = cluster.Flush(context.Background(), 2, "ch-1", nil)
	suite.NoError(err)
}

func (suite *ClusterSuite) TestFlushChannels() {
	suite.Run("empty channel", func() {
		suite.SetupTest()

		cluster := NewClusterImpl(suite.mockSession, suite.mockChManager)
		err := cluster.FlushChannels(context.Background(), 1, 0, nil)
		suite.NoError(err)
	})

	suite.Run("channel not match with node", func() {
		suite.SetupTest()

		suite.mockChManager.EXPECT().Match(mock.Anything, mock.Anything).Return(false).Once()
		cluster := NewClusterImpl(suite.mockSession, suite.mockChManager)
		err := cluster.FlushChannels(context.Background(), 1, 0, []string{"ch-1", "ch-2"})
		suite.Error(err)
	})

	suite.Run("channel match with node", func() {
		suite.SetupTest()

		channels := []string{"ch-1", "ch-2"}
		suite.mockChManager.EXPECT().Match(mock.Anything, mock.Anything).Return(true).Times(len(channels))
		suite.mockSession.EXPECT().FlushChannels(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		cluster := NewClusterImpl(suite.mockSession, suite.mockChManager)
		err := cluster.FlushChannels(context.Background(), 1, 0, channels)
		suite.NoError(err)
	})
}

func (suite *ClusterSuite) TestQuerySlot() {
	suite.Run("query slot failed", func() {
		suite.SetupTest()
		suite.mockSession.EXPECT().GetSessionIDs().Return([]int64{1}).Once()
		suite.mockSession.EXPECT().QuerySlot(int64(1)).Return(nil, errors.New("mock err")).Once()
		cluster := NewClusterImpl(suite.mockSession, suite.mockChManager)
		nodeSlots := cluster.QuerySlots()
		suite.Equal(0, len(nodeSlots))
	})

	suite.Run("normal", func() {
		suite.SetupTest()
		suite.mockSession.EXPECT().GetSessionIDs().Return([]int64{1, 2, 3, 4}).Once()
		suite.mockSession.EXPECT().QuerySlot(int64(1)).Return(&datapb.QuerySlotResponse{NumSlots: 1}, nil).Once()
		suite.mockSession.EXPECT().QuerySlot(int64(2)).Return(&datapb.QuerySlotResponse{NumSlots: 2}, nil).Once()
		suite.mockSession.EXPECT().QuerySlot(int64(3)).Return(&datapb.QuerySlotResponse{NumSlots: 3}, nil).Once()
		suite.mockSession.EXPECT().QuerySlot(int64(4)).Return(&datapb.QuerySlotResponse{NumSlots: 4}, nil).Once()
		cluster := NewClusterImpl(suite.mockSession, suite.mockChManager)
		nodeSlots := cluster.QuerySlots()
		suite.Equal(int64(1), nodeSlots[1])
		suite.Equal(int64(2), nodeSlots[2])
		suite.Equal(int64(3), nodeSlots[3])
		suite.Equal(int64(4), nodeSlots[4])
	})
}
