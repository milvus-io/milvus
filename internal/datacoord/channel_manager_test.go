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
	"path"
	"strconv"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/kv"
	kvmock "github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

func TestChannelManagerHelperFunc(t *testing.T) {
	suite.Run(t, new(ChannelManagerHelperFuncSuite))
}

func TestChannelManager(t *testing.T) {
	suite.Run(t, new(ChannelManagerSuite))
}

type ChannelManagerSuite struct {
	suite.Suite

	kv      kv.MetaKv
	manager *ChannelManager
	session *SessionManager
	prefix  string
}

func getSuccessDNMock() *mocks.MockDataNode {
	mockDN := &mocks.MockDataNode{}

	mockDN.EXPECT().NotifyChannelOperation(mock.Anything, mock.Anything).
		Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
	mockDN.EXPECT().CheckChannelOperationProgress(mock.Anything, mock.Anything).
		Call.Return(func(ctx context.Context, req *datapb.ChannelWatchInfo) *datapb.ChannelOperationProgressResponse {
		return &datapb.ChannelOperationProgressResponse{
			Status:   &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			State:    getSuccessState(req.GetState()),
			Progress: 100}
	}, nil)
	return mockDN
}

func getNotifyFailedDNMock() *mocks.MockDataNode {
	mockDN := &mocks.MockDataNode{}

	mockDN.EXPECT().NotifyChannelOperation(mock.Anything, mock.Anything).
		Return(&commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "Mock notify channel operation failed",
		}, nil)
	return mockDN
}

// waitAndCheckState checks if the DataCoord writes expected state into Etcd
func (s *ChannelManagerSuite) checkState(expectedState datapb.ChannelWatchState, nodeID UniqueID, channelName string, collectionID UniqueID) {
	v, err := s.kv.Load(path.Join(s.prefix, strconv.FormatInt(nodeID, 10), channelName))
	s.NoError(err)
	s.NotEmpty(v)
	watchInfo, err := parseWatchInfo("fake", []byte(v))
	s.NoError(err)

	if watchInfo.GetState() == expectedState {
		s.Equal(watchInfo.Vchan.GetChannelName(), channelName)
		s.Equal(watchInfo.Vchan.GetCollectionID(), collectionID)
	}
}

func (s *ChannelManagerSuite) TearDownTest() {
	s.manager.Close()
	s.kv.RemoveWithPrefix("")
	s.kv.Close()
}

func (s *ChannelManagerSuite) SetupTest() {
	s.kv = &kvmock.MetaKv{}
	s.kv = getMetaKv(s.T())
	s.prefix = Params.CommonCfg.DataCoordWatchSubPath.GetValue()
	s.session = NewSessionManager()
	m, err := NewChannelManager(s.kv, newMockHandler(), s.session, newMockAllocator())
	s.NoError(err)
	s.manager = m
}

func (s *ChannelManagerSuite) TestNotifyFail() {
	s.session.sessions.data = map[UniqueID]*Session{
		1: {
			info:   &NodeInfo{1, "mock_address"},
			client: getNotifyFailedDNMock(),
		},
	}

	err := s.manager.RegisterNode(1)
	s.NoError(err)
	err = s.manager.AddChannel(&channel{Name: "channel1", CollectionID: 100})
	s.NoError(err)

	result := <-s.manager.channelChecker.Watcher()
	s.Equal(datapb.ChannelWatchState_WatchFailure, result.state)
}

func (s *ChannelManagerSuite) TestRegisterAddChannelUnregister() {
	var nodeID UniqueID = 1
	// Register
	err := s.manager.RegisterNode(nodeID)
	s.NoError(err)

	allNodes := s.manager.store.GetNodes()
	s.ElementsMatch(allNodes, []UniqueID{nodeID})

	// AddChannel
	s.session.sessions.data[nodeID] = &Session{
		info:   &NodeInfo{nodeID, "localhost:1234"},
		client: getSuccessDNMock(),
	}

	err = s.manager.AddChannel(&channel{Name: "channel1", CollectionID: 100})
	s.NoError(err)

	count := s.manager.store.GetNodeChannelCount(1)
	s.Equal(1, count)
	s.checkState(datapb.ChannelWatchState_WatchSuccess, nodeID, "channel1", 100)
	watcher, err := s.manager.FindWatcher("channel1")
	s.Equal(nodeID, watcher)
	s.NoError(err)

	// Unregister
	err = s.manager.UnregisterNode(nodeID)
	s.NoError(err)

	allNodes = s.manager.store.GetNodes()
	s.Equal(len(allNodes), 0)

	channelInfo := s.manager.store.GetBufferChannelInfo()
	s.Equal(int64(bufferID), channelInfo.NodeID)
	s.Equal(1, len(channelInfo.Channels))
	s.Equal("channel1", channelInfo.Channels[0].Name)
	s.checkState(datapb.ChannelWatchState_ToWatch, bufferID, "channel1", 100)
	watcher, err = s.manager.FindWatcher("channel1")
	s.Equal(int64(bufferID), watcher)
	s.ErrorIs(errChannelInBuffer, err)
}

func (s *ChannelManagerSuite) TestRegister1Node3Channel() {
	s.session.sessions.data = map[UniqueID]*Session{
		1: {
			info:   &NodeInfo{1, "mock_address"},
			client: getSuccessDNMock(),
		},
		2: {
			info:   &NodeInfo{2, "mock_address"},
			client: getSuccessDNMock(),
		},
	}

	// Prepare Node 1: {channel1, channel2, channel3}
	err := s.manager.RegisterNode(1)
	s.NoError(err)

	allNodes := s.manager.store.GetNodes()
	s.ElementsMatch(allNodes, []UniqueID{1})

	err = s.manager.AddChannel(&channel{Name: "channel1", CollectionID: 100})
	s.NoError(err)
	err = s.manager.AddChannel(&channel{Name: "channel2", CollectionID: 200})
	s.NoError(err)
	err = s.manager.AddChannel(&channel{Name: "channel3", CollectionID: 300})
	s.NoError(err)

	count := s.manager.store.GetNodeChannelCount(1)
	s.Equal(3, count)
	s.True(s.manager.Match(1, "channel1"))
	s.True(s.manager.Match(1, "channel2"))
	s.True(s.manager.Match(1, "channel3"))
	s.checkState(datapb.ChannelWatchState_WatchSuccess, 1, "channel1", 100)
	s.checkState(datapb.ChannelWatchState_WatchSuccess, 1, "channel2", 200)
	s.checkState(datapb.ChannelWatchState_WatchSuccess, 1, "channel3", 300)

	// Register Node 2
	err = s.manager.RegisterNode(2)
	s.NoError(err)

	allNodes = s.manager.store.GetNodes()
	s.Equal(len(allNodes), 2)
	s.ElementsMatch(allNodes, []UniqueID{1, 2})

	s.True(s.manager.Match(1, "channel1"))
	s.True(s.manager.Match(1, "channel2"))
	s.True(s.manager.Match(1, "channel3"))
	s.checkState(datapb.ChannelWatchState_ToRelease, 1, "channel1", 100)
}

type ChannelManagerHelperFuncSuite struct {
	suite.Suite

	manager *ChannelManager
}

func (s *ChannelManagerHelperFuncSuite) SetupSuite() {
	s.manager = &ChannelManager{}
}

func (s *ChannelManagerHelperFuncSuite) TestAllHelperFunc() {
	s.Run("getOldOnlines", func() {
		tests := []struct {
			nodes  []int64
			oNodes []int64

			expectedOut []int64
			desription  string
		}{
			{[]int64{}, []int64{}, []int64{}, "empty both"},
			{[]int64{1}, []int64{}, []int64{}, "empty oNodes"},
			{[]int64{}, []int64{1}, []int64{}, "empty nodes"},
			{[]int64{1}, []int64{1}, []int64{1}, "same one"},
			{[]int64{1, 2}, []int64{1}, []int64{1}, "same one 2"},
			{[]int64{1}, []int64{1, 2}, []int64{1}, "same one 3"},
			{[]int64{1, 2}, []int64{1, 2}, []int64{1, 2}, "same two"},
		}

		for _, test := range tests {
			s.Run(test.desription, func() {
				nodes := getOldOnlines(test.nodes, test.oNodes)
				s.ElementsMatch(test.expectedOut, nodes)
			})
		}
	})

	s.Run("getNewOnLines", func() {
		tests := []struct {
			nodes  []int64
			oNodes []int64

			expectedOut []int64
			desription  string
		}{
			{[]int64{}, []int64{}, []int64{}, "empty both"},
			{[]int64{1}, []int64{}, []int64{1}, "empty oNodes"},
			{[]int64{}, []int64{1}, []int64{}, "empty nodes"},
			{[]int64{1}, []int64{1}, []int64{}, "same one"},
			{[]int64{1, 2}, []int64{1}, []int64{2}, "same one 2"},
			{[]int64{1}, []int64{1, 2}, []int64{}, "same one 3"},
			{[]int64{1, 2}, []int64{1, 2}, []int64{}, "same two"},
		}

		for _, test := range tests {
			s.Run(test.desription, func() {
				nodes := getNewOnLines(test.nodes, test.oNodes)
				s.ElementsMatch(test.expectedOut, nodes)
			})
		}
	})

	s.Run("getOffLines", func() {
		tests := []struct {
			nodes  []int64
			oNodes []int64

			expectedOut []int64
			desription  string
		}{
			{[]int64{}, []int64{}, []int64{}, "empty both"},
			{[]int64{1}, []int64{}, []int64{}, "empty oNodes"},
			{[]int64{}, []int64{1}, []int64{1}, "empty nodes"},
			{[]int64{1}, []int64{1}, []int64{}, "same one"},
			{[]int64{1, 2}, []int64{1}, []int64{}, "same one 2"},
			{[]int64{1}, []int64{1, 2}, []int64{2}, "same one 3"},
			{[]int64{1, 2}, []int64{1, 2}, []int64{}, "same two"},
		}

		for _, test := range tests {
			s.Run(test.desription, func() {
				nodes := getOffLines(test.nodes, test.oNodes)
				s.ElementsMatch(test.expectedOut, nodes)
			})
		}
	})
}
