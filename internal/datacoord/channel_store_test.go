package datacoord

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/kv/predicates"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/testutils"
)

func TestStateChannelStore(t *testing.T) {
	suite.Run(t, new(StateChannelStoreSuite))
}

type StateChannelStoreSuite struct {
	testutils.PromMetricsSuite

	mockTxn *mocks.TxnKV
}

func (s *StateChannelStoreSuite) SetupTest() {
	s.mockTxn = mocks.NewTxnKV(s.T())
}

func generateWatchInfo(name string, state datapb.ChannelWatchState) *datapb.ChannelWatchInfo {
	return &datapb.ChannelWatchInfo{
		Vchan: &datapb.VchannelInfo{
			ChannelName: name,
		},
		Schema: &schemapb.CollectionSchema{},
		State:  state,
	}
}

func (s *StateChannelStoreSuite) createChannelInfo(nodeID int64, channels ...RWChannel) *NodeChannelInfo {
	cInfo := &NodeChannelInfo{
		NodeID:   nodeID,
		Channels: make(map[string]RWChannel),
	}
	for _, channel := range channels {
		cInfo.Channels[channel.GetName()] = channel
	}
	return cInfo
}

func (s *StateChannelStoreSuite) TestGetNodeChannelsBy() {
	nodes := []int64{bufferID, 100, 101, 102}
	nodesExcludeBufferID := []int64{100, 101, 102}
	channels := []*StateChannel{
		getChannel("ch1", 1),
		getChannel("ch2", 1),
		getChannel("ch3", 1),
		getChannel("ch4", 1),
		getChannel("ch5", 1),
		getChannel("ch6", 1),
		getChannel("ch7", 1),
	}

	channelsInfo := map[int64]*NodeChannelInfo{
		bufferID: s.createChannelInfo(bufferID, channels[0]),
		100:      s.createChannelInfo(100, channels[1], channels[2]),
		101:      s.createChannelInfo(101, channels[3], channels[4]),
		102:      s.createChannelInfo(102, channels[5], channels[6]), // legacy nodes
	}

	store := NewStateChannelStore(s.mockTxn)
	lo.ForEach(nodes, func(nodeID int64, _ int) { store.AddNode(nodeID) })
	store.channelsInfo = channelsInfo
	lo.ForEach(channels, func(ch *StateChannel, _ int) {
		if ch.GetName() == "ch6" || ch.GetName() == "ch7" {
			ch.setState(Legacy)
		}
		s.Require().True(store.HasChannel(ch.GetName()))
	})
	s.Require().ElementsMatch(nodesExcludeBufferID, store.GetNodes())
	store.SetLegacyChannelByNode(102)

	s.Run("test AddNode RemoveNode", func() {
		var nodeID int64 = 19530
		_, ok := store.channelsInfo[nodeID]
		s.Require().False(ok)
		store.AddNode(nodeID)
		_, ok = store.channelsInfo[nodeID]
		s.True(ok)

		store.RemoveNode(nodeID)
		_, ok = store.channelsInfo[nodeID]
		s.False(ok)
	})

	s.Run("test GetNodeChannels", func() {
		infos := store.GetNodesChannels()
		expectedResults := map[int64][]string{
			100: {"ch2", "ch3"},
			101: {"ch4", "ch5"},
			102: {"ch6", "ch7"},
		}

		s.Equal(3, len(infos))

		lo.ForEach(infos, func(info *NodeChannelInfo, _ int) {
			expectedChannels, ok := expectedResults[info.NodeID]
			s.True(ok)

			gotChannels := lo.Keys(info.Channels)
			s.ElementsMatch(expectedChannels, gotChannels)
		})
	})

	s.Run("test GetBufferChannelInfo", func() {
		info := store.GetBufferChannelInfo()
		s.NotNil(info)

		gotChannels := lo.Keys(info.Channels)
		s.ElementsMatch([]string{"ch1"}, gotChannels)
	})

	s.Run("test GetNode", func() {
		info := store.GetNode(19530)
		s.Nil(info)

		info = store.GetNode(bufferID)
		s.NotNil(info)

		gotChannels := lo.Keys(info.Channels)
		s.ElementsMatch([]string{"ch1"}, gotChannels)
	})

	tests := []struct {
		description      string
		nodeSelector     NodeSelector
		channelSelectors []ChannelSelector

		expectedResult map[int64][]string
	}{
		{"test withnodeIDs bufferID", WithNodeIDs(bufferID), nil, map[int64][]string{bufferID: {"ch1"}}},
		{"test withnodeIDs 100", WithNodeIDs(100), nil, map[int64][]string{100: {"ch2", "ch3"}}},
		{"test withnodeIDs 101 102", WithNodeIDs(101, 102), nil, map[int64][]string{
			101: {"ch4", "ch5"},
			102: {"ch6", "ch7"},
		}},
		{"test withAllNodes", WithAllNodes(), nil, map[int64][]string{
			bufferID: {"ch1"},
			100:      {"ch2", "ch3"},
			101:      {"ch4", "ch5"},
			102:      {"ch6", "ch7"},
		}},
		{"test WithoutBufferNode", WithoutBufferNode(), nil, map[int64][]string{
			100: {"ch2", "ch3"},
			101: {"ch4", "ch5"},
			102: {"ch6", "ch7"},
		}},
		{"test WithoutNodeIDs 100, 101", WithoutNodeIDs(100, 101), nil, map[int64][]string{
			bufferID: {"ch1"},
			102:      {"ch6", "ch7"},
		}},
		{
			"test WithChannelName ch1", WithNodeIDs(bufferID),
			[]ChannelSelector{WithChannelName("ch1")},
			map[int64][]string{
				bufferID: {"ch1"},
			},
		},
		{
			"test WithChannelName ch1, collectionID 1", WithNodeIDs(100),
			[]ChannelSelector{
				WithChannelName("ch2"),
				WithCollectionIDV2(1),
			},
			map[int64][]string{100: {"ch2"}},
		},
		{
			"test WithCollectionID 1", WithAllNodes(),
			[]ChannelSelector{
				WithCollectionIDV2(1),
			},
			map[int64][]string{
				bufferID: {"ch1"},
				100:      {"ch2", "ch3"},
				101:      {"ch4", "ch5"},
				102:      {"ch6", "ch7"},
			},
		},
		{
			"test WithChannelState", WithNodeIDs(102),
			[]ChannelSelector{
				WithChannelStates(Legacy),
			},
			map[int64][]string{
				102: {"ch6", "ch7"},
			},
		},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			if test.channelSelectors == nil {
				test.channelSelectors = []ChannelSelector{}
			}

			infos := store.GetNodeChannelsBy(test.nodeSelector, test.channelSelectors...)
			log.Info("got test infos", zap.Any("infos", infos))
			s.Equal(len(test.expectedResult), len(infos))

			lo.ForEach(infos, func(info *NodeChannelInfo, _ int) {
				expectedChannels, ok := test.expectedResult[info.NodeID]
				s.True(ok)

				gotChannels := lo.Keys(info.Channels)
				s.ElementsMatch(expectedChannels, gotChannels)
			})
		})
	}
}

func (s *StateChannelStoreSuite) TestUpdateWithTxnLimit() {
	tests := []struct {
		description string
		inOpCount   int
		outTxnCount int
	}{
		{"operations count < maxPerTxn", maxOperationsPerTxn - 1, 1},
		{"operations count = maxPerTxn", maxOperationsPerTxn, 1},
		{"operations count > maxPerTxn", maxOperationsPerTxn + 1, 2},
		{"operations count = 2*maxPerTxn", maxOperationsPerTxn * 2, 2},
		{"operations count = 2*maxPerTxn+1", maxOperationsPerTxn*2 + 1, 3},
	}

	for _, test := range tests {
		s.SetupTest()
		s.Run(test.description, func() {
			s.mockTxn.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything).
				Run(func(saves map[string]string, removals []string, preds ...predicates.Predicate) {
					log.Info("test save and remove", zap.Any("saves", saves), zap.Any("removals", removals))
				}).Return(nil).Times(test.outTxnCount)

			store := NewStateChannelStore(s.mockTxn)
			store.AddNode(1)
			s.Require().ElementsMatch([]int64{1}, store.GetNodes())
			s.Require().Equal(0, store.GetNodeChannelCount(1))

			// Get operations
			ops := genChannelOperations(1, Watch, test.inOpCount)
			err := store.Update(ops)
			s.NoError(err)
		})
	}
}

func (s *StateChannelStoreSuite) TestUpdateMeta2000kSegs() {
	ch := getChannel("ch1", 1)
	info := ch.GetWatchInfo()
	// way larger than limit=2097152
	seg2000k := make([]int64, 2000000)
	for i := range seg2000k {
		seg2000k[i] = int64(i)
	}
	info.Vchan.FlushedSegmentIds = seg2000k
	ch.UpdateWatchInfo(info)

	opSet := NewChannelOpSet(
		NewChannelOp(bufferID, Delete, ch),
		NewChannelOp(100, Watch, ch),
	)
	s.SetupTest()
	s.mockTxn.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything).
		Run(func(saves map[string]string, removals []string, preds ...predicates.Predicate) {
		}).Return(nil).Once()

	store := NewStateChannelStore(s.mockTxn)
	store.AddNode(100)
	s.Require().Equal(0, store.GetNodeChannelCount(100))
	store.addAssignment(bufferID, ch)
	s.Require().Equal(1, store.GetNodeChannelCount(bufferID))

	err := store.updateMeta(opSet)
	s.NoError(err)

	got := store.GetNodeChannelsBy(WithNodeIDs(100))
	s.NotNil(got)
	s.Require().Equal(1, len(got))
	gotInfo := got[0]
	s.ElementsMatch([]string{"ch1"}, lo.Keys(gotInfo.Channels))
}

func (s *StateChannelStoreSuite) TestUpdateMeta() {
	tests := []struct {
		description string

		opSet       *ChannelOpSet
		nodeIDs     []int64
		channels    []*StateChannel
		assignments map[int64][]string

		outAssignments map[int64][]string
	}{
		{
			"delete_watch_ch1 from bufferID to nodeID=100",
			NewChannelOpSet(
				NewChannelOp(bufferID, Delete, getChannel("ch1", 1)),
				NewChannelOp(100, Watch, getChannel("ch1", 1)),
			),
			[]int64{bufferID, 100},
			[]*StateChannel{getChannel("ch1", 1)},
			map[int64][]string{
				bufferID: {"ch1"},
			},
			map[int64][]string{
				100: {"ch1"},
			},
		},
		{
			"delete_watch_ch1 from lagecyID=99 to nodeID=100",
			NewChannelOpSet(
				NewChannelOp(99, Delete, getChannel("ch1", 1)),
				NewChannelOp(100, Watch, getChannel("ch1", 1)),
			),
			[]int64{bufferID, 99, 100},
			[]*StateChannel{getChannel("ch1", 1)},
			map[int64][]string{
				99: {"ch1"},
			},
			map[int64][]string{
				100: {"ch1"},
			},
		},
		{
			"release from nodeID=100",
			NewChannelOpSet(
				NewChannelOp(100, Release, getChannel("ch1", 1)),
			),
			[]int64{bufferID, 100},
			[]*StateChannel{getChannel("ch1", 1)},
			map[int64][]string{
				100: {"ch1"},
			},
			map[int64][]string{
				100: {"ch1"},
			},
		},
		{
			"watch a new channel from nodeID=100",
			NewChannelOpSet(
				NewChannelOp(100, Watch, getChannel("ch1", 1)),
			),
			[]int64{bufferID, 100},
			[]*StateChannel{getChannel("ch1", 1)},
			map[int64][]string{
				100: {"ch1"},
			},
			map[int64][]string{
				100: {"ch1"},
			},
		},
		{
			"Delete remove a channelfrom nodeID=100",
			NewChannelOpSet(
				NewChannelOp(100, Delete, getChannel("ch1", 1)),
			),
			[]int64{bufferID, 100},
			[]*StateChannel{getChannel("ch1", 1)},
			map[int64][]string{
				100: {"ch1"},
			},
			map[int64][]string{
				100: {},
			},
		},
	}
	s.SetupTest()
	s.mockTxn.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything).
		Run(func(saves map[string]string, removals []string, preds ...predicates.Predicate) {
		}).Return(nil).Times(len(tests))

	for _, test := range tests {
		s.Run(test.description, func() {
			store := NewStateChannelStore(s.mockTxn)

			lo.ForEach(test.nodeIDs, func(nodeID int64, _ int) {
				store.AddNode(nodeID)
				s.Require().Equal(0, store.GetNodeChannelCount(nodeID))
			})
			c := make(map[string]*StateChannel)
			lo.ForEach(test.channels, func(ch *StateChannel, _ int) { c[ch.GetName()] = ch })
			for nodeID, channels := range test.assignments {
				lo.ForEach(channels, func(ch string, _ int) {
					store.addAssignment(nodeID, c[ch])
				})
				s.Require().Equal(1, store.GetNodeChannelCount(nodeID))
			}

			err := store.updateMeta(test.opSet)
			s.NoError(err)

			for nodeID, channels := range test.outAssignments {
				got := store.GetNodeChannelsBy(WithNodeIDs(nodeID))
				s.NotNil(got)
				s.Require().Equal(1, len(got))
				info := got[0]
				s.ElementsMatch(channels, lo.Keys(info.Channels))
			}
		})
	}
}

func (s *StateChannelStoreSuite) TestUpdateState() {
	tests := []struct {
		description string

		inSuccess       bool
		inChannelState  ChannelState
		outChannelState ChannelState
	}{
		{"input standby, fail", false, Standby, Standby},
		{"input standby, success", true, Standby, ToWatch},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			store := NewStateChannelStore(s.mockTxn)

			ch := "ch-1"
			channel := NewStateChannel(getChannel(ch, 1))
			channel.setState(test.inChannelState)
			store.channelsInfo[1] = &NodeChannelInfo{
				NodeID: bufferID,
				Channels: map[string]RWChannel{
					ch: channel,
				},
			}

			store.UpdateState(test.inSuccess, channel)
			s.Equal(test.outChannelState, channel.currentState)
		})
	}
}

func (s *StateChannelStoreSuite) TestReload() {
	type item struct {
		nodeID      int64
		channelName string
	}
	type testCase struct {
		tag    string
		items  []item
		expect map[int64]int
	}

	cases := []testCase{
		{
			tag:    "empty",
			items:  []item{},
			expect: map[int64]int{},
		},
		{
			tag: "normal",
			items: []item{
				{nodeID: 1, channelName: "dml1_v0"},
				{nodeID: 1, channelName: "dml2_v1"},
				{nodeID: 2, channelName: "dml3_v0"},
			},
			expect: map[int64]int{1: 2, 2: 1},
		},
		{
			tag: "buffer",
			items: []item{
				{nodeID: bufferID, channelName: "dml1_v0"},
			},
			expect: map[int64]int{bufferID: 1},
		},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			s.mockTxn.ExpectedCalls = nil

			var keys, values []string
			for _, item := range tc.items {
				keys = append(keys, fmt.Sprintf("channel_store/%d/%s", item.nodeID, item.channelName))
				info := generateWatchInfo(item.channelName, datapb.ChannelWatchState_WatchSuccess)
				bs, err := proto.Marshal(info)
				s.Require().NoError(err)
				values = append(values, string(bs))
			}
			s.mockTxn.EXPECT().LoadWithPrefix(mock.AnythingOfType("string")).Return(keys, values, nil)

			store := NewStateChannelStore(s.mockTxn)
			err := store.Reload()
			s.Require().NoError(err)

			for nodeID, expect := range tc.expect {
				s.MetricsEqual(metrics.DataCoordDmlChannelNum.WithLabelValues(strconv.FormatInt(nodeID, 10)), float64(expect))
			}
		})
	}
}

func genChannelOperations(nodeID int64, opType ChannelOpType, num int) *ChannelOpSet {
	channels := make([]RWChannel, 0, num)
	for i := 0; i < num; i++ {
		name := fmt.Sprintf("ch%d", i)
		channel := NewStateChannel(getChannel(name, 1))
		channel.Info = generateWatchInfo(name, datapb.ChannelWatchState_ToWatch)
		channels = append(channels, channel)
	}

	ops := NewChannelOpSet(NewChannelOp(nodeID, opType, channels...))
	return ops
}
