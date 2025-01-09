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

package meta

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type LeaderViewManagerSuite struct {
	suite.Suite
	// Data
	collections     []int64
	channels        map[int64][]string
	growingSegments map[int64]map[string]int64
	segments        map[int64]map[string][]int64
	nodes           []int64
	leaders         map[int64]map[string]*LeaderView

	// Test object
	mgr *LeaderViewManager
}

func (suite *LeaderViewManagerSuite) SetupSuite() {
	suite.collections = []int64{100, 101}
	suite.channels = map[int64][]string{
		100: {"100-dmc0", "100-dmc1"},
		101: {"101-dmc0", "101-dmc1"},
	}
	suite.growingSegments = map[int64]map[string]int64{
		100: {
			"100-dmc0": 10,
			"100-dmc1": 11,
		},
		101: {
			"101-dmc0": 12,
			"101-dmc1": 13,
		},
	}
	suite.segments = map[int64]map[string][]int64{
		100: {
			"100-dmc0": []int64{1, 2},
			"100-dmc1": {3, 4},
		},
		101: {
			"101-dmc0": {5, 6},
			"101-dmc1": {7, 8},
		},
	}
	suite.nodes = []int64{1, 2}

	// Leaders: 1, 2
	suite.leaders = make(map[int64]map[string]*LeaderView)
	for _, collection := range suite.collections {
		for j := 0; j < 2; j++ {
			channel := suite.channels[collection][j]
			node := suite.nodes[j]
			view := &LeaderView{
				ID:              node,
				CollectionID:    collection,
				Channel:         channel,
				GrowingSegments: map[int64]*Segment{suite.growingSegments[collection][channel]: nil},
				Segments:        make(map[int64]*querypb.SegmentDist),
			}

			for _, segment := range suite.segments[collection][channel] {
				view.Segments[segment] = &querypb.SegmentDist{
					NodeID:  node,
					Version: 0,
				}
			}
			if suite.leaders[node] == nil {
				suite.leaders[node] = map[string]*LeaderView{
					channel: view,
				}
			} else {
				suite.leaders[node][channel] = view
			}
		}
	}
}

func (suite *LeaderViewManagerSuite) SetupTest() {
	suite.mgr = NewLeaderViewManager()
	for id, views := range suite.leaders {
		suite.mgr.Update(id, lo.Values(views)...)
	}
}

func (suite *LeaderViewManagerSuite) TestGetByFilter() {
	// Test WithChannelName
	for collectionID, channels := range suite.channels {
		for _, channel := range channels {
			views := suite.mgr.GetByFilter(WithChannelName2LeaderView(channel))
			suite.Len(views, 1)
			suite.Equal(collectionID, views[0].CollectionID)
		}
	}

	// Test WithCollection
	for _, collectionID := range suite.collections {
		views := suite.mgr.GetByFilter(WithCollectionID2LeaderView(collectionID))
		suite.Len(views, 2)
		suite.Equal(collectionID, views[0].CollectionID)
	}

	// Test WithNodeID
	for _, nodeID := range suite.nodes {
		views := suite.mgr.GetByFilter(WithNodeID2LeaderView(nodeID))
		suite.Len(views, 2)
		for _, view := range views {
			suite.Equal(nodeID, view.ID)
		}
	}

	// Test WithReplica
	for i, collectionID := range suite.collections {
		replica := newReplica(&querypb.Replica{
			ID:           int64(i),
			CollectionID: collectionID,
			Nodes:        suite.nodes,
		})
		views := suite.mgr.GetByFilter(WithReplica2LeaderView(replica))
		suite.Len(views, 2)
	}

	// Test WithSegment
	for _, leaders := range suite.leaders {
		for _, leader := range leaders {
			for sid := range leader.Segments {
				views := suite.mgr.GetByFilter(WithSegment2LeaderView(sid, false))
				suite.Len(views, 1)
				suite.Equal(views[0].ID, leader.ID)
				suite.Equal(views[0].Channel, leader.Channel)
			}

			for sid := range leader.GrowingSegments {
				views := suite.mgr.GetByFilter(WithSegment2LeaderView(sid, true))
				suite.Len(views, 1)
				suite.Equal(views[0].ID, leader.ID)
				suite.Equal(views[0].Channel, leader.Channel)
			}

			view := suite.mgr.GetLeaderShardView(leader.ID, leader.Channel)
			suite.Equal(view.ID, leader.ID)
			suite.Equal(view.Channel, leader.Channel)
		}
	}
}

func (suite *LeaderViewManagerSuite) TestGetLatestShardLeader() {
	nodeID := int64(1001)
	collectionID := suite.collections[0]
	channel := suite.channels[collectionID][0]
	// add duplicate shard leader
	view := &LeaderView{
		ID:              nodeID,
		CollectionID:    collectionID,
		Channel:         channel,
		GrowingSegments: map[int64]*Segment{suite.growingSegments[collectionID][channel]: nil},
		Segments:        make(map[int64]*querypb.SegmentDist),
	}

	for _, segment := range suite.segments[collectionID][channel] {
		view.Segments[segment] = &querypb.SegmentDist{
			NodeID:  nodeID,
			Version: 1000,
		}
	}
	view.Version = 1000

	suite.mgr.Update(nodeID, view)

	leader := suite.mgr.GetLatestShardLeaderByFilter(WithChannelName2LeaderView(channel))
	suite.Equal(nodeID, leader.ID)

	// test replica is nil
	leader = suite.mgr.GetLatestShardLeaderByFilter(WithReplica2LeaderView(nil))
	suite.Nil(leader)
}

func (suite *LeaderViewManagerSuite) TestClone() {
	for _, leaders := range suite.leaders {
		for _, leader := range leaders {
			clone := leader.Clone()
			suite.Equal(leader.ID, clone.ID)
			suite.Equal(leader.Channel, clone.Channel)
			suite.Equal(leader.CollectionID, clone.CollectionID)
		}
	}
}

func (suite *LeaderViewManagerSuite) TestNotifyDelegatorChanges() {
	mgr := NewLeaderViewManager()

	oldViews := []*LeaderView{
		{
			ID:           1,
			CollectionID: 100,
			Channel:      "test-channel-1",
		},
		{
			ID:           1,
			CollectionID: 101,
			Channel:      "test-channel-2",
		},
		{
			ID:           1,
			CollectionID: 102,
			Channel:      "test-channel-3",
		},
	}
	mgr.Update(1, oldViews...)

	newViews := []*LeaderView{
		{
			ID:           1,
			CollectionID: 101,
			Channel:      "test-channel-2",
		},
		{
			ID:           1,
			CollectionID: 102,
			Channel:      "test-channel-3",
		},
		{
			ID:           1,
			CollectionID: 103,
			Channel:      "test-channel-4",
		},
	}

	retSet := typeutil.NewUniqueSet()
	mgr.SetNotifyFunc(func(collectionIDs ...int64) {
		retSet.Insert(collectionIDs...)
	})

	mgr.Update(1, newViews...)
	suite.Equal(2, retSet.Len())
	suite.True(retSet.Contain(100))
	suite.True(retSet.Contain(103))

	newViews1 := []*LeaderView{
		{
			ID:                 1,
			CollectionID:       101,
			Channel:            "test-channel-2",
			UnServiceableError: errors.New("test error"),
		},
		{
			ID:                 1,
			CollectionID:       102,
			Channel:            "test-channel-3",
			UnServiceableError: errors.New("test error"),
		},
		{
			ID:                 1,
			CollectionID:       103,
			Channel:            "test-channel-4",
			UnServiceableError: errors.New("test error"),
		},
	}

	retSet.Clear()
	mgr.Update(1, newViews1...)
	suite.Equal(3, len(retSet))
	suite.True(retSet.Contain(101))
	suite.True(retSet.Contain(102))
	suite.True(retSet.Contain(103))

	newViews2 := []*LeaderView{
		{
			ID:                 1,
			CollectionID:       101,
			Channel:            "test-channel-2",
			UnServiceableError: errors.New("test error"),
		},
		{
			ID:           1,
			CollectionID: 102,
			Channel:      "test-channel-3",
		},
		{
			ID:           1,
			CollectionID: 103,
			Channel:      "test-channel-4",
		},
	}

	retSet.Clear()
	mgr.Update(1, newViews2...)
	suite.Equal(2, len(retSet))
	suite.True(retSet.Contain(102))
	suite.True(retSet.Contain(103))
}

func TestLeaderViewManager(t *testing.T) {
	suite.Run(t, new(LeaderViewManagerSuite))
}

func TestGetLeaderView(t *testing.T) {
	manager := NewLeaderViewManager()
	leaderView1 := &LeaderView{
		ID:           1,
		CollectionID: 100,
		Channel:      "channel-1",
		Version:      1,
		Segments:     map[int64]*querypb.SegmentDist{1: {NodeID: 1}},
		GrowingSegments: map[int64]*Segment{
			1: {SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "channel-1", NumOfRows: 1000, State: commonpb.SegmentState_Growing}, Node: 1},
		},
		TargetVersion:      1,
		NumOfGrowingRows:   1000,
		UnServiceableError: nil,
	}

	leaderView2 := &LeaderView{
		ID:           2,
		CollectionID: 200,
		Channel:      "channel-2",
		Version:      1,
		Segments:     map[int64]*querypb.SegmentDist{2: {NodeID: 2}},
		GrowingSegments: map[int64]*Segment{
			2: {SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 200, PartitionID: 20, InsertChannel: "channel-2", NumOfRows: 2000, State: commonpb.SegmentState_Growing}, Node: 2},
		},
		TargetVersion:      1,
		NumOfGrowingRows:   2000,
		UnServiceableError: nil,
	}

	manager.Update(1, leaderView1)
	manager.Update(2, leaderView2)

	// Call GetLeaderView
	leaderViews := manager.GetLeaderView(0)
	jsonOutput, err := json.Marshal(leaderViews)
	assert.NoError(t, err)

	var result []*metricsinfo.LeaderView
	err = json.Unmarshal(jsonOutput, &result)
	assert.NoError(t, err)
	assert.Len(t, result, 2)

	checkResult := func(lv *metricsinfo.LeaderView) {
		if lv.LeaderID == 1 {
			assert.Equal(t, int64(100), lv.CollectionID)
			assert.Equal(t, "channel-1", lv.Channel)
			assert.Equal(t, int64(1), lv.Version)
			assert.Len(t, lv.SealedSegments, 1)
			assert.Len(t, lv.GrowingSegments, 1)
			assert.Equal(t, int64(1), lv.SealedSegments[0].SegmentID)
			assert.Equal(t, int64(1), lv.GrowingSegments[0].SegmentID)
		} else if lv.LeaderID == 2 {
			assert.Equal(t, int64(200), lv.CollectionID)
			assert.Equal(t, "channel-2", lv.Channel)
			assert.Equal(t, int64(1), lv.Version)
			assert.Len(t, lv.SealedSegments, 1)
			assert.Len(t, lv.GrowingSegments, 1)
			assert.Equal(t, int64(2), lv.SealedSegments[0].SegmentID)
			assert.Equal(t, int64(2), lv.GrowingSegments[0].SegmentID)
		} else {
			assert.Failf(t, "unexpected leader id", "unexpected leader id %d", lv.LeaderID)
		}
	}

	for _, lv := range result {
		checkResult(lv)
	}

	leaderViews = manager.GetLeaderView(1)
	assert.Len(t, leaderViews, 0)

	leaderViews = manager.GetLeaderView(100)
	assert.Len(t, leaderViews, 1)
}
