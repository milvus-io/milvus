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

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type LeaderViewManagerSuite struct {
	suite.Suite
	// Data
	collections     []int64
	channels        map[int64][]string
	growingSegments map[int64]map[string]int64
	segments        map[int64][]int64
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
	suite.segments = map[int64][]int64{
		100: {1, 2, 3, 4},
		101: {5, 6, 7, 8},
	}
	suite.nodes = []int64{1, 2, 3, 4}

	// Leaders: 1, 2
	suite.leaders = make(map[int64]map[string]*LeaderView)
	for _, collection := range suite.collections {
		for j := 1; j <= 2; j++ {
			channel := suite.channels[collection][j-1]
			view := &LeaderView{
				ID:              int64(j),
				CollectionID:    collection,
				Channel:         channel,
				GrowingSegments: map[int64]*Segment{suite.growingSegments[collection][channel]: nil},
				Segments:        make(map[int64]*querypb.SegmentDist),
			}
			for k, segment := range suite.segments[collection] {
				view.Segments[segment] = &querypb.SegmentDist{
					NodeID:  suite.nodes[k],
					Version: 0,
				}
			}
			suite.leaders[int64(j)] = map[string]*LeaderView{
				suite.channels[collection][j-1]: view,
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

func (suite *LeaderViewManagerSuite) TestGetDist() {
	mgr := suite.mgr

	// Test GetSegmentDist
	for segmentID := int64(1); segmentID <= 13; segmentID++ {
		nodes := mgr.GetSegmentDist(segmentID)
		suite.AssertSegmentDist(segmentID, nodes)

		for _, node := range nodes {
			segments := mgr.GetSegmentByNode(node)
			suite.Contains(segments, segmentID)
		}
	}

	// Test GetSealedSegmentDist
	for segmentID := int64(1); segmentID <= 13; segmentID++ {
		nodes := mgr.GetSealedSegmentDist(segmentID)
		suite.AssertSegmentDist(segmentID, nodes)

		for _, node := range nodes {
			segments := mgr.GetSegmentByNode(node)
			suite.Contains(segments, segmentID)
		}
	}

	// Test GetGrowingSegmentDist
	for segmentID := int64(1); segmentID <= 13; segmentID++ {
		nodes := mgr.GetGrowingSegmentDist(segmentID)

		for _, node := range nodes {
			segments := mgr.GetSegmentByNode(node)
			suite.Contains(segments, segmentID)
			suite.Contains(suite.leaders, node)
		}
	}

	// Test GetChannelDist
	for _, shards := range suite.channels {
		for _, shard := range shards {
			nodes := mgr.GetChannelDist(shard)
			suite.AssertChannelDist(shard, nodes)
		}
	}

	// test get growing segments
	segments := mgr.GetGrowingSegments(101, 1)
	suite.Len(segments, 1)
}

func (suite *LeaderViewManagerSuite) TestGetLeader() {
	mgr := suite.mgr

	// Test GetLeaderView
	for leader, view := range suite.leaders {
		leaderView := mgr.GetLeaderView(leader)
		suite.Equal(view, leaderView)
	}

	// Test GetLeadersByShard
	for leader, leaderViews := range suite.leaders {
		for shard, view := range leaderViews {
			views := mgr.GetLeadersByShard(shard)
			suite.Len(views, 1)
			suite.Equal(view, views[leader])
		}
	}

	// Test GetByCollectionAndNode
	leaders := mgr.GetByCollectionAndNode(101, 1)
	suite.Len(leaders, 1)
}

func (suite *LeaderViewManagerSuite) AssertSegmentDist(segment int64, nodes []int64) bool {
	nodeSet := typeutil.NewUniqueSet(nodes...)
	for leader, views := range suite.leaders {
		for _, view := range views {
			version, ok := view.Segments[segment]
			if ok {
				_, ok = view.GrowingSegments[version.NodeID]
				if !suite.True(nodeSet.Contain(version.NodeID) || version.NodeID == leader && ok) {
					return false
				}
			}
		}
	}
	return true
}

func (suite *LeaderViewManagerSuite) AssertChannelDist(channel string, nodes []int64) bool {
	nodeSet := typeutil.NewUniqueSet(nodes...)
	for leader, views := range suite.leaders {
		_, ok := views[channel]
		if ok {
			if !suite.True(nodeSet.Contain(leader)) {
				return false
			}
		}
	}
	return true
}

func TestLeaderViewManager(t *testing.T) {
	suite.Run(t, new(LeaderViewManagerSuite))
}
