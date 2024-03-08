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

func (suite *LeaderViewManagerSuite) TestGetBy() {
	// Test WithChannelName
	for collectionID, channels := range suite.channels {
		for _, channel := range channels {
			views := suite.mgr.GetByFilter(WithChannelName(channel))
			suite.Len(views, 1)
			suite.Equal(collectionID, views[0].CollectionID)
		}
	}

	// Test WithCollection
	for _, collectionID := range suite.collections {
		views := suite.mgr.GetByFilter(WithCollectionID(collectionID))
		suite.Len(views, 2)
		suite.Equal(collectionID, views[0].CollectionID)
	}

	// Test WithNodeID
	for _, nodeID := range suite.nodes {
		views := suite.mgr.GetByFilter(WithNodeID(nodeID))
		suite.Len(views, 2)
		for _, view := range views {
			suite.Equal(nodeID, view.ID)
		}
	}

	// Test WithReplica
	for i, collectionID := range suite.collections {
		replica := &Replica{
			Replica: &querypb.Replica{
				ID:           int64(i),
				CollectionID: collectionID,
				Nodes:        suite.nodes,
			},
			nodes: typeutil.NewUniqueSet(suite.nodes...),
		}
		views := suite.mgr.GetByFilter(WithReplica(replica))
		suite.Len(views, 2)
	}

	// Test WithSegment
	for _, leaders := range suite.leaders {
		for _, leader := range leaders {
			for sid := range leader.Segments {
				views := suite.mgr.GetByFilter(WithSegment(sid, false))
				suite.Len(views, 1)
				suite.Equal(views[0].ID, leader.ID)
				suite.Equal(views[0].Channel, leader.Channel)
			}

			for sid := range leader.GrowingSegments {
				views := suite.mgr.GetByFilter(WithSegment(sid, true))
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

func TestLeaderViewManager(t *testing.T) {
	suite.Run(t, new(LeaderViewManagerSuite))
}
