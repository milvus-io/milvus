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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type ChannelDistManagerSuite struct {
	suite.Suite
	dist       *ChannelDistManager
	collection int64
	nodes      []int64
	channels   map[string]*DmChannel
}

func (suite *ChannelDistManagerSuite) SetupSuite() {
	// Replica 0: 0, 2
	// Replica 1: 1
	suite.collection = 10
	suite.nodes = []int64{0, 1, 2}
	suite.channels = map[string]*DmChannel{
		"dmc0": DmChannelFromVChannel(&datapb.VchannelInfo{
			CollectionID: suite.collection,
			ChannelName:  "dmc0",
		}),
		"dmc1": DmChannelFromVChannel(&datapb.VchannelInfo{
			CollectionID: suite.collection,
			ChannelName:  "dmc1",
		}),
	}
}

func (suite *ChannelDistManagerSuite) SetupTest() {
	suite.dist = NewChannelDistManager()
	// Distribution:
	// node 0 contains channel dmc0
	// node 1 contains channel dmc0, dmc1
	// node 2 contains channel dmc1
	suite.dist.Update(suite.nodes[0], suite.channels["dmc0"].Clone())
	suite.dist.Update(suite.nodes[1], suite.channels["dmc0"].Clone(), suite.channels["dmc1"].Clone())
	suite.dist.Update(suite.nodes[2], suite.channels["dmc1"].Clone())
}

func (suite *ChannelDistManagerSuite) TestGetBy() {
	dist := suite.dist

	// Test GetAll
	channels := dist.GetByFilter()
	suite.Len(channels, 4)

	// Test GetByNode
	for _, node := range suite.nodes {
		channels := dist.GetByFilter(WithNodeID2Channel(node))
		suite.AssertNode(channels, node)
	}

	// Test GetByCollection
	channels = dist.GetByCollectionAndFilter(suite.collection)
	suite.Len(channels, 4)
	suite.AssertCollection(channels, suite.collection)
	channels = dist.GetByCollectionAndFilter(-1)
	suite.Len(channels, 0)

	// Test GetByNodeAndCollection
	// 1. Valid node and valid collection
	for _, node := range suite.nodes {
		channels := dist.GetByCollectionAndFilter(suite.collection, WithNodeID2Channel(node))
		suite.AssertNode(channels, node)
		suite.AssertCollection(channels, suite.collection)
	}

	// 2. Valid node and invalid collection
	channels = dist.GetByCollectionAndFilter(-1, WithNodeID2Channel(suite.nodes[1]))
	suite.Len(channels, 0)

	// 3. Invalid node and valid collection
	channels = dist.GetByCollectionAndFilter(suite.collection, WithNodeID2Channel(-1))
	suite.Len(channels, 0)
}

func (suite *ChannelDistManagerSuite) TestGetShardLeader() {
	replicas := []*Replica{
		NewReplica(
			&querypb.Replica{
				CollectionID: suite.collection,
			},
			typeutil.NewUniqueSet(suite.nodes[0], suite.nodes[2]),
		),
		NewReplica(
			&querypb.Replica{
				CollectionID: suite.collection,
			},
			typeutil.NewUniqueSet(suite.nodes[1]),
		),
	}

	// Test on replica 0
	leader0, ok := suite.dist.GetShardLeader(replicas[0], "dmc0")
	suite.True(ok)
	suite.Equal(suite.nodes[0], leader0)
	leader1, ok := suite.dist.GetShardLeader(replicas[0], "dmc1")
	suite.True(ok)
	suite.Equal(suite.nodes[2], leader1)

	// Test on replica 1
	leader0, ok = suite.dist.GetShardLeader(replicas[1], "dmc0")
	suite.True(ok)
	suite.Equal(suite.nodes[1], leader0)
	leader1, ok = suite.dist.GetShardLeader(replicas[1], "dmc1")
	suite.True(ok)
	suite.Equal(suite.nodes[1], leader1)

	// Test no shard leader for given channel
	_, ok = suite.dist.GetShardLeader(replicas[0], "invalid-shard")
	suite.False(ok)

	// Test on replica 0
	leaders := suite.dist.GetShardLeadersByReplica(replicas[0])
	suite.Len(leaders, 2)
	suite.Equal(leaders["dmc0"], suite.nodes[0])
	suite.Equal(leaders["dmc1"], suite.nodes[2])

	// Test on replica 1
	leaders = suite.dist.GetShardLeadersByReplica(replicas[1])
	suite.Len(leaders, 2)
	suite.Equal(leaders["dmc0"], suite.nodes[1])
	suite.Equal(leaders["dmc1"], suite.nodes[1])
}

func (suite *ChannelDistManagerSuite) AssertNames(channels []*DmChannel, names ...string) bool {
	for _, channel := range channels {
		hasChannel := false
		for _, name := range names {
			if channel.ChannelName == name {
				hasChannel = true
				break
			}
		}
		if !suite.True(hasChannel, "channel %v not in the given expected list %+v", channel.ChannelName, names) {
			return false
		}
	}
	return true
}

func (suite *ChannelDistManagerSuite) AssertNode(channels []*DmChannel, node int64) bool {
	for _, channel := range channels {
		if !suite.Equal(node, channel.Node) {
			return false
		}
	}
	return true
}

func (suite *ChannelDistManagerSuite) AssertCollection(channels []*DmChannel, collection int64) bool {
	for _, channel := range channels {
		if !suite.Equal(collection, channel.GetCollectionID()) {
			return false
		}
	}
	return true
}

func TestChannelDistManager(t *testing.T) {
	suite.Run(t, new(ChannelDistManagerSuite))
}

func TestGetChannelDistJSON(t *testing.T) {
	manager := NewChannelDistManager()
	channel1 := DmChannelFromVChannel(&datapb.VchannelInfo{
		CollectionID: 100,
		ChannelName:  "channel-1",
	})
	channel1.Node = 1
	channel1.Version = 1

	channel2 := DmChannelFromVChannel(&datapb.VchannelInfo{
		CollectionID: 200,
		ChannelName:  "channel-2",
	})
	channel2.Node = 2
	channel2.Version = 1

	manager.Update(1, channel1)
	manager.Update(2, channel2)

	channels := manager.GetChannelDist(0)
	assert.Equal(t, 2, len(channels))

	checkResult := func(channel *metricsinfo.DmChannel) {
		if channel.NodeID == 1 {
			assert.Equal(t, "channel-1", channel.ChannelName)
			assert.Equal(t, int64(100), channel.CollectionID)
		} else if channel.NodeID == 2 {
			assert.Equal(t, "channel-2", channel.ChannelName)
			assert.Equal(t, int64(200), channel.CollectionID)
		} else {
			assert.Failf(t, "unexpected node id", "unexpected node id %d", channel.NodeID)
		}
	}

	for _, channel := range channels {
		checkResult(channel)
	}
}
