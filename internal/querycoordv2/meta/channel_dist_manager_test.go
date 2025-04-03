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
		"dmc0": {
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: suite.collection,
				ChannelName:  "dmc0",
			},
			Node:    0,
			Version: 1,
			View: &LeaderView{
				ID:           1,
				CollectionID: suite.collection,
				Channel:      "dmc0",
				Version:      1,
				Status: &querypb.LeaderViewStatus{
					Serviceable: true,
				},
			},
		},
		"dmc1": {
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: suite.collection,
				ChannelName:  "dmc1",
			},
			Node:    1,
			Version: 1,
			View: &LeaderView{
				ID:           1,
				CollectionID: suite.collection,
				Channel:      "dmc1",
				Version:      1,
				Status: &querypb.LeaderViewStatus{
					Serviceable: true,
				},
			},
		},
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

func TestDmChannelClone(t *testing.T) {
	// Test that Clone properly copies the View field including Status
	originalChannel := &DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 100,
			ChannelName:  "test-channel",
		},
		Node:    1,
		Version: 10,
		View: &LeaderView{
			ID:           5,
			CollectionID: 100,
			Channel:      "test-channel",
			Version:      20,
			Status: &querypb.LeaderViewStatus{
				Serviceable: true,
			},
		},
	}

	clonedChannel := originalChannel.Clone()

	// Check all fields were properly cloned
	assert.Equal(t, originalChannel.GetCollectionID(), clonedChannel.GetCollectionID())
	assert.Equal(t, originalChannel.GetChannelName(), clonedChannel.GetChannelName())
	assert.Equal(t, originalChannel.Node, clonedChannel.Node)
	assert.Equal(t, originalChannel.Version, clonedChannel.Version)

	// Check that View was properly cloned
	assert.NotNil(t, clonedChannel.View)
	assert.Equal(t, originalChannel.View.ID, clonedChannel.View.ID)
	assert.Equal(t, originalChannel.View.CollectionID, clonedChannel.View.CollectionID)
	assert.Equal(t, originalChannel.View.Channel, clonedChannel.View.Channel)
	assert.Equal(t, originalChannel.View.Version, clonedChannel.View.Version)

	// Check that Status was properly cloned
	assert.NotNil(t, clonedChannel.View.Status)
	assert.Equal(t, originalChannel.View.Status.GetServiceable(), clonedChannel.View.Status.GetServiceable())

	// Verify that modifying the clone doesn't affect the original
	clonedChannel.View.Status.Serviceable = false
	assert.True(t, originalChannel.View.Status.GetServiceable())
	assert.False(t, clonedChannel.View.Status.GetServiceable())
}

func TestDmChannelIsServiceable(t *testing.T) {
	// Test serviceable channel
	serviceableChannel := &DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 100,
			ChannelName:  "serviceable",
		},
		View: &LeaderView{
			Status: &querypb.LeaderViewStatus{
				Serviceable: true,
			},
		},
	}
	assert.True(t, serviceableChannel.IsServiceable())

	// Test non-serviceable channel
	nonServiceableChannel := &DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 100,
			ChannelName:  "non-serviceable",
		},
		View: &LeaderView{
			Status: &querypb.LeaderViewStatus{
				Serviceable: false,
			},
		},
	}
	assert.False(t, nonServiceableChannel.IsServiceable())
}

func (suite *ChannelDistManagerSuite) TestUpdateReturnsNewServiceableChannels() {
	dist := NewChannelDistManager()

	// Create a non-serviceable channel
	nonServiceableChannel := suite.channels["dmc0"].Clone()
	nonServiceableChannel.View.Status.Serviceable = false

	// Update with non-serviceable channel first
	newServiceableChannels := dist.Update(suite.nodes[0], nonServiceableChannel)
	suite.Len(newServiceableChannels, 0, "No new serviceable channels should be returned")

	// Now update with a serviceable channel
	serviceableChannel := nonServiceableChannel.Clone()
	serviceableChannel.View.Status.Serviceable = true

	newServiceableChannels = dist.Update(suite.nodes[0], serviceableChannel)
	suite.Len(newServiceableChannels, 1, "One new serviceable channel should be returned")
	suite.Equal("dmc0", newServiceableChannels[0].GetChannelName())

	// Update with same serviceable channel should not return it again
	newServiceableChannels = dist.Update(suite.nodes[0], serviceableChannel)
	suite.Len(newServiceableChannels, 0, "Already serviceable channel should not be returned")

	// Add a different channel that's serviceable
	newChannel := suite.channels["dmc1"].Clone()
	newChannel.View.Status.Serviceable = true

	newServiceableChannels = dist.Update(suite.nodes[0], serviceableChannel, newChannel)
	suite.Len(newServiceableChannels, 1, "Only the new serviceable channel should be returned")
	suite.Equal("dmc1", newServiceableChannels[0].GetChannelName())
}

func (suite *ChannelDistManagerSuite) TestGetShardLeader() {
	dist := NewChannelDistManager()

	// Create a replica
	replicaPB := &querypb.Replica{
		ID:           1,
		CollectionID: suite.collection,
		Nodes:        []int64{0, 2},
	}
	replica := NewReplica(replicaPB)

	// Create channels with different versions and serviceability
	channel1Node0 := suite.channels["dmc0"].Clone()
	channel1Node0.Version = 1
	channel1Node0.View.Status.Serviceable = false

	channel1Node2 := suite.channels["dmc0"].Clone()
	channel1Node2.Node = 2
	channel1Node2.Version = 2
	channel1Node2.View.Status.Serviceable = false

	// Update with non-serviceable channels
	dist.Update(0, channel1Node0)
	dist.Update(2, channel1Node2)

	// Test getting leader with no serviceable channels - should return highest version
	leader := dist.GetShardLeader("dmc0", replica)
	suite.NotNil(leader)
	suite.Equal(int64(2), leader.Node)
	suite.Equal(int64(2), leader.Version)

	// Now make one channel serviceable
	channel1Node0.View.Status.Serviceable = true
	dist.Update(0, channel1Node0)

	// Test that serviceable channel is preferred even with lower version
	leader = dist.GetShardLeader("dmc0", replica)
	suite.NotNil(leader)
	suite.Equal(int64(0), leader.Node)
	suite.Equal(int64(1), leader.Version)
	suite.True(leader.IsServiceable())

	// Make both channels serviceable but with different versions
	channel1Node2.View.Status.Serviceable = true
	dist.Update(2, channel1Node2)

	// Test that highest version is chosen among serviceable channels
	leader = dist.GetShardLeader("dmc0", replica)
	suite.NotNil(leader)
	suite.Equal(int64(2), leader.Node)
	suite.Equal(int64(2), leader.Version)
	suite.True(leader.IsServiceable())

	// Test channel not in replica
	// Create a new replica with different nodes
	replicaPB = &querypb.Replica{
		ID:           1,
		CollectionID: suite.collection,
		Nodes:        []int64{1},
	}
	replicaWithDifferentNodes := NewReplica(replicaPB)
	leader = dist.GetShardLeader("dmc0", replicaWithDifferentNodes)
	suite.Nil(leader)

	// Test nonexistent channel
	leader = dist.GetShardLeader("nonexistent", replica)
	suite.Nil(leader)
}

func TestGetChannelDistJSON(t *testing.T) {
	manager := NewChannelDistManager()
	channel1 := &DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 100,
			ChannelName:  "channel-1",
		},
		Node:    1,
		Version: 1,
		View: &LeaderView{
			ID:           1,
			CollectionID: 100,
			Channel:      "channel-1",
			Version:      1,
			Status: &querypb.LeaderViewStatus{
				Serviceable: true,
			},
		},
	}
	channel2 := &DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 200,
			ChannelName:  "channel-2",
		},
		Node:    2,
		Version: 1,
		View: &LeaderView{
			ID:           1,
			CollectionID: 200,
			Channel:      "channel-2",
			Version:      1,
			Status: &querypb.LeaderViewStatus{
				Serviceable: true,
			},
		},
	}
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
