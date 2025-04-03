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
)

type ShardLeaderManagerSuite struct {
	suite.Suite
	mgr *ShardLeaderManager
}

func (suite *ShardLeaderManagerSuite) SetupTest() {
	suite.mgr = NewShardLeaderManager()
}

func (suite *ShardLeaderManagerSuite) TestNewShardLeaderManager() {
	mgr := NewShardLeaderManager()
	assert.NotNil(suite.T(), mgr)
	assert.NotNil(suite.T(), mgr.channelLeaders)
	assert.NotNil(suite.T(), mgr.notifyFunc)
}

func (suite *ShardLeaderManagerSuite) TestSetNotifyFunc() {
	notified := false
	notifyFunc := func(collectionID ...int64) {
		notified = true
	}

	suite.mgr.SetNotifyFunc(notifyFunc)
	suite.mgr.notifyFunc(1)

	assert.True(suite.T(), notified)
}

func (suite *ShardLeaderManagerSuite) TestUpdate() {
	// Test case 1: No existing leader
	shard1 := &ShardLeader{
		NodeID:       1,
		ChannelName:  "channel-1",
		CollectionID: 100,
		ReplicaID:    1,
		Version:      1,
		Serviceable:  true,
	}

	suite.mgr.Update(shard1)

	// Verify the leader was added
	leaders := suite.mgr.GetShardLeaderList("channel-1")
	assert.NotNil(suite.T(), leaders)
	assert.Equal(suite.T(), shard1, leaders[1])

	// Test case 2: Replace unserviceable with serviceable
	shard2 := &ShardLeader{
		NodeID:       2,
		ChannelName:  "channel-1",
		CollectionID: 100,
		ReplicaID:    1,
		Version:      1,
		Serviceable:  false,
	}

	suite.mgr.Update(shard2)

	// Verify the unserviceable leader was not added
	leaders = suite.mgr.GetShardLeaderList("channel-1")
	assert.Equal(suite.T(), shard1, leaders[1])

	// Test case 3: Higher version replacement
	shard3 := &ShardLeader{
		NodeID:       2,
		ChannelName:  "channel-1",
		CollectionID: 100,
		ReplicaID:    1,
		Version:      2,
		Serviceable:  true,
	}

	suite.mgr.Update(shard3)

	// Verify the higher version leader replaced the old one
	leaders = suite.mgr.GetShardLeaderList("channel-1")
	assert.Equal(suite.T(), shard3, leaders[1])

	// Test case 4: Lower version not replacing higher version
	shard4 := &ShardLeader{
		NodeID:       1,
		ChannelName:  "channel-1",
		CollectionID: 100,
		ReplicaID:    1,
		Version:      1,
		Serviceable:  true,
	}

	suite.mgr.Update(shard4)

	// Verify the lower version leader did not replace the higher version one
	leaders = suite.mgr.GetShardLeaderList("channel-1")
	assert.Equal(suite.T(), shard3, leaders[1])

	// Test case 5: Multiple shards update
	shard5 := &ShardLeader{
		NodeID:       3,
		ChannelName:  "channel-2",
		CollectionID: 100,
		ReplicaID:    2,
		Version:      1,
		Serviceable:  true,
	}

	suite.mgr.Update(shard3, shard5)

	// Verify both shards were updated
	leaders1 := suite.mgr.GetShardLeaderList("channel-1")
	leaders2 := suite.mgr.GetShardLeaderList("channel-2")
	assert.Equal(suite.T(), shard3, leaders1[1])
	assert.Equal(suite.T(), shard5, leaders2[2])
}

func (suite *ShardLeaderManagerSuite) TestRemove() {
	// Add some shard leaders
	shard1 := &ShardLeader{
		NodeID:       1,
		ChannelName:  "channel-1",
		CollectionID: 100,
		ReplicaID:    1,
		Version:      1,
		Serviceable:  true,
	}

	shard2 := &ShardLeader{
		NodeID:       2,
		ChannelName:  "channel-1",
		CollectionID: 100,
		ReplicaID:    2,
		Version:      1,
		Serviceable:  true,
	}

	shard3 := &ShardLeader{
		NodeID:       1,
		ChannelName:  "channel-2",
		CollectionID: 100,
		ReplicaID:    3,
		Version:      1,
		Serviceable:  true,
	}

	suite.mgr.Update(shard1, shard2, shard3)

	// Remove node 1
	suite.mgr.Remove(1)

	// Verify node 1's shards were removed
	leaders1 := suite.mgr.GetShardLeaderList("channel-1")
	leaders2 := suite.mgr.GetShardLeaderList("channel-2")

	assert.Nil(suite.T(), leaders1[1])
	assert.Equal(suite.T(), shard2, leaders1[2])
	assert.Nil(suite.T(), leaders2[3])
}

func (suite *ShardLeaderManagerSuite) TestGetShardLeaderList() {
	// Add some shard leaders
	shard1 := &ShardLeader{
		NodeID:       1,
		ChannelName:  "channel-1",
		CollectionID: 100,
		ReplicaID:    1,
		Version:      1,
		Serviceable:  true,
	}

	shard2 := &ShardLeader{
		NodeID:       2,
		ChannelName:  "channel-1",
		CollectionID: 100,
		ReplicaID:    2,
		Version:      1,
		Serviceable:  true,
	}

	suite.mgr.Update(shard1, shard2)

	// Test getting existing channel
	leaders := suite.mgr.GetShardLeaderList("channel-1")
	assert.NotNil(suite.T(), leaders)
	assert.Equal(suite.T(), shard1, leaders[1])
	assert.Equal(suite.T(), shard2, leaders[2])

	// Test getting non-existing channel
	leaders = suite.mgr.GetShardLeaderList("non-existing")
	assert.Nil(suite.T(), leaders)
}

func (suite *ShardLeaderManagerSuite) TestGetShardLeader() {
	// Add a shard leader
	shard := &ShardLeader{
		NodeID:       1,
		ChannelName:  "channel-1",
		CollectionID: 100,
		ReplicaID:    1,
		Version:      1,
		Serviceable:  true,
	}

	suite.mgr.Update(shard)

	// Test getting existing shard leader
	leader := suite.mgr.GetShardLeader(1, "channel-1")
	assert.Equal(suite.T(), shard, leader)

	// Test getting non-existing shard leader
	leader = suite.mgr.GetShardLeader(2, "channel-1")
	assert.Nil(suite.T(), leader)

	// Test getting shard leader from non-existing channel
	leader = suite.mgr.GetShardLeader(1, "non-existing")
	assert.Nil(suite.T(), leader)
}

func TestShardLeaderManager(t *testing.T) {
	suite.Run(t, new(ShardLeaderManagerSuite))
}
