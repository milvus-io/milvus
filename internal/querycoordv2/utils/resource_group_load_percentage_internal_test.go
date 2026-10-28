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

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// TestPrefetchDelegatorsByChannelIsPerChannelNotPerSegment pins the
// performance half of the target/distribution hoist.
//
// The segment walk in replicaLoadPercentage only ever asks about a segment's
// INSERT CHANNEL, so the distribution lookup belongs once per distinct
// channel. It used to run once per sealed segment, per replica --
// ChannelDistManager.GetByFilter with no node filter walks every node's
// channel collection under an RLock and allocates a result slice, so on a
// collection with tens of thousands of segments that was the dominant cost on
// a path a caller waiting for a resource group polls.
//
// The returned map has exactly one entry per lookup performed, so asserting
// its key set is asserting the call count. 50 segments spread over 2 channels
// must produce 2 entries; a per-segment implementation produces 50.
func TestPrefetchDelegatorsByChannelIsPerChannelNotPerSegment(t *testing.T) {
	nodeMgr := session.NewNodeManager()
	dist := meta.NewDistributionManager(nodeMgr)

	channelTargets := map[string]*meta.DmChannel{
		"dmc0": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "dmc0"}},
		"dmc1": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "dmc1"}},
	}
	segmentTargets := make(map[int64]*datapb.SegmentInfo, 50)
	for i := int64(1); i <= 50; i++ {
		channel := "dmc0"
		if i%2 == 0 {
			channel = "dmc1"
		}
		segmentTargets[i] = &datapb.SegmentInfo{ID: i, InsertChannel: channel}
	}

	delegators := prefetchDelegatorsByChannel(dist, channelTargets, segmentTargets)

	assert.Len(t, delegators, 2,
		"one entry per distinct channel; a per-segment lookup would produce one per segment")
	assert.Contains(t, delegators, "dmc0")
	assert.Contains(t, delegators, "dmc1")
}

// TestPrefetchDelegatorsByChannelCoversSegmentOnlyChannels guards the boundary
// the grouping introduces: a channel can be named ONLY by a segment target,
// never by a channel target, and replicaLoadPercentage would then index the
// map with a key that was never prefetched. Go returns a nil slice for a
// missing key and ranging it is silently zero iterations, so the segment would
// read as not loaded on every replica -- an under-report with no error.
func TestPrefetchDelegatorsByChannelCoversSegmentOnlyChannels(t *testing.T) {
	nodeMgr := session.NewNodeManager()
	dist := meta.NewDistributionManager(nodeMgr)

	delegators := prefetchDelegatorsByChannel(
		dist,
		map[string]*meta.DmChannel{},
		map[int64]*datapb.SegmentInfo{7: {ID: 7, InsertChannel: "orphan-dmc"}},
	)

	assert.Contains(t, delegators, "orphan-dmc",
		"a channel named only by a segment target must still be prefetched")
}

// TestReplicaLoadPercentageUsesTheSuppliedSnapshots states the other half of
// the same change as a property of the function: replicaLoadPercentage takes
// the target set and the delegator map as arguments and touches neither
// manager, which is what lets one snapshot serve every replica. Handing it
// snapshots that no manager would produce and getting an answer derived
// purely from them is the proof.
func TestReplicaLoadPercentageUsesTheSuppliedSnapshots(t *testing.T) {
	replica := meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: 1, ResourceGroup: "rg", Nodes: []int64{10},
	})

	channelTargets := map[string]*meta.DmChannel{
		"dmc0": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "dmc0"}},
	}
	segmentTargets := map[int64]*datapb.SegmentInfo{
		1: {ID: 1, InsertChannel: "dmc0"},
	}
	// Node 10 is in the replica and carries segment 1 on dmc0: both targets covered.
	delegators := map[string][]*meta.DmChannel{
		"dmc0": {{
			VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "dmc0"},
			Node:         10,
			View:         &meta.LeaderView{ID: 10, Segments: map[int64]*querypb.SegmentDist{1: {NodeID: 10}}},
		}},
	}

	assert.EqualValues(t, 100, replicaLoadPercentage(replica, channelTargets, segmentTargets, delegators))

	// Same replica, same targets, empty delegator snapshot: 0. The function
	// cannot reach past what it was handed.
	assert.EqualValues(t, 0, replicaLoadPercentage(replica, channelTargets, segmentTargets,
		map[string][]*meta.DmChannel{}))
}
