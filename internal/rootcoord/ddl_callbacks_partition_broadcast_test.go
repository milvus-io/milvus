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

package rootcoord

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// twiceSplitVChannels is the vchannel list of a 2-shard collection whose two
// shards have each been split once: the two sources stay in the list after they
// are retired, and each split appends its two targets. ShardsNum is 4 — the
// routable count — so slicing the list by it yields the two dead sources plus
// the first split's two targets, and misses the second split's.
var twiceSplitVChannels = []string{
	"by-dev-rootcoord-dml_0_100v0", // retired source of the first split
	"by-dev-rootcoord-dml_1_100v1", // retired source of the second split
	"by-dev-rootcoord-dml_10_100v2",
	"by-dev-rootcoord-dml_11_100v3",
	"by-dev-rootcoord-dml_12_100v4",
	"by-dev-rootcoord-dml_13_100v5",
}

func TestPartitionDDLReachesEveryShardOfASplitCollection(t *testing.T) {
	// The bug: the broadcast was VirtualChannelNames[0:ShardsNum]. On the
	// topology above that is the two retired sources and two of the four live
	// shards, so v4 and v5 never learn a partition was created or dropped —
	// their segments for it are never dropped, and the streamingnode's
	// per-vchannel partition set disagrees with the collection's forever.
	channels := partitionDDLBroadcastChannels(twiceSplitVChannels)

	require.Len(t, channels, len(twiceSplitVChannels), "the broadcaster adds the control channel itself")
	for _, vchannel := range twiceSplitVChannels {
		assert.Contains(t, channels, vchannel)
	}

	// and specifically the two the old slicing dropped.
	assert.Contains(t, channels, "by-dev-rootcoord-dml_12_100v4")
	assert.Contains(t, channels, "by-dev-rootcoord-dml_13_100v5")
}

func TestPartitionDDLIncludesRetiredSplitSources(t *testing.T) {
	// A fenced source is included on purpose: it stays in the collection's
	// vchannel list until adoption, and the shard interceptor's DoAppend name
	// gate appends a partition DDL replica addressed to a vchannel the pchannel
	// no longer holds with no shard effect, so the broadcast completes.
	channels := partitionDDLBroadcastChannels(twiceSplitVChannels)
	assert.Contains(t, channels, "by-dev-rootcoord-dml_0_100v0")
	assert.Contains(t, channels, "by-dev-rootcoord-dml_1_100v1")
}

func TestPartitionDDLOnANeverSplitCollectionIsUnchanged(t *testing.T) {
	// The common case must not move: control channel first, then the shards in
	// order — exactly what the old slicing produced when the list length and
	// ShardsNum agreed. The broadcaster adds the control channel itself.
	vchannels := []string{"v0", "v1"}
	assert.Equal(t, []string{"v0", "v1"}, partitionDDLBroadcastChannels(vchannels))
}
