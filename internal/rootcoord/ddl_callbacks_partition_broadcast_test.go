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

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
)

// rehashedVChannels is the vchannel list of a collection rehashed from 2 shards
// to 3: the two sources stay in the list after they are retired, and the three
// targets are appended. ShardsNum is 3 — the routable count — so slicing the
// list by it yields the two dead sources plus one live shard.
var rehashedVChannels = []string{
	"by-dev-rootcoord-dml_0_100v0", // retired source
	"by-dev-rootcoord-dml_1_100v1", // retired source
	"by-dev-rootcoord-dml_10_100v2",
	"by-dev-rootcoord-dml_11_100v3",
	"by-dev-rootcoord-dml_12_100v4",
}

func TestPartitionDDLReachesEveryShardOfASplitCollection(t *testing.T) {
	// The bug: the broadcast was VirtualChannelNames[0:ShardsNum]. On the
	// topology above that is the two retired sources and one of the three live
	// shards, so v3 and v4 never learn a partition was created or dropped —
	// their segments for it are never dropped, and the streamingnode's
	// per-vchannel partition set disagrees with the collection's forever.
	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().ControlChannel().Return("by-dev-rootcoord-dml_0_vcchan").Once()
	streaming.SetWALForTest(wal)
	defer streaming.SetWALForTest(nil)

	channels := partitionDDLBroadcastChannels(rehashedVChannels)

	require.Len(t, channels, len(rehashedVChannels)+1)
	assert.Equal(t, "by-dev-rootcoord-dml_0_vcchan", channels[0], "the control channel orders the DDL")
	for _, vchannel := range rehashedVChannels {
		assert.Contains(t, channels, vchannel)
	}

	// and specifically the two the old slicing dropped.
	assert.Contains(t, channels, "by-dev-rootcoord-dml_11_100v3")
	assert.Contains(t, channels, "by-dev-rootcoord-dml_12_100v4")
}

func TestPartitionDDLIncludesRetiredSplitSources(t *testing.T) {
	// A retired source is included on purpose: it still holds the partition's
	// segments until adoption drops them, the streamingnode tracks partitions
	// per vchannel, and neither partition handler is gated on the split fence,
	// so the append lands and the two views stay in agreement.
	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().ControlChannel().Return("ctrl").Once()
	streaming.SetWALForTest(wal)
	defer streaming.SetWALForTest(nil)

	channels := partitionDDLBroadcastChannels(rehashedVChannels)
	assert.Contains(t, channels, "by-dev-rootcoord-dml_0_100v0")
	assert.Contains(t, channels, "by-dev-rootcoord-dml_1_100v1")
}

func TestPartitionDDLOnANeverSplitCollectionIsUnchanged(t *testing.T) {
	// The common case must not move: control channel first, then the shards in
	// order — exactly what the old slicing produced when the list length and
	// ShardsNum agreed.
	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().ControlChannel().Return("ctrl").Once()
	streaming.SetWALForTest(wal)
	defer streaming.SetWALForTest(nil)

	vchannels := []string{"v0", "v1"}
	assert.Equal(t, []string{"ctrl", "v0", "v1"}, partitionDDLBroadcastChannels(vchannels))
}
