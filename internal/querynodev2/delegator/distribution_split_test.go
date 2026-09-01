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

package delegator

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestPinReadableSegmentsAsChild(t *testing.T) {
	// a freshly spawned split child is non-serviceable: it has no querycoord
	// target version yet (syncedByCoord == false).
	dist := NewDistribution("child-channel", NewChannelQueryView(nil, nil, []int64{1}, initialTargetVersion))
	assert.False(t, dist.Serviceable())

	// the serviceability-gated pin rejects the non-serviceable child, so a proxy
	// read routed through the leader path can never reach it.
	_, _, _, _, err := dist.PinReadableSegments(1.0)
	assert.Error(t, err)

	// the fronting bypass pin lets the source delegator read the child's view
	// in-process even though the child is externally non-serviceable.
	_, _, _, version, err := dist.PinReadableSegmentsAsChild(1.0)
	assert.NoError(t, err)
	dist.Unpin(version)
}

func TestPinReadableSegmentsAsChildReturnsGrowing(t *testing.T) {
	// a fronted child consumes its WAL and builds growing segments at the initial
	// target version (querycoord has not adopted it).
	dist := NewDistribution("child-channel", NewChannelQueryView(nil, nil, []int64{1}, initialTargetVersion))
	dist.AddGrowing(SegmentEntry{
		NodeID:        1,
		SegmentID:     100,
		PartitionID:   1,
		TargetVersion: initialTargetVersion,
		Level:         datapb.SegmentLevel_L1,
	})

	// the fronting pin must actually return that growing data — it is the merged
	// shard's post-T_switch data that the source fans out for. (The partial-result
	// branch would filter it out because the child's queryView.growingSegments is
	// only populated by querycoord adoption, which never runs while fronted.)
	_, growing, _, version, err := dist.PinReadableSegmentsAsChild(1.0)
	assert.NoError(t, err)
	defer dist.Unpin(version)
	assert.Len(t, growing, 1, "the child's growing segment must be readable for fronting")
	assert.Equal(t, int64(100), growing[0].SegmentID)
}

func TestSyncPartitionsWidensAChildsReadableSet(t *testing.T) {
	// A child is spawned with the partitions that existed at the fence. One
	// created afterwards is not in that set, and the pin rejects it outright --
	// which is what surfaced as "partition not loaded" on a namespace created
	// after a split.
	dist := NewDistribution("child-channel", NewChannelQueryView(nil, nil, []int64{1}, initialTargetVersion))
	_, _, _, _, err := dist.PinReadableSegmentsAsChild(1.0, 2)
	assert.Error(t, err, "a partition the child has never heard of must not be served")

	dist.SyncPartitions([]int64{1, 2})
	_, _, _, version, err := dist.PinReadableSegmentsAsChild(1.0, 2)
	assert.NoError(t, err, "once the source hands down the partition, the child serves it")
	dist.Unpin(version)
}

func TestSyncPartitionsIsANoOpWhenNothingChanged(t *testing.T) {
	// Called on every SyncTargetVersion of the source, so an unchanged set must
	// not churn a new snapshot: every snapshot expires the previous one and the
	// pins taken against it.
	dist := NewDistribution("child-channel", NewChannelQueryView(nil, nil, []int64{1, 2}, initialTargetVersion))
	before := dist.current.Load().version

	dist.SyncPartitions([]int64{2, 1}) // same set, different order
	assert.Equal(t, before, dist.current.Load().version)

	dist.SyncPartitions([]int64{1, 2, 3})
	assert.NotEqual(t, before, dist.current.Load().version)
}

func TestSyncPartitionsAlsoNarrows(t *testing.T) {
	// A dropped partition must stop being readable on the child too, or the
	// child would answer for data the source no longer admits.
	dist := NewDistribution("child-channel", NewChannelQueryView(nil, nil, []int64{1, 2}, initialTargetVersion))
	_, _, _, version, err := dist.PinReadableSegmentsAsChild(1.0, 2)
	assert.NoError(t, err)
	dist.Unpin(version)

	dist.SyncPartitions([]int64{1})
	_, _, _, _, err = dist.PinReadableSegmentsAsChild(1.0, 2)
	assert.Error(t, err)
}
