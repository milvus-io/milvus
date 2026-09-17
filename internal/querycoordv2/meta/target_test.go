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
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestCollectionTarget_IDSetBased(t *testing.T) {
	store := metacache.NewMetaStore(nil)
	store.PutSegment(&datapb.SegmentInfo{
		ID: 1, CollectionID: 100, PartitionID: 10,
		InsertChannel: "ch-0", NumOfRows: 500,
		State: commonpb.SegmentState_Flushed,
	})
	store.PutSegment(&datapb.SegmentInfo{
		ID: 2, CollectionID: 100, PartitionID: 10,
		InsertChannel: "ch-1", NumOfRows: 300,
		State: commonpb.SegmentState_Flushed,
	})

	segments := map[int64]*datapb.SegmentInfo{
		1: {ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0", NumOfRows: 500},
		2: {ID: 2, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-1", NumOfRows: 300},
	}
	dmChannels := map[string]*DmChannel{
		"ch-0": {VchannelInfo: &datapb.VchannelInfo{ChannelName: "ch-0", CollectionID: 100}},
		"ch-1": {VchannelInfo: &datapb.VchannelInfo{ChannelName: "ch-1", CollectionID: 100}},
	}

	target := NewCollectionTarget(segments, dmChannels, []int64{10}, store)

	allSegs := target.GetAllSegments()
	assert.Len(t, allSegs, 2)
	assert.Equal(t, int64(500), allSegs[1].GetNumOfRows())

	chSegs := target.GetChannelSegments("ch-0")
	assert.Len(t, chSegs, 1)

	assert.Equal(t, int64(800), target.GetRowCount())
}

// TestCollectionTargetReadyFromDefiningInfos pins readiness: a target built
// from broker infos is always ready, whatever the shared store holds. Only a
// target recovered from a pre-v2.4.19 persisted form can lack segment info,
// and only for a segment the store cannot resolve either - see
// TestFromPbCollectionTargetReadiness.
func TestCollectionTargetReadyFromDefiningInfos(t *testing.T) {
	store := metacache.NewMetaStore(nil)
	dmChannels := map[string]*DmChannel{
		"ch-0": {VchannelInfo: &datapb.VchannelInfo{ChannelName: "ch-0", CollectionID: 100}},
	}

	// Nothing is in the store at all, yet the target is ready.
	ready := NewCollectionTarget(map[int64]*datapb.SegmentInfo{
		1: {ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0", NumOfRows: 10},
	}, dmChannels, []int64{10}, store)
	assert.True(t, ready.Ready())
	assert.Len(t, ready.GetChannelSegmentIDs("ch-0"), 1)
	assert.True(t, ready.ContainSegment(1))

	// A freshly built target is always ready: "lacking segment info" is a
	// property of a target persisted before v2.4.19, not of a live build.
	noRows := NewCollectionTarget(map[int64]*datapb.SegmentInfo{
		1: {ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0"},
	}, dmChannels, []int64{10}, store)
	assert.True(t, noRows.Ready())
}

// TestCollectionTargetGroupingSurvivesStoreRemoval is the invariant the ID-set
// design rests on: the target keeps its membership and its channel/partition
// grouping when DataCoord drops a segment. Only the resolved details go away.
func TestCollectionTargetGroupingSurvivesStoreRemoval(t *testing.T) {
	store := metacache.NewMetaStore(nil)
	store.PutSegment(&datapb.SegmentInfo{ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0", NumOfRows: 10})

	target := NewCollectionTarget(map[int64]*datapb.SegmentInfo{
		1: {ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0", NumOfRows: 10},
	}, map[string]*DmChannel{
		"ch-0": {VchannelInfo: &datapb.VchannelInfo{ChannelName: "ch-0", CollectionID: 100}},
	}, []int64{10}, store)

	require.Len(t, target.GetAllSegments(), 1)

	store.RemoveSegment(1)

	assert.Empty(t, target.GetAllSegments(), "details are resolved live")
	assert.True(t, target.ContainSegment(1), "membership must survive")
	assert.Len(t, target.GetChannelSegmentIDs("ch-0"), 1)
	assert.Len(t, target.GetPartitionSegmentIDs(10), 1)
	assert.Zero(t, target.GetRowCount(), "row count is summed live, so a removed segment stops counting")

	// The persisted form must still carry the segment.
	pb := target.toPbMsg()
	require.Len(t, pb.GetChannelTargets(), 1)
	require.Len(t, pb.GetChannelTargets()[0].GetPartitionTargets(), 1)
	assert.Len(t, pb.GetChannelTargets()[0].GetPartitionTargets()[0].GetSegments(), 1)
}

// TestRemovePartitionKeepsUnresolvableSegments asserts that releasing one
// partition only drops the segments known to belong to it. A segment the store
// cannot resolve right now must stay in the target: dropping it would silently
// shrink the target for partitions that were never touched, and the rebuilt
// target would not even report missing segment info.
func TestRemovePartitionKeepsUnresolvableSegments(t *testing.T) {
	store := metacache.NewMetaStore(nil)
	store.PutSegment(&datapb.SegmentInfo{
		ID: 1, CollectionID: 100, PartitionID: 10,
		InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed,
	})
	store.PutSegment(&datapb.SegmentInfo{
		ID: 2, CollectionID: 100, PartitionID: 11,
		InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed,
	})

	// Segment 3 belongs to the surviving partition but is not in the store.
	old := NewCollectionTarget(map[int64]*datapb.SegmentInfo{
		1: {ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0", NumOfRows: 1},
		2: {ID: 2, CollectionID: 100, PartitionID: 11, InsertChannel: "ch-0", NumOfRows: 1},
		3: {ID: 3, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0", NumOfRows: 1},
	}, map[string]*DmChannel{
		"ch-0": {VchannelInfo: &datapb.VchannelInfo{ChannelName: "ch-0", CollectionID: 100}},
	}, []int64{10, 11}, store)

	mgr := &TargetManager{metaView: store}
	updated := mgr.removePartitionFromCollectionTarget(old, typeutil.NewUniqueSet(11))

	assert.Contains(t, updated.segmentIDs, int64(1))
	assert.NotContains(t, updated.segmentIDs, int64(2))
	assert.Contains(t, updated.segmentIDs, int64(3), "an unresolvable segment must not be dropped")
	assert.Len(t, updated.GetPartitionSegmentIDs(10), 2)
	assert.Empty(t, updated.GetPartitionSegmentIDs(11))
	assert.False(t, updated.partitions.Contain(11))
}

// TestTargetRowCountIsLive pins where the row count comes from: the shared
// store, summed on every call, so the balancer scores against current counts
// rather than a build-time snapshot. NumOfRows is authoritative for every
// storage version here — DataCoord reconciles a V1/V2 segment against its
// insert binlogs before the segment enters the store (see
// TestSetSegmentNormalizesRowCount), and a V3 count is what the writer
// reported.
func TestTargetRowCountIsLive(t *testing.T) {
	store := metacache.NewMetaStore(nil)
	v3 := &datapb.SegmentInfo{
		ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0",
		NumOfRows: 70, StorageVersion: storage.StorageV3,
	}
	v2 := &datapb.SegmentInfo{
		ID: 2, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0",
		NumOfRows: 30, StorageVersion: storage.StorageV2,
		Binlogs: []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{EntriesNum: 30}},
		}},
	}
	store.PutSegment(v3)
	store.PutSegment(v2)

	target := NewCollectionTarget(map[int64]*datapb.SegmentInfo{1: v3, 2: v2}, map[string]*DmChannel{
		"ch-0": {VchannelInfo: &datapb.VchannelInfo{ChannelName: "ch-0", CollectionID: 100}},
	}, []int64{10}, store)

	assert.EqualValues(t, 100, target.GetRowCount())

	// A later count for the same ID is picked up without rebuilding the target.
	store.PutSegment(&datapb.SegmentInfo{
		ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0",
		NumOfRows: 90, StorageVersion: storage.StorageV3,
	})
	assert.EqualValues(t, 120, target.GetRowCount())
}

// TestFromPbCollectionTargetReadiness covers the recovered-target rule: a
// persisted segment with no row count only means "info missing" when the
// shared store cannot resolve it either. Otherwise the live value stands, so a
// target saved while one segment happened to be unresolvable does not disable
// balancing for the whole collection.
func TestFromPbCollectionTargetReadiness(t *testing.T) {
	dmChannels := []*querypb.ChannelTarget{{
		ChannelName: "ch-0",
		PartitionTargets: []*querypb.PartitionTarget{{
			PartitionID: 10,
			Segments:    []*querypb.SegmentTarget{{ID: 1, NumOfRows: 0}},
		}},
	}}
	persisted := &querypb.CollectionTarget{CollectionID: 100, ChannelTargets: dmChannels, Version: 7}

	// The store knows the segment: the persisted zero is stale, not missing.
	resolvable := metacache.NewMetaStore(nil)
	resolvable.PutSegment(&datapb.SegmentInfo{
		ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0", NumOfRows: 25,
	})
	target := FromPbCollectionTarget(persisted, resolvable)
	assert.True(t, target.Ready())
	assert.EqualValues(t, 25, target.GetRowCount())
	assert.EqualValues(t, 7, target.GetTargetVersion())

	// Nobody knows the segment: that is the pre-v2.4.19 case.
	empty := metacache.NewMetaStore(nil)
	assert.False(t, FromPbCollectionTarget(persisted, empty).Ready())
}
