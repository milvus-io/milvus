package balancer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestAdoptionSeedsOnlyEligibleSourcePlacementsInTheSameReplica(t *testing.T) {
	c := newTestCache(cfgFor(1, 10, []int64{1}, nil))
	source := cacheShard(1, 10)
	c.PublishShard(source, &coordview.ShardStats{Segments: map[int64]*coordview.SegmentStats{
		100: {SegmentID: 100, Nodes: map[int64]coordview.SegmentState{1: coordview.SegmentStateUp, 2: coordview.SegmentStateReady}},
		101: {SegmentID: 101, Nodes: map[int64]coordview.SegmentState{1: coordview.SegmentStatePreparing}},
	}})
	c.PublishShard(qviews.ShardID{ReplicaID: 11, VChannel: source.VChannel}, &coordview.ShardStats{Segments: map[int64]*coordview.SegmentStats{102: {SegmentID: 102, Nodes: map[int64]coordview.SegmentState{2: coordview.SegmentStateUp}}}})
	target := qviews.ShardID{ReplicaID: 10, VChannel: "p1_1v1"}
	entries := []*SegmentDataView{{SegmentID: 100}, {SegmentID: 101}, {SegmentID: 102}}
	seeded := seedAdoptedPlacements(c.GetCollection(1), target, entries, nil, []int64{2})
	require.Equal(t, map[int64]int64{100: 2}, seeded, "do not pick failed node 1, a preparing base, or another replica")
	current := map[int64]int64{100: 3}
	seeded = seedAdoptedPlacements(c.GetCollection(1), target, entries, current, []int64{2, 3})
	require.Equal(t, map[int64]int64{100: 3}, seeded, "existing target assignments win")
	require.Equal(t, map[int64]int64{100: 3}, current, "published assignments remain immutable")
}
