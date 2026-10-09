package balancer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestBalancerSnapshotAccessorsHandleMissingSources(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}

	var nilSnapshot *BalancerSnapshot
	assert.Nil(t, nilSnapshot.ConfigForShard(shardID))
	_, ok := nilSnapshot.SegmentInfo(1)
	assert.False(t, ok)
	_, ok = nilSnapshot.DataVersionForCollection(1)
	assert.False(t, ok)
	assert.Nil(t, nilSnapshot.DataViewForShard(shardID))
	assert.Nil(t, nilSnapshot.ConfigsMap())
	assert.Nil(t, nilSnapshot.ShardStatsMap())

	ranged := false
	nilSnapshot.RangeDataShards(1, func(qviews.ShardID) bool {
		ranged = true
		return true
	})
	assert.False(t, ranged)

	emptySnapshot := &BalancerSnapshot{}
	assert.Nil(t, emptySnapshot.ConfigForShard(shardID))
	_, ok = emptySnapshot.SegmentInfo(1)
	assert.False(t, ok)
	_, ok = emptySnapshot.DataVersionForCollection(1)
	assert.False(t, ok)
	assert.Nil(t, emptySnapshot.DataViewForShard(shardID))
	emptySnapshot.RangeDataShards(1, func(qviews.ShardID) bool {
		ranged = true
		return true
	})
	assert.False(t, ranged)
}

func TestNodeSnapshotAccessorsAreNilSafeAndSupportEarlyStop(t *testing.T) {
	var nilSnapshot *NodeSnapshot
	assert.Zero(t, nilSnapshot.Version())

	visited := false
	nilSnapshot.Range(func(int64, *NodeInfo) bool {
		visited = true
		return true
	})
	assert.False(t, visited)

	snapshot := NewNodeSnapshot(7, map[int64]*NodeInfo{
		1: {NodeID: 1},
		2: {NodeID: 2},
	})
	assert.Equal(t, uint64(7), snapshot.Version())

	count := 0
	snapshot.Range(func(int64, *NodeInfo) bool {
		count++
		return false
	})
	assert.Equal(t, 1, count)
}
