package balancer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestTriggerQueueDirtyNodeExpandsOnlyAffectedShards(t *testing.T) {
	shardA := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	shardB := qviews.ShardID{ReplicaID: 1, VChannel: "v1"}
	shardWithoutStats := qviews.ShardID{ReplicaID: 1, VChannel: "v2"}
	snap := &BalancerSnapshot{
		ShardViewSnapshot: coordview.NewShardViewSnapshot(1, map[qviews.ShardID]*coordview.ShardStats{
			shardA: testShardStats(nil, 0,
				placement(101, 1, 10, coordview.SegmentStateUp),
			),
			shardB: testShardStats(nil, 0,
				placement(201, 1, 20, coordview.SegmentStatePreparing),
			),
			shardWithoutStats: nil,
		}),
	}

	queue := newTriggerQueue()
	queue.add(TriggerScope{DirtyNodes: []int64{10, 10}})

	assert.Equal(t, []qviews.ShardID{shardA}, queue.drain(snap))
	assert.Empty(t, queue.drain(snap), "drain must clear the pending node set")
}

func TestTriggerQueueNilSnapshotDropsPendingWork(t *testing.T) {
	queue := newTriggerQueue()
	queue.add(TriggerScope{
		DirtyNodes:       []int64{1},
		DirtyShards:      []qviews.ShardID{{ReplicaID: 1, VChannel: "v0"}},
		DirtyCollections: []int64{10},
	})

	assert.Nil(t, queue.drain(nil))
	assert.Empty(t, queue.drain(&BalancerSnapshot{}))
}
