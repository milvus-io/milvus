package balancer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestTriggerBatchResolveDirtyCollection(t *testing.T) {
	registry := triggerTestRegistry(t)
	shardA := triggerShard(2, 1, 0)
	shardB := triggerShard(1, 1, 1)
	unrelated := triggerShard(1, 2, 0)
	registry.Ensure(shardA)
	registry.Ensure(shardB)
	registry.Ensure(unrelated)

	scope := (triggerBatch{dirtyColls: setOf[int64](1)}).resolveScope(nil, registry)

	assert.Equal(t, setOf[int64](1), scope.collectionIDs)
	assert.Equal(t, setOf(shardB, shardA), scope.targetShards)
}

func TestTriggerBatchResolveDirtyShard(t *testing.T) {
	shard := triggerShard(10, 2, 0)
	scope := (triggerBatch{dirtyShards: setOf(shard)}).resolveScope(nil, nil)

	assert.Equal(t, setOf[int64](2), scope.collectionIDs)
	assert.Equal(t, setOf(shard), scope.targetShards)
}

func TestTriggerBatchResolveDirtyNode(t *testing.T) {
	registry := triggerTestRegistry(t)
	shardA := triggerShard(10, 1, 0)
	shardB := triggerShard(20, 2, 0)
	unrelated := triggerShard(30, 3, 0)
	addShardWithPreparingView(t, registry, shardA, map[int64]map[int64][]int64{
		100: {10: {101}},
	})
	addShardWithPreparingView(t, registry, shardB, map[int64]map[int64][]int64{
		100: {20: {201}},
	})
	addShardWithPreparingView(t, registry, unrelated, map[int64]map[int64][]int64{
		200: {30: {301}},
	})

	scope := (triggerBatch{dirtyNodes: setOf[int64](100)}).resolveScope(nil, registry)

	assert.Equal(t, setOf[int64](1, 2), scope.collectionIDs)
	assert.Empty(t, scope.collectionWideIDs)
	assert.Equal(t, setOf(shardA, shardB), scope.targetShards)
}

func TestTriggerBatchMalformedNodeShardFallsBackToFull(t *testing.T) {
	loadSnapshot := triggerLoadSnapshot(triggerLoadConfig(1, 10), triggerLoadConfig(2, 20))
	registry := triggerTestRegistry(t)
	validShard := triggerShard(20, 2, 0)
	malformedShard := qviews.ShardID{ReplicaID: 10, VChannel: "malformed"}
	addShardWithPreparingView(t, registry, validShard, map[int64]map[int64][]int64{
		100: {20: {201}},
	})
	addShardWithPreparingView(t, registry, malformedShard, map[int64]map[int64][]int64{
		100: {10: {101}},
	})

	scope := (triggerBatch{dirtyNodes: setOf[int64](100)}).resolveScope(loadSnapshot, registry)

	assert.Equal(t, setOf[int64](1, 2), scope.collectionIDs)
	assert.Equal(t, setOf(registry.ShardIDs()...), scope.targetShards)
}

func TestTriggerBatchResolveMergesScopes(t *testing.T) {
	registry := triggerTestRegistry(t)
	collectionShard := triggerShard(1, 1, 0)
	dirtyShard := triggerShard(2, 2, 0)
	registry.Ensure(collectionShard)

	scope := (triggerBatch{
		dirtyColls:  setOf[int64](1),
		dirtyShards: setOf(dirtyShard),
	}).resolveScope(nil, registry)

	assert.Equal(t, setOf[int64](1, 2), scope.collectionIDs)
	assert.Equal(t, setOf(collectionShard, dirtyShard), scope.targetShards)
}

func TestTriggerBatchResolveFull(t *testing.T) {
	registry := triggerTestRegistry(t)
	shardA := triggerShard(2, 1, 0)
	residualShard := triggerShard(3, 3, 0)
	malformedShard := qviews.ShardID{ReplicaID: 1, VChannel: "malformed"}
	registry.Ensure(shardA)
	registry.Ensure(residualShard)
	registry.Ensure(malformedShard)
	loadSnapshot := triggerLoadSnapshot(
		triggerLoadConfig(2, 20),
		triggerLoadConfig(1, 10),
	)

	queue := newTriggerQueue()
	queue.add(TriggerScope{NodeChanged: true})
	scope := queue.takePending().resolveScope(loadSnapshot, registry)

	assert.Equal(t, setOf[int64](1, 2, 3), scope.collectionIDs)
	assert.Equal(t, setOf(malformedShard, shardA, residualShard), scope.targetShards)
	assert.True(t, queue.takePending().empty())
}

func TestTriggerBatchMalformedDirtyShardFallsBackToFull(t *testing.T) {
	loadSnapshot := triggerLoadSnapshot(triggerLoadConfig(1, 10), triggerLoadConfig(2, 20))
	registry := triggerTestRegistry(t)
	validShard := triggerShard(2, 2, 0)
	malformedShard := qviews.ShardID{ReplicaID: 1, VChannel: "malformed"}
	registry.Ensure(validShard)
	registry.Ensure(malformedShard)

	scope := (triggerBatch{dirtyShards: setOf(malformedShard)}).resolveScope(loadSnapshot, registry)

	assert.Equal(t, setOf[int64](1, 2), scope.collectionIDs)
	assert.Equal(t, setOf(registry.ShardIDs()...), scope.targetShards)
}

func TestTriggerBatchReleasedCollectionIncludesResidualShards(t *testing.T) {
	registry := triggerTestRegistry(t)
	residualShard := triggerShard(10, 1, 0)
	unrelatedShard := triggerShard(20, 2, 0)
	registry.Ensure(residualShard)
	registry.Ensure(unrelatedShard)

	scope := (triggerBatch{dirtyColls: setOf[int64](1)}).resolveScope(
		triggerLoadSnapshot(triggerLoadConfig(2, 20)),
		registry,
	)

	assert.Equal(t, setOf[int64](1), scope.collectionIDs)
	assert.Equal(t, setOf(residualShard), scope.targetShards)
}

func TestTriggerBatchDirtyCollectionUsesCollectionIndexOnly(t *testing.T) {
	registry := triggerTestRegistry(t)
	malformedResidual := qviews.ShardID{ReplicaID: 20, VChannel: "malformed"}
	registry.Ensure(malformedResidual)
	loadSnapshot := triggerLoadSnapshot(triggerLoadConfig(1, 10))

	scope := (triggerBatch{dirtyColls: setOf[int64](99)}).resolveScope(loadSnapshot, registry)

	assert.Equal(t, setOf[int64](99), scope.collectionIDs)
	assert.Empty(t, scope.targetShards)
}

func TestReconcileScopeAddDataViewShards(t *testing.T) {
	loadSnapshot := triggerLoadSnapshot(&loadmgr.LoadConfig{
		CollectionID: 1,
		Replicas: []*loadmgr.ReplicaAssignment{
			{ReplicaID: 20},
			{ReplicaID: 10},
		},
	})
	dataSnapshot := NewDataViewSnapshot(1, []*viewpb.DataViewOfCollection{
		{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{},
			Shards: []*viewpb.DataViewOfShard{
				{Vchannel: "by-dev-rootcoord-dml_0_1v1"},
				nil,
				{Vchannel: "by-dev-rootcoord-dml_0_1v0"},
			},
		},
		{
			CollectionId: 2,
			DataVersion:  &viewpb.DataVersion{},
			Shards:       []*viewpb.DataViewOfShard{{Vchannel: "by-dev-rootcoord-dml_0_2v0"}},
		},
	}, nil)
	scope := (triggerBatch{dirtyColls: setOf[int64](1)}).resolveScope(loadSnapshot, nil)

	scope.AddDataViewShards(loadSnapshot, dataSnapshot)

	assert.Equal(t, setOf(
		triggerShard(10, 1, 0),
		triggerShard(10, 1, 1),
		triggerShard(20, 1, 0),
		triggerShard(20, 1, 1),
	), scope.targetShards)
}

func TestReconcileScopeDoesNotExpandDirtyShard(t *testing.T) {
	loadSnapshot := triggerLoadSnapshot(&loadmgr.LoadConfig{
		CollectionID: 1,
		Replicas: []*loadmgr.ReplicaAssignment{
			{ReplicaID: 10},
			{ReplicaID: 20},
		},
	})
	dataSnapshot := NewDataViewSnapshot(1, []*viewpb.DataViewOfCollection{
		{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{},
			Shards: []*viewpb.DataViewOfShard{
				{Vchannel: "by-dev-rootcoord-dml_0_1v0"},
				{Vchannel: "by-dev-rootcoord-dml_0_1v1"},
			},
		},
	}, nil)
	dirtyShard := triggerShard(10, 1, 0)

	scope := (triggerBatch{dirtyShards: setOf(dirtyShard)}).resolveScope(loadSnapshot, nil)

	scope.AddDataViewShards(loadSnapshot, dataSnapshot)

	assert.Equal(t, setOf(dirtyShard), scope.targetShards)
}

func triggerTestRegistry(t *testing.T) *coordview.ShardViewRegistry {
	t.Helper()
	registry := emptyRegistry(t)
	t.Cleanup(registry.Close)
	return registry
}

func triggerLoadSnapshot(configs ...*loadmgr.LoadConfig) *loadmgr.LoadConfigSnapshot {
	byCollection := make(map[int64]*loadmgr.LoadConfig, len(configs))
	for _, cfg := range configs {
		byCollection[cfg.CollectionID] = cfg
	}
	return loadmgr.NewLoadConfigSnapshot(1, byCollection)
}

func triggerLoadConfig(collectionID, replicaID int64) *loadmgr.LoadConfig {
	return &loadmgr.LoadConfig{
		CollectionID: collectionID,
		Replicas:     []*loadmgr.ReplicaAssignment{{ReplicaID: replicaID}},
	}
}

func triggerShard(replicaID, collectionID, shardIndex int64) qviews.ShardID {
	return qviews.ShardID{
		ReplicaID: replicaID,
		VChannel:  fmt.Sprintf("by-dev-rootcoord-dml_0_%dv%d", collectionID, shardIndex),
	}
}

func setOf[T comparable](values ...T) map[T]struct{} {
	set := make(map[T]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}
