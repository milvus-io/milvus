package coordview

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type generationTestCatalog struct {
	saved []*viewpb.ServingGeneration
	err   error
}

func (c *generationTestCatalog) ListServingGenerations(context.Context) ([]*viewpb.ServingGeneration, error) {
	return c.saved, nil
}

func (c *generationTestCatalog) SaveServingGeneration(_ context.Context, g *viewpb.ServingGeneration) error {
	if c.err != nil {
		return c.err
	}
	c.saved = append(c.saved, proto.Clone(g).(*viewpb.ServingGeneration))
	return nil
}

func generationTestView(epoch uint64, channel string, replica int64, lineage *viewpb.QueryViewLineage) *viewpb.QueryViewOfShard {
	return &viewpb.QueryViewOfShard{Meta: &viewpb.QueryViewMeta{
		CollectionId: 100, ReplicaId: replica, Vchannel: channel, ShardSetEpoch: epoch,
		Version:         &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, QueryVersion: 1},
		LoadInfoVersion: 10, SchemaVersion: 1, State: viewpb.QueryViewState_QueryViewStateUp, Lineage: lineage,
	}, StreamingNode: &viewpb.QueryViewOfStreamingNode{}}
}

func installGenerationView(reg *ShardViewRegistry, epoch uint64, channel string, replica int64, lineage *viewpb.QueryViewLineage) {
	sid := qviews.ShardID{ReplicaID: replica, VChannel: channel}
	manager := reg.Ensure(sid)
	manager.mu.Lock()
	defer manager.mu.Unlock()
	view := generationTestView(epoch, channel, replica, lineage)
	view.Meta.Version.DataVersion.CompactVersion = int64(epoch)
	sm := RecoverCoordQueryViewStateMachineWithRef(view, nil)
	sm.snState = qviews.QueryViewStateUp
	manager.views[sm.Version()] = sm
}

func TestServingGenerationWaitsForEveryReplicaAndLineage(t *testing.T) {
	reg := newTestRegistry(t, newMockCatalog(), newMockSyncer())
	catalog := &generationTestCatalog{}
	reg.servingCatalog = catalog
	reg.PublishCollectionSchema(100, 1)
	lineage := &viewpb.QueryViewLineage{Vchannels: []string{"p_100v1", "p_100v2"}, SwitchTimetick: 100}
	desired := &viewpb.DataViewOfCollection{CollectionId: 100, ShardSetEpoch: 2, SchemaVersion: 1, Shards: []*viewpb.DataViewOfShard{
		{Vchannel: "p_100v0", Lineage: lineage}, {Vchannel: "p_100v1"}, {Vchannel: "p_100v2"},
	}}
	for _, replica := range []int64{1, 2} {
		for _, shard := range desired.Shards {
			installGenerationView(reg, 2, shard.Vchannel, replica, shard.Lineage)
		}
	}
	// One replica still has S without target subscriptions. It must not publish.
	installGenerationView(reg, 2, "p_100v0", 2, nil)
	_, err := reg.ResolveServingGeneration(t.Context(), desired, 10, []int64{1, 2})
	require.Error(t, err)
	require.Empty(t, catalog.saved)
	installGenerationView(reg, 2, "p_100v0", 2, lineage)
	catalog.err = errors.New("catalog unavailable")
	_, err = reg.ResolveServingGeneration(t.Context(), desired, 10, []int64{1, 2})
	require.ErrorIs(t, err, catalog.err)
	require.Empty(t, reg.serving)
	catalog.err = nil
	got, err := reg.ResolveServingGeneration(t.Context(), desired, 10, []int64{1, 2})
	require.NoError(t, err)
	require.EqualValues(t, 2, got.GetShardSetEpoch())
	require.Len(t, catalog.saved, 1)
	got.Shards[0].Vchannel = "mutated"
	again, err := reg.ResolveServingGeneration(t.Context(), desired, 10, []int64{1, 2})
	require.NoError(t, err)
	require.Equal(t, "p_100v0", again.GetShards()[0].GetVchannel())
	require.Len(t, catalog.saved, 1)
}

func (c *generationTestCatalog) RemoveServingGeneration(context.Context, int64) error { return c.err }

func TestServingPublicationWaitsForSNAndReplaysPreemptionAfterRecovery(t *testing.T) {
	reg := newTestRegistry(t, newMockCatalog(), newMockSyncer())
	catalog := &generationTestCatalog{}
	reg.servingCatalog = catalog
	reg.PublishCollectionSchema(100, 1)
	source := "p_100v0"
	lineage := &viewpb.QueryViewLineage{Vchannels: []string{"p_100v1", "p_100v2"}, SwitchTimetick: 100}
	desired := &viewpb.DataViewOfCollection{CollectionId: 100, ShardSetEpoch: 2, SchemaVersion: 1, Shards: []*viewpb.DataViewOfShard{{Vchannel: source, Lineage: lineage}, {Vchannel: "p_100v1"}, {Vchannel: "p_100v2"}}}
	installGenerationView(reg, 0, source, 1, nil)
	for _, shard := range desired.Shards {
		installGenerationView(reg, 2, shard.Vchannel, 1, shard.Lineage)
	}
	manager := reg.Get(qviews.ShardID{ReplicaID: 1, VChannel: source})
	manager.mu.Lock()
	var old, next *CoordQueryViewStateMachine
	for _, sm := range manager.views {
		if sm.View().GetMeta().GetShardSetEpoch() == 0 {
			old = sm
		} else {
			next = sm
		}
	}
	next.snState = qviews.QueryViewStateReady
	manager.mu.Unlock()
	first, err := reg.ResolveServingGeneration(t.Context(), desired, 10, []int64{1})
	require.NoError(t, err)
	require.Zero(t, first.GetShardSetEpoch(), "Coord Up intent cannot publish before SN confirms Up")
	require.Len(t, first.GetShards(), 1)
	manager.mu.Lock()
	next.snState = qviews.QueryViewStateUp
	manager.mu.Unlock()
	published, err := reg.ResolveServingGeneration(t.Context(), desired, 10, []int64{1})
	require.NoError(t, err)
	require.EqualValues(t, 2, published.GetShardSetEpoch())
	require.False(t, reg.ServingTransitionComplete(100, 2, false), "persisted preemption is not delivery proof")
	manager.mu.Lock()
	require.True(t, old.View().GetMeta().GetControl().GetPreemptServingLease())
	old.snState = qviews.QueryViewStateNil // recovery does not assume prior SN acknowledgments
	manager.mu.Unlock()
	_, err = reg.ResolveServingGeneration(t.Context(), desired, 10, []int64{1})
	require.NoError(t, err)
	require.False(t, reg.ServingTransitionComplete(100, 2, false))
	manager.mu.Lock()
	old.snState = qviews.QueryViewStateDown
	manager.mu.Unlock()
	require.True(t, reg.ServingTransitionComplete(100, 2, false))
	require.NoError(t, reg.RemoveServingGeneration(t.Context(), 100))
	require.Empty(t, reg.serving)
}

func TestServingPublicationBindsSameEpochLoadAndPublicSchema(t *testing.T) {
	reg := newTestRegistry(t, newMockCatalog(), newMockSyncer())
	catalog := &generationTestCatalog{}
	reg.servingCatalog = catalog
	reg.PublishCollectionSchema(100, 1)
	desired := &viewpb.DataViewOfCollection{CollectionId: 100, ShardSetEpoch: 2, SchemaVersion: 1, Shards: []*viewpb.DataViewOfShard{{Vchannel: "p_100v0", Partitions: []*viewpb.DataViewOfPartition{{PartitionId: 20}}, SnapshotHistoryFloor: 123}}}
	installGenerationView(reg, 2, "p_100v0", 1, nil)
	first, err := reg.ResolveServingGeneration(t.Context(), desired, 10, []int64{1})
	require.NoError(t, err)
	require.EqualValues(t, 10, first.GetLoadInfoVersion())
	require.EqualValues(t, 1, first.GetReadSchemaVersion())
	require.Empty(t, first.Shards[0].Partitions)
	require.Zero(t, first.Shards[0].SnapshotHistoryFloor)
	// A new Load may retain exactly the same shard epoch. Until its resource
	// scope is Up, the retained old publication is only a shard-set fence.
	_, err = reg.ResolveServingGeneration(t.Context(), desired, 11, []int64{1})
	require.Error(t, err)
	require.Len(t, catalog.saved, 1)
	manager := reg.Get(qviews.ShardID{ReplicaID: 1, VChannel: "p_100v0"})
	for _, sm := range manager.views {
		sm.View().Meta.LoadInfoVersion = 11
	}
	reloaded, err := reg.ResolveServingGeneration(t.Context(), desired, 11, []int64{1})
	require.NoError(t, err)
	require.EqualValues(t, 11, reloaded.GetLoadInfoVersion())
	require.Equal(t, first.GetShardSetEpoch(), reloaded.GetShardSetEpoch())
	_, err = reg.ResolveServingGeneration(t.Context(), desired, 10, []int64{1})
	require.Error(t, err, "an older config cannot regress Reload")
	// Prepared schema is not public. Existing read identity is retained even
	// if DataView and a newly prepared view already carry the target schema.
	desired.SchemaVersion = 2
	for _, sm := range manager.views {
		sm.View().Meta.SchemaVersion = 2
	}
	prepared, err := reg.ResolveServingGeneration(t.Context(), desired, 11, []int64{1})
	require.NoError(t, err)
	require.EqualValues(t, 1, prepared.GetReadSchemaVersion())
	require.Len(t, catalog.saved, 2)
	reg.PublishCollectionSchema(100, 2)
	published, err := reg.ResolveServingGeneration(t.Context(), desired, 11, []int64{1})
	require.NoError(t, err)
	require.EqualValues(t, 2, published.GetReadSchemaVersion())
	require.Len(t, catalog.saved, 3)
	require.EqualValues(t, 11, manager.publishedServingLoadInfoVersion)
}

func TestServingPublicationRequiresActiveLoadAndPublishedSchema(t *testing.T) {
	reg := newTestRegistry(t, newMockCatalog(), newMockSyncer())
	catalog := &generationTestCatalog{}
	reg.servingCatalog = catalog
	desired := &viewpb.DataViewOfCollection{CollectionId: 100, Shards: []*viewpb.DataViewOfShard{{Vchannel: "p_100v0"}}}
	installGenerationView(reg, 0, "p_100v0", 1, nil)
	_, err := reg.ResolveServingGeneration(t.Context(), desired, 10, []int64{1})
	require.Error(t, err)
	reg.PublishCollectionSchema(100, 1)
	_, err = reg.ResolveServingGeneration(t.Context(), desired, 0, []int64{1})
	require.Error(t, err)
	_, err = reg.ResolveServingGeneration(t.Context(), desired, 10, nil)
	require.Error(t, err)
	require.Empty(t, catalog.saved)
}

type servingRecoveryCatalog struct {
	*mockCatalog
	*generationTestCatalog
}

func TestServingPublicationRecoveryRetainsExactLoadIdentity(t *testing.T) {
	catalog := &servingRecoveryCatalog{mockCatalog: newMockCatalog(), generationTestCatalog: &generationTestCatalog{saved: []*viewpb.ServingGeneration{{CollectionId: 100, LoadInfoVersion: 42, ReadSchemaVersion: 7, ShardSetEpoch: 3, Shards: []*viewpb.DataViewOfShard{{Vchannel: "p_100v1"}}}}}}
	reg, err := RecoverShardViewRegistry(t.Context(), catalog, newMockSyncer())
	require.NoError(t, err)
	defer reg.Close()
	manager := reg.Ensure(qviews.ShardID{ReplicaID: 1, VChannel: "p_100v1"})
	require.EqualValues(t, 42, manager.publishedServingLoadInfoVersion)
	require.EqualValues(t, 7, manager.publishedServingSchema)
	require.EqualValues(t, 3, *manager.publishedServingEpoch)
	reg.PublishCollectionSchema(100, 7)
	_, err = reg.ResolveServingGeneration(t.Context(), &viewpb.DataViewOfCollection{CollectionId: 100, ShardSetEpoch: 3}, 43, []int64{1})
	require.Error(t, err, "recovering a publication does not admit a new Load before exact Up coverage")
	manager.mu.Lock()
	old := generationTestView(3, "p_100v1", 1, nil)
	old.Meta.SchemaVersion = 7
	old.Meta.LoadInfoVersion = 41
	sm := RecoverCoordQueryViewStateMachineWithRef(old, nil)
	manager.views[sm.Version()] = sm
	manager.mu.Unlock()
	require.Nil(t, manager.PublishedGenerationRecovery(), "an older load must not recreate a published generation")
	manager.mu.Lock()
	sm.view = proto.Clone(old).(*viewpb.QueryViewOfShard)
	sm.view.Meta.LoadInfoVersion = 42
	manager.mu.Unlock()
	require.NotNil(t, manager.PublishedGenerationRecovery())
}

func TestServingPublicationDropClearsSchemaOnlyAfterDurableRemoval(t *testing.T) {
	for name, schema := range map[string]int32{"zero": 0, "nonzero": 7} {
		t.Run(name, func(t *testing.T) {
			catalog := &servingRecoveryCatalog{
				mockCatalog: newMockCatalog(),
				generationTestCatalog: &generationTestCatalog{saved: []*viewpb.ServingGeneration{
					{CollectionId: 100, LoadInfoVersion: 42, ReadSchemaVersion: schema, ShardSetEpoch: 3},
					{CollectionId: 200, LoadInfoVersion: 50, ReadSchemaVersion: 9, ShardSetEpoch: 2},
				}},
			}
			reg, err := RecoverShardViewRegistry(t.Context(), catalog, newMockSyncer())
			require.NoError(t, err)
			t.Cleanup(reg.Close)
			reg.PublishCollectionSchema(100, schema)
			reg.PublishCollectionSchema(200, 9)

			// Release removes resident views while keeping the live collection's
			// public schema and shard-set fence for a later Load.
			shard := qviews.ShardID{ReplicaID: 1, VChannel: "p_100v0"}
			require.NoError(t, reg.Ensure(shard).RequestRelease(t.Context()))
			require.Contains(t, reg.publishedSchemas, int64(100))
			require.Equal(t, schema, reg.publishedSchemas[100])
			require.ElementsMatch(t, []int64{100, 200}, reg.ServingCollectionIDs())

			catalog.err = errors.New("catalog delete unavailable")
			require.ErrorIs(t, reg.RemoveServingGeneration(t.Context(), 100), catalog.err)
			require.Contains(t, reg.publishedSchemas, int64(100))
			require.Equal(t, schema, reg.publishedSchemas[100])
			require.ElementsMatch(t, []int64{100, 200}, reg.ServingCollectionIDs())

			catalog.err = nil
			require.NoError(t, reg.RemoveServingGeneration(t.Context(), 100))
			require.NotContains(t, reg.publishedSchemas, int64(100))
			require.Equal(t, map[int64]int32{200: 9}, reg.publishedSchemas)
			require.Equal(t, []int64{200}, reg.ServingCollectionIDs())
			// An eventual repeated deletion must leave other collections intact.
			require.NoError(t, reg.RemoveServingGeneration(t.Context(), 100))
			require.Equal(t, map[int64]int32{200: 9}, reg.publishedSchemas)
		})
	}
}
