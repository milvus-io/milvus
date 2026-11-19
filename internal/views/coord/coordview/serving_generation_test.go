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
	saved []*viewpb.DataViewOfCollection
	err   error
}

func (c *generationTestCatalog) ListServingGenerations(context.Context) ([]*viewpb.DataViewOfCollection, error) {
	return c.saved, nil
}

func (c *generationTestCatalog) SaveServingGeneration(_ context.Context, g *viewpb.DataViewOfCollection) error {
	if c.err != nil {
		return c.err
	}
	c.saved = append(c.saved, proto.Clone(g).(*viewpb.DataViewOfCollection))
	return nil
}

func generationTestView(epoch uint64, channel string, replica int64, lineage *viewpb.QueryViewLineage) *viewpb.QueryViewOfShard {
	return &viewpb.QueryViewOfShard{Meta: &viewpb.QueryViewMeta{
		CollectionId: 100, ReplicaId: replica, Vchannel: channel, ShardSetEpoch: epoch,
		Version:       &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, QueryVersion: 1},
		SchemaVersion: 1, State: viewpb.QueryViewState_QueryViewStateUp, Lineage: lineage,
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
	_, err := reg.ResolveServingGeneration(t.Context(), desired, []int64{1, 2})
	require.Error(t, err)
	require.Empty(t, catalog.saved)
	installGenerationView(reg, 2, "p_100v0", 2, lineage)
	catalog.err = errors.New("catalog unavailable")
	_, err = reg.ResolveServingGeneration(t.Context(), desired, []int64{1, 2})
	require.ErrorIs(t, err, catalog.err)
	require.Empty(t, reg.serving)
	catalog.err = nil
	got, err := reg.ResolveServingGeneration(t.Context(), desired, []int64{1, 2})
	require.NoError(t, err)
	require.EqualValues(t, 2, got.GetShardSetEpoch())
	require.Len(t, catalog.saved, 1)
	got.Shards[0].Vchannel = "mutated"
	again, err := reg.ResolveServingGeneration(t.Context(), desired, []int64{1, 2})
	require.NoError(t, err)
	require.Equal(t, "p_100v0", again.GetShards()[0].GetVchannel())
	require.Len(t, catalog.saved, 1)
}

func (c *generationTestCatalog) RemoveServingGeneration(context.Context, int64) error { return c.err }

func TestServingPublicationWaitsForSNAndReplaysPreemptionAfterRecovery(t *testing.T) {
	reg := newTestRegistry(t, newMockCatalog(), newMockSyncer())
	catalog := &generationTestCatalog{}
	reg.servingCatalog = catalog
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
	first, err := reg.ResolveServingGeneration(t.Context(), desired, []int64{1})
	require.NoError(t, err)
	require.Zero(t, first.GetShardSetEpoch(), "Coord Up intent cannot publish before SN confirms Up")
	require.Len(t, first.GetShards(), 1)
	manager.mu.Lock()
	next.snState = qviews.QueryViewStateUp
	manager.mu.Unlock()
	published, err := reg.ResolveServingGeneration(t.Context(), desired, []int64{1})
	require.NoError(t, err)
	require.EqualValues(t, 2, published.GetShardSetEpoch())
	require.False(t, reg.ServingTransitionComplete(100, 2, false), "persisted preemption is not delivery proof")
	manager.mu.Lock()
	require.True(t, old.View().GetMeta().GetControl().GetPreemptServingLease())
	old.snState = qviews.QueryViewStateNil // recovery does not assume prior SN acknowledgments
	manager.mu.Unlock()
	_, err = reg.ResolveServingGeneration(t.Context(), desired, []int64{1})
	require.NoError(t, err)
	require.False(t, reg.ServingTransitionComplete(100, 2, false))
	manager.mu.Lock()
	old.snState = qviews.QueryViewStateDown
	manager.mu.Unlock()
	require.True(t, reg.ServingTransitionComplete(100, 2, false))
	require.NoError(t, reg.RemoveServingGeneration(t.Context(), 100))
	require.Empty(t, reg.serving)
}
