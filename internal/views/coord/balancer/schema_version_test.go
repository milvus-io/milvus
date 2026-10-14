package balancer

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func testLatestSchema(context.Context, int64) (int32, error) { panic("mock with mockey") }

func TestReconcileUsesLatestSchemaAndPreemptsObsoletePreparing(t *testing.T) {
	latest, reads := int32(1), 0
	var readErr error
	patch := mockey.Mock(testLatestSchema).To(func(_ context.Context, id int64) (int32, error) {
		require.Equal(t, int64(1), id)
		reads++
		return latest, readErr
	}).Build()
	defer patch.UnPatch()
	reg := emptyRegistry(t)
	cfg := cfgFor(1, 10, []int64{1}, nil)
	c := newTestCache(cfg)
	id := cacheShard(1, 10)
	c.PublishDataView(1, cacheData(1, id.VChannel, 100))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	t.Cleanup(reg.RegisterPublicationListener(c.PublishShard))
	c.MarkReady()
	b := NewDefaultBalancer(c, reg, nil, testLatestSchema)
	originalLoadVersion := c.GetCollection(1).ConfigVersion()
	b.Trigger()
	require.NoError(t, b.Reconcile(t.Context()))
	first := reg.Get(id).Stats()
	require.Equal(t, int32(1), first.PreparingSchemaVersion)
	require.Equal(t, 1, reads)

	// A publication/DDL notification arriving while the old view is preparing
	// must supersede it, even though no load or data configuration changed.
	latest = 2
	b.Trigger(TriggerScope{DirtyCollections: []int64{1}})
	require.NoError(t, b.Reconcile(t.Context()))
	second := reg.Get(id).Stats()
	require.Equal(t, latest, second.PreparingSchemaVersion)
	require.True(t, second.PreparingVersion.GT(*first.PreparingVersion))
	require.Equal(t, first.PreparingVersion.DataVersion, second.PreparingVersion.DataVersion)
	require.Equal(t, originalLoadVersion, c.GetCollection(1).ConfigVersion())

	// A duplicate notification cannot stack another Preparing view.
	b.Trigger()
	require.NoError(t, b.Reconcile(t.Context()))
	require.Equal(t, second.PreparingVersion, reg.Get(id).Stats().PreparingVersion)

	// Failed metadata reads preserve the existing target and requeue work.
	readErr = merr.WrapErrServiceUnavailableMsg("metadata temporarily unavailable")
	b.Trigger()
	require.ErrorIs(t, b.Reconcile(t.Context()), merr.ErrServiceUnavailable)
	require.Equal(t, second.PreparingVersion, reg.Get(id).Stats().PreparingVersion)
	readErr, latest = nil, 3
	require.NoError(t, b.Reconcile(t.Context()))
	require.Equal(t, latest, reg.Get(id).Stats().PreparingSchemaVersion)

	// A new controller reconstructs the target from current metadata, with no
	// persistent load-config schema or remembered DDL notification.
	latest = 4
	recovered := NewDefaultBalancer(c, reg, nil, testLatestSchema)
	recovered.Trigger()
	require.NoError(t, recovered.Reconcile(t.Context()))
	require.Equal(t, latest, reg.Get(id).Stats().PreparingSchemaVersion)
	require.Equal(t, originalLoadVersion, c.GetCollection(1).ConfigVersion())
}

func TestSchemaChangeRebuildsUpWithoutChangingLoadIdentity(t *testing.T) {
	cfg := cfgFor(1, 10, []int64{1}, nil)
	c := newTestCache(cfg)
	id := cacheShard(1, 10)
	c.PublishDataView(1, cacheData(1, id.VChannel, 100))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	version := c.GetCollection(1).ConfigVersion()
	stats := testShardStats(ver(1, 0, 1), version, placement(1000, 1, 1, coordview.SegmentStateUp))
	stats.UpSchemaVersion = 1
	c.PublishShard(id, stats)
	reader := newPlanningContext(c)
	reader.schemaVersions = map[int64]int32{1: 2}
	config := *c.GetBalanceConfig()
	config.AutoBalance = false
	c.UpdateBalanceConfig(&config)
	reader.config = &config
	plan := NewDefaultBalancePolicy().Plan(reader, []qviews.ShardID{id})
	require.Contains(t, plan.Prepares, id)
	require.Equal(t, int32(2), plan.Prepares[id].Build().Meta.SchemaVersion)
	require.Equal(t, version, plan.Prepares[id].Build().Meta.LoadInfoVersion)
	require.Equal(t, stats.UpVersion.DataVersion, plan.Prepares[id].DataVersion())
}

func TestSchemaMetadataReadIsScopedAndFailuresAreIndependent(t *testing.T) {
	calls := map[int64]int{}
	patch := mockey.Mock(testLatestSchema).To(func(_ context.Context, id int64) (int32, error) {
		calls[id]++
		if id == 2 {
			return 0, merr.WrapErrServiceUnavailableMsg("metadata unavailable")
		}
		return 0, nil // Zero is a valid initial schema revision.
	}).Build()
	defer patch.UnPatch()
	c := newTestCache(cfgFor(1, 10, nil, nil))
	c.PublishLoadConfig(2, cfgFor(2, 20, nil, nil), 1)
	first, failed, released := cacheShard(1, 10), cacheShard(2, 20), cacheShard(3, 30)
	second := first
	second.VChannel = "by-dev-rootcoord-dml_0_1v1"
	b := NewDefaultBalancer(c, nil, nil, testLatestSchema)
	reader := newPlanningContext(c)
	ready, err := b.readSchemaVersions(t.Context(), reader, []qviews.ShardID{first, second, failed, released})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.ElementsMatch(t, []qviews.ShardID{first, second, released}, ready)
	require.Equal(t, map[int64]int{1: 1, 2: 1}, calls)
	require.Equal(t, map[int64]int32{1: 0}, reader.schemaVersions)
}

func TestSchemaMustBeReadBeforePreparingNewlyObservedCollection(t *testing.T) {
	c := newTestCache(cfgFor(1, 10, nil, nil))
	id := cacheShard(1, 10)
	c.PublishDataView(1, cacheData(1, id.VChannel, 100))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	reader := newPlanningContext(c)
	reader.schemaVersions = map[int64]int32{} // Collection was absent at metadata read.
	plan := NewDefaultBalancePolicy().Plan(reader, []qviews.ShardID{id})
	require.Empty(t, plan.Prepares)
	require.Equal(t, []qviews.ShardID{id}, plan.Retries)
}

func TestSchemaPreparationFollowsMaterializedDataView(t *testing.T) {
	cfg := cfgFor(1, 10, []int64{1}, nil)
	c := newTestCache(cfg)
	id := cacheShard(1, 10)
	data := cacheData(1, id.VChannel, 100)
	data.DataVersion.CompactVersion = 1
	c.PublishDataView(1, data)
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	version := c.GetCollection(1).ConfigVersion()
	stats := testShardStats(ver(1, 0, 1), version, placement(1000, 1, 1, coordview.SegmentStateUp))
	stats.PreparingVersion = ver(1, 0, 2)
	stats.PreparingSchemaVersion = 1
	c.PublishShard(id, stats)
	reader := newPlanningContext(c)
	reader.schemaVersions = map[int64]int32{1: 1}
	require.Equal(t, actionMust, classifyShard(reader, id), "new Function outputs require the compaction's new Manifest")
	plan := NewDefaultBalancePolicy().Plan(reader, []qviews.ShardID{id})
	require.Contains(t, plan.Prepares, id)
	require.Equal(t, data.DataVersion, plan.Prepares[id].DataVersion())
	require.EqualValues(t, 1, plan.Prepares[id].Build().Meta.SchemaVersion)

	stats.PreparingVersion = ver(1, 1, 1)
	c.PublishShard(id, stats)
	require.Equal(t, actionNone, classifyShard(newPlanningContext(c), id), "do not repeatedly supersede the same physical base")
}
