package balancer

import (
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestReleaseReloadDetachesPlanningState(t *testing.T) {
	c := replicaCache(1, 2)
	t.Cleanup(c.Close)
	policy := NewDefaultBalancePolicy()
	id := cacheShard(1, 10)
	cfg := c.GetCollection(1).LoadConfig()
	for i := 0; i < 100; i++ {
		pinned := newPlanningContext(c)
		policy.Plan(pinned, []qviews.ShardID{id})
		old := pinned.GetCollection(1)
		require.NotNil(t, old.Planning().Layout("rg1"))
		require.Positive(t, old.Planning().Replica(10).Fanout(id.VChannel))
		c.PublishLoadConfig(1, nil, 0)
		require.Nil(t, c.GetCollection(1).Planning().Replica(10))
		require.Nil(t, c.GetCollection(1).Planning().Layout("rg1"))
		// Even replaying the same config pointer, version and replica ID
		// must not let a late layout, activity or allocation enter this load.
		c.PublishLoadConfig(1, cfg, 1)
		fresh := c.GetCollection(1)
		pinned.refreshTargetInputs()
		require.Same(t, old, pinned.GetCollection(1))
		old.Planning().Replica(10).StoreFanout(id.VChannel, 0)
		require.NotNil(t, allocate(pinned, id, nil, false))
		require.Positive(t, old.Planning().Replica(10).Fanout(id.VChannel))
		old.Planning().StoreLayout("rg1", &balancercache.ReplicaLayout{})
		require.False(t, c.PublishReplicaActivity(old, map[int64]bool{10: false}))
		require.Zero(t, fresh.Planning().Replica(10).Fanout(id.VChannel))
		require.Nil(t, fresh.Planning().Layout("rg1"))
	}
}

func TestPartialRetirementPreservesSurvivingReplica(t *testing.T) {
	c := replicaCache(2, 4)
	t.Cleanup(c.Close)
	keep, retire := cacheShard(1, 10), cacheShard(1, 11)
	policy := NewDefaultBalancePolicy()
	policy.Plan(c, []qviews.ShardID{keep, retire})
	old := c.GetCollection(1)
	layout := old.Planning().Layout("rg1")
	h := 3
	old.Planning().Replica(10).StoreFanout(keep.VChannel, h)
	// AutoLoad and priority are scheduling inputs; preserve the planning handles.
	cfg := old.LoadConfig().Clone()
	cfg.AutoLoad = !cfg.AutoLoad
	cfg.Replicas[0].Priority++
	c.PublishLoadConfig(1, cfg, 2)
	current := c.GetCollection(1)
	require.Same(t, old.Planning(), current.Planning())
	require.Same(t, layout, current.Planning().Layout("rg1"))
	cfg = cfg.Clone()
	cfg.Replicas = cfg.Replicas[:1]
	c.PublishLoadConfig(1, cfg, 3)
	current = c.GetCollection(1)
	require.Nil(t, current.Planning().Replica(11))
	require.Equal(t, h, current.Planning().Replica(10).Fanout(keep.VChannel))
	old.Planning().StoreLayout("rg1", &balancercache.ReplicaLayout{})
	require.Same(t, layout, current.Planning().Layout("rg1"), "preserve the previous ownership as a repair seed")
	policy.Plan(c, []qviews.ShardID{keep})
	require.Equal(t, []int64{10}, current.Planning().Layout("rg1").Replicas)
}

func TestReleaseDiscoveryCanAdmitColdShardWhilePolicyIsLocked(t *testing.T) {
	c := scanTestCache(1, 1)
	t.Cleanup(c.Close)
	b := NewDefaultBalancer(c, nil, nil, nil)
	p := b.policy.(*DefaultBalancePolicy)
	old := c.GetCollection(1)
	entered, resume, planned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var build func(*balancercache.CollectionEntry, *balancercache.ReplicaLayout, []int64, []int64) *balancercache.ReplicaLayout
	patch := mockey.Mock(buildReplicaLayout).Origin(&build).To(func(entry *balancercache.CollectionEntry, previous *balancercache.ReplicaLayout, replicas, nodes []int64) *balancercache.ReplicaLayout {
		close(entered)
		<-resume
		return build(entry, previous, replicas, nodes)
	}).Build()
	defer patch.UnPatch()
	go func() {
		defer close(planned)
		p.Plan(c, []qviews.ShardID{cacheShard(1, 10)})
	}()
	finish := sync.OnceFunc(func() { close(resume); <-planned })
	defer finish()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("planner did not reach layout computation")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.PublishLoadConfig(1, nil, 2)
		b.Trigger(TriggerScope{DirtyCollections: []int64{2}})
		for b.dispatch(localChanges) >= 0 {
		}
		c.PublishLoadConfig(1, old.LoadConfig(), 1)
	}()
	select {
	case <-done:
		require.Contains(t, b.ready.items, cacheShard(2, 20))
	case <-time.After(5 * time.Second):
		t.Fatal("release or cold shard admission waited for the in-flight plan")
	}
	finish()
	require.NotNil(t, old.Planning().Layout("rg1"))
	require.Nil(t, c.GetCollection(1).Planning().Layout("rg1"))
}

// Several config publications can precede the next layout computation. A
// replica which left and later returned cannot carry an older layout forward.
func TestLayoutSeedRequiresContinuousMembership(t *testing.T) {
	c := replicaCache(2, 4)
	t.Cleanup(c.Close)
	p := NewDefaultBalancePolicy()
	p.Plan(c, []qviews.ShardID{cacheShard(1, 10)})
	old := c.GetCollection(1)
	cfg := old.LoadConfig().Clone()
	cfg.Replicas[0].ReplicaID = 12
	c.PublishLoadConfig(1, cfg, 2)
	require.Same(t, old.Planning().Layout("rg1"), c.GetCollection(1).Planning().Layout("rg1"))
	next := cfg.Clone()
	next.Replicas[0].ReplicaID = 10
	next.Replicas[1].ReplicaID = 13
	c.PublishLoadConfig(1, next, 3)
	fresh := c.GetCollection(1)
	require.NotSame(t, old.Planning().Replica(10), fresh.Planning().Replica(10))
	require.Nil(t, fresh.Planning().Layout("rg1"))
}
