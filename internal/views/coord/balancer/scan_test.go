package balancer

import (
	"fmt"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func scanTestCache(sizes ...int) *balancercache.Cache {
	c := balancercache.New(nil)
	for i, size := range sizes {
		id := int64(i + 1)
		c.PublishLoadConfig(id, cfgFor(id, id*10, nil, nil), 1)
		data := &CollectionDataView{CollectionID: id}
		for channel := 0; channel < size; channel++ {
			data.Shards = append(data.Shards, &ShardDataView{VChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv%d", id, channel)})
		}
		c.PublishDataView(id, api.PrepareCollectionDataView(data))
	}
	c.MarkReady()
	return c
}

func TestScanBatchesYieldToLocalEventsAndManagement(t *testing.T) {
	c := scanTestCache(60, 60, 60)
	t.Cleanup(c.Close)
	b := NewDefaultBalancer(c, &coordview.ShardViewRegistry{}, nil, nil)
	var batches [][]qviews.ShardID
	patch := mockey.Mock((*DefaultBalancePolicy).Plan).To(func(_ *DefaultBalancePolicy, _ balancercache.Reader, dirty []qviews.ShardID) *BalancePlan {
		batches = append(batches, append([]qviews.ShardID(nil), dirty...))
		if len(batches) == 1 {
			b.Trigger(TriggerScope{DirtyCollections: []int64{3}})
		}
		return nil
	}).Build()
	defer patch.UnPatch()
	b.Trigger()
	require.NoError(t, b.Reconcile(t.Context()))
	require.Len(t, batches[0], 120, "finish the collection that crosses 100")
	require.True(t, b.scanning)
	// The synchronous management entry can acquire the same execution lock
	// while a scan round is still in progress.
	_, err := b.Placement(t.Context(), 1)
	require.NoError(t, err)
	require.NoError(t, b.Reconcile(t.Context()))
	require.Len(t, batches[1], 60)
	for _, id := range batches[1] {
		require.Equal(t, int64(30), id.ReplicaID, "new local work precedes scan continuation")
	}
	require.True(t, b.scanning)
	require.NoError(t, b.Reconcile(t.Context()))
	require.Len(t, batches[2], 60, "the EOF tail is still planned")
	require.False(t, b.scanning)
	require.NoError(t, b.Reconcile(t.Context()))
	require.Len(t, batches, 3, "EOF must not cause an unrequested new round")
}

func TestScanKeepsLargeCollectionAndResidualShards(t *testing.T) {
	c := scanTestCache(150, 1)
	t.Cleanup(c.Close)
	residual := cacheShard(1, 99)
	c.PublishShard(residual, &coordview.ShardStats{})
	b := NewDefaultBalancer(c, &coordview.ShardViewRegistry{}, nil, nil)
	var batches [][]qviews.ShardID
	patch := mockey.Mock((*DefaultBalancePolicy).Plan).To(func(_ *DefaultBalancePolicy, _ balancercache.Reader, dirty []qviews.ShardID) *BalancePlan {
		batches = append(batches, append([]qviews.ShardID(nil), dirty...))
		return nil
	}).Build()
	defer patch.UnPatch()
	b.Trigger()
	require.NoError(t, b.Reconcile(t.Context()))
	require.Len(t, batches[0], 151)
	require.Contains(t, batches[0], residual)
	b.Trigger() // Another scan request must not reset the current cursor.
	require.NoError(t, b.Reconcile(t.Context()))
	require.Equal(t, []qviews.ShardID{cacheShard(2, 20)}, batches[1])
	require.NoError(t, b.Reconcile(t.Context()))
	require.Len(t, batches[2], 151, "coalesced request starts after the old round")
}

func TestScanEmptyCollectionsYieldAndLayoutRemovalIsScoped(t *testing.T) {
	sizes := make([]int, scanSlotBudget+1)
	c := scanTestCache(sizes...)
	t.Cleanup(c.Close)
	b := NewDefaultBalancer(c, &coordview.ShardViewRegistry{}, nil, nil)
	policy := b.policy.(*DefaultBalancePolicy)
	policy.layouts.domains = map[int64]map[string]*replicaLayout{1: {}, 2: {}}
	b.Trigger()
	require.NoError(t, b.Reconcile(t.Context()))
	require.True(t, b.scanning, "empty collections still count toward the scan budget")
	c.PublishLoadConfig(1, nil, 2)
	c.PublishDataView(1, nil)
	require.NoError(t, b.Reconcile(t.Context()))
	require.NotContains(t, policy.layouts.domains, int64(1))
	require.Contains(t, policy.layouts.domains, int64(2))
	require.True(t, b.scanning, "local removal does not advance the scanner")
	require.NoError(t, b.Reconcile(t.Context()))
	require.False(t, b.scanning)
}

func TestRetryDeadlineDoesNotBlockFreshScopes(t *testing.T) {
	q := newTriggerQueue()
	failed, fresh := cacheShard(1, 10), cacheShard(2, 20)
	q.addRetry(TriggerScope{DirtyShards: []qviews.ShardID{failed}})
	require.False(t, q.takePending().hasScopes(), "failed work waits for its deadline")
	q.add(TriggerScope{DirtyShards: []qviews.ShardID{fresh}})
	require.Equal(t, setOf(fresh), q.takePending().dirtyShards)
	q.mu.Lock()
	q.retryAt = time.Time{}
	q.mu.Unlock()
	q.add(TriggerScope{DirtyShards: []qviews.ShardID{fresh}})
	require.Equal(t, setOf(fresh), q.takePending().dirtyShards, "fresh work also precedes due retries")
	require.Equal(t, setOf(failed), q.takePending().dirtyShards)
	q.resetRetryDelay()
	require.Equal(t, 100*time.Millisecond, q.retryDelay)
}

func TestBalancerLoopServicesAPIEventDuringRetryBackoff(t *testing.T) {
	c := scanTestCache()
	t.Cleanup(c.Close)
	b := NewDefaultBalancer(c, &coordview.ShardViewRegistry{}, nil, nil)
	failed, fresh := cacheShard(1, 10), cacheShard(2, 20)
	b.queue.retryDelay = time.Hour
	b.queue.addRetry(TriggerScope{DirtyShards: []qviews.ShardID{failed}})
	planned := make(chan []qviews.ShardID, 4)
	patch := mockey.Mock((*DefaultBalancePolicy).Plan).To(func(_ *DefaultBalancePolicy, _ balancercache.Reader, dirty []qviews.ShardID) *BalancePlan {
		planned <- append([]qviews.ShardID(nil), dirty...)
		return nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	b.Start(t.Context())
	t.Cleanup(b.Stop)
	b.Trigger(TriggerScope{DirtyShards: []qviews.ShardID{fresh}})
	select {
	case ids := <-planned:
		require.Equal(t, []qviews.ShardID{fresh}, ids)
	case <-time.After(5 * time.Second):
		t.Fatal("a retry deadline prevented a fresh API event from reaching the algorithm")
	}
}
