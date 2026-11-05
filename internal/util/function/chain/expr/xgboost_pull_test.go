package expr

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestXGBoostPullCacheHitAndIdleLifetime(t *testing.T) {
	mockey.PatchConvey("pull only resolves cache misses and eviction never destroys an active model", t, func() {
		paramtable.Init()
		key := paramtable.Get().CommonCfg.FileResourceCacheTTL.Key
		require.NoError(t, paramtable.Get().Save(key, "20ms"))
		defer paramtable.Get().Reset(key)
		var resourceID atomic.Int64
		resourceID.Store(1)
		mockey.Mock(fileresource.IsPullEnabled).Return(true).Build()
		fetch := mockey.Mock(fileresource.AcquireNamed).To(func(ctx context.Context, name string) (*fileresource.Resources, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			require.Equal(t, "model", name)
			return &fileresource.Resources{}, nil
		}).Build()
		mockey.Mock((*fileresource.Resources).Resolved).To(func(*fileresource.Resources) []*fileresource.ResolvedFileResource {
			return []*fileresource.ResolvedFileResource{testXGBoostResource(resourceID.Load(), "model")}
		}).Build()
		var loads, closes atomic.Int64
		cache := newXGBoostModelCache(func(*fileresource.ResolvedFileResource) (*modelHandle, error) {
			loads.Add(1)
			return &modelHandle{}, nil
		}, func(*modelHandle) error { closes.Add(1); return nil })
		defer cache.evictStaleModels(nil)
		first, err := cache.acquire(t.Context(), "model")
		require.NoError(t, err)
		second, err := cache.acquire(t.Context(), "model")
		require.NoError(t, err)
		require.Same(t, first.Model(), second.Model())
		require.Equal(t, 1, fetch.Times(), "a warm model must not resolve its name again")
		first.Release()
		require.EqualValues(t, 0, closes.Load())
		resourceID.Store(2)
		third, err := cache.acquire(t.Context(), "model")
		require.NoError(t, err)
		require.Same(t, second.Model(), third.Model(), "name recreation is observed only after eviction")
		require.Equal(t, 1, fetch.Times())
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = cache.acquire(ctx, "model")
		require.ErrorIs(t, err, context.Canceled, "cancellation still applies on cache hits")
		cache.evictStaleModels(nil)
		require.Zero(t, closes.Load(), "active leases survive invalidation")
		fourth, err := cache.acquire(t.Context(), "model")
		require.NoError(t, err)
		require.NotSame(t, third.Model(), fourth.Model())
		require.Equal(t, 2, fetch.Times())
		require.EqualValues(t, 2, loads.Load())
		second.Release()
		third.Release()
		require.Eventually(t, func() bool { return closes.Load() == 1 }, time.Second, time.Millisecond)
		require.False(t, fourth.cached.closed.Load())
		fourth.Release()
		require.Eventually(t, func() bool { return closes.Load() == 2 && cache.len() == 0 }, time.Second, time.Millisecond)
		fifth, err := cache.acquire(t.Context(), "model")
		require.NoError(t, err)
		defer fifth.Release()
		require.Equal(t, 3, fetch.Times(), "idle expiry removes the name binding as well as the model")
		require.EqualValues(t, 3, loads.Load())
		_, err = cache.acquire(ctx, "model")
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 3, fetch.Times())
	})
}

func TestXGBoostNameIndexKeepsReplacement(t *testing.T) {
	cache := newXGBoostModelCache(func(*fileresource.ResolvedFileResource) (*modelHandle, error) {
		return &modelHandle{}, nil
	}, func(*modelHandle) error { return nil })
	defer cache.evictStaleModels(nil)
	old, _, err := cache.acquireOrLoad(testXGBoostResource(1, "model"))
	require.NoError(t, err)
	defer old.Release()
	resource := testXGBoostResource(2, "model")
	newer, _, err := cache.acquireOrLoad(resource)
	require.NoError(t, err)
	defer newer.Release()
	cache.evictStaleModels(map[string]struct{}{xgboostModelCacheKey(resource): {}})
	lease, ok := cache.tryAcquireByName("model")
	require.True(t, ok, "evicting the old ID must not remove the replacement's name binding")
	defer lease.Release()
	require.Same(t, newer.Model(), lease.Model())
}
