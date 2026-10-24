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

func TestXGBoostPullIdentityAndIdleLifetime(t *testing.T) {
	mockey.PatchConvey("pull resolves names afresh and idle eviction never destroys an active model", t, func() {
		paramtable.Init()
		key := paramtable.Get().CommonCfg.FileResourceCacheTTL.Key
		require.NoError(t, paramtable.Get().Save(key, "20ms"))
		defer paramtable.Get().Reset(key)
		var resourceID atomic.Int64
		resourceID.Store(1)
		mockey.Mock(fileresource.IsPullEnabled).Return(true).Build()
		mockey.Mock(fileresource.AcquireNamed).To(func(ctx context.Context, name string) (*fileresource.Resources, error) {
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
		first.Release()
		require.EqualValues(t, 0, closes.Load())
		resourceID.Store(2)
		third, err := cache.acquire(t.Context(), "model")
		require.NoError(t, err)
		require.NotSame(t, second.Model(), third.Model())
		require.EqualValues(t, 2, loads.Load())
		second.Release()
		require.Eventually(t, func() bool { return closes.Load() == 1 }, time.Second, time.Millisecond)
		require.False(t, third.cached.closed.Load())
		third.Release()
		require.Eventually(t, func() bool { return closes.Load() == 2 && cache.len() == 0 }, time.Second, time.Millisecond)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = cache.acquire(ctx, "model")
		require.ErrorIs(t, err, context.Canceled)
		require.EqualValues(t, 2, loads.Load())
	})
}
