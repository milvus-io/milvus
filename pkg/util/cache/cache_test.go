package cache

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v2/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

var errTimeout = errors.New("timeout")

func TestLRUCache(t *testing.T) {
	cacheBuilder := NewCacheBuilder[int, int]().WithLoader(func(ctx context.Context, key int) (int, error) {
		return key, nil
	})

	t.Run("test loader", func(t *testing.T) {
		size := 10
		cache := cacheBuilder.WithCapacity(int64(size)).Build()

		for i := 0; i < size; i++ {
			missing, err := cache.Do(context.Background(), i, func(_ context.Context, v int) error {
				assert.Equal(t, i, v)
				return nil
			})
			assert.True(t, missing)
			assert.NoError(t, err)
		}
	})

	t.Run("test finalizer", func(t *testing.T) {
		size := 10
		finalizeSeq := make([]int, 0)
		cache := cacheBuilder.WithCapacity(int64(size)).WithFinalizer(func(ctx context.Context, key, value int) error {
			finalizeSeq = append(finalizeSeq, key)
			return nil
		}).Build()

		for i := 0; i < size*2; i++ {
			missing, err := cache.Do(context.Background(), i, func(_ context.Context, v int) error {
				assert.Equal(t, i, v)
				return nil
			})
			assert.True(t, missing)
			assert.NoError(t, err)
		}
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, finalizeSeq)

		// Hit the cache again, there should be no swap-out
		for i := size; i < size*2; i++ {
			missing, err := cache.Do(context.Background(), i, func(_ context.Context, v int) error {
				assert.Equal(t, i, v)
				return nil
			})
			assert.False(t, missing)
			assert.NoError(t, err)
		}
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, finalizeSeq)
	})

	t.Run("test scavenger", func(t *testing.T) {
		finalizeSeq := make([]int, 0)
		sumCapacity := 20 // inserting 1 to 19, capacity is set to sum of 20, expecting (19) at last.
		cache := cacheBuilder.WithLazyScavenger(func(key int) int64 {
			return int64(key)
		}, int64(sumCapacity)).WithFinalizer(func(ctx context.Context, key, value int) error {
			finalizeSeq = append(finalizeSeq, key)
			return nil
		}).Build()

		for i := 0; i < 20; i++ {
			missing, err := cache.Do(context.Background(), i, func(_ context.Context, v int) error {
				assert.Equal(t, i, v)
				return nil
			})
			assert.True(t, missing)
			assert.NoError(t, err)
		}
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}, finalizeSeq)
	})

	t.Run("test do negative", func(t *testing.T) {
		cache := cacheBuilder.Build()
		theErr := errors.New("error")
		missing, err := cache.Do(context.Background(), -1, func(_ context.Context, v int) error {
			return theErr
		})
		assert.True(t, missing)
		assert.Equal(t, theErr, err)
	})

	t.Run("test scavenge negative", func(t *testing.T) {
		finalizeSeq := make([]int, 0)
		sumCapacity := 20 // inserting 1 to 19, capacity is set to sum of 20, expecting (19) at last.
		cache := cacheBuilder.WithLazyScavenger(func(key int) int64 {
			return int64(key)
		}, int64(sumCapacity)).WithFinalizer(func(ctx context.Context, key, value int) error {
			finalizeSeq = append(finalizeSeq, key)
			return nil
		}).Build()

		for i := 0; i < 20; i++ {
			missing, err := cache.Do(context.Background(), i, func(_ context.Context, v int) error {
				assert.Equal(t, i, v)
				return nil
			})
			assert.True(t, missing)
			assert.NoError(t, err)
		}
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}, finalizeSeq)
		ctx, cancel := contextutil.WithTimeoutCause(context.Background(), time.Second, errTimeout)
		defer cancel()

		missing, err := cache.Do(ctx, 100, func(_ context.Context, v int) error {
			return nil
		})
		assert.True(t, missing)
		assert.ErrorIs(t, err, errTimeout)
		assert.ErrorIs(t, context.Cause(ctx), errTimeout)
	})

	t.Run("test load negative", func(t *testing.T) {
		cache := NewCacheBuilder[int, int]().WithLoader(func(ctx context.Context, key int) (int, error) {
			if key < 0 {
				return 0, merr.ErrParameterInvalid
			}
			return key, nil
		}).Build()
		missing, err := cache.Do(context.Background(), 0, func(_ context.Context, v int) error {
			return nil
		})
		assert.True(t, missing)
		assert.NoError(t, err)
		missing, err = cache.Do(context.Background(), -1, func(_ context.Context, v int) error {
			return nil
		})
		assert.True(t, missing)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("test reloader", func(t *testing.T) {
		cache := cacheBuilder.WithReloader(func(ctx context.Context, key int) (int, error) {
			return -key, nil
		}).Build()
		_, err := cache.Do(context.Background(), 1, func(_ context.Context, i int) error { return nil })
		assert.NoError(t, err)
		exist := cache.MarkItemNeedReload(context.Background(), 1)
		assert.True(t, exist)
		cache.Do(context.Background(), 1, func(_ context.Context, i int) error {
			assert.Equal(t, -1, i)
			return nil
		})
	})

	t.Run("test mark", func(t *testing.T) {
		cache := cacheBuilder.WithCapacity(1).Build()
		exist := cache.MarkItemNeedReload(context.Background(), 1)
		assert.False(t, exist)
		_, err := cache.Do(context.Background(), 1, func(_ context.Context, i int) error { return nil })
		assert.NoError(t, err)
		exist = cache.MarkItemNeedReload(context.Background(), 1)
		assert.True(t, exist)
	})
}

func TestStats(t *testing.T) {
	cacheBuilder := NewCacheBuilder[int, int]().WithLoader(func(ctx context.Context, key int) (int, error) {
		return key, nil
	})

	t.Run("test loader", func(t *testing.T) {
		size := 10
		cache := cacheBuilder.WithCapacity(int64(size)).Build()
		stats := cache.Stats()
		assert.Equal(t, uint64(0), stats.HitCount.Load())
		assert.Equal(t, uint64(0), stats.MissCount.Load())
		assert.Equal(t, uint64(0), stats.EvictionCount.Load())
		assert.Equal(t, uint64(0), stats.TotalLoadTimeMs.Load())
		assert.Equal(t, uint64(0), stats.TotalFinalizeTimeMs.Load())
		assert.Equal(t, uint64(0), stats.LoadSuccessCount.Load())
		assert.Equal(t, uint64(0), stats.LoadFailCount.Load())

		for i := 0; i < size; i++ {
			_, err := cache.Do(context.Background(), i, func(_ context.Context, v int) error {
				assert.Equal(t, i, v)
				return nil
			})
			assert.NoError(t, err)
		}
		assert.Equal(t, uint64(0), stats.HitCount.Load())
		assert.Equal(t, uint64(size), stats.MissCount.Load())
		assert.Equal(t, uint64(0), stats.EvictionCount.Load())
		// assert.True(t, stats.TotalLoadTimeMs.Load() > 0)
		assert.Equal(t, uint64(0), stats.TotalFinalizeTimeMs.Load())
		assert.Equal(t, uint64(size), stats.LoadSuccessCount.Load())
		assert.Equal(t, uint64(0), stats.LoadFailCount.Load())

		for i := 0; i < size; i++ {
			_, err := cache.Do(context.Background(), i, func(_ context.Context, v int) error {
				assert.Equal(t, i, v)
				return nil
			})
			assert.NoError(t, err)
		}
		assert.Equal(t, uint64(size), stats.HitCount.Load())
		assert.Equal(t, uint64(size), stats.MissCount.Load())
		assert.Equal(t, uint64(0), stats.EvictionCount.Load())
		assert.Equal(t, uint64(0), stats.TotalFinalizeTimeMs.Load())
		assert.Equal(t, uint64(size), stats.LoadSuccessCount.Load())
		assert.Equal(t, uint64(0), stats.LoadFailCount.Load())

		for i := size; i < size*2; i++ {
			_, err := cache.Do(context.Background(), i, func(_ context.Context, v int) error {
				assert.Equal(t, i, v)
				return nil
			})
			assert.NoError(t, err)
		}
		assert.Equal(t, uint64(size), stats.HitCount.Load())
		assert.Equal(t, uint64(size*2), stats.MissCount.Load())
		assert.Equal(t, uint64(size), stats.EvictionCount.Load())
		// assert.True(t, stats.TotalFinalizeTimeMs.Load() > 0)
		assert.Equal(t, uint64(size*2), stats.LoadSuccessCount.Load())
		assert.Equal(t, uint64(0), stats.LoadFailCount.Load())
	})
}

func TestLRUCacheConcurrency(t *testing.T) {
	t.Run("test race condition", func(t *testing.T) {
		numEvict := new(atomic.Int32)
		cache := NewCacheBuilder[int, int]().WithLoader(func(ctx context.Context, key int) (int, error) {
			return key, nil
		}).WithCapacity(10).WithFinalizer(func(ctx context.Context, key, value int) error {
			numEvict.Add(1)
			return nil
		}).Build()

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					_, err := cache.Do(context.Background(), j, func(_ context.Context, v int) error {
						return nil
					})
					assert.NoError(t, err)
				}
			}(i)
		}
		wg.Wait()
	})

	t.Run("test not enough space", func(t *testing.T) {
		cache := NewCacheBuilder[int, int]().WithLoader(func(ctx context.Context, key int) (int, error) {
			return key, nil
		}).WithCapacity(1).WithFinalizer(func(ctx context.Context, key, value int) error {
			return nil
		}).Build()

		var wg sync.WaitGroup  // Let key 1000 be blocked
		var wg1 sync.WaitGroup // Make sure goroutine is started
		wg.Add(1)
		wg1.Add(1)
		go cache.Do(context.Background(), 1000, func(_ context.Context, v int) error {
			wg1.Done()
			wg.Wait()
			return nil
		})
		wg1.Wait()

		ctx, cancel := contextutil.WithTimeoutCause(context.Background(), time.Second, errTimeout)
		defer cancel()
		_, err := cache.Do(ctx, 1001, func(_ context.Context, v int) error {
			return nil
		})
		wg.Done()
		assert.ErrorIs(t, err, errTimeout)
		assert.ErrorIs(t, context.Cause(ctx), errTimeout)
	})

	t.Run("test time out", func(t *testing.T) {
		cache := NewCacheBuilder[int, int]().WithLoader(func(ctx context.Context, key int) (int, error) {
			return key, nil
		}).WithCapacity(1).WithFinalizer(func(ctx context.Context, key, value int) error {
			return nil
		}).Build()

		var wg sync.WaitGroup  // Let key 1000 be blocked
		var wg1 sync.WaitGroup // Make sure goroutine is started
		wg.Add(1)
		wg1.Add(1)
		go cache.Do(context.Background(), 1000, func(_ context.Context, v int) error {
			wg1.Done()
			wg.Wait()
			return nil
		})
		wg1.Wait()

		ctx, cancel := contextutil.WithTimeoutCause(context.Background(), time.Nanosecond, errTimeout)
		defer cancel()
		missing, err := cache.Do(ctx, 1001, func(ctx context.Context, v int) error {
			return nil
		})
		wg.Done()
		assert.True(t, missing)
		assert.ErrorIs(t, err, errTimeout)
		assert.ErrorIs(t, context.Cause(ctx), errTimeout)
	})

	t.Run("test wait", func(t *testing.T) {
		cache := NewCacheBuilder[int, int]().WithLoader(func(ctx context.Context, key int) (int, error) {
			return key, nil
		}).WithCapacity(1).WithFinalizer(func(ctx context.Context, key, value int) error {
			return nil
		}).Build()

		var wg1 sync.WaitGroup // Make sure goroutine is started

		wg1.Add(1)
		go cache.Do(context.Background(), 1000, func(_ context.Context, v int) error {
			wg1.Done()
			time.Sleep(time.Second)
			return nil
		})
		wg1.Wait()

		ctx, cancel := contextutil.WithTimeoutCause(context.Background(), time.Second*2, errTimeout)
		defer cancel()
		missing, err := cache.Do(ctx, 1001, func(ctx context.Context, v int) error {
			return nil
		})
		assert.True(t, missing)
		assert.NoError(t, err)
	})

	t.Run("test wait race condition", func(t *testing.T) {
		numEvict := new(atomic.Int32)
		cache := NewCacheBuilder[int, int]().WithLoader(func(ctx context.Context, key int) (int, error) {
			return key, nil
		}).WithCapacity(5).WithFinalizer(func(ctx context.Context, key, value int) error {
			numEvict.Add(1)
			return nil
		}).Build()

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					ctx, cancel := contextutil.WithTimeoutCause(context.Background(), 2*time.Second, errTimeout)
					defer cancel()
					_, err := cache.Do(ctx, j, func(_ context.Context, v int) error {
						return nil
					})
					assert.NoError(t, err)
				}
			}(i)
		}
		wg.Wait()
	})

	t.Run("test concurrent reload and mark", func(t *testing.T) {
		cache := NewCacheBuilder[int, int]().WithLoader(func(ctx context.Context, key int) (int, error) {
			return key, nil
		}).WithCapacity(5).WithFinalizer(func(ctx context.Context, key, value int) error {
			return nil
		}).WithReloader(func(ctx context.Context, key int) (int, error) {
			return key, nil
		}).Build()

		for i := 0; i < 100; i++ {
			ctx, cancel := contextutil.WithTimeoutCause(context.Background(), 2*time.Second, errTimeout)
			defer cancel()
			cache.Do(ctx, i, func(ctx context.Context, v int) error { return nil })
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				for j := 0; j < 100; j++ {
					cache.MarkItemNeedReload(context.Background(), j)
				}
			}
		}()

		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				for j := 0; j < 100; j++ {
					ctx, cancel := contextutil.WithTimeoutCause(context.Background(), 2*time.Second, errTimeout)
					defer cancel()
					cache.Do(ctx, j, func(ctx context.Context, v int) error { return nil })
				}
			}
		}()
		wg.Wait()
	})

	t.Run("test remove", func(t *testing.T) {
		cache := NewCacheBuilder[int, int]().WithLoader(func(ctx context.Context, key int) (int, error) {
			return key, nil
		}).WithCapacity(5).WithFinalizer(func(ctx context.Context, key, value int) error {
			return nil
		}).WithReloader(func(ctx context.Context, key int) (int, error) {
			return key, nil
		}).Build()

		for i := 0; i < 100; i++ {
			ctx, cancel := contextutil.WithTimeoutCause(context.Background(), 2*time.Second, errTimeout)
			defer cancel()
			cache.Do(ctx, i, func(ctx context.Context, v int) error { return nil })
		}

		evicted := 0
		for i := 0; i < 100; i++ {
			if cache.Remove(context.Background(), i) == nil {
				evicted++
			}
		}
		assert.Equal(t, 100, evicted)

		for i := 0; i < 5; i++ {
			ctx, cancel := contextutil.WithTimeoutCause(context.Background(), 2*time.Second, errTimeout)
			defer cancel()
			cache.Do(ctx, i, func(ctx context.Context, v int) error { return nil })
		}
		wg := sync.WaitGroup{}
		wg.Add(5)
		for i := 0; i < 5; i++ {
			go func(i int) {
				defer wg.Done()
				cache.Do(context.Background(), i, func(ctx context.Context, v int) error {
					time.Sleep(3 * time.Second)
					return nil
				})
			}(i)
		}
		// wait for all goroutine to start
		time.Sleep(1 * time.Second)

		// all item shouldn't be evicted if they are in-used in 500ms.
		evictedCount := atomic.NewInt32(0)
		wgEvict := sync.WaitGroup{}
		wgEvict.Add(5)
		for i := 0; i < 5; i++ {
			go func(i int) {
				defer wgEvict.Done()
				ctx, cancel := contextutil.WithTimeoutCause(context.Background(), 500*time.Millisecond, errTimeout)
				defer cancel()

				if cache.Remove(ctx, i) == nil {
					evictedCount.Inc()
				}
			}(i)
		}
		wgEvict.Wait()
		assert.Zero(t, evictedCount.Load())

		// given enough time, all item should be evicted.
		evicted = 0
		for i := 0; i < 5; i++ {
			if cache.Remove(context.Background(), i) == nil {
				evicted++
			}
		}
		assert.Equal(t, 5, evicted)
	})
}
