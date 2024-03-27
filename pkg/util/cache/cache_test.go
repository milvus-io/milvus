package cache

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestLRUCache(t *testing.T) {
	cacheBuilder := NewCacheBuilder[int, int]().WithLoader(func(key int) (int, bool) {
		return key, true
	})

	t.Run("test loader", func(t *testing.T) {
		size := 10
		cache := cacheBuilder.WithCapacity(int64(size)).Build()

		for i := 0; i < size; i++ {
			missing, err := cache.Do(i, func(v int) error {
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
		cache := cacheBuilder.WithCapacity(int64(size)).WithFinalizer(func(key, value int) error {
			finalizeSeq = append(finalizeSeq, key)
			return nil
		}).Build()

		for i := 0; i < size*2; i++ {
			missing, err := cache.Do(i, func(v int) error {
				assert.Equal(t, i, v)
				return nil
			})
			assert.True(t, missing)
			assert.NoError(t, err)
		}
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, finalizeSeq)

		// Hit the cache again, there should be no swap-out
		for i := size; i < size*2; i++ {
			missing, err := cache.Do(i, func(v int) error {
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
		}, int64(sumCapacity)).WithFinalizer(func(key, value int) error {
			finalizeSeq = append(finalizeSeq, key)
			return nil
		}).Build()

		for i := 0; i < 20; i++ {
			missing, err := cache.Do(i, func(v int) error {
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
		missing, err := cache.Do(-1, func(v int) error {
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
		}, int64(sumCapacity)).WithFinalizer(func(key, value int) error {
			finalizeSeq = append(finalizeSeq, key)
			return nil
		}).Build()

		for i := 0; i < 20; i++ {
			missing, err := cache.Do(i, func(v int) error {
				assert.Equal(t, i, v)
				return nil
			})
			assert.True(t, missing)
			assert.NoError(t, err)
		}
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}, finalizeSeq)
		missing, err := cache.Do(100, func(v int) error {
			return nil
		})
		assert.True(t, missing)
		assert.Equal(t, ErrNotEnoughSpace, err)
	})

	t.Run("test load negative", func(t *testing.T) {
		cache := NewCacheBuilder[int, int]().WithLoader(func(key int) (int, bool) {
			if key < 0 {
				return 0, false
			}
			return key, true
		}).Build()
		missing, err := cache.Do(0, func(v int) error {
			return nil
		})
		assert.True(t, missing)
		assert.NoError(t, err)
		missing, err = cache.Do(-1, func(v int) error {
			return nil
		})
		assert.True(t, missing)
		assert.Equal(t, ErrNoSuchItem, err)
	})

	t.Run("test reloader", func(t *testing.T) {
		cache := cacheBuilder.WithReloader(func(key int) (int, bool) {
			return -key, true
		}).Build()
		err := cache.Do(1, func(i int) error { return nil })
		assert.NoError(t, err)
		exist := cache.MarkItemNeedReload(1)
		assert.True(t, exist)
		cache.Do(1, func(i int) error {
			assert.Equal(t, -1, i)
			return nil
		})
	})

	t.Run("test mark", func(t *testing.T) {
		cache := cacheBuilder.WithCapacity(1).Build()
		exist := cache.MarkItemNeedReload(1)
		assert.False(t, exist)
		err := cache.Do(1, func(i int) error { return nil })
		assert.NoError(t, err)
		exist = cache.MarkItemNeedReload(1)
		assert.True(t, exist)
	})
}

func TestStats(t *testing.T) {
	cacheBuilder := NewCacheBuilder[int, int]().WithLoader(func(key int) (int, bool) {
		return key, true
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
			_, err := cache.Do(i, func(v int) error {
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
			_, err := cache.Do(i, func(v int) error {
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
			_, err := cache.Do(i, func(v int) error {
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
		cache := NewCacheBuilder[int, int]().WithLoader(func(key int) (int, bool) {
			return key, true
		}).WithCapacity(10).WithFinalizer(func(key, value int) error {
			numEvict.Add(1)
			return nil
		}).Build()

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					_, err := cache.Do(j, func(v int) error {
						return nil
					})
					assert.NoError(t, err)
				}
			}(i)
		}
		wg.Wait()
	})

	t.Run("test not enough space", func(t *testing.T) {
		cache := NewCacheBuilder[int, int]().WithLoader(func(key int) (int, bool) {
			return key, true
		}).WithCapacity(1).WithFinalizer(func(key, value int) error {
			return nil
		}).Build()

		var wg sync.WaitGroup  // Let key 1000 be blocked
		var wg1 sync.WaitGroup // Make sure goroutine is started
		wg.Add(1)
		wg1.Add(1)
		go cache.Do(1000, func(v int) error {
			wg1.Done()
			wg.Wait()
			return nil
		})
		wg1.Wait()
		_, err := cache.Do(1001, func(v int) error {
			return nil
		})
		wg.Done()
		assert.Equal(t, ErrNotEnoughSpace, err)
	})

	t.Run("test time out", func(t *testing.T) {
		cache := NewCacheBuilder[int, int]().WithLoader(func(key int) (int, bool) {
			return key, true
		}).WithCapacity(1).WithFinalizer(func(key, value int) error {
			return nil
		}).Build()

		var wg sync.WaitGroup  // Let key 1000 be blocked
		var wg1 sync.WaitGroup // Make sure goroutine is started
		wg.Add(1)
		wg1.Add(1)
		go cache.Do(1000, func(v int) error {
			wg1.Done()
			wg.Wait()
			return nil
		})
		wg1.Wait()
		missing, err := cache.DoWait(1001, time.Nanosecond, func(v int) error {
			return nil
		})
		wg.Done()
		assert.True(t, missing)
		assert.Equal(t, ErrTimeOut, err)
	})

	t.Run("test wait", func(t *testing.T) {
		cache := NewCacheBuilder[int, int]().WithLoader(func(key int) (int, bool) {
			return key, true
		}).WithCapacity(1).WithFinalizer(func(key, value int) error {
			return nil
		}).Build()

		var wg1 sync.WaitGroup // Make sure goroutine is started

		wg1.Add(1)
		go cache.Do(1000, func(v int) error {
			wg1.Done()
			time.Sleep(time.Second)
			return nil
		})
		wg1.Wait()
		missing, err := cache.DoWait(1001, time.Second*2, func(v int) error {
			return nil
		})
		assert.True(t, missing)
		assert.NoError(t, err)
	})

	t.Run("test wait race condition", func(t *testing.T) {
		numEvict := new(atomic.Int32)
		cache := NewCacheBuilder[int, int]().WithLoader(func(key int) (int, bool) {
			return key, true
		}).WithCapacity(5).WithFinalizer(func(key, value int) error {
			numEvict.Add(1)
			return nil
		}).Build()

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for j := 0; j < 20; j++ {
					_, err := cache.DoWait(j, time.Second, func(v int) error {
						time.Sleep(time.Duration(rand.Intn(3)))
						return nil
					})
					assert.NoError(t, err)
				}
			}(i)
		}
		wg.Wait()
	})

	t.Run("test concurrent reload and mark", func(t *testing.T) {
		cache := NewCacheBuilder[int, int]().WithLoader(func(key int) (int, bool) {
			return key, true
		}).WithCapacity(5).WithFinalizer(func(key, value int) error {
			return nil
		}).WithReloader(func(key int) (int, bool) {
			return key, true
		}).Build()

		for i := 0; i < 100; i++ {
			cache.DoWait(i, 2*time.Second, func(v int) error { return nil })
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				for j := 0; j < 100; j++ {
					cache.MarkItemNeedReload(j)
				}
			}
		}()

		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				for j := 0; j < 100; j++ {
					cache.DoWait(j, 2*time.Second, func(v int) error { return nil })
				}
			}
		}()
		wg.Wait()
	})
}
