// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/stretchr/testify/assert"
)

func TestCache(t *testing.T) {
	data := map[string]int{
		"1": 1,
		"2": 2,
	}

	wg := sync.WaitGroup{}
	c := NewCache(WithInsertionListener(func(string, int) {
		wg.Done()
	}))
	defer c.Close()

	wg.Add(len(data))
	for k, v := range data {
		c.Put(k, v)
	}
	wg.Wait()

	for k, dv := range data {
		v, ok := c.GetIfPresent(k)
		assert.True(t, ok)
		assert.Equal(t, v, dv)
	}

	ret := c.Scan(
		func(k string, v int) bool {
			return true
		},
	)
	for k, v := range ret {
		dv, ok := data[k]
		assert.True(t, ok)
		assert.Equal(t, dv, v)
	}
}

func TestMaximumSize(t *testing.T) {
	max := 10
	wg := sync.WaitGroup{}
	insFunc := func(k int, v int) {
		wg.Done()
	}
	c := NewCache(WithMaximumSize[int, int](int64(max)), WithInsertionListener(insFunc)).(*localCache[int, int])
	defer c.Close()

	wg.Add(max)
	for i := 0; i < max; i++ {
		c.Put(i, i)
	}
	wg.Wait()
	n := cacheSize(&c.cache)
	assert.Equal(t, n, max)

	c.onInsertion = nil
	for i := 0; i < 2*max; i++ {
		k := rand.Intn(2 * max)
		c.Put(k, k)
		time.Sleep(time.Duration(i+1) * time.Millisecond)
		n = cacheSize(&c.cache)
		assert.Equal(t, n, max)
	}
}

func TestRemovalListener(t *testing.T) {
	removed := make(map[int]int)
	wg := sync.WaitGroup{}
	remFunc := func(k int, v int) {
		removed[k] = v
		wg.Done()
	}
	insFunc := func(k int, v int) {
		wg.Done()
	}
	max := 3
	c := NewCache(WithMaximumSize[int, int](int64(max)), WithRemovalListener(remFunc),
		WithInsertionListener(insFunc))
	defer c.Close()

	wg.Add(max + 2)
	for i := 1; i < max+2; i++ {
		c.Put(i, i)
	}
	wg.Wait()
	assert.Equal(t, 1, len(removed))
	assert.Equal(t, 1, removed[1])

	wg.Add(1)
	c.Invalidate(3)
	wg.Wait()
	assert.Equal(t, 2, len(removed))
	assert.Equal(t, 3, removed[3])

	wg.Add(2)
	c.InvalidateAll()
	wg.Wait()
	assert.Equal(t, 4, len(removed))
	assert.Equal(t, 2, removed[2])
	assert.Equal(t, 4, removed[4])
}

func TestClose(t *testing.T) {
	removed := 0
	wg := sync.WaitGroup{}
	remFunc := func(k int, v int) {
		removed++
		wg.Done()
	}
	insFunc := func(k int, v int) {
		wg.Done()
	}
	c := NewCache(WithRemovalListener(remFunc), WithInsertionListener(insFunc))
	n := 10
	wg.Add(n)
	for i := 0; i < n; i++ {
		c.Put(i, i)
	}
	wg.Wait()
	wg.Add(n)
	c.Close()
	wg.Wait()
	assert.Equal(t, n, removed)
}

func TestLoadingCache(t *testing.T) {
	loadCount := 0
	loader := func(k int) (int, error) {
		loadCount++
		if k%2 != 0 {
			return 0, errors.New("odd")
		}
		return k, nil
	}
	wg := sync.WaitGroup{}
	insFunc := func(int, int) {
		wg.Done()
	}
	c := NewLoadingCache(loader, WithInsertionListener(insFunc))
	defer c.Close()
	wg.Add(1)
	v, err := c.Get(2)
	assert.NoError(t, err)
	assert.Equal(t, 2, v)
	assert.Equal(t, 1, loadCount)

	wg.Wait()
	v, err = c.Get(2)
	assert.NoError(t, err)
	assert.Equal(t, 2, v)
	assert.Equal(t, 1, loadCount)

	_, err = c.Get(1)
	assert.Error(t, err)
	// Should not insert
	wg.Wait()
}

func TestCacheStats(t *testing.T) {
	wg := sync.WaitGroup{}
	loader := func(k string) (string, error) {
		return k, nil
	}
	insFunc := func(string, string) {
		wg.Done()
	}
	c := NewLoadingCache(loader, WithInsertionListener(insFunc))
	defer c.Close()

	wg.Add(1)
	_, err := c.Get("x")
	assert.NoError(t, err)

	st := c.Stats()
	assert.Equal(t, uint64(1), st.MissCount)
	assert.Equal(t, uint64(1), st.LoadSuccessCount)
	assert.True(t, st.TotalLoadTime > 0)

	wg.Wait()
	_, err = c.Get("x")
	assert.NoError(t, err)

	st = c.Stats()
	assert.Equal(t, uint64(1), st.HitCount)
}

func TestExpireAfterAccess(t *testing.T) {
	wg := sync.WaitGroup{}
	fn := func(k uint, v uint) {
		wg.Done()
	}
	mockTime := newMockTime()
	currentTime = mockTime.now
	defer resetCurrentTime()
	c := NewCache(WithExpireAfterAccess[uint, uint](1*time.Second), WithRemovalListener(fn),
		WithInsertionListener(fn)).(*localCache[uint, uint])
	defer c.Close()

	wg.Add(1)
	c.Put(1, 1)
	wg.Wait()

	mockTime.add(1 * time.Second)
	wg.Add(2)
	c.Put(2, 2)
	c.Put(3, 3)
	wg.Wait()
	n := cacheSize(&c.cache)
	if n != 3 {
		wg.Add(n)
		assert.Fail(t, fmt.Sprintf("unexpected cache size: %d, want: %d", n, 3))
	}

	mockTime.add(1 * time.Nanosecond)
	wg.Add(2)
	c.Put(4, 4)
	wg.Wait()
	n = cacheSize(&c.cache)
	wg.Add(n)
	assert.Equal(t, 3, n)

	_, ok := c.GetIfPresent(1)
	assert.False(t, ok)
}

func TestExpireAfterWrite(t *testing.T) {
	loadCount := 0
	loader := func(k string) (int, error) {
		loadCount++
		return loadCount, nil
	}

	mockTime := newMockTime()
	currentTime = mockTime.now
	defer resetCurrentTime()
	c := NewLoadingCache(loader, WithExpireAfterWrite[string, int](1*time.Second))
	defer c.Close()

	// New value
	v, err := c.Get("refresh")
	assert.NoError(t, err)
	assert.Equal(t, 1, v)
	assert.Equal(t, 1, loadCount)

	time.Sleep(200 * time.Millisecond)
	// Within 1s, the value should not yet expired.
	mockTime.add(1 * time.Second)
	v, err = c.Get("refresh")
	assert.NoError(t, err)
	assert.Equal(t, 1, v)
	assert.Equal(t, 1, loadCount)

	// After 1s, the value should be expired and refresh triggered.
	mockTime.add(1 * time.Nanosecond)
	v, err = c.Get("refresh")
	assert.NoError(t, err)
	assert.Equal(t, 2, v)
	assert.Equal(t, 2, loadCount)

	// value has already been loaded.
	v, err = c.Get("refresh")
	assert.NoError(t, err)
	assert.Equal(t, 2, v)
	assert.Equal(t, 2, loadCount)
}

func TestRefreshAfterWrite(t *testing.T) {
	var mutex sync.Mutex
	loaded := make(map[int]int)
	loader := func(k int) (int, error) {
		mutex.Lock()
		n := loaded[k]
		n++
		loaded[k] = n
		mutex.Unlock()
		return n, nil
	}
	wg := sync.WaitGroup{}
	insFunc := func(int, int) {
		wg.Done()
	}
	mockTime := newMockTime()
	currentTime = mockTime.now
	defer resetCurrentTime()
	c := NewLoadingCache(loader,
		WithExpireAfterAccess[int, int](4*time.Second),
		WithRefreshAfterWrite[int, int](2*time.Second),
		WithInsertionListener(insFunc))
	defer c.Close()

	wg.Add(3)
	v, err := c.Get(1)
	assert.NoError(t, err)
	assert.Equal(t, 1, v)

	// 3s
	mockTime.add(3 * time.Second)
	v, err = c.Get(2)
	assert.NoError(t, err)
	assert.Equal(t, 1, v)

	wg.Wait()
	assert.Equal(t, 2, loaded[1])
	assert.Equal(t, 1, loaded[2])

	v, err = c.Get(1)
	assert.NoError(t, err)
	assert.Equal(t, 2, v)

	// 8s
	mockTime.add(5 * time.Second)
	wg.Add(1)
	v, err = c.Get(1)
	assert.NoError(t, err)
	assert.Equal(t, 3, v)
}

func TestGetIfPresentExpired(t *testing.T) {
	wg := sync.WaitGroup{}
	insFunc := func(int, string) {
		wg.Done()
	}
	mockTime := newMockTime()
	currentTime = mockTime.now
	defer resetCurrentTime()
	c := NewCache(WithExpireAfterWrite[int, string](1*time.Second), WithInsertionListener(insFunc))
	defer c.Close()

	v, ok := c.GetIfPresent(0)
	assert.False(t, ok)

	wg.Add(1)
	c.Put(0, "0")
	v, ok = c.GetIfPresent(0)
	assert.True(t, ok)
	assert.Equal(t, "0", v)

	wg.Wait()
	mockTime.add(2 * time.Second)
	_, ok = c.GetIfPresent(0)
	assert.False(t, ok)
}

func TestWithAsyncInitPreLoader(t *testing.T) {
	wg := sync.WaitGroup{}
	data := map[string]string{
		"1": "1",
		"2": "1",
		"3": "1",
	}

	wg.Add(1)
	cnt := len(data)
	i := 0
	insFunc := func(k string, v string) {
		r, ok := data[k]
		assert.True(t, ok)
		assert.Equal(t, v, r)
		i++
		if i == cnt {
			wg.Done()
		}
	}

	loader := func(k string) (string, error) {
		assert.Fail(t, "should not reach here!")
		return "", nil
	}

	preLoaderFunc := func() (map[string]string, error) {
		return data, nil
	}

	c := NewLoadingCache(loader, WithMaximumSize[string, string](3),
		WithInsertionListener(insFunc), WithAsyncInitPreLoader(preLoaderFunc))
	defer c.Close()
	wg.Wait()

	_, ok := c.GetIfPresent("1")
	assert.True(t, ok)
	_, ok = c.GetIfPresent("2")
	assert.True(t, ok)
	_, ok = c.GetIfPresent("3")
	assert.True(t, ok)
}

func TestSynchronousReload(t *testing.T) {
	var val string
	loader := func(k int) (string, error) {
		if val == "" {
			return "", errors.New("empty")
		}
		return val, nil
	}

	c := NewLoadingCache(loader, WithExpireAfterWrite[int, string](200*time.Millisecond))
	val = "a"
	v, err := c.Get(1)
	assert.NoError(t, err)
	assert.Equal(t, val, v)

	val = "b"
	time.Sleep(300 * time.Millisecond)
	v, err = c.Get(1)
	assert.NoError(t, err)
	assert.Equal(t, val, v)

	val = ""
	_, err = c.Get(2)
	assert.Error(t, err)
}

func TestCloseMultiple(t *testing.T) {
	c := NewCache[int, int]()
	start := make(chan bool)
	const n = 10
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			c.Close()
		}()
	}
	close(start)
	wg.Wait()
	// Should not panic
	assert.NotPanics(t, func() {
		c.GetIfPresent(0)
	})
	assert.NotPanics(t, func() {
		c.Put(1, 1)
	})
	assert.NotPanics(t, func() {
		c.Invalidate(0)
	})
	assert.NotPanics(t, func() {
		c.InvalidateAll()
	})
	assert.NotPanics(t, func() {
		c.Close()
	})
}

func BenchmarkGetSame(b *testing.B) {
	c := NewCache[string, string]()
	defer c.Close()
	c.Put("*", "*")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.GetIfPresent("*")
		}
	})
}

// mockTime is used for tests which required current system time.
type mockTime struct {
	mu    sync.RWMutex
	value time.Time
}

func newMockTime() *mockTime {
	return &mockTime{
		value: time.Now(),
	}
}

func (t *mockTime) add(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.value = t.value.Add(d)
}

func (t *mockTime) now() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.value
}

func resetCurrentTime() {
	currentTime = time.Now
}
