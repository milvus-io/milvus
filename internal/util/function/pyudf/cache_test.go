// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pyudf

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type fakeResourceLoader struct {
	load func(context.Context, fileresource.ResolvedFileResource, string) (LoadedResource, error)
}

func (l *fakeResourceLoader) Load(ctx context.Context, resource fileresource.ResolvedFileResource, stage string) (LoadedResource, error) {
	return l.load(ctx, resource, stage)
}

type fakeLoadedResource struct {
	run        func(context.Context, *schemapb.FunctionParamObject, []*arrow.Chunked) ([]*arrow.Chunked, error)
	close      func() error
	closeCount atomic.Int64
}

func (r *fakeLoadedResource) Run(ctx context.Context, params *schemapb.FunctionParamObject, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	if r.run == nil {
		return []*arrow.Chunked{}, nil
	}
	return r.run(ctx, params, inputs)
}

func (r *fakeLoadedResource) Close() error {
	r.closeCount.Add(1)
	if r.close == nil {
		return nil
	}
	return r.close()
}

func testWheelResource(id int64, name string) *fileresource.ResolvedFileResource {
	return &fileresource.ResolvedFileResource{
		ID:        id,
		Name:      name,
		Path:      fmt.Sprintf("/remote/%s-%d.whl", name, id),
		LocalPath: fmt.Sprintf("/local/%d/%s.whl", id, name),
	}
}

func newTestCache(t *testing.T, loader ResourceLoader) *Cache {
	t.Helper()
	cache, err := NewCache(context.Background(), loader, time.Second)
	require.NoError(t, err)
	t.Cleanup(cache.Close)
	return cache
}

func syncCacheResource(t *testing.T, cache *Cache, resources ...*fileresource.ResolvedFileResource) {
	t.Helper()
	cache.mu.RLock()
	version := cache.snapshotVersion + 1
	cache.mu.RUnlock()
	require.NoError(t, cache.OnFileResourceSync(fileresource.SyncEvent{Version: version, Resources: resources}))
}

func TestCacheSnapshotReadinessAndVersion(t *testing.T) {
	cache := newTestCache(t, nil)
	_, err := cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	assert.True(t, merr.IsRetryableErr(err))

	require.NoError(t, cache.OnFileResourceSync(fileresource.SyncEvent{Version: cache.snapshotVersion + 1}))
	_, err = cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)

	newResource := testWheelResource(2, "rank_udf")
	require.NoError(t, cache.OnFileResourceSync(fileresource.SyncEvent{Version: 3, Resources: []*fileresource.ResolvedFileResource{newResource}}))
	oldResource := testWheelResource(1, "rank_udf")
	require.NoError(t, cache.OnFileResourceSync(fileresource.SyncEvent{Version: 1, Resources: []*fileresource.ResolvedFileResource{oldResource}}))
	resolved, err := cache.resolveResource("rank_udf")
	require.NoError(t, err)
	assert.Equal(t, int64(2), resolved.ID)
}

func TestCacheResourceIndex(t *testing.T) {
	cache := newTestCache(t, nil)
	wheel := testWheelResource(1, "rank_udf")
	upperWheel := testWheelResource(2, "upper_udf")
	upperWheel.Path = "/remote/upper.WHL"
	upperWheel.LocalPath = "/local/upper.WHL"
	notWheel := testWheelResource(3, "not_wheel")
	notWheel.Path = "/remote/not_wheel.zip"

	syncCacheResource(t, cache, nil, wheel, upperWheel, notWheel)

	resolved, err := cache.resolveResource("rank_udf")
	require.NoError(t, err)
	assert.Equal(t, *wheel, resolved)
	resolved.LocalPath = "mutated"
	resolvedAgain, err := cache.resolveResource("rank_udf")
	require.NoError(t, err)
	assert.Equal(t, wheel.LocalPath, resolvedAgain.LocalPath)

	_, err = cache.resolveResource("upper_udf")
	require.NoError(t, err)
	_, err = cache.resolveResource("not_wheel")
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	_, err = cache.Acquire(context.Background(), "", "L2_rerank")
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
}

func TestCacheAcquireHitRunAndRelease(t *testing.T) {
	var loadCount atomic.Int64
	resource := &fakeLoadedResource{}
	params := &schemapb.FunctionParamObject{}
	ctx := context.WithValue(context.Background(), cacheTestContextKey{}, "value")
	resource.run = func(actualCtx context.Context, actualParams *schemapb.FunctionParamObject, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
		assert.Same(t, ctx, actualCtx)
		assert.Same(t, params, actualParams)
		assert.Empty(t, inputs)
		return []*arrow.Chunked{}, nil
	}
	cache := newTestCache(t, &fakeResourceLoader{load: func(_ context.Context, resolved fileresource.ResolvedFileResource, stage string) (LoadedResource, error) {
		loadCount.Add(1)
		assert.Equal(t, "rank_udf", resolved.Name)
		assert.Equal(t, "L2_rerank", stage)
		return resource, nil
	}})
	syncCacheResource(t, cache, testWheelResource(1, "rank_udf"))

	lease, err := cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
	require.NoError(t, err)
	outputs, err := lease.Run(ctx, params, nil)
	require.NoError(t, err)
	assert.Empty(t, outputs)
	lease.Release()
	lease.Release()
	_, err = lease.Run(ctx, params, nil)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)

	second, err := cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
	require.NoError(t, err)
	second.Release()
	assert.Equal(t, int64(1), loadCount.Load())
	assert.Equal(t, 1, cache.len())
}

type cacheTestContextKey struct{}

func TestCacheSingleflight(t *testing.T) {
	var loadCount atomic.Int64
	startLoad := make(chan struct{})
	allowLoad := make(chan struct{})
	resource := &fakeLoadedResource{}
	cache := newTestCache(t, &fakeResourceLoader{load: func(context.Context, fileresource.ResolvedFileResource, string) (LoadedResource, error) {
		if loadCount.Add(1) == 1 {
			close(startLoad)
		}
		<-allowLoad
		return resource, nil
	}})
	syncCacheResource(t, cache, testWheelResource(1, "rank_udf"))

	const goroutines = 16
	leases := make([]Lease, goroutines)
	errs := make([]error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(index int) {
			defer wg.Done()
			leases[index], errs[index] = cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
		}(i)
	}
	<-startLoad
	close(allowLoad)
	wg.Wait()

	for i := range leases {
		require.NoError(t, errs[i])
		require.NotNil(t, leases[i])
		leases[i].Release()
	}
	assert.Equal(t, int64(1), loadCount.Load())
}

func TestCacheLoadsDifferentStages(t *testing.T) {
	var loadCount atomic.Int64
	stages := sync.Map{}
	cache := newTestCache(t, &fakeResourceLoader{load: func(_ context.Context, _ fileresource.ResolvedFileResource, stage string) (LoadedResource, error) {
		loadCount.Add(1)
		stages.Store(stage, true)
		return &fakeLoadedResource{}, nil
	}})
	resource := testWheelResource(1, "rank_udf")
	syncCacheResource(t, cache, resource)

	l2, err := cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
	require.NoError(t, err)
	l2.Release()
	l1, err := cache.Acquire(context.Background(), "rank_udf", "L1_rerank")
	require.NoError(t, err)
	l1.Release()
	require.NoError(t, cache.OnFileResourceSync(fileresource.SyncEvent{Version: cache.snapshotVersion + 1, Resources: []*fileresource.ResolvedFileResource{resource}}))

	assert.Equal(t, int64(2), loadCount.Load())
	assert.Equal(t, 2, cache.len())
	_, ok := stages.Load("L2_rerank")
	assert.True(t, ok)
	_, ok = stages.Load("L1_rerank")
	assert.True(t, ok)
}

func TestCacheEvictionWaitsForLeases(t *testing.T) {
	resource := &fakeLoadedResource{}
	cache := newTestCache(t, &fakeResourceLoader{load: func(context.Context, fileresource.ResolvedFileResource, string) (LoadedResource, error) {
		return resource, nil
	}})
	syncCacheResource(t, cache, testWheelResource(1, "rank_udf"))
	first, err := cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
	require.NoError(t, err)
	second, err := cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
	require.NoError(t, err)

	require.NoError(t, cache.OnFileResourceSync(fileresource.SyncEvent{Version: cache.snapshotVersion + 1}))
	assert.Equal(t, 0, cache.len())
	assert.Equal(t, int64(0), resource.closeCount.Load())
	first.Release()
	assert.Equal(t, int64(0), resource.closeCount.Load())
	second.Release()
	assert.Equal(t, int64(1), resource.closeCount.Load())
	second.Release()
	assert.Equal(t, int64(1), resource.closeCount.Load())
}

func TestCacheRetriesWhenStaleLoadFails(t *testing.T) {
	var loadCount atomic.Int64
	var cache *Cache
	resourceV1 := testWheelResource(1, "rank_udf")
	resourceV2 := testWheelResource(2, "rank_udf")
	cache = newTestCache(t, &fakeResourceLoader{load: func(_ context.Context, resource fileresource.ResolvedFileResource, _ string) (LoadedResource, error) {
		if loadCount.Add(1) == 1 {
			require.Equal(t, int64(1), resource.ID)
			require.NoError(t, cache.OnFileResourceSync(fileresource.SyncEvent{Version: cache.snapshotVersion + 1, Resources: []*fileresource.ResolvedFileResource{resourceV2}}))
			return nil, merr.WrapErrServiceInternalMsg("old wheel was removed")
		}
		require.Equal(t, int64(2), resource.ID)
		return &fakeLoadedResource{}, nil
	}})
	syncCacheResource(t, cache, resourceV1)

	lease, err := cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
	require.NoError(t, err)
	lease.Release()
	assert.Equal(t, int64(2), loadCount.Load())
}

func TestCacheRetriesAfterSyncDuringLoad(t *testing.T) {
	var loadCount atomic.Int64
	var cache *Cache
	oldResource := &fakeLoadedResource{}
	newResource := &fakeLoadedResource{}
	resourceV1 := testWheelResource(1, "rank_udf")
	resourceV2 := testWheelResource(2, "rank_udf")
	loader := &fakeResourceLoader{load: func(_ context.Context, resource fileresource.ResolvedFileResource, _ string) (LoadedResource, error) {
		if loadCount.Add(1) == 1 {
			require.Equal(t, int64(1), resource.ID)
			require.NoError(t, cache.OnFileResourceSync(fileresource.SyncEvent{Version: cache.snapshotVersion + 1, Resources: []*fileresource.ResolvedFileResource{resourceV2}}))
			return oldResource, nil
		}
		require.Equal(t, int64(2), resource.ID)
		return newResource, nil
	}}
	cache = newTestCache(t, loader)
	syncCacheResource(t, cache, resourceV1)

	lease, err := cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
	require.NoError(t, err)
	lease.Release()
	assert.Equal(t, int64(2), loadCount.Load())
	assert.Equal(t, int64(1), oldResource.closeCount.Load())
	assert.Equal(t, int64(0), newResource.closeCount.Load())
}

func TestCacheCallerCancellationDoesNotCancelSharedLoad(t *testing.T) {
	startLoad := make(chan struct{})
	allowLoad := make(chan struct{})
	loadCanceled := make(chan struct{}, 1)
	resource := &fakeLoadedResource{}
	cache := newTestCache(t, &fakeResourceLoader{load: func(ctx context.Context, _ fileresource.ResolvedFileResource, _ string) (LoadedResource, error) {
		close(startLoad)
		select {
		case <-allowLoad:
			return resource, nil
		case <-ctx.Done():
			loadCanceled <- struct{}{}
			return nil, ctx.Err()
		}
	}})
	syncCacheResource(t, cache, testWheelResource(1, "rank_udf"))

	canceledCtx, cancel := context.WithCancel(context.Background())
	firstErr := make(chan error, 1)
	go func() {
		_, err := cache.Acquire(canceledCtx, "rank_udf", "L2_rerank")
		firstErr <- err
	}()
	<-startLoad

	secondLease := make(chan Lease, 1)
	secondErr := make(chan error, 1)
	go func() {
		lease, err := cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
		secondLease <- lease
		secondErr <- err
	}()
	cancel()
	assert.ErrorIs(t, <-firstErr, context.Canceled)
	close(allowLoad)
	lease := <-secondLease
	require.NoError(t, <-secondErr)
	require.NotNil(t, lease)
	lease.Release()
	select {
	case <-loadCanceled:
		t.Fatal("caller cancellation canceled the shared load")
	default:
	}
}

func TestCacheLoadErrorsAndRetry(t *testing.T) {
	t.Run("raw error becomes function failed and retries", func(t *testing.T) {
		rawErr := errors.New("load failed")
		var loadCount atomic.Int64
		cache := newTestCache(t, &fakeResourceLoader{load: func(context.Context, fileresource.ResolvedFileResource, string) (LoadedResource, error) {
			if loadCount.Add(1) == 1 {
				return nil, rawErr
			}
			return &fakeLoadedResource{}, nil
		}})
		syncCacheResource(t, cache, testWheelResource(1, "rank_udf"))

		_, err := cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrFunctionFailed)
		assert.ErrorIs(t, err, rawErr)
		lease, err := cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
		require.NoError(t, err)
		lease.Release()
		assert.Equal(t, int64(2), loadCount.Load())
	})

	t.Run("typed error preserved", func(t *testing.T) {
		typedErr := merr.WrapErrServiceUnavailableMsg("not ready")
		cache := newTestCache(t, &fakeResourceLoader{load: func(context.Context, fileresource.ResolvedFileResource, string) (LoadedResource, error) {
			return nil, typedErr
		}})
		syncCacheResource(t, cache, testWheelResource(1, "rank_udf"))
		_, err := cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
		assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	})

	t.Run("nil loader", func(t *testing.T) {
		cache := newTestCache(t, nil)
		syncCacheResource(t, cache, testWheelResource(1, "rank_udf"))
		_, err := cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
	})

	t.Run("nil loaded resource", func(t *testing.T) {
		cache := newTestCache(t, &fakeResourceLoader{load: func(context.Context, fileresource.ResolvedFileResource, string) (LoadedResource, error) {
			return nil, nil
		}})
		syncCacheResource(t, cache, testWheelResource(1, "rank_udf"))
		_, err := cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
	})

	t.Run("empty local path", func(t *testing.T) {
		cache := newTestCache(t, nil)
		resource := testWheelResource(1, "rank_udf")
		resource.LocalPath = ""
		syncCacheResource(t, cache, resource)
		_, err := cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
	})
}

func TestCacheLoadTimeout(t *testing.T) {
	var loadCount atomic.Int64
	cache, err := NewCache(context.Background(), &fakeResourceLoader{load: func(ctx context.Context, _ fileresource.ResolvedFileResource, _ string) (LoadedResource, error) {
		if loadCount.Add(1) == 1 {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return &fakeLoadedResource{}, nil
	}}, 10*time.Millisecond)
	require.NoError(t, err)
	defer cache.Close()
	syncCacheResource(t, cache, testWheelResource(1, "rank_udf"))

	_, err = cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	lease, err := cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
	require.NoError(t, err)
	lease.Release()
	assert.Equal(t, int64(2), loadCount.Load())
}

func TestCacheClose(t *testing.T) {
	resource := &fakeLoadedResource{close: func() error { return errors.New("close failed") }}
	cache := newTestCache(t, &fakeResourceLoader{load: func(context.Context, fileresource.ResolvedFileResource, string) (LoadedResource, error) {
		return resource, nil
	}})
	syncCacheResource(t, cache, testWheelResource(1, "rank_udf"))
	lease, err := cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
	require.NoError(t, err)

	cache.Close()
	cache.Close()
	assert.Equal(t, int64(0), resource.closeCount.Load())
	_, err = cache.Acquire(context.Background(), "rank_udf", "L2_rerank")
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	require.NoError(t, cache.OnFileResourceSync(fileresource.SyncEvent{Resources: []*fileresource.ResolvedFileResource{testWheelResource(2, "new")}}))

	lease.Release()
	assert.Equal(t, int64(1), resource.closeCount.Load())
}

func TestNewCacheValidation(t *testing.T) {
	_, err := NewCache(context.Background(), nil, 0)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	cache, err := NewCache(context.TODO(), nil, time.Second)
	require.NoError(t, err)
	cache.Close()
}
