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

package expr

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/fileresource"
)

func testXGBoostResource(id int64, name string) *fileresource.ResolvedFileResource {
	return &fileresource.ResolvedFileResource{
		ID:        id,
		Name:      name,
		Path:      fmt.Sprintf("/remote/%s-%d.ubj", name, id),
		LocalPath: fmt.Sprintf("/local/%d/%s.ubj", id, name),
	}
}

func TestXGBoostModelCacheResourceIndex(t *testing.T) {
	cache := newXGBoostModelCache(nil, nil)
	resource := testXGBoostResource(1, "rank_model")

	err := cache.OnFileResourceSync(fileresource.SyncEvent{Version: 1, Resources: []*fileresource.ResolvedFileResource{resource}})
	require.NoError(t, err)

	resolved, err := cache.resolveResource("rank_model")
	require.NoError(t, err)
	assert.Equal(t, resource.ID, resolved.ID)
	assert.Equal(t, resource.Name, resolved.Name)
	assert.Equal(t, resource.Path, resolved.Path)
	assert.Equal(t, resource.LocalPath, resolved.LocalPath)

	resolved.LocalPath = "mutated"
	resolvedAgain, err := cache.resolveResource("rank_model")
	require.NoError(t, err)
	assert.Equal(t, resource.LocalPath, resolvedAgain.LocalPath)

	_, err = cache.resolveResource("missing")
	assert.Error(t, err)
}

func TestXGBoostModelCacheResourceIndexIgnoresNonUBJ(t *testing.T) {
	cache := newXGBoostModelCache(nil, nil)
	jsonResource := testXGBoostResource(1, "rank_model_json")
	jsonResource.Path = "/remote/rank_model.json"
	jsonResource.LocalPath = "/local/rank_model.json"
	ubjResource := testXGBoostResource(2, "rank_model_ubj")
	ubjResource.Path = "/remote/rank_model.UBJ"
	ubjResource.LocalPath = "/local/rank_model.UBJ"

	err := cache.OnFileResourceSync(fileresource.SyncEvent{Version: 1, Resources: []*fileresource.ResolvedFileResource{jsonResource, ubjResource}})
	require.NoError(t, err)

	_, err = cache.resolveResource("rank_model_json")
	assert.Error(t, err)
	resolved, err := cache.resolveResource("rank_model_ubj")
	require.NoError(t, err)
	assert.Equal(t, ubjResource.ID, resolved.ID)
}

func TestXGBoostModelCacheAcquireOrLoadHit(t *testing.T) {
	var loadCount atomic.Int64
	var closeCount atomic.Int64
	cache := newXGBoostModelCache(func(resource *fileresource.ResolvedFileResource) (*modelHandle, error) {
		loadCount.Add(1)
		return &modelHandle{numFeatures: int(resource.ID)}, nil
	}, func(model *modelHandle) error {
		closeCount.Add(1)
		return nil
	})
	resource := testXGBoostResource(1, "rank_model")
	require.NoError(t, cache.OnFileResourceSync(fileresource.SyncEvent{Version: 1, Resources: []*fileresource.ResolvedFileResource{resource}}))

	lease, err := cache.acquireByResourceName("rank_model")
	require.NoError(t, err)
	require.NotNil(t, lease)
	assert.Equal(t, 1, lease.Model().numFeatures)
	lease.Release()

	lease, err = cache.acquireByResourceName("rank_model")
	require.NoError(t, err)
	lease.Release()

	assert.Equal(t, int64(1), loadCount.Load())
	assert.Equal(t, int64(0), closeCount.Load())
}

func TestXGBoostModelCacheSingleflight(t *testing.T) {
	var loadCount atomic.Int64
	startLoad := make(chan struct{})
	allowLoad := make(chan struct{})
	cache := newXGBoostModelCache(func(resource *fileresource.ResolvedFileResource) (*modelHandle, error) {
		if loadCount.Add(1) == 1 {
			close(startLoad)
		}
		<-allowLoad
		return &modelHandle{numFeatures: int(resource.ID)}, nil
	}, nil)
	resource := testXGBoostResource(1, "rank_model")
	require.NoError(t, cache.OnFileResourceSync(fileresource.SyncEvent{Version: 1, Resources: []*fileresource.ResolvedFileResource{resource}}))

	const goroutines = 16
	leases := make([]*xgboostModelLease, goroutines)
	errs := make([]error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			leases[idx], errs[idx] = cache.acquireByResourceName("rank_model")
		}(i)
	}
	<-startLoad
	close(allowLoad)
	wg.Wait()

	for i := 0; i < goroutines; i++ {
		require.NoError(t, errs[i])
		require.NotNil(t, leases[i])
		leases[i].Release()
	}
	assert.Equal(t, int64(1), loadCount.Load())
}

func TestXGBoostModelCacheEvictStaleModelAfterLeaseRelease(t *testing.T) {
	var closeCount atomic.Int64
	cache := newXGBoostModelCache(func(resource *fileresource.ResolvedFileResource) (*modelHandle, error) {
		return &modelHandle{}, nil
	}, func(model *modelHandle) error {
		closeCount.Add(1)
		return nil
	})
	resource := testXGBoostResource(1, "rank_model")
	require.NoError(t, cache.OnFileResourceSync(fileresource.SyncEvent{Version: 1, Resources: []*fileresource.ResolvedFileResource{resource}}))
	lease, err := cache.acquireByResourceName("rank_model")
	require.NoError(t, err)

	require.NoError(t, cache.OnFileResourceSync(fileresource.SyncEvent{Version: 2, Resources: nil}))
	assert.Equal(t, 0, cache.len())
	assert.Equal(t, int64(0), closeCount.Load())

	lease.Release()
	assert.Equal(t, int64(1), closeCount.Load())
	lease.Release()
	assert.Equal(t, int64(1), closeCount.Load())
}

func TestXGBoostModelCacheAcquireRetriesAfterSyncEviction(t *testing.T) {
	var loadCount atomic.Int64
	var closeCount atomic.Int64
	resourceV1 := testXGBoostResource(1, "rank_model")
	resourceV2 := testXGBoostResource(2, "rank_model")
	cache := newXGBoostModelCache(func(resource *fileresource.ResolvedFileResource) (*modelHandle, error) {
		loadCount.Add(1)
		return &modelHandle{numFeatures: int(resource.ID)}, nil
	}, func(model *modelHandle) error {
		closeCount.Add(1)
		return nil
	})
	cache.onModelStored = func() {
		if loadCount.Load() == 1 {
			require.NoError(t, cache.OnFileResourceSync(fileresource.SyncEvent{Version: 2, Resources: []*fileresource.ResolvedFileResource{resourceV2}}))
		}
	}
	require.NoError(t, cache.OnFileResourceSync(fileresource.SyncEvent{Version: 1, Resources: []*fileresource.ResolvedFileResource{resourceV1}}))

	lease, err := cache.acquireByResourceName("rank_model")
	require.NoError(t, err)
	require.NotNil(t, lease)
	assert.Equal(t, 2, lease.Model().numFeatures)
	lease.Release()

	assert.Equal(t, int64(2), loadCount.Load())
	assert.Equal(t, int64(1), closeCount.Load())
}
