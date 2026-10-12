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
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type (
	modelHandle struct {
		h           unsafe.Pointer
		numFeatures int
	}

	xgboostModelLoader func(resource *fileresource.ResolvedFileResource) (*modelHandle, error)
	xgboostModelCloser func(model *modelHandle) error

	xgboostModelCache struct {
		resources atomic.Value // map[string]*fileresource.ResolvedFileResource

		mu     sync.RWMutex
		models map[string]*cachedXGBoostModel
		sf     conc.Singleflight[*cachedXGBoostModel]

		loader        xgboostModelLoader
		closer        xgboostModelCloser
		onModelStored func()
	}

	cachedXGBoostModel struct {
		key          string
		resourceID   int64
		resourceName string
		resourcePath string
		model        *modelHandle

		refs    atomic.Int64
		closing atomic.Bool
		closed  atomic.Bool
		closeMu sync.Mutex

		closer xgboostModelCloser
	}

	xgboostModelLease struct {
		cached   *cachedXGBoostModel
		released atomic.Bool
	}
)

var globalXGBoostModelCache = newXGBoostModelCache(loadXGBoostModel, closeXGBoostModel)

const maxXGBoostAcquireAttempts = 3

func init() {
	fileresource.RegisterListener("xgboost", globalXGBoostModelCache)
}

func newXGBoostModelCache(loader xgboostModelLoader, closer xgboostModelCloser) *xgboostModelCache {
	cache := &xgboostModelCache{
		models: make(map[string]*cachedXGBoostModel),
		loader: loader,
		closer: closer,
	}
	cache.resources.Store(map[string]*fileresource.ResolvedFileResource{})
	return cache
}

func xgboostModelCacheKey(resource *fileresource.ResolvedFileResource) string {
	if resource == nil {
		return ""
	}
	return fmt.Sprintf("%d:%s", resource.ID, resource.Path)
}

func isXGBoostUBJResource(resource *fileresource.ResolvedFileResource) bool {
	if resource == nil {
		return false
	}
	return strings.EqualFold(filepath.Ext(resource.Path), ".ubj")
}

func (c *xgboostModelCache) OnFileResourceSync(event fileresource.SyncEvent) error {
	resources := make(map[string]*fileresource.ResolvedFileResource, len(event.Resources))
	activeKeys := make(map[string]struct{}, len(event.Resources))
	for _, resource := range event.Resources {
		if !isXGBoostUBJResource(resource) {
			continue
		}
		resolved := *resource
		resources[resource.Name] = &resolved
		activeKeys[xgboostModelCacheKey(resource)] = struct{}{}
	}
	c.resources.Store(resources)
	c.evictStaleModels(activeKeys)
	return nil
}

func (c *xgboostModelCache) resolveResource(name string) (*fileresource.ResolvedFileResource, error) {
	if name == "" {
		return nil, merr.WrapErrParameterInvalidMsg("xgboost: model_resource is empty")
	}
	resources, _ := c.resources.Load().(map[string]*fileresource.ResolvedFileResource)
	resource, ok := resources[name]
	if !ok || resource == nil {
		return nil, merr.WrapErrParameterInvalidMsg("xgboost: file resource %q not found", name)
	}
	resolved := *resource
	return &resolved, nil
}

func (c *xgboostModelCache) acquireByResourceName(name string) (*xgboostModelLease, error) {
	var lastKey string
	for attempt := 0; attempt < maxXGBoostAcquireAttempts; attempt++ {
		resource, err := c.resolveResource(name)
		if err != nil {
			return nil, err
		}
		lastKey = xgboostModelCacheKey(resource)
		lease, retry, err := c.acquireOrLoad(resource)
		if err != nil {
			return nil, err
		}
		if !retry {
			return lease, nil
		}
	}
	return nil, merr.WrapErrServiceInternalMsg("xgboost: model %q was repeatedly evicted before acquire", lastKey)
}

func (c *xgboostModelCache) acquireOrLoad(resource *fileresource.ResolvedFileResource) (*xgboostModelLease, bool, error) {
	if resource == nil {
		return nil, false, merr.WrapErrParameterInvalidMsg("xgboost: file resource is nil")
	}
	key := xgboostModelCacheKey(resource)
	if lease, ok := c.tryAcquire(key); ok {
		return lease, false, nil
	}

	cached, err, _ := c.sf.Do(key, func() (*cachedXGBoostModel, error) {
		if lease, ok := c.tryAcquire(key); ok {
			lease.Release()
			return lease.cached, nil
		}
		if c.loader == nil {
			return nil, merr.WrapErrServiceInternalMsg("xgboost: model loader is nil")
		}
		model, err := c.loader(resource)
		if err != nil {
			return nil, err
		}
		cached := &cachedXGBoostModel{
			key:          key,
			resourceID:   resource.ID,
			resourceName: resource.Name,
			resourcePath: resource.Path,
			model:        model,
			closer:       c.closer,
		}

		c.mu.Lock()
		if existing, ok := c.models[key]; ok {
			c.mu.Unlock()
			cached.markClosing()
			return existing, nil
		}
		c.models[key] = cached
		onModelStored := c.onModelStored
		c.mu.Unlock()
		if onModelStored != nil {
			onModelStored()
		}
		return cached, nil
	})
	if err != nil {
		return nil, false, err
	}
	if cached == nil {
		return nil, false, merr.WrapErrServiceInternalMsg("xgboost: loaded model is nil")
	}
	if lease, ok := c.acquireCached(cached); ok {
		return lease, false, nil
	}
	return nil, true, nil
}

func (c *xgboostModelCache) tryAcquire(key string) (*xgboostModelLease, bool) {
	c.mu.RLock()
	cached := c.models[key]
	c.mu.RUnlock()
	if cached == nil {
		return nil, false
	}
	return c.acquireCached(cached)
}

func (c *xgboostModelCache) acquireCached(cached *cachedXGBoostModel) (*xgboostModelLease, bool) {
	return cached.acquire()
}

func (m *cachedXGBoostModel) acquire() (*xgboostModelLease, bool) {
	for {
		if m.closing.Load() {
			return nil, false
		}
		refs := m.refs.Load()
		if m.refs.CompareAndSwap(refs, refs+1) {
			if m.closing.Load() {
				m.release()
				return nil, false
			}
			return &xgboostModelLease{cached: m}, true
		}
	}
}

func (l *xgboostModelLease) Model() *modelHandle {
	if l == nil || l.cached == nil {
		return nil
	}
	return l.cached.model
}

func (l *xgboostModelLease) Release() {
	if l == nil || l.cached == nil || !l.released.CompareAndSwap(false, true) {
		return
	}
	l.cached.release()
}

func (m *cachedXGBoostModel) release() {
	refs := m.refs.Add(-1)
	if refs == 0 && m.closing.Load() {
		m.close()
	}
}

func (m *cachedXGBoostModel) markClosing() {
	if !m.closing.CompareAndSwap(false, true) {
		return
	}
	if m.refs.Load() == 0 {
		m.close()
	}
}

func (m *cachedXGBoostModel) close() {
	m.closeMu.Lock()
	defer m.closeMu.Unlock()
	if !m.closed.CompareAndSwap(false, true) {
		return
	}
	if m.closer == nil || m.model == nil {
		return
	}
	if err := m.closer(m.model); err != nil {
		mlog.Warn(context.TODO(), "close xgboost model failed", mlog.String("resource", m.resourceName), mlog.Int64("resourceID", m.resourceID), mlog.Err(err))
	}
}

func (c *xgboostModelCache) evictStaleModels(activeKeys map[string]struct{}) {
	c.mu.Lock()
	evicted := make([]*cachedXGBoostModel, 0)
	for key, cached := range c.models {
		if _, ok := activeKeys[key]; ok {
			continue
		}
		delete(c.models, key)
		evicted = append(evicted, cached)
	}
	c.mu.Unlock()
	for _, model := range evicted {
		model.markClosing()
	}
}

func (c *xgboostModelCache) len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.models)
}
