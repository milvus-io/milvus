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
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const maxCacheAcquireAttempts = 3

// ResourceLoader loads one stage-specific PyUDF resource.
type ResourceLoader interface {
	Load(ctx context.Context, resource fileresource.ResolvedFileResource, stage string) (LoadedResource, error)
}

// LoadedResource is a loaded PyUDF resource whose lifetime is managed by Cache leases.
type LoadedResource interface {
	Run(ctx context.Context, params *schemapb.FunctionParamObject, inputs []*arrow.Chunked) ([]*arrow.Chunked, error)
	Close() error
}

type resourceIdentity struct {
	id   int64
	name string
	path string
}

type cacheKey struct {
	resourceIdentity
	stage string
}

func (k cacheKey) singleflightKey() string {
	return fmt.Sprintf("%d:%q:%q:%q", k.id, k.name, k.path, k.stage)
}

// Cache indexes synchronized wheel resources and lazily loads stage-specific resources.
type Cache struct {
	ctx         context.Context
	cancel      context.CancelFunc
	loader      ResourceLoader
	loadTimeout time.Duration
	closed      atomic.Bool

	mu              sync.RWMutex
	snapshotReady   bool
	snapshotVersion uint64
	resources       map[string]fileresource.ResolvedFileResource
	loaded          map[cacheKey]*cachedResource
	sf              conc.Singleflight[*cachedResource]
}

type cachedResource struct {
	key      cacheKey
	resource LoadedResource

	refs    atomic.Int64
	closing atomic.Bool
	closed  atomic.Bool
	closeMu sync.Mutex
}

type cacheLease struct {
	cached   *cachedResource
	released atomic.Bool
}

// NewCache creates a PyUDF FileResource cache. It does not register a global listener.
func NewCache(ctx context.Context, loader ResourceLoader, loadTimeout time.Duration) (*Cache, error) {
	if loadTimeout <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: load timeout must be positive")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	lifecycleCtx, cancel := context.WithCancel(ctx)
	return &Cache{
		ctx:         lifecycleCtx,
		cancel:      cancel,
		loader:      loader,
		loadTimeout: loadTimeout,
		resources:   make(map[string]fileresource.ResolvedFileResource),
		loaded:      make(map[cacheKey]*cachedResource),
	}, nil
}

func isWheelResource(resource *fileresource.ResolvedFileResource) bool {
	return resource != nil && strings.EqualFold(filepath.Ext(resource.Path), ".whl")
}

func identityOf(resource fileresource.ResolvedFileResource) resourceIdentity {
	return resourceIdentity{
		id:   resource.ID,
		name: resource.Name,
		path: resource.Path,
	}
}

// OnFileResourceSync replaces the current wheel resource snapshot and evicts stale loaded resources.
func (c *Cache) OnFileResourceSync(event fileresource.SyncEvent) error {
	if c == nil || c.closed.Load() {
		return nil
	}

	resources := make(map[string]fileresource.ResolvedFileResource, len(event.Resources))
	for _, resource := range event.Resources {
		if !isWheelResource(resource) {
			continue
		}
		resources[resource.Name] = *resource
	}

	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		return nil
	}
	if c.snapshotReady && event.Version <= c.snapshotVersion {
		c.mu.Unlock()
		return nil
	}
	c.snapshotReady = true
	c.snapshotVersion = event.Version
	c.resources = resources
	evicted := make([]*cachedResource, 0)
	for key, cached := range c.loaded {
		resource, ok := resources[key.name]
		if ok && identityOf(resource) == key.resourceIdentity {
			continue
		}
		delete(c.loaded, key)
		evicted = append(evicted, cached)
	}
	c.mu.Unlock()

	for _, cached := range evicted {
		cached.markClosing()
	}
	return nil
}

// Acquire resolves and lazily loads one resource for the requested stage.
func (c *Cache) Acquire(ctx context.Context, resourceName, stage string) (Lease, error) {
	if c == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: cache is nil")
	}
	if ctx == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: acquire context is nil")
	}
	resourceName = strings.TrimSpace(resourceName)
	if resourceName == "" {
		return nil, merr.WrapErrParameterInvalidMsg("py_udf: resource_name is empty")
	}
	if stage == "" {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: stage is empty")
	}

	var lastKey cacheKey
	for attempt := 0; attempt < maxCacheAcquireAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		resource, err := c.resolveResource(resourceName)
		if err != nil {
			return nil, err
		}
		lastKey = cacheKey{resourceIdentity: identityOf(resource), stage: stage}
		if lease, ok := c.tryAcquire(lastKey); ok {
			return lease, nil
		}

		cached, err := c.load(ctx, lastKey, resource)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			if !c.isCurrentResource(resource) {
				continue
			}
			return nil, err
		}
		if lease, ok := cached.acquire(); ok {
			return lease, nil
		}
	}
	return nil, merr.WrapErrServiceUnavailableMsg(
		"py_udf: resource %q for stage %q was repeatedly replaced while acquiring key %q",
		resourceName,
		stage,
		lastKey.singleflightKey(),
	)
}

func (c *Cache) resolveResource(name string) (fileresource.ResolvedFileResource, error) {
	if c.closed.Load() {
		return fileresource.ResolvedFileResource{}, merr.WrapErrServiceInternalMsg("py_udf: cache is closed")
	}
	c.mu.RLock()
	ready := c.snapshotReady
	resource, ok := c.resources[name]
	c.mu.RUnlock()
	if !ready {
		return fileresource.ResolvedFileResource{}, merr.WrapErrServiceUnavailableMsg("py_udf: file resource snapshot is not ready")
	}
	if !ok {
		return fileresource.ResolvedFileResource{}, merr.WrapErrParameterInvalidMsg("py_udf: file resource %q not found", name)
	}
	if resource.LocalPath == "" {
		return fileresource.ResolvedFileResource{}, merr.WrapErrServiceInternalMsg("py_udf: local path is empty for resource %q", name)
	}
	return resource, nil
}

func (c *Cache) isCurrentResource(resource fileresource.ResolvedFileResource) bool {
	c.mu.RLock()
	current, ok := c.resources[resource.Name]
	c.mu.RUnlock()
	return ok && identityOf(current) == identityOf(resource)
}

func (c *Cache) tryAcquire(key cacheKey) (*cacheLease, bool) {
	c.mu.RLock()
	cached := c.loaded[key]
	c.mu.RUnlock()
	if cached == nil {
		return nil, false
	}
	return cached.acquire()
}

func (c *Cache) load(ctx context.Context, key cacheKey, resource fileresource.ResolvedFileResource) (*cachedResource, error) {
	resultCh := c.sf.DoChan(key.singleflightKey(), func() (*cachedResource, error) {
		if lease, ok := c.tryAcquire(key); ok {
			lease.Release()
			return lease.cached, nil
		}
		if c.loader == nil {
			return nil, merr.WrapErrServiceInternalMsg("py_udf: resource loader is nil")
		}

		loadCtx, cancel := context.WithTimeout(c.ctx, c.loadTimeout)
		defer cancel()
		loaded, err := c.loader.Load(loadCtx, resource, key.stage)
		if err != nil {
			return nil, wrapLoaderError(err, resource.Name, key.stage)
		}
		if loaded == nil {
			return nil, merr.WrapErrServiceInternalMsg("py_udf: loader returned nil resource for %q", resource.Name)
		}

		cached := &cachedResource{key: key, resource: loaded}
		if err := loadCtx.Err(); err != nil {
			cached.markClosing()
			return nil, err
		}

		c.mu.Lock()
		if c.closed.Load() {
			c.mu.Unlock()
			cached.markClosing()
			return nil, merr.WrapErrServiceInternalMsg("py_udf: cache is closed")
		}
		current, active := c.resources[resource.Name]
		if !active || identityOf(current) != key.resourceIdentity {
			c.mu.Unlock()
			cached.markClosing()
			return cached, nil
		}
		if existing := c.loaded[key]; existing != nil {
			c.mu.Unlock()
			cached.markClosing()
			return existing, nil
		}
		c.loaded[key] = cached
		c.mu.Unlock()
		return cached, nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultCh:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if result.Err != nil {
			return nil, result.Err
		}
		if result.Val == nil {
			return nil, merr.WrapErrServiceInternalMsg("py_udf: singleflight returned nil resource")
		}
		return result.Val, nil
	}
}

func wrapLoaderError(err error, resourceName, stage string) error {
	if merr.IsMilvusError(err) || merr.IsCanceledOrTimeout(err) {
		return merr.Wrapf(err, "py_udf: load resource %q for stage %q", resourceName, stage)
	}
	return merr.WrapErrFunctionFailed(err, "py_udf: load resource %q for stage %q", resourceName, stage)
}

func (r *cachedResource) acquire() (*cacheLease, bool) {
	if r == nil {
		return nil, false
	}
	for {
		if r.closing.Load() {
			return nil, false
		}
		refs := r.refs.Load()
		if r.refs.CompareAndSwap(refs, refs+1) {
			if r.closing.Load() {
				r.release()
				return nil, false
			}
			return &cacheLease{cached: r}, true
		}
	}
}

func (l *cacheLease) Run(ctx context.Context, params *schemapb.FunctionParamObject, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	if l == nil || l.cached == nil || l.released.Load() {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: resource lease is released")
	}
	if l.cached.resource == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: loaded resource is nil")
	}
	return l.cached.resource.Run(ctx, params, inputs)
}

func (l *cacheLease) Release() {
	if l == nil || l.cached == nil || !l.released.CompareAndSwap(false, true) {
		return
	}
	l.cached.release()
}

func (r *cachedResource) release() {
	refs := r.refs.Add(-1)
	if refs == 0 && r.closing.Load() {
		r.close()
	}
}

func (r *cachedResource) markClosing() {
	if r == nil || !r.closing.CompareAndSwap(false, true) {
		return
	}
	if r.refs.Load() == 0 {
		r.close()
	}
}

func (r *cachedResource) close() {
	r.closeMu.Lock()
	defer r.closeMu.Unlock()
	if !r.closed.CompareAndSwap(false, true) || r.resource == nil {
		return
	}
	if err := r.resource.Close(); err != nil {
		mlog.Warn(
			context.TODO(),
			"close py_udf resource failed",
			mlog.String("resource", r.key.name),
			mlog.Int64("resourceID", r.key.id),
			mlog.String("stage", r.key.stage),
			mlog.Err(err),
		)
	}
}

// Close prevents new acquisitions and retires all loaded resources.
func (c *Cache) Close() {
	if c == nil || !c.closed.CompareAndSwap(false, true) {
		return
	}
	c.cancel()

	c.mu.Lock()
	c.resources = make(map[string]fileresource.ResolvedFileResource)
	closing := make([]*cachedResource, 0, len(c.loaded))
	for key, cached := range c.loaded {
		delete(c.loaded, key)
		closing = append(closing, cached)
	}
	c.mu.Unlock()

	for _, cached := range closing {
		cached.markClosing()
	}
}

func (c *Cache) len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.loaded)
}
