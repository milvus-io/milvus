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
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type resourceSnapshot struct {
	ready     bool
	version   uint64
	resources map[string]fileresource.ResolvedFileResource
}

type resourceSource interface {
	Resolve(string) (fileresource.ResolvedFileResource, uint64, error)
	IsCurrent(fileresource.ResolvedFileResource) bool
	Subscribe(resourceObserver) func()
}

type resourceObserver func(*resourceSnapshot)

// resourceInfo tracks the latest synchronized PyUDF wheel resources without
// initializing the PyUDF execution runtime.
type resourceInfo struct {
	mu             sync.Mutex
	current        atomic.Pointer[resourceSnapshot]
	nextObserverID uint64
	observers      map[uint64]resourceObserver
}

func newResourceInfo() *resourceInfo {
	info := &resourceInfo{observers: make(map[uint64]resourceObserver)}
	info.current.Store(&resourceSnapshot{
		resources: make(map[string]fileresource.ResolvedFileResource),
	})
	return info
}

func isWheelResource(resource *fileresource.ResolvedFileResource) bool {
	return resource != nil && strings.EqualFold(filepath.Ext(resource.Path), ".whl")
}

// OnFileResourceSync replaces the current PyUDF resource snapshot.
func (r *resourceInfo) OnFileResourceSync(event fileresource.SyncEvent) error {
	resources := make(map[string]fileresource.ResolvedFileResource, len(event.Resources))
	for _, resource := range event.Resources {
		if !isWheelResource(resource) {
			continue
		}
		resources[resource.Name] = *resource
	}

	next := &resourceSnapshot{
		ready:     true,
		version:   event.Version,
		resources: resources,
	}

	r.mu.Lock()
	current := r.current.Load()
	if current != nil && current.ready && event.Version <= current.version {
		r.mu.Unlock()
		return nil
	}
	r.current.Store(next)
	observers := make([]resourceObserver, 0, len(r.observers))
	for _, observer := range r.observers {
		observers = append(observers, observer)
	}
	r.mu.Unlock()

	for _, observer := range observers {
		observer(next)
	}
	return nil
}

// Snapshot returns an immutable resource snapshot. Callers must not mutate the
// snapshot or its resource map.
func (r *resourceInfo) Snapshot() *resourceSnapshot {
	if r == nil {
		return nil
	}
	return r.current.Load()
}

func (r *resourceInfo) Resolve(name string) (fileresource.ResolvedFileResource, uint64, error) {
	snapshot := r.Snapshot()
	if snapshot == nil || !snapshot.ready {
		return fileresource.ResolvedFileResource{}, 0,
			merr.WrapErrServiceUnavailableMsg("py_udf: file resource snapshot is not ready")
	}
	resource, ok := snapshot.resources[name]
	if !ok {
		return fileresource.ResolvedFileResource{}, snapshot.version,
			merr.WrapErrParameterInvalidMsg("py_udf: file resource %q not found", name)
	}
	return resource, snapshot.version, nil
}

func (r *resourceInfo) IsCurrent(resource fileresource.ResolvedFileResource) bool {
	snapshot := r.Snapshot()
	if snapshot == nil {
		return false
	}
	current, ok := snapshot.resources[resource.Name]
	return ok && identityOf(current) == identityOf(resource)
}

func (r *resourceInfo) Subscribe(observer resourceObserver) func() {
	if r == nil || observer == nil {
		return func() {}
	}

	r.mu.Lock()
	id := r.nextObserverID
	r.nextObserverID++
	r.observers[id] = observer
	current := r.current.Load()
	r.mu.Unlock()

	if current != nil && current.ready {
		observer(current)
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			delete(r.observers, id)
			r.mu.Unlock()
		})
	}
}

var globalResourceInfo = newResourceInfo()

func init() {
	fileresource.RegisterListener("pyudf", globalResourceInfo)
}

var _ fileresource.Listener = (*resourceInfo)(nil)
