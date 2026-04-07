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

package resource

import (
	"runtime"
	"sync"
	"unsafe"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

// PinnedResourceManager is a registry that attaches cleanup functions to objects by pointer.
// It is safe for concurrent use and designed for high-frequency, short-lived entries.
//
// Typical lifecycle:
//
//	Pin(obj, cleanup)   — called when a resource-backed object is created
//	Release(obj)        — called when the object has been fully consumed (triggers cleanup)
//
// A GC finalizer on the pinned object acts as a safety net: if Release is never
// called (e.g., gRPC drops a response), the finalizer triggers cleanup.
// This requires using map[uintptr] instead of sync.Map — sync.Map boxes keys
// as any, and Go's GC may treat the boxed uintptr as a live pointer, preventing
// the object from being collected and the finalizer from firing.
type PinnedResourceManager struct {
	name string
	mu   sync.Mutex
	// cannot use sync.Map here, it will treat entries as gc root.
	entries map[uintptr]func()
}

// NewPinnedResourceManager creates a named registry. The name appears in debug/warning logs.
func NewPinnedResourceManager(name string) *PinnedResourceManager {
	return &PinnedResourceManager{
		name:    name,
		entries: make(map[uintptr]func()),
	}
}

// MsgPins is the process-wide registry for gRPC message resource cleanup.
// Components that back proto message fields with external memory (e.g., C allocations)
// pin a cleanup function here; the gRPC codec triggers Release after marshaling.
var MsgPins = NewPinnedResourceManager("msg-pins")

// Pin attaches a cleanup function to obj (keyed by pointer).
// If obj is nil, a warning is logged and the call is a no-op.
// If obj is already pinned, the new cleanup is ignored and a warning is logged —
// this usually indicates a programming error (e.g., registering the same response twice).
//
// A GC finalizer is set on obj as a safety net: if obj is garbage-collected
// without an explicit Release, the finalizer triggers cleanup automatically.
func (r *PinnedResourceManager) Pin(obj any, cleanup func()) {
	key := ptrOf(obj)
	if key == 0 {
		log.Warn("PinnedResourceManager.Pin: nil obj, ignored", zap.String("registry", r.name))
		return
	}
	r.mu.Lock()
	if _, existed := r.entries[key]; existed {
		r.mu.Unlock()
		log.Warn("PinnedResourceManager: double-Pin detected — new cleanup ignored",
			zap.String("registry", r.name))
		return
	}
	r.entries[key] = cleanup
	r.mu.Unlock()

	// Safety net: the closure captures only r (the manager), not obj.
	// The parameter o is provided by the runtime when the finalizer fires.
	runtime.SetFinalizer(obj, func(o any) {
		log.Warn("PinnedResourceManager: obj GC'd without Release — triggering cleanup",
			zap.String("registry", r.name))
		r.Release(o)
	})
}

// Release triggers the cleanup for obj and removes it from the registry.
// If obj is not registered (already released or never pinned), this is a no-op.
func (r *PinnedResourceManager) Release(obj any) {
	key := ptrOf(obj)
	if key == 0 {
		return
	}
	r.mu.Lock()
	fn, ok := r.entries[key]
	if ok {
		delete(r.entries, key)
	}
	r.mu.Unlock()
	if ok {
		runtime.SetFinalizer(obj, nil) // disarm safety net
		fn()
	}
}

// PinnedCount returns the number of currently pinned entries.
// Intended for metrics/monitoring — a steadily growing count indicates a leak.
func (r *PinnedResourceManager) PinnedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// HasPinned reports whether obj currently has a pinned cleanup. Intended for tests.
func (r *PinnedResourceManager) HasPinned(obj any) bool {
	key := ptrOf(obj)
	if key == 0 {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.entries[key]
	return ok
}

// ResetForTest clears all entries.
// Must only be called from unit tests.
func (r *PinnedResourceManager) ResetForTest() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = make(map[uintptr]func())
}

// eface is the runtime representation of an empty interface (any).
type eface struct {
	_type uintptr
	data  unsafe.Pointer
}

// ptrOf extracts the pointer value from an interface value.
// Returns 0 if obj is a nil interface or a nil pointer.
func ptrOf(obj any) uintptr {
	e := (*eface)(unsafe.Pointer(&obj))
	if e.data == nil {
		return 0
	}
	return uintptr(e.data)
}
