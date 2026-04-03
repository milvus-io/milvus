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
	"fmt"
	"sync/atomic"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// PinnedRef is a smart-pointer-like owner of a resource of type T.
// It is analogous to C++ unique_ptr: ownership is explicit and the resource is
// freed exactly once when Release() is called.
//
// PinnedRef itself does NOT carry a GC finalizer. When used with
// PinnedResourceManager (MsgPins), the manager sets a finalizer on the
// pinned object — not on the PinnedRef — because the manager's closure
// would keep the PinnedRef reachable and prevent the finalizer from firing.
type PinnedRef[T any] struct {
	val      T
	release  func(T)
	released atomic.Bool
	tag      string // human-readable label for debug logs
}

// NewPinnedRef creates a PinnedRef that owns val.
// release(val) is called exactly once when Release() is invoked.
// tag identifies the resource in warning logs (e.g. "SearchResultDataBlobs").
func NewPinnedRef[T any](val T, release func(T), tag string) *PinnedRef[T] {
	return &PinnedRef[T]{val: val, release: release, tag: tag}
}

// NewSharedPinnedRefs creates n PinnedRef instances that share ownership of val.
// val is freed (via release) only when every holder has called Release().
// This mirrors C++ shared_ptr: each returned ref is one "share" of the resource.
//
// Callers must not use val after any holder has called Release(), because the
// cleanup may run as soon as the last holder releases.
func NewSharedPinnedRefs[T any](val T, n int, release func(T), tag string) ([]*PinnedRef[T], error) {
	if n <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("NewSharedPinnedRefs: n must be > 0, got %d", n)
	}
	refCount := new(atomic.Int32)
	refCount.Store(int32(n))

	refs := make([]*PinnedRef[T], n)
	for i := range refs {
		sharedRelease := func(v T) {
			if refCount.Add(-1) <= 0 {
				release(v)
			}
		}
		refs[i] = NewPinnedRef(val, sharedRelease, fmt.Sprintf("%s[%d/%d]", tag, i+1, n))
	}
	return refs, nil
}

// Value returns the underlying resource. Do not retain the value after Release().
func (r *PinnedRef[T]) Value() T { return r.val }

// Release frees the resource. Idempotent: subsequent calls are no-ops.
// After Release(), Value() may return a dangling reference — do not use it.
func (r *PinnedRef[T]) Release() {
	if r.released.CompareAndSwap(false, true) {
		r.release(r.val)
	}
}

// IsReleased reports whether Release has been called.
func (r *PinnedRef[T]) IsReleased() bool { return r.released.Load() }
