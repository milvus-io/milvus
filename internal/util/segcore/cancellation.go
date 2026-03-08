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

package segcore

/*
#cgo pkg-config: milvus_core

#include "segcore/segment_c.h"
*/
import "C"

import (
	"context"
	"sync"
	"unsafe"
)

// CancellationGuard manages a C cancellation source with proper lifecycle management.
// It monitors context cancellation and propagates it to the C layer, ensuring
// proper cleanup to prevent use-after-free issues.
type CancellationGuard struct {
	source C.CLoadCancellationSource
	wg     sync.WaitGroup
	done   chan struct{}
}

// NewCancellationGuard creates a new CancellationGuard that monitors the provided
// context for cancellation and propagates it to the C layer.
func NewCancellationGuard(ctx context.Context) *CancellationGuard {
	source := C.NewLoadCancellationSource()
	done := make(chan struct{})

	guard := &CancellationGuard{
		source: source,
		done:   done,
	}

	guard.wg.Add(1)
	go func() {
		defer guard.wg.Done()
		select {
		case <-ctx.Done():
			C.CancelLoadCancellationSource(source)
		case <-done:
		}
	}()

	return guard
}

// Source returns the underlying C cancellation source handle.
func (g *CancellationGuard) Source() unsafe.Pointer {
	return unsafe.Pointer(g.source)
}

// Close releases the cancellation guard resources.
// It signals the monitoring goroutine to stop and waits for it to complete
// before releasing the C resources, preventing use-after-free.
func (g *CancellationGuard) Close() {
	close(g.done)
	g.wg.Wait()
	C.ReleaseLoadCancellationSource(g.source)
}
