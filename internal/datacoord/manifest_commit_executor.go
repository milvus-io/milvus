// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// manifestCommitExecutor is the long-lived executor for manifest commits only.
// Each caller leases it for its I/O phase. Shutdown takes the write
// lock to drain all leases before closing the context. No caller may acquire
// another lease while holding one; pass its IO context to nested operations.
type manifestCommitExecutor struct {
	mu          sync.RWMutex
	io          *packed.ManifestIOContext
	concurrency int
	closed      bool
}

// newManifestCommitExecutor fixes capacity for this component's lifetime.
// The owner resolves configuration before constructing it.
func newManifestCommitExecutor(concurrency int) *manifestCommitExecutor {
	concurrency = max(1, concurrency)
	return &manifestCommitExecutor{
		io:          packed.NewManifestIOContext(concurrency),
		concurrency: concurrency,
	}
}

// acquire borrows the component's context without changing its capacity.
// Release is idempotent to support both early release before catalog publication
// and a deferred release on error paths.
func (e *manifestCommitExecutor) acquire(ctx context.Context) (*packed.ManifestIOContext, func(), error) {
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, nil, merr.WrapErrServiceUnavailable("manifest commit executor is closed")
	}
	if err := ctx.Err(); err != nil {
		e.mu.RUnlock()
		return nil, nil, err
	}
	return e.io, sync.OnceFunc(e.mu.RUnlock), nil
}

func (e *manifestCommitExecutor) close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	if e.io != nil {
		e.io.Close()
	}
}
