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

package importv2

import (
	"context"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/util/importutilv2/binlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// executeSnapshotSharedRead shares prepared row masks only within this execution.
// The first runnable pool worker admits memory and folds deletes before any data
// reader starts. Workers drain a common file queue; even one admitted worker can
// finish the task without waiting for more execution-pool slots.
func executeSnapshotSharedRead(ctx context.Context, task Task, manager TaskManager, fileCount int,
	load func(context.Context, int64, int64) (*binlog.SnapshotL0Deletes, error),
	read func(context.Context, int, int64, int64, *binlog.SnapshotL0Deletes) error,
) []*conc.Future[any] {
	ctx, cancel := context.WithCancelCause(ctx)
	pool := GetExecPool()
	workers := min(fileCount, pool.Cap())
	budget := paramtable.Get().DataNodeCfg.ImportDeleteBufferSize.GetAsInt64()
	allocator := GetMemoryAllocator()
	var (
		once      sync.Once
		mu        sync.Mutex
		active    int
		closed    bool
		next      atomic.Int64
		rowBuffer int64
		shared    *binlog.SnapshotL0Deletes
		slots     chan struct{}
		release   func()
	)
	finish := func() {
		mu.Lock()
		active--
		var cleanup func()
		if active == 0 {
			closed = true
			shared = nil
			cleanup = release
		}
		mu.Unlock()
		if cleanup != nil {
			cleanup()
			debug.FreeOSMemory()
		}
	}
	futures := make([]*conc.Future[any], 0, workers)
	for i := 0; i < workers; i++ {
		futures = append(futures, pool.Submit(func() (any, error) {
			mu.Lock()
			if closed {
				mu.Unlock()
				return nil, nil // Earlier workers already drained or failed this task.
			}
			active++
			mu.Unlock()
			defer finish()
			once.Do(func() {
				var count int
				var err error
				rowBuffer, count, err = allocator.AllocateSnapshotTask(ctx, task.GetTaskID(), task.GetBufferSize(), budget, workers)
				if err != nil {
					cancel(err)
					return
				}
				size := budget + int64(count)*(rowBuffer+budget)
				release = func() { allocator.Release(task.GetTaskID(), size) }
				slots = make(chan struct{}, count)
				// Reuse the old private-map allowance as a bounded bitmap pool.
				// Preparation scans one source at a time and finishes before any
				// output reader starts; no additional execution-pool slot is needed.
				shared, err = load(ctx, budget, int64(count)*budget)
				if err != nil {
					cancel(err)
				}
			})
			if err := context.Cause(ctx); err != nil {
				return nil, err
			}
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			}
			for {
				if err := context.Cause(ctx); err != nil {
					return nil, err
				}
				index := int(next.Add(1) - 1)
				if index >= fileCount {
					return nil, nil
				}
				if err := read(ctx, index, rowBuffer, budget, shared); err != nil {
					cancel(err)
					return nil, err
				}
			}
		}))
	}
	// AwaitAll returns on the first error. BlockOnAll also waits for every
	// sibling to close its reader, so the returned future covers cleanup.
	return []*conc.Future[any]{conc.Go(func() (any, error) {
		defer cancel(nil)
		err := conc.BlockOnAll(futures...)
		if cause := context.Cause(ctx); cause != nil {
			err = cause
		}
		if err != nil {
			manager.Update(task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(err.Error()))
		}
		return nil, err
	})}
}
