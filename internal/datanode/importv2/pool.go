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
	"runtime"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var (
	execPool         *orderedExecPool
	execPoolInitOnce sync.Once
)

// orderedExecPool preserves Submit start order before handing work to the inner pool.
// Task completion remains independent, so later short work can finish first.
type orderedExecPool struct {
	inner *conc.Pool[any]

	queue   conc.ConcurrentQueue[*orderedExecJob]
	notify  chan struct{}
	closeCh chan struct{}
}

type orderedExecJob struct {
	action func() (any, error)
	future chan *conc.Future[any]
}

func newOrderedExecPool(inner *conc.Pool[any]) *orderedExecPool {
	pool := &orderedExecPool{
		inner:   inner,
		notify:  make(chan struct{}, 1),
		closeCh: make(chan struct{}),
	}
	go pool.dispatch()
	return pool
}

func (pool *orderedExecPool) Submit(fn func() (any, error)) *conc.Future[any] {
	job := &orderedExecJob{
		action: fn,
		future: make(chan *conc.Future[any], 1),
	}
	pool.queue.Enqueue(job)
	select {
	case pool.notify <- struct{}{}:
	default:
	}

	return conc.Go(func() (any, error) {
		future := <-job.future
		return future.Await()
	})
}

func (pool *orderedExecPool) dispatch() {
	for {
		job, ok := pool.queue.Dequeue()
		if !ok {
			select {
			case <-pool.notify:
				continue
			case <-pool.closeCh:
				return
			}
		}

		started := make(chan struct{})
		future := pool.inner.Submit(func() (any, error) {
			close(started)
			return job.action()
		})

		select {
		case <-started:
			job.future <- future
		case <-future.Inner():
			job.future <- future
		}
		close(job.future)
	}
}

func (pool *orderedExecPool) Cap() int {
	return pool.inner.Cap()
}

func (pool *orderedExecPool) Running() int {
	return pool.inner.Running()
}

func (pool *orderedExecPool) Free() int {
	return pool.inner.Free()
}

func (pool *orderedExecPool) Waiting() int {
	return pool.inner.Waiting() + pool.queue.Len()
}

func (pool *orderedExecPool) IsClosed() bool {
	return pool.inner.IsClosed()
}

// Release and ReleaseTimeout is not implemented
// since it is not used in the current implementation.

func (pool *orderedExecPool) Resize(size int) error {
	return pool.inner.Resize(size)
}

func initExecPool() {
	pt := paramtable.Get()
	cpuNum := hardware.GetCPUNum()
	initPoolSize := cpuNum * pt.DataNodeCfg.ImportConcurrencyPerCPUCore.GetAsInt()
	innerPool := conc.NewPool[any](
		initPoolSize,
		conc.WithPreAlloc(false), // pre alloc must be false to resize pool dynamically, use warmup to alloc worker here
		conc.WithDisablePurge(true),
	)
	conc.WarmupPool(innerPool, runtime.LockOSThread)
	execPool = newOrderedExecPool(innerPool)

	watchKey := pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key
	pt.Watch(watchKey, config.NewHandler(watchKey, resizeExecPool))
	mlog.Info(context.TODO(), "init import execution pool done", mlog.Int("size", initPoolSize))
}

func resizeExecPool(evt *config.Event) {
	if evt.HasUpdated {
		cpuNum := hardware.GetCPUNum()
		newSize := cpuNum * paramtable.Get().DataNodeCfg.ImportConcurrencyPerCPUCore.GetAsInt()
		log := mlog.With(mlog.Int("newSize", newSize))

		err := GetExecPool().Resize(newSize)
		if err != nil {
			log.Warn(context.TODO(), "failed to resize pool", mlog.Err(err))
			return
		}
		log.Info(context.TODO(), "pool resize successfully")
	}
}

func GetExecPool() *orderedExecPool {
	execPoolInitOnce.Do(initExecPool)
	return execPool
}
