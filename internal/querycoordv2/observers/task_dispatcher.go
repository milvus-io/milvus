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

package observers

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// taskDispatcher is the utility to provide task dedup and dispatch feature
type taskDispatcher[K comparable] struct {
	tasks      *typeutil.ConcurrentSet[K]
	pool       *conc.Pool[any]
	notifyCh   chan struct{}
	taskRunner task[K]
	wg         sync.WaitGroup
	cancel     context.CancelFunc
	stopOnce   sync.Once
}

type task[K comparable] func(context.Context, K)

func newTaskDispatcher[K comparable](runner task[K]) *taskDispatcher[K] {
	return &taskDispatcher[K]{
		tasks:      typeutil.NewConcurrentSet[K](),
		pool:       conc.NewPool[any](paramtable.Get().QueryCoordCfg.ObserverTaskParallel.GetAsInt()),
		notifyCh:   make(chan struct{}, 1),
		taskRunner: runner,
	}
}

func (d *taskDispatcher[K]) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.schedule(ctx)
	}()
}

func (d *taskDispatcher[K]) Stop() {
	d.stopOnce.Do(func() {
		if d.cancel != nil {
			d.cancel()
		}
		d.wg.Wait()
	})
}

func (d *taskDispatcher[K]) AddTask(keys ...K) {
	var added bool
	for _, key := range keys {
		added = d.tasks.Insert(key) || added
	}
	if added {
		d.notify()
	}
}

func (d *taskDispatcher[K]) notify() {
	select {
	case d.notifyCh <- struct{}{}:
	default:
	}
}

func (d *taskDispatcher[K]) schedule(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.notifyCh:
			d.tasks.Range(func(k K) bool {
				d.tasks.Insert(k)
				d.pool.Submit(func() (any, error) {
					d.taskRunner(ctx, k)
					d.tasks.Remove(k)
					return struct{}{}, nil
				})
				return true
			})
		}
	}
}
