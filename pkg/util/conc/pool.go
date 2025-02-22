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

package conc

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"

	"github.com/milvus-io/milvus/pkg/v2/util/generic"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// A goroutine pool
type Pool[T any] struct {
	inner *ants.Pool
	opt   *poolOption
}

// NewPool returns a goroutine pool.
// cap: the number of workers.
// This panic if provide any invalid option.
func NewPool[T any](cap int, opts ...PoolOption) *Pool[T] {
	opt := defaultPoolOption()
	for _, o := range opts {
		o(opt)
	}

	pool, err := ants.NewPool(cap, opt.antsOptions()...)
	if err != nil {
		panic(err)
	}

	return &Pool[T]{
		inner: pool,
		opt:   opt,
	}
}

// NewDefaultPool returns a pool with cap of the number of logical CPU,
// and pre-alloced goroutines.
func NewDefaultPool[T any]() *Pool[T] {
	return NewPool[T](hardware.GetCPUNum(), WithPreAlloc(true))
}

// Submit a task into the pool,
// executes it asynchronously.
// This will block if the pool has finite workers and no idle worker.
// NOTE: As now golang doesn't support the member method being generic, we use Future[any]
func (pool *Pool[T]) Submit(method func() (T, error)) *Future[T] {
	future := newFuture[T]()
	err := pool.inner.Submit(func() {
		defer close(future.ch)
		defer func() {
			if x := recover(); x != nil {
				future.err = fmt.Errorf("panicked with error: %v", x)
				panic(x) // throw panic out
			}
		}()
		// execute pre handler
		if pool.opt.preHandler != nil {
			pool.opt.preHandler()
		}
		res, err := method()
		if err != nil {
			future.err = err
		}
		future.value = res
	})
	if err != nil {
		future.err = err
		close(future.ch)
	}

	return future
}

// The number of workers
func (pool *Pool[T]) Cap() int {
	return pool.inner.Cap()
}

// The number of running workers
func (pool *Pool[T]) Running() int {
	return pool.inner.Running()
}

// Free returns the number of free workers
func (pool *Pool[T]) Free() int {
	return pool.inner.Free()
}

func (pool *Pool[T]) IsClosed() bool {
	return pool.inner.IsClosed()
}

func (pool *Pool[T]) Release() {
	pool.inner.Release()
}

func (pool *Pool[T]) ReleaseTimeout(timeout time.Duration) error {
	return pool.inner.ReleaseTimeout(timeout)
}

func (pool *Pool[T]) Resize(size int) error {
	if pool.opt.preAlloc {
		return merr.WrapErrServiceInternal("cannot resize pre-alloc pool")
	}
	if size <= 0 {
		return merr.WrapErrParameterInvalid("positive size", strconv.FormatInt(int64(size), 10))
	}
	pool.inner.Tune(size)
	return nil
}

// WarmupPool do warm up logic for each goroutine in pool
func WarmupPool[T any](pool *Pool[T], warmup func()) {
	cap := pool.Cap()
	ch := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(cap)
	for i := 0; i < cap; i++ {
		pool.Submit(func() (T, error) {
			warmup()
			wg.Done()
			<-ch
			return generic.Zero[T](), nil
		})
	}
	wg.Wait()
	close(ch)
}
