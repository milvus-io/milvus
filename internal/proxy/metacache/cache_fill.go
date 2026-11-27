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

package metacache

import (
	"context"
	"math"
	"sync"

	"golang.org/x/sync/semaphore"
)

// A fill holds one permit. A DDL invalidation takes all permits, draining
// active fills and blocking new ones until the eviction is complete.
type fillBarrier struct {
	*semaphore.Weighted
}

func newFillBarrier() fillBarrier {
	return fillBarrier{semaphore.NewWeighted(math.MaxInt64)}
}

func (b fillBarrier) Lock() {
	// An invalidation must drain earlier fills even if its RPC caller leaves.
	_ = b.Acquire(context.Background(), math.MaxInt64)
}

func (b fillBarrier) Unlock() {
	b.Release(math.MaxInt64)
}

type cacheFillGroup[T any] struct {
	flights sync.Map // string -> *cacheFill[T]
}

type cacheFill[T any] struct {
	done   chan struct{}
	result func() (T, error)
}

// Run the first caller's RPC synchronously with its original context. Other
// callers can leave on their own deadline. The read permit spans registration,
// write-back, and flight removal, including when all followers have left.
func (g *cacheFillGroup[T]) Do(ctx context.Context, barrier fillBarrier, key string, load func() (T, error)) (T, error) {
	var zero T
	if err := barrier.Acquire(ctx, 1); err != nil {
		return zero, err
	}
	defer barrier.Release(1)
	call := &cacheFill[T]{done: make(chan struct{}), result: sync.OnceValues(load)}
	if existing, loaded := g.flights.LoadOrStore(key, call); loaded {
		active := existing.(*cacheFill[T])
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-active.done:
			return active.result()
		}
	}
	defer close(call.done)
	// A retry may forget a partition flight and start another one for this key.
	defer g.flights.CompareAndDelete(key, call)
	return call.result()
}
