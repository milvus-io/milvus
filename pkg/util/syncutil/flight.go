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

package syncutil

import (
	"context"
	"sync"
)

// FlightGroup coalesces unfinished work for the same key. Completed results are
// not cached. The zero value is ready to use; a FlightGroup must not be copied
// after first use.
type FlightGroup[K comparable, V any] struct {
	mu      sync.Mutex
	flights map[K]*flight[V]
}

type flight[V any] struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int
	value   V
	err     error
}

// Do runs work in a shared goroutine, or joins the unfinished work for key.
// Concurrent callers for the same key must supply equivalent work functions.
//
// Each caller waits until the work completes or its own ctx is canceled. The
// worker context inherits values from the first caller, but not its cancellation
// or deadline. It is canceled when the work finishes or all waiters leave.
// After all waiters leave, a new caller can start new work even if the abandoned
// worker has not returned yet. Work must observe its context to stop promptly.
func (g *FlightGroup[K, V]) Do(ctx context.Context, key K, work func(context.Context) (V, error)) (V, error) {
	if err := ctx.Err(); err != nil {
		var zero V
		return zero, err
	}
	g.mu.Lock()
	call := g.flights[key]
	if call == nil {
		if g.flights == nil {
			g.flights = make(map[K]*flight[V])
		}
		// Preserve tracing values, but no individual deadline or cancellation
		// may end another caller's wait. The final waiter cancels this context.
		workCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		call = &flight[V]{done: make(chan struct{}), cancel: cancel}
		g.flights[key] = call
		go func() {
			value, err := work(workCtx)
			g.mu.Lock()
			call.value, call.err = value, err
			if g.flights[key] == call {
				delete(g.flights, key)
			}
			close(call.done)
			cancel()
			g.mu.Unlock()
		}()
	}
	call.waiters++
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		call.waiters--
		if call.waiters == 0 {
			// Detach before cancellation so a subsequent caller can start fresh
			// even if the abandoned worker is still unwinding its I/O.
			if g.flights[key] == call {
				delete(g.flights, key)
			}
			call.cancel()
		}
		g.mu.Unlock()
	}()

	select {
	case <-ctx.Done():
		var zero V
		return zero, ctx.Err()
	case <-call.done:
		if err := ctx.Err(); err != nil {
			var zero V
			return zero, err
		}
		return call.value, call.err
	}
}
