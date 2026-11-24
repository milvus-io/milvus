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

package extension

import (
	"context"
	"sync"
)

// DQLObserver observes processing attempts and scheduler phases of one request.
// Implementations must be concurrency-safe, bounded and non-blocking. The
// observer must not alter request results or perform synchronous I/O.
// An attempt includes Proxy preparation, queueing and shard retries, but not
// outer retry backoff. It is not QueryNode CPU time.
type DQLObserver interface {
	BeginAttempt() func()
	SetRequestPhase(string)
}

type (
	dqlObserverKey struct{}
	dqlAttemptKey  struct{}
)

type dqlAttempt struct {
	mu       sync.Mutex
	finished bool
}

// WithDQLObserver attaches an optional observer to an admitted request. Stock
// Milvus does not install one. No customer identifiers or payload are retained
// by this interface.
func WithDQLObserver(ctx context.Context, observer DQLObserver) context.Context {
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, dqlObserverKey{}, observer)
}

// BeginDQLAttempt brackets one top-level processing attempt. Nested queries
// belong to their caller's attempt and are not counted twice. Call finish even
// on failure; it is safe to invoke more than once.
func BeginDQLAttempt(ctx context.Context) (context.Context, func()) {
	observer, _ := ctx.Value(dqlObserverKey{}).(DQLObserver)
	if observer == nil || ctx.Value(dqlAttemptKey{}) != nil {
		return ctx, func() {}
	}
	finish := observer.BeginAttempt()
	attempt := &dqlAttempt{}
	return context.WithValue(ctx, dqlAttemptKey{}, attempt), func() {
		attempt.mu.Lock()
		defer attempt.mu.Unlock()
		if attempt.finished {
			return
		}
		attempt.finished = true
		if finish != nil {
			finish()
		}
	}
}

// ObserveDQLPhase is called only for top-level Search/Query scheduler tasks.
// Internal subtasks must not overwrite the outer request's phase.
func ObserveDQLPhase(ctx context.Context, phase string) {
	if phase != "queue" && phase != "execute" {
		return
	}
	if attempt, ok := ctx.Value(dqlAttemptKey{}).(*dqlAttempt); ok {
		// A canceled task may leave its queue after its caller has returned.
		// Serialize phase delivery with completion so it cannot change a retry.
		attempt.mu.Lock()
		defer attempt.mu.Unlock()
		if attempt.finished {
			return
		}
	}
	if observer, ok := ctx.Value(dqlObserverKey{}).(DQLObserver); ok {
		observer.SetRequestPhase(phase)
	}
}
