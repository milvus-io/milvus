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

package requestbudget

import (
	"context"
	"net/http"
	"time"
)

type activeBudgetKey struct{}

// Active reports whether the outer transport has already installed the REST
// budget. Route middleware must not start a second timer or response buffer.
func Active(ctx context.Context) bool {
	active, _ := ctx.Value(activeBudgetKey{}).(bool)
	return active
}

// Run applies a single absolute budget to handler work and transport I/O.
// The handler runs synchronously: a timed-out response cannot outlive its
// request in a detached middleware goroutine.
func Run(w http.ResponseWriter, r *http.Request, policy Policy, startedAt time.Time, next http.Handler) error {
	if err := r.Context().Err(); err != nil {
		return err
	}
	deadline, err := policy.Resolve(r.Context(), startedAt, r.Header.Get("Request-Timeout"))
	if err != nil {
		return err
	}
	if !deadline.After(time.Now()) {
		return context.DeadlineExceeded
	}
	if err := ApplyIODeadline(w, deadline); err != nil {
		return err
	}
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	defer cancel()
	ctx = context.WithValue(ctx, activeBudgetKey{}, true)
	next.ServeHTTP(w, r.WithContext(ctx))
	return nil
}
