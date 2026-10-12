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

package types

import "context"

// RequestContext contains host-owned execution metadata. User identifies the
// authenticated caller when available; anonymous/internal calls leave it empty.
// ResourceName identifies the current operator for diagnostics, independently
// of the registered wheel resource used by PyUDF loading and caching.
// This is descriptive scope, not credentials or an authorization capability.
type RequestContext struct {
	Database     string
	Collection   string
	User         string
	ResourceName string
}

type requestContextKey struct{}

// WithRequestContext attaches a value snapshot while retaining the parent's
// deadline, cancellation, trace context and other request-owned values.
func WithRequestContext(ctx context.Context, scope RequestContext) context.Context {
	return context.WithValue(ctx, requestContextKey{}, scope)
}

// GetRequestContext returns a value copy. Missing metadata is a valid empty
// scope for internal calls, and callers cannot mutate another execution's scope.
func GetRequestContext(ctx context.Context) RequestContext {
	if ctx == nil {
		return RequestContext{}
	}
	scope, _ := ctx.Value(requestContextKey{}).(RequestContext)
	return scope
}

func (ctx *FuncContext) RequestContext() RequestContext { return GetRequestContext(ctx.ctx) }

// WithContext changes only the underlying request context. Query snapshots,
// reduction mode, Arrow allocator and expansion budget remain unchanged.
func (ctx *FuncContext) WithContext(request context.Context) *FuncContext {
	result := *ctx
	result.ctx = request
	return &result
}
