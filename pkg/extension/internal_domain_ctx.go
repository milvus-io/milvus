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

import "context"

// internalDomainKey marks a request that arrived on an internal-domain
// listener. An unexported struct key cannot be forged from outside this
// package by accident, and never collides with a string key some middleware
// stuffed into the same context.
type internalDomainKey struct{}

// WithInternalDomain stamps ctx as originating on an internal-domain
// listener. Only the proxy.internalDomain.* listeners should stamp it: the
// mark is how a handler-level seam tells the control
// plane's call, arriving on the trusted internal port, from a tenant's call
// arriving on the external one - the handler itself is shared and cannot see
// which listener accepted the connection.
func WithInternalDomain(ctx context.Context) context.Context {
	return context.WithValue(ctx, internalDomainKey{}, true)
}

// FromInternalDomain reports whether ctx carries the internal-domain mark.
func FromInternalDomain(ctx context.Context) bool {
	v, _ := ctx.Value(internalDomainKey{}).(bool)
	return v
}
