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

type internalDomainKey struct{}

// WithInternalDomain marks a request that arrived on one of the proxy's
// internal-domain listeners (proxy.internalDomain.*), which is how a request
// hook tells the control plane's call from a tenant's on the shared handler.
func WithInternalDomain(ctx context.Context) context.Context {
	return context.WithValue(ctx, internalDomainKey{}, true)
}

// FromInternalDomain reports whether ctx carries the internal-domain mark.
func FromInternalDomain(ctx context.Context) bool {
	v, _ := ctx.Value(internalDomainKey{}).(bool)
	return v
}
