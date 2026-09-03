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

// queryResourceGroupKey is the private type of the routing-scope context key,
// so nothing outside this package can collide with it or plant a scope milvus
// did not decide on.
type queryResourceGroupKey struct{}

// WithQueryResourceGroup binds the resource group a query must be routed to
// onto ctx.
//
// milvus never invents one: it binds only what EnsureQueryReady returned in
// QueryPlacement.ResourceGroup, so with no provider installed nothing calls
// this and no request context carries a scope. The value travels on the
// context rather than on the request structs because every stage that has to
// honor it - the task, the load balancer, the shard-client cache, the
// coordinator call - already has the request's context in hand, and threading
// a field through all of them instead would put the routing scope in reach of
// code that has no business changing it.
func WithQueryResourceGroup(ctx context.Context, resourceGroup string) context.Context {
	return context.WithValue(ctx, queryResourceGroupKey{}, resourceGroup)
}

// QueryResourceGroupFromContext returns the resource group WithQueryResourceGroup
// bound onto ctx, or the empty string when nothing bound one.
//
// The empty answer means "no scope", which is what every request in a stock
// binary looks like, and what a request that named no cluster looks like in any
// binary.
func QueryResourceGroupFromContext(ctx context.Context) string {
	resourceGroup, _ := ctx.Value(queryResourceGroupKey{}).(string)
	return resourceGroup
}
