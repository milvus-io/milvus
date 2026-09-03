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

// QueryPlacement is what EnsureQueryReady decided about one search or query:
// where it must run, and what has to be released once it is done.
//
// It is a struct rather than two return values so that a later decision can be
// added as a field without breaking every implementation - the same reason
// Capabilities and ResourceGroupUpdate are structs.
type QueryPlacement struct {
	// ResourceGroup names the resource group whose replicas must serve this
	// query. It is carried on the shard-leader lookup, so the query is routed
	// to the very resource group EnsureQueryReady just made serviceable.
	//
	// The two cannot be separated. A form that loads one collection into
	// several resource groups independently gets a collection-wide shard-leader
	// answer from milvus by default, so a query made ready on one resource
	// group would be free to land on another one's query nodes - one that may
	// hold no data yet, or that is being torn down. Naming the resource group
	// here, in the same answer that says the query may proceed, is what makes
	// "made ready" and "routed to" the same place by construction.
	//
	// The empty string is the absence of a scope, not a scope that matches
	// nothing: milvus then routes exactly as it would have without a provider,
	// across every replica of the collection.
	ResourceGroup string

	// Finish releases whatever readiness took hold of - typically the pin that
	// stops an idle-timeout sweep from reclaiming the resource group's query
	// nodes while the query is still running - and is the caller's to run.
	//
	// It may be nil, which is the answer of an implementation that took
	// nothing. Callers must go through Release rather than calling it
	// directly, so that the nil case is handled in one place.
	Finish func()
}

// Release runs Finish, at most once, and tolerates both a nil Finish and being
// called again.
//
// milvus calls it from a defer installed before the error of EnsureQueryReady
// is even examined, so it runs on every exit path the request has: success,
// rejection, an early return anywhere downstream, and a panic. That is the
// whole point. A pin that is taken and not released does not fail anything
// visibly - it makes the resource group look permanently busy, and the first
// symptom is query nodes that stop scaling down long after the request that
// leaked it is gone.
//
// The receiver is a pointer and Finish is cleared as it runs, so a caller that
// releases twice through the SAME value does not release the underlying pin
// twice. That is the whole guarantee: Release is not goroutine-safe, and a
// COPY of the placement carries its own Finish, so a caller that copies the
// struct and releases both copies releases the pin twice. The seam keeps one
// placement per request, on one goroutine, and defers Release on it - the
// shape TestQueryPlacementReleaseIsReachableThroughADefer pins - and an
// implementation that needs more than that wraps its own Finish in a
// sync.Once.
func (p *QueryPlacement) Release() {
	if p == nil || p.Finish == nil {
		return
	}
	finish := p.Finish
	p.Finish = nil
	finish()
}

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
