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

// LoadPlacementScope answers one question about a load request: are the
// resource groups it names the collection's whole desired placement, or only
// the part of it this request speaks for?
//
// Native milvus has exactly one answer, and it is "the whole placement". A
// load request is declarative: it states where the collection is to live, so a
// resource group holding a replica the request does not name is holding a
// replica the request has just declared unwanted. Every native caller sends the
// collection's complete resource group list, so that reading is the correct one
// and nothing here changes it.
//
// A deployment form that loads one collection into several resource groups
// independently has the other answer. Each of its resource groups is loaded by
// whoever queried it, at the moment they queried it, with no knowledge of which
// other resource groups hold the same collection - and no way to acquire that
// knowledge that is safe to depend on, since the index it would have to read is
// empty after a coordinator restart. Its load requests therefore name one
// resource group and mean one resource group, and the resource groups they do
// not name are out of scope rather than unwanted.
//
// # What a "true" answer changes
//
// Exactly one thing, at exactly one point: when milvus has resolved the
// per-resource-group replica counts this request asks for, the counts the
// collection already has in resource groups the request did not name are
// carried through alongside them. The request is completed into the cumulative
// one a native caller would have sent, before it is recorded, and from there
// every downstream step runs natively on a request it fully understands.
//
// That completion has to happen before the request is recorded rather than
// while it is carried out, and the reason is worth stating because it is the
// whole argument for this capability's placement. Milvus reconciles the
// requested placement against the stored one while building the record: a
// replica in a resource group the request leaves at zero is not merely dropped,
// it is picked up and reused for a resource group the request does ask for.
// The record that results says the replica now lives in the new resource group
// and does not mention the old one at all. Anything downstream of that has lost
// the information needed to put it back, and would have to allocate a fresh
// replica identity to try - which is not replay-deterministic, and so cannot
// live behind a durable record that may be applied more than once.
//
// # Load and release are deliberately asymmetric
//
// This capability governs load. It says nothing about release, and release
// keeps its native whole-placement meaning in every deployment form.
//
// The asymmetry is intended, not an omission. A form that loads per resource
// group still has to be able to say "this collection now lives in these
// resource groups and no others" when one of them goes away, and release is the
// only request that can say it. Load is the direction where a partial statement
// is ambiguous - "put it here" does not tell you what to do about anywhere else
// - while release names what is going away and is unambiguous either way.
//
// Nothing had to be done to preserve that: release travels a different path
// through querycoord, which reassigns replicas against the stored placement
// directly instead of reconciling a requested one, and never consults this
// capability. A future change that unified the two paths would have to keep
// this distinction by hand, which is why it is written down here.
//
// # Short-circuit contract
//
// This capability does not short-circuit anything. It classifies a request;
// milvus then does what that classification implies, and there is no answer it
// can give that stops milvus from carrying the load out, fails the request, or
// substitutes a result of its own.
//
// With no provider installed the capability is nil, milvus asks nothing, and
// every load request is read as the whole placement exactly as before.
//
// # Concurrency
//
// The method is called from querycoord's load path, from concurrent load
// jobs and without any lock of milvus's held. It runs while a load is being
// recorded, so it must be cheap and must not call back into the coordinator.
//
// NoopLoadPlacementScope is the Noop base under the package evolution policy.
type LoadPlacementScope interface {
	// ScopedToNamedResourceGroups reports whether this load request states the
	// placement of only the resource groups it names.
	//
	// resourceGroups is the resolved list milvus is about to place the
	// collection into - after defaults and cluster-level overrides have been
	// applied, not the raw list off the wire - so an implementation sees the
	// same names milvus does. It is never empty at the call site.
	//
	// Returning false is the native reading and must stay the safe default: an
	// implementation that cannot decide says false, and the request keeps the
	// meaning it has always had.
	ScopedToNamedResourceGroups(ctx context.Context, collectionID int64, resourceGroups []string) bool
}

// NoopLoadPlacementScope reads every load request as the whole placement,
// which is the native meaning.
type NoopLoadPlacementScope struct{}

var _ LoadPlacementScope = NoopLoadPlacementScope{}

func (NoopLoadPlacementScope) ScopedToNamedResourceGroups(context.Context, int64, []string) bool {
	return false
}
