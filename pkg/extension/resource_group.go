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

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// ResourceGroupUpdate is what a ResourceGroupInterceptor decided about one
// UpdateResourceGroups call. It is a struct rather than a list of results so
// that a later decision can be added as a field without breaking every
// implementation, the same reason Capabilities is a struct.
type ResourceGroupUpdate struct {
	// Forward replaces the request milvus applies. A nil Forward means "apply
	// the request as it arrived", so an interceptor that only observes leaves
	// this unset rather than echoing its input back.
	Forward *querypb.UpdateResourceGroupsRequest

	// Applied reports that the interceptor already carried the whole update
	// out itself. milvus then applies nothing and answers the caller success.
	// An interceptor that wants the call to fail returns an error instead.
	//
	// Applied takes precedence over Forward: when both are set, Forward is
	// ignored, since there is nothing left for milvus to apply.
	Applied bool

	// FollowUpGroups is carried, untouched, to AfterUpdateResourceGroups once
	// the update commits. It exists so an interceptor can decide before the
	// commit what it will need afterwards without keeping request-keyed state
	// of its own across two concurrent calls. milvus never reads its contents.
	FollowUpGroups []string
}

// RequestToApply returns the request milvus must apply: the interceptor's
// replacement when it supplied one, otherwise the original. It exists so the
// "nil Forward means unchanged" rule is written down once, here, instead of at
// every call site.
func (u ResourceGroupUpdate) RequestToApply(original *querypb.UpdateResourceGroupsRequest) *querypb.UpdateResourceGroupsRequest {
	if u.Forward != nil {
		return u.Forward
	}
	return original
}

// ResourceGroupInterceptor lets a deployment form that manages resource groups
// itself see, adjust and complete the resource-group requests milvus receives.
//
// A form like that keeps state milvus has no concept of - how long an idle
// resource group is kept, how much compute it is sized for, how it is
// accounted for - and encodes it in the requests it sends itself. It
// therefore has to strip that state before the request reaches querycoord (a
// private node label left in place becomes a node filter, and the resource
// group would never accept a node again), and it has to learn what committed.
//
// With no provider installed the capability is nil, milvus consults nothing,
// and every resource-group request reaches querycoord exactly as it arrived.
//
// # Short-circuit contract
//
// Each method states whether it may replace milvus's native outcome. An
// undocumented method may not: it observes, and milvus does what it would have
// done anyway. This is the convention borrowed from HBASE-18770.
//
// # Mutation
//
// An implementation must not mutate the request it is handed. milvus applies
// what BeforeCreateResourceGroup and BeforeUpdateResourceGroups return, so an
// adjustment is expressed by returning a replacement, which keeps the caller's
// request - which milvus may still log or retry - intact.
//
// # Errors
//
// The one method that may fail reports its error to the caller through
// merr.Status, so the error must be or wrap a merr sentinel; anything else
// collapses to UnexpectedError.
//
// # Concurrency
//
// Every method is called from the coordinator's request goroutines,
// concurrently and without any lock of milvus's held. The Before/After pair
// of one update runs on the same goroutine, but another update's pair may
// interleave with it, which is why FollowUpGroups exists: the state a
// Before decision needs afterwards travels with the call, not in the
// interceptor.
//
// NoopResourceGroupInterceptor is the Noop base under the package evolution
// policy.
type ResourceGroupInterceptor interface {
	// BeforeCreateResourceGroup runs before milvus creates a resource group.
	//
	// MAY REPLACE: the returned request is created in place of the original.
	// Returning nil keeps the original.
	//
	// It cannot refuse the create. Refusing user DDL is what milvus's own quota
	// is for, and a second interface able to reject the same request would
	// leave two places to look when one is rejected. The create may still fail
	// afterwards in milvus, so an implementation that registers state here must
	// tolerate a resource group that never came into being.
	BeforeCreateResourceGroup(ctx context.Context, req *milvuspb.CreateResourceGroupRequest) *milvuspb.CreateResourceGroupRequest

	// BeforeUpdateResourceGroups runs before milvus applies an update.
	//
	// MAY REPLACE: the returned ResourceGroupUpdate can substitute the request
	// milvus applies, or declare the update already carried out so that milvus
	// applies nothing at all.
	//
	// A non-nil error aborts the update, and milvus reports it to the caller
	// without applying anything. Unlike the create hook this one can fail
	// because the failure is not a policy refusal: an interceptor that applies
	// part of an update to its own state and then cannot finish must be able to
	// stop milvus from committing the rest.
	BeforeUpdateResourceGroups(ctx context.Context, req *querypb.UpdateResourceGroupsRequest) (ResourceGroupUpdate, error)

	// AfterUpdateResourceGroups runs after milvus committed the update, and
	// only then: an update the interceptor aborted, declared already applied,
	// or that milvus failed to commit never reaches it. update is the value
	// BeforeUpdateResourceGroups returned for this same call, so what the
	// interceptor does here can read the just-committed state.
	AfterUpdateResourceGroups(ctx context.Context, update ResourceGroupUpdate)

	// BeforeDropResourceGroup runs before milvus drops a resource group, which
	// is the last point at which the interceptor can still read what the group
	// holds.
	//
	// It cannot refuse or defer the drop, and milvus proceeds whether or not
	// the interceptor's own teardown succeeded: a control plane that asked for
	// the group to go away is not served by milvus keeping it.
	BeforeDropResourceGroup(ctx context.Context, req *milvuspb.DropResourceGroupRequest)

	// AfterDropResourceGroupFailed runs when the native drop did NOT commit -
	// milvus refused it or the call errored - after BeforeDropResourceGroup
	// already ran. The interceptor's teardown is done by then and cannot be
	// undone; what this call gives the implementation is the knowledge that
	// the resource group it just emptied still exists in milvus, so its own
	// reconciliation (or an operator) can finish the job instead of the two
	// sides diverging in silence. A committed drop is not reported: the
	// Before hook saw everything there was to see.
	AfterDropResourceGroupFailed(ctx context.Context, req *milvuspb.DropResourceGroupRequest)
}

// NoopResourceGroupInterceptor forwards every request unchanged and observes
// nothing, which is what a stock binary does.
type NoopResourceGroupInterceptor struct{}

var _ ResourceGroupInterceptor = NoopResourceGroupInterceptor{}

func (NoopResourceGroupInterceptor) BeforeCreateResourceGroup(context.Context, *milvuspb.CreateResourceGroupRequest) *milvuspb.CreateResourceGroupRequest {
	return nil
}

func (NoopResourceGroupInterceptor) BeforeUpdateResourceGroups(context.Context, *querypb.UpdateResourceGroupsRequest) (ResourceGroupUpdate, error) {
	return ResourceGroupUpdate{}, nil
}

func (NoopResourceGroupInterceptor) AfterUpdateResourceGroups(context.Context, ResourceGroupUpdate) {}

func (NoopResourceGroupInterceptor) BeforeDropResourceGroup(context.Context, *milvuspb.DropResourceGroupRequest) {
}

func (NoopResourceGroupInterceptor) AfterDropResourceGroupFailed(context.Context, *milvuspb.DropResourceGroupRequest) {
}
