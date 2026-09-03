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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// stubResourceGroupInterceptor answers every hook inertly; the tests here are
// about the capability table and the ResourceGroupUpdate contract, not about
// what an interceptor decides.
type stubResourceGroupInterceptor struct{}

func (stubResourceGroupInterceptor) BeforeCreateResourceGroup(context.Context, *milvuspb.CreateResourceGroupRequest) *milvuspb.CreateResourceGroupRequest {
	return nil
}

func (stubResourceGroupInterceptor) BeforeUpdateResourceGroups(context.Context, *querypb.UpdateResourceGroupsRequest) (ResourceGroupUpdate, error) {
	return ResourceGroupUpdate{}, nil
}

func (stubResourceGroupInterceptor) AfterUpdateResourceGroups(context.Context, ResourceGroupUpdate) {
}

func (stubResourceGroupInterceptor) BeforeDropResourceGroup(context.Context, *milvuspb.DropResourceGroupRequest) {
}

func (stubResourceGroupInterceptor) AfterDropResourceGroupFailed(context.Context, *milvuspb.DropResourceGroupRequest) {
}

func TestResourceGroupInterceptorAbsentWithoutProvider(t *testing.T) {
	ResetForTest()
	assert.Nil(t, Caps().ResourceGroups,
		"with no provider installed the resource-group path must have nothing to consult")
}

func TestResourceGroupInterceptorIsInstalledAndRequirable(t *testing.T) {
	ResetForTest()
	defer ResetForTest()

	interceptor := stubResourceGroupInterceptor{}
	err := SetProvider(fakeProvider{
		name:     "testprovider",
		requires: []CapabilityID{CapResourceGroupInterceptor},
		caps:     Capabilities{ResourceGroups: interceptor},
	})
	assert.NoError(t, err)
	assert.Equal(t, interceptor, Caps().ResourceGroups)
}

func TestSetProviderRejectsMissingResourceGroupInterceptor(t *testing.T) {
	ResetForTest()
	defer ResetForTest()

	err := SetProvider(fakeProvider{
		name:     "testprovider",
		requires: []CapabilityID{CapResourceGroupInterceptor},
		caps:     Capabilities{},
	})
	assert.ErrorContains(t, err, string(CapResourceGroupInterceptor),
		"a form that declared it manages resource groups must not start without an interceptor")
	assert.Nil(t, Caps().ResourceGroups, "a failed install must leave no trace")
}

// The "nil Forward means unchanged" rule is what keeps an observing
// interceptor from having to echo its input back, so it is pinned here rather
// than left to each call site.
func TestResourceGroupUpdateRequestToApply(t *testing.T) {
	original := &querypb.UpdateResourceGroupsRequest{}
	replacement := &querypb.UpdateResourceGroupsRequest{}

	assert.Same(t, original, ResourceGroupUpdate{}.RequestToApply(original),
		"an interceptor that supplied no replacement must leave the caller's request in force")
	assert.Same(t, replacement, ResourceGroupUpdate{Forward: replacement}.RequestToApply(original),
		"a supplied replacement must be what milvus applies")
}

// The Noop interceptor forwards everything unchanged: nil replacements and a
// zero update, so a form that embeds it keeps the native path for every hook
// it does not override.
func TestNoopResourceGroupInterceptorForwardsUnchanged(t *testing.T) {
	type embedder struct{ NoopResourceGroupInterceptor }
	var i ResourceGroupInterceptor = embedder{}
	ctx := context.Background()

	assert.Nil(t, i.BeforeCreateResourceGroup(ctx, &milvuspb.CreateResourceGroupRequest{}),
		"a nil replacement keeps the caller's create")
	update, err := i.BeforeUpdateResourceGroups(ctx, &querypb.UpdateResourceGroupsRequest{})
	assert.NoError(t, err)
	assert.Equal(t, ResourceGroupUpdate{}, update, "a zero update applies the request as it arrived")
	assert.NotPanics(t, func() {
		i.AfterUpdateResourceGroups(ctx, update)
		i.BeforeDropResourceGroup(ctx, &milvuspb.DropResourceGroupRequest{})
		i.AfterDropResourceGroupFailed(ctx, &milvuspb.DropResourceGroupRequest{})
	})
}
