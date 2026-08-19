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
	assert.Same(t, replacement, ResourceGroupUpdate{Forward: replacement}.RequestToApply(nil),
		"the replacement stands even when there is no original to fall back to")
	assert.Nil(t, ResourceGroupUpdate{}.RequestToApply(nil))
}
