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

package shardclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/extension"
)

// scopeOnlyProvider installs a ProxyExt capability and nothing else. Only the
// nil-ness of that field matters to this seam: the scope itself travels on the
// context, never through a call back into the provider.
type scopeOnlyProvider struct{}

func (scopeOnlyProvider) Name() string                       { return "test" }
func (scopeOnlyProvider) Requires() []extension.CapabilityID { return nil }
func (scopeOnlyProvider) Capabilities() extension.Capabilities {
	return extension.Capabilities{ProxyExt: extension.NoopProxyExtension{}}
}

func installScopeProvider(t *testing.T) {
	t.Helper()
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	require.NoError(t, extension.SetProvider(scopeOnlyProvider{}))
}

// With no provider installed a request cannot carry a scope, and the seam must
// not even look: a stock binary pays one atomic load per search and per query,
// not a context walk.
func TestRoutingResourceGroupIsEmptyWithoutAProvider(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	// Even a context somebody bound a scope onto answers empty, because the
	// gate that binds one never runs without a provider.
	ctx := extension.WithQueryResourceGroup(context.Background(), "rg-a")
	assert.Empty(t, routingResourceGroup(ctx))
}

// With a provider installed the scope is whatever EnsureQueryReady bound onto
// the request's context, and nothing else.
func TestRoutingResourceGroupReadsTheContextScope(t *testing.T) {
	installScopeProvider(t)

	assert.Empty(t, routingResourceGroup(context.Background()),
		"a request that was never scoped must stay unscoped")
	assert.Equal(t, "rg-a",
		routingResourceGroup(extension.WithQueryResourceGroup(context.Background(), "rg-a")))
}

// The stamping happens at the load balancer's entry, so a workload built
// anywhere - including the three namespace fast paths that construct a
// ChannelWorkload directly - carries the scope without its builder having to
// remember. Forgetting is the failure this placement removes: it would route
// that subset of requests to another resource group's leader, silently.
func TestWorkloadsAreStampedWithTheRequestScope(t *testing.T) {
	installScopeProvider(t)
	ctx := extension.WithQueryResourceGroup(context.Background(), "rg-b")

	collection := scopedCollectionWorkload(ctx, CollectionWorkLoad{CollectionID: 1})
	assert.Equal(t, "rg-b", collection.ResourceGroup)

	channel := scopedChannelWorkload(ctx, ChannelWorkload{CollectionID: 1, Channel: "ch0"})
	assert.Equal(t, "rg-b", channel.ResourceGroup)

	// ForChannel carries it onward to every channel of the fan-out.
	assert.Equal(t, "rg-b", collection.ForChannel("ch0", 0).ResourceGroup)
}

// An explicit scope on the workload wins, so a caller that has a reason to
// name one is not overridden by the context.
func TestAnExplicitWorkloadScopeIsKept(t *testing.T) {
	installScopeProvider(t)
	ctx := extension.WithQueryResourceGroup(context.Background(), "rg-b")

	collection := scopedCollectionWorkload(ctx, CollectionWorkLoad{CollectionID: 1, ResourceGroup: "rg-explicit"})
	assert.Equal(t, "rg-explicit", collection.ResourceGroup)

	channel := scopedChannelWorkload(ctx, ChannelWorkload{CollectionID: 1, ResourceGroup: "rg-explicit"})
	assert.Equal(t, "rg-explicit", channel.ResourceGroup)
}

// A stock binary's workloads stay unscoped, which is what leaves
// FilterByResourceGroup a no-op and routing byte-for-byte what it was.
func TestWorkloadsStayUnscopedWithoutAProvider(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	ctx := extension.WithQueryResourceGroup(context.Background(), "rg-b")

	assert.Empty(t, scopedCollectionWorkload(ctx, CollectionWorkLoad{CollectionID: 1}).ResourceGroup)
	assert.Empty(t, scopedChannelWorkload(ctx, ChannelWorkload{CollectionID: 1}).ResourceGroup)
}
