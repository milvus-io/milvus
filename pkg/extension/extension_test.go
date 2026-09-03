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
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type fakeProvider struct {
	name     string
	requires []CapabilityID
	caps     Capabilities
}

func (f fakeProvider) Name() string               { return f.name }
func (f fakeProvider) Requires() []CapabilityID   { return f.requires }
func (f fakeProvider) Capabilities() Capabilities { return f.caps }

// stubProxyExtension is a placeholder used to check the capability field is stored.
type stubProxyExtension struct{ NoopProxyExtension }

// allCapabilityIDs is the list the registry tests walk. It is written out by
// hand rather than derived from entries() so that a capability added to the
// struct and to entries() but not to this list - or the other way round -
// fails a test instead of going unnoticed.
var allCapabilityIDs = []CapabilityID{
	CapProxyExtension,
	CapRBACBootstrap,
	CapCoordinatorEngine,
	CapResourceGroupInterceptor,
	CapIndexDrain,
	CapLoadPlacementScope,
	CapHook,
}

// fullCapabilities supplies every capability with a non-nil implementation,
// so a test can ask "is every id requirable when supplied".
func fullCapabilities() Capabilities {
	return Capabilities{
		ProxyExt:          stubProxyExtension{},
		RBACBootstrap:     &fakeBootstrapper{},
		CoordinatorEngine: &fakeCoordinatorEngine{},
		ResourceGroups:    stubResourceGroupInterceptor{},
		IndexDrain:        stubIndexDrainer{},
		LoadPlacement:     stubLoadPlacementScope{},
		Hook:              stubHook{},
	}
}

func TestCapsIsZeroWithoutProvider(t *testing.T) {
	ResetForTest()
	assert.Nil(t, Caps().ProxyExt)
	assert.Same(t, zeroCaps, Caps(), "the no-provider path must hand out the shared zero table, not allocate")
}

func TestSetProviderInstallsCapabilities(t *testing.T) {
	ResetForTest()
	defer ResetForTest()

	ext := stubProxyExtension{}
	err := SetProvider(fakeProvider{
		name: "testprovider",
		caps: Capabilities{ProxyExt: ext},
	})
	assert.NoError(t, err)
	assert.Equal(t, ext, Caps().ProxyExt)
}

func TestSetProviderRejectsNil(t *testing.T) {
	ResetForTest()
	err := SetProvider(nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal, "a wiring failure is a system error, never an input one")
}

func TestSetProviderRejectsMissingRequiredCapability(t *testing.T) {
	ResetForTest()
	defer ResetForTest()

	err := SetProvider(fakeProvider{
		name:     "testprovider",
		requires: []CapabilityID{CapProxyExtension},
		caps:     Capabilities{},
	})
	assert.ErrorContains(t, err, string(CapProxyExtension))
	assert.ErrorContains(t, err, "did not supply")
	assert.Nil(t, Caps().ProxyExt, "a failed install must leave no trace")
}

// A mistyped requirement is a different mistake from a missing capability and
// must read differently: "did not supply it" sends the implementer to look at
// the table, when the table is fine and the string is wrong.
func TestSetProviderRejectsUnknownRequiredCapability(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)

	err := SetProvider(fakeProvider{
		name:     "testprovider",
		requires: []CapabilityID{"proxy_extenison"},
		caps:     fullCapabilities(),
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "unknown capability")
	assert.ErrorContains(t, err, "proxy_extenison")
	assert.Nil(t, installed.Load(), "a failed install must leave no trace")
}

// Every declared CapabilityID must be requirable when supplied and refusable
// when absent. This is the test the per-capability files each used to carry
// a copy of; walking the list here means a new capability that is declared
// but not wired into entries() fails immediately.
func TestEveryCapabilityIsRequirable(t *testing.T) {
	full := fullCapabilities()
	entries := full.entries()
	require.Len(t, entries, len(allCapabilityIDs), "entries() and the CapabilityID list must enumerate the same capabilities")

	for _, id := range allCapabilityIDs {
		t.Run(string(id), func(t *testing.T) {
			ResetForTest()
			t.Cleanup(ResetForTest)

			_, known := Capabilities{}.lookup(id)
			assert.True(t, known, "the id must be one entries() knows")
			assert.False(t, Capabilities{}.has(id), "an empty table must not claim to supply it")
			assert.True(t, full.has(id), "a full table must supply it")

			err := SetProvider(fakeProvider{name: "absent", requires: []CapabilityID{id}, caps: Capabilities{}})
			assert.ErrorContains(t, err, string(id), "requiring it while absent must fail and name it")

			ResetForTest()
			assert.NoError(t, SetProvider(fakeProvider{name: "present", requires: []CapabilityID{id}, caps: full}),
				"requiring it while supplied must succeed")
		})
	}
}

func TestSetProviderRejectsDoubleInstall(t *testing.T) {
	ResetForTest()
	defer ResetForTest()

	first := fakeProvider{name: "first", caps: Capabilities{ProxyExt: stubProxyExtension{}}}
	assert.NoError(t, SetProvider(first))
	err := SetProvider(fakeProvider{name: "second"})
	assert.ErrorContains(t, err, "first", "the error should name the provider already installed")
	assert.ErrorContains(t, err, "second", "and the one it refused")
	assert.Equal(t, stubProxyExtension{}, Caps().ProxyExt, "the first installation must stay in force")
}

// Under the test tag the slot can be cleared between a failed CAS and the
// error message that names the previous provider; that window must produce an
// error, not a nil dereference.
func TestSetProviderDoubleInstallToleratesAClearedSlot(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)

	// Occupy the slot with a box whose provider is a typed nil, which is the
	// worst shape the message path can meet.
	installed.Store(&box{})
	err := SetProvider(fakeProvider{name: "second"})
	require.Error(t, err)
	assert.ErrorContains(t, err, "already installed")
}

// A typed-nil capability passes the non-nil interface check and then panics at
// its first seam call. SetProvider refuses it at install time, with the
// capability named, instead of letting a wiring mistake surface as a panic in
// whichever request happens to consult the capability first.
func TestSetProviderRefusesATypedNilCapability(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)

	var nilDrainer *typedNilDrainer
	err := SetProvider(fakeProvider{name: "typed-nil-form", caps: Capabilities{IndexDrain: nilDrainer}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), string(CapIndexDrain))
}

// The provider itself can be a typed nil too - a nil *concreteProvider in the
// interface - which passes the untyped-nil check and would panic on the first
// method call. It is refused up front like a typed-nil capability.
func TestSetProviderRefusesATypedNilProvider(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)

	var p *fakeProvider
	assert.NotPanics(t, func() {
		err := SetProvider(p)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "typed nil")
	})
	assert.Nil(t, installed.Load(), "a failed install must leave no trace")
}

type typedNilDrainer struct{}

func (*typedNilDrainer) AllowVectorIndexDropWhileLoaded(context.Context, int64, string) bool {
	return false
}
func (*typedNilDrainer) BeginDropIndex(context.Context, *indexpb.DropIndexRequest) bool { return false }
func (*typedNilDrainer) AfterDropIndex(context.Context, *indexpb.DropIndexRequest)      {}
func (*typedNilDrainer) AbortDropIndex(context.Context, *indexpb.DropIndexRequest)      {}
func (*typedNilDrainer) AfterCreateIndex(context.Context, *indexpb.CreateIndexRequest)  {}
func (*typedNilDrainer) CollectionDraining(context.Context, int64) bool                 { return false }

// stubHook is a hook.Hook that answers nothing, which is all the registry
// tests need: they only ever ask whether the field is filled in.
type stubHook struct{}

func (stubHook) Init(map[string]string) error { return nil }
func (stubHook) Mock(ctx context.Context, req interface{}, fullMethod string) (bool, interface{}, error) {
	return false, nil, nil
}

func (stubHook) Before(ctx context.Context, req interface{}, fullMethod string) (context.Context, error) {
	return ctx, nil
}

func (stubHook) After(ctx context.Context, result interface{}, err error, fullMethod string) error {
	return nil
}
func (stubHook) Release()                            {}
func (stubHook) VerifyAPIKey(string) (string, error) { return "", nil }
