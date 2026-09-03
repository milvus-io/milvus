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
)

type stubLoadPlacementScope struct{ answer bool }

func (s stubLoadPlacementScope) ScopedToNamedResourceGroups(_ context.Context, _ int64, _ []string) bool {
	return s.answer
}

func TestLoadPlacementIsNilWithoutProvider(t *testing.T) {
	ResetForTest()
	assert.Nil(t, Caps().LoadPlacement,
		"with no provider installed nothing may answer the load-placement question")
}

func TestSetProviderStoresLoadPlacement(t *testing.T) {
	ResetForTest()
	defer ResetForTest()

	scope := stubLoadPlacementScope{answer: true}
	assert.NoError(t, SetProvider(fakeProvider{
		name: "testprovider",
		caps: Capabilities{LoadPlacement: scope},
	}))
	assert.Equal(t, scope, Caps().LoadPlacement)
}

// The capability table's has() is what turns a Requires entry into a startup
// failure. A capability whose field it does not know about is silently
// unrequirable, so the entry has to be exercised, not just declared.
func TestLoadPlacementIsRequirable(t *testing.T) {
	ResetForTest()
	defer ResetForTest()

	err := SetProvider(fakeProvider{
		name:     "testprovider",
		requires: []CapabilityID{CapLoadPlacementScope},
		caps:     Capabilities{},
	})
	assert.ErrorContains(t, err, string(CapLoadPlacementScope),
		"a provider that requires the scope but supplies none must fail to install")

	ResetForTest()
	assert.NoError(t, SetProvider(fakeProvider{
		name:     "testprovider",
		requires: []CapabilityID{CapLoadPlacementScope},
		caps:     Capabilities{LoadPlacement: stubLoadPlacementScope{}},
	}), "supplying it must satisfy the requirement")
}

// The Noop scope answers false, which is the native reading and the safe
// default the interface doc requires of an implementation that cannot decide.
func TestNoopLoadPlacementScopeIsTheNativeReading(t *testing.T) {
	type embedder struct{ NoopLoadPlacementScope }
	var s LoadPlacementScope = embedder{}
	assert.False(t, s.ScopedToNamedResourceGroups(context.Background(), 1, []string{"rg-a"}))
}
