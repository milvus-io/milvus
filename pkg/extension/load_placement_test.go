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
