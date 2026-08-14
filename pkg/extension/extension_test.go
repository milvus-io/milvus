package extension

import (
	"testing"

	"github.com/stretchr/testify/assert"
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

func TestCapsIsZeroWithoutProvider(t *testing.T) {
	ResetForTest()
	assert.Nil(t, Caps().ProxyExt)
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
	assert.Error(t, SetProvider(nil))
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
	assert.Nil(t, Caps().ProxyExt, "a failed install must leave no trace")
}

func TestSetProviderRejectsDoubleInstall(t *testing.T) {
	ResetForTest()
	defer ResetForTest()

	assert.NoError(t, SetProvider(fakeProvider{name: "first"}))
	err := SetProvider(fakeProvider{name: "second"})
	assert.ErrorContains(t, err, "first", "the error should name the provider already installed")
}
