package extension

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
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

type typedNilDrainer struct{}

func (*typedNilDrainer) AllowVectorIndexDropWhileLoaded(context.Context, int64, string) bool {
	return false
}
func (*typedNilDrainer) BeginDropIndex(context.Context, *indexpb.DropIndexRequest) bool { return false }
func (*typedNilDrainer) AfterDropIndex(context.Context, *indexpb.DropIndexRequest)      {}
func (*typedNilDrainer) AbortDropIndex(context.Context, *indexpb.DropIndexRequest)      {}
func (*typedNilDrainer) AfterCreateIndex(context.Context, *indexpb.CreateIndexRequest)  {}
func (*typedNilDrainer) CollectionDraining(context.Context, int64) bool                 { return false }
