package extension

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

type fakeVerifier struct {
	user     string
	err      error
	external bool
	seen     []string
}

func (f *fakeVerifier) Verify(rawToken string) (string, error) {
	f.seen = append(f.seen, rawToken)
	return f.user, f.err
}

func (f *fakeVerifier) RequireAPIKeyOnExternalListener() bool { return f.external }

func TestCapabilitiesReportsAPIKeyPresence(t *testing.T) {
	assert.False(t, Capabilities{}.has(CapAPIKey),
		"an empty table must not claim to supply the api key capability")
	assert.True(t, Capabilities{APIKey: &fakeVerifier{}}.has(CapAPIKey))
}

func TestSetProviderRejectsMissingAPIKeyCapability(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)

	err := SetProvider(fakeProvider{
		name:     "testprovider",
		requires: []CapabilityID{CapAPIKey},
		caps:     Capabilities{},
	})
	assert.ErrorContains(t, err, string(CapAPIKey))
}

func TestInstalledVerifierIsReachableThroughCaps(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)

	v := &fakeVerifier{user: "alice", external: true}
	assert.NoError(t, SetProvider(fakeProvider{name: "testprovider", caps: Capabilities{APIKey: v}}))

	got := Caps().APIKey
	assert.NotNil(t, got)

	user, err := got.Verify("tok-1")
	assert.NoError(t, err)
	assert.Equal(t, "alice", user)
	assert.Equal(t, []string{"tok-1"}, v.seen, "the raw token must reach the verifier unchanged")
	assert.True(t, got.RequireAPIKeyOnExternalListener())
}

func TestVerifierErrorIsPropagated(t *testing.T) {
	v := &fakeVerifier{err: errors.New("boom")}
	_, err := v.Verify("tok")
	assert.ErrorContains(t, err, "boom")
}
