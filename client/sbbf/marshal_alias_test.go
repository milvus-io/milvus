package sbbf

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMarshalAliasesBuilder verifies the ownership contract: Marshal hands out
// the Builder's own buffer rather than a copy.
//
// Comparing two Marshal results for equality is NOT enough — the previous
// implementation allocated a fresh, identical buffer each call and would pass
// that. These assertions are about identity: the same backing array, and a
// later Add visible through a slice handed out earlier.
func TestMarshalAliasesBuilder(t *testing.T) {
	b, err := NewBuilder(100, 0.01)
	require.NoError(t, err)
	for i := int64(0); i < 100; i++ {
		b.AddInt64(i)
	}

	first := b.Marshal()
	second := b.Marshal()
	require.Equal(t, first, second)
	require.Same(t, &first[0], &second[0],
		"repeated Marshal must return the same buffer, not an equal copy")

	// A value inserted after Marshal is visible through the slice already
	// handed out. This is the hazard the method documents, and asserting it is
	// what stops a copy from being reintroduced unnoticed.
	before := append([]byte(nil), first...)
	for v := int64(1_000_000); v < 1_000_064; v++ {
		b.AddInt64(v)
	}
	require.NotEqual(t, before, first,
		"an Add after Marshal must be visible through the earlier slice")

	f, err := Parse(b.Marshal())
	require.NoError(t, err)
	for i := int64(0); i < 100; i++ {
		require.Truef(t, f.TestInt64(i), "member %d must be present", i)
	}
}
