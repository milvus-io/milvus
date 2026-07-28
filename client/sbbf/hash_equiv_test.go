package sbbf

import (
	"encoding/binary"
	"math"
	"math/rand"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

// hashInt64Generic is the straightforward implementation hashInt64 replaced:
// encode to 8 little-endian bytes, hash with the general xxhash routine.
func hashInt64Generic(v int64) uint64 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(v))
	return xxhash.Sum64(buf[:])
}

// TestHashInt64Equivalence pins the specialized 8-byte XXH64 to the general
// xxhash implementation. A divergence here would silently change every blob
// this package produces and break cross-language reproducibility, so it is
// checked over boundaries, small values and a large random sample.
func TestHashInt64Equivalence(t *testing.T) {
	edge := []int64{
		0, 1, -1, 2, -2,
		math.MaxInt64, math.MinInt64,
		math.MaxInt32, math.MinInt32,
		1 << 31, 1 << 32, 1 << 62, -(1 << 62),
		0x0101010101010101, -0x0101010101010101,
	}
	for _, v := range edge {
		require.Equalf(t, hashInt64Generic(v), hashInt64(v), "edge value %d", v)
	}

	for i := int64(-100_000); i < 100_000; i++ {
		if hashInt64Generic(i) != hashInt64(i) {
			t.Fatalf("sequential value %d: generic=%#x specialized=%#x", i, hashInt64Generic(i), hashInt64(i))
		}
	}

	rng := rand.New(rand.NewSource(20260728))
	for i := 0; i < 5_000_000; i++ {
		v := int64(rng.Uint64())
		if hashInt64Generic(v) != hashInt64(v) {
			t.Fatalf("random value %d: generic=%#x specialized=%#x", v, hashInt64Generic(v), hashInt64(v))
		}
	}
}

// TestMarshalAliasesBuilder pins the ownership contract: Marshal hands out the
// Builder's own buffer rather than a copy.
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
