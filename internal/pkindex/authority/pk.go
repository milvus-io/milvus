package authority

import (
	"encoding/binary"
	"hash/fnv"
)

// PK is one primary key, either an int64 or a varchar.
// All keys of one vchannel have the same type, so the encoding carries no type tag.
type PK struct {
	varchar bool
	i       int64
	s       string
}

func Int64PK(v int64) PK { return PK{i: v} }

func VarCharPK(v string) PK { return PK{varchar: true, s: v} }

func (p PK) IsVarChar() bool { return p.varchar }

func (p PK) Int64() int64 { return p.i }

func (p PK) VarChar() string { return p.s }

// Encode returns the order-preserving key of the primary key.
// An int64 becomes 8 big-endian bytes with the sign bit flipped.
// A varchar is its raw bytes.
func (p PK) Encode() []byte {
	if p.varchar {
		return []byte(p.s)
	}
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(p.i)^(1<<63))
	return key
}

// Hash is used to spread keys over lock stripes.
func (p PK) Hash() uint64 {
	if p.varchar {
		h := fnv.New64a()
		_, _ = h.Write([]byte(p.s))
		return h.Sum64()
	}
	// splitmix64 finalizer. Fibonacci hashing alone keeps the low-bit structure
	// of the key, so keys that are multiples of the stripe count would all land
	// on stripe 0. The finalizer mixes the high bits back down before the caller
	// takes Hash() % stripeCount.
	z := uint64(p.i) + 0x9E3779B97F4A7C15
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}
