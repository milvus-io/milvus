package reduce

import (
	"encoding/binary"
	"hash/fnv"
	"math"

	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// NullHashSentinel is a fixed hash value for null group-by values so that two
// null values in the same column always collapse into the same bucket
// (null == null for grouping). Must never collide with any normalized scalar
// encoding below.
const NullHashSentinel uint64 = 0xdeadbeefcafebabe

// HashGroupValues produces a uint64 hash over a sequence of normalized
// group-by values. Callers must pass values that have been pre-normalized by
// NormalizeScalar so that hash equality matches EqualGroupValues semantics.
func HashGroupValues(values []any) uint64 {
	var combined uint64
	for i, v := range values {
		h := hashOneGroupValue(v)
		if i == 0 {
			combined = h
			continue
		}
		combined = typeutil.HashMix(combined, h)
	}
	return combined
}

func hashOneGroupValue(v any) uint64 {
	if v == nil {
		return NullHashSentinel
	}
	h := fnv.New64a()
	var buf [8]byte
	switch val := v.(type) {
	case bool:
		if val {
			buf[0] = 1
		}
		h.Write(buf[:1])
	case int64:
		binary.LittleEndian.PutUint64(buf[:], uint64(val))
		h.Write(buf[:8])
	case float64:
		bits := math.Float64bits(val)
		binary.LittleEndian.PutUint64(buf[:], bits)
		h.Write(buf[:8])
	case string:
		h.Write([]byte(val))
	default:
		// Fallback flags unnormalized input; equalGroupValues will reject
		// mismatched types in secondary validation.
		return NullHashSentinel ^ 0x1
	}
	return h.Sum64()
}

// EqualGroupValues compares two already-normalized group-by value slices.
// Must stay in sync with HashGroupValues so hash-collision chains resolve
// correctly. Both-null counts as equal; NaN never matches, not even another
// NaN, mirroring standard comparison semantics so distinct NaN rows stay in
// distinct buckets.
func EqualGroupValues(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i, av := range a {
		bv := b[i]
		if av == nil || bv == nil {
			if av == nil && bv == nil {
				continue
			}
			return false
		}
		switch lhs := av.(type) {
		case float64:
			rhs, ok := bv.(float64)
			if !ok {
				return false
			}
			if math.IsNaN(lhs) || math.IsNaN(rhs) {
				return false
			}
			if lhs != rhs {
				return false
			}
		default:
			if av != bv {
				return false
			}
		}
	}
	return true
}

// MakeCompositeKeyExtractor returns a function that yields (hash, normalized
// values) for the row at idx, given a per-field iterator list. The closure
// form is shared by every group-reduce layer (QN, proxy) so the composite-key
// representation stays byte-for-byte consistent across packages.
//
// An iter entry may be nil when the corresponding field is absent on the
// current shard; the extractor treats that as a null in that column.
func MakeCompositeKeyExtractor(iters []func(int) any) func(idx int) (uint64, []any) {
	return func(idx int) (uint64, []any) {
		values := make([]any, len(iters))
		for j, it := range iters {
			if it == nil {
				continue
			}
			raw := it(idx)
			if raw == nil {
				continue
			}
			values[j] = NormalizeScalar(raw)
		}
		return HashGroupValues(values), values
	}
}

// NormalizeScalar collapses integer / floating widths into a canonical form so
// hashing and equality behave consistently regardless of the raw Go type the
// iterator surface returns. int8/16/32/uint* → int64, float32 → float64,
// []byte → string. All other types pass through unchanged.
func NormalizeScalar(v any) any {
	switch val := v.(type) {
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case int:
		return int64(val)
	case int64:
		return val
	case uint8:
		return int64(val)
	case uint16:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		return int64(val)
	case float32:
		return float64(val)
	case float64:
		return val
	case bool:
		return val
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return val
	}
}
