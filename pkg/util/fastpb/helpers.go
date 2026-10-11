package fastpb

import (
	"math"
	"slices"

	"google.golang.org/protobuf/proto"

	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// cloneBytes returns a heap copy of v (v aliases the source buffer).
func cloneBytes(v []byte) []byte {
	out := make([]byte, len(v))
	copy(out, v)
	return out
}

// protoUnmarshal delegates a cold/complex sub-message to the official codec
// (into a fresh message).
func protoUnmarshal(b []byte, m proto.Message) error { return proto.Unmarshal(b, m) }

// protoMerge decodes b into m WITHOUT resetting m first — used to fold the raw
// bytes of unhandled (cold/future) fields back into a message whose hot fields
// were already populated by the fast path. proto wire merge semantics make this
// byte-for-byte equivalent to a single official proto.Unmarshal of the whole
// message, as long as no oneof member was deferred here (see callers).
func protoMerge(b []byte, m proto.Message) error {
	return proto.UnmarshalOptions{Merge: true}.Unmarshal(b, m)
}

// fallbackUnmarshal decodes the whole submessage with the official codec. Leaf
// *Array decoders call this the moment they see any field other than the single
// `data` field, so an unknown/future field on a leaf message is preserved exactly
// (rather than dropped). The fast path is unaffected for the common all-data case.
func fallbackUnmarshal(b []byte, m proto.Message) error {
	proto.Reset(m)
	return proto.Unmarshal(b, m)
}

// decodeScalarFallback decodes the rare ScalarField oneof variants via the
// official codec. Returns false if num is not a known cold variant, so the
// caller can route the raw field bytes to protoMerge instead of dropping it.
func decodeScalarFallback(num int, v []byte, sf *schemapb.ScalarField) (bool, error) {
	switch num {
	case 8:
		m := &schemapb.ArrayArray{}
		if err := protoUnmarshal(v, m); err != nil {
			return true, err
		}
		sf.Data = &schemapb.ScalarField_ArrayData{ArrayData: m}
	case 10:
		m := &schemapb.GeometryArray{}
		if err := protoUnmarshal(v, m); err != nil {
			return true, err
		}
		sf.Data = &schemapb.ScalarField_GeometryData{GeometryData: m}
	case 11:
		m := &schemapb.TimestamptzArray{}
		if err := protoUnmarshal(v, m); err != nil {
			return true, err
		}
		sf.Data = &schemapb.ScalarField_TimestamptzData{TimestamptzData: m}
	case 12:
		m := &schemapb.GeometryWktArray{}
		if err := protoUnmarshal(v, m); err != nil {
			return true, err
		}
		sf.Data = &schemapb.ScalarField_GeometryWktData{GeometryWktData: m}
	case 13:
		m := &schemapb.MolArray{}
		if err := protoUnmarshal(v, m); err != nil {
			return true, err
		}
		sf.Data = &schemapb.ScalarField_MolData{MolData: m}
	case 14:
		m := &schemapb.MolSmilesArray{}
		if err := protoUnmarshal(v, m); err != nil {
			return true, err
		}
		sf.Data = &schemapb.ScalarField_MolSmilesData{MolSmilesData: m}
	case 15:
		m := &schemapb.DateArray{}
		if err := protoUnmarshal(v, m); err != nil {
			return true, err
		}
		sf.Data = &schemapb.ScalarField_DateData{DateData: m}
	case 16:
		m := &schemapb.TimeArray{}
		if err := protoUnmarshal(v, m); err != nil {
			return true, err
		}
		sf.Data = &schemapb.ScalarField_TimeData{TimeData: m}
	default:
		return false, nil
	}
	return true, nil
}

// --- array-message decoders: b is the *Array submessage; field 1 holds the data ---

// protobuf-go already has specialized packed-scalar decoders that preallocate
// once and fast-path common varint lengths. Keep these arrays on that decoder
// rather than the slower hand-written element-by-element path.
func decodePackedBool(b []byte, _ *[]bool, m proto.Message) error {
	return fallbackUnmarshal(b, m)
}

func decodePackedI32(b []byte, _ *[]int32, m proto.Message) error {
	return fallbackUnmarshal(b, m)
}

func decodePackedI64(b []byte, _ *[]int64, m proto.Message) error {
	return fallbackUnmarshal(b, m)
}

func countPackedVarints(b []byte) int {
	count := 0
	for _, c := range b {
		if c < 0x80 {
			count++
		}
	}
	return count
}

func decodePackedF32(b []byte, dst *[]float32, m proto.Message) error {
	full := b
	for len(b) > 0 {
		num, wtype, n := consumeTag(b)
		if n <= 0 {
			return errMalformed
		}
		b = b[n:]
		if isProto2Group(wtype) {
			return errProto2
		}
		if num != 1 {
			return fallbackUnmarshal(full, m) // unknown field → official codec, preserves it
		}
		switch wtype {
		case 2:
			v, n := consumeBytes(b)
			if n <= 0 {
				return errMalformed
			}
			if len(v)%4 != 0 {
				return errMalformed
			}
			chunk := bytesToF32(v) // single memcpy
			if len(*dst) == 0 {
				*dst = chunk
			} else {
				*dst = append(*dst, chunk...)
			}
			b = b[n:]
		case 5:
			if len(b) < 4 {
				return errMalformed
			}
			*dst = append(*dst, math.Float32frombits(le32(b)))
			b = b[4:]
		default:
			return fallbackUnmarshal(full, m) // field 1 with unexpected wire type → official
		}
	}
	return nil
}

func decodePackedF64(b []byte, dst *[]float64, m proto.Message) error {
	full := b
	for len(b) > 0 {
		num, wtype, n := consumeTag(b)
		if n <= 0 {
			return errMalformed
		}
		b = b[n:]
		if isProto2Group(wtype) {
			return errProto2
		}
		if num != 1 {
			return fallbackUnmarshal(full, m) // unknown field → official codec, preserves it
		}
		switch wtype {
		case 2:
			v, n := consumeBytes(b)
			if n <= 0 {
				return errMalformed
			}
			if len(v)%8 != 0 {
				return errMalformed
			}
			chunk := bytesToF64(v) // single memcpy
			if len(*dst) == 0 {
				*dst = chunk
			} else {
				*dst = append(*dst, chunk...)
			}
			b = b[n:]
		case 1:
			if len(b) < 8 {
				return errMalformed
			}
			*dst = append(*dst, math.Float64frombits(le64(b)))
			b = b[8:]
		default:
			return fallbackUnmarshal(full, m) // field 1 with unexpected wire type → official
		}
	}
	return nil
}

// appendPackedF32 appends a raw packed-fixed32 payload (the field value itself,
// not wrapped in an *Array submessage) to dst.
func appendPackedF32(v []byte, dst *[]float32) error {
	if len(v)%4 != 0 {
		return errMalformed
	}
	chunk := bytesToF32(v) // single memcpy
	if len(*dst) == 0 {
		*dst = chunk
	} else {
		*dst = append(*dst, chunk...)
	}
	return nil
}

// appendPackedBool appends a raw packed-varint bool payload to dst.
func appendPackedBool(v []byte, dst *[]bool) error {
	*dst = slices.Grow(*dst, countPackedVarints(v))
	s := *dst
	for len(v) > 0 {
		var x uint64
		var m int
		if len(v) >= 1 && v[0] < 0x80 {
			x, m = uint64(v[0]), 1
		} else if len(v) >= 2 && v[1] < 0x80 {
			x, m = uint64(v[0]&0x7f)|uint64(v[1])<<7, 2
		} else {
			x, m = consumeVarint(v)
		}
		if m <= 0 {
			*dst = s
			return errMalformed
		}
		s = append(s, x != 0)
		v = v[m:]
	}
	*dst = s
	return nil
}

// appendPackedU32 appends a raw packed-varint payload to dst ([]uint32).
func appendPackedU32(v []byte, dst *[]uint32) error {
	*dst = slices.Grow(*dst, countPackedVarints(v))
	s := *dst
	for len(v) > 0 {
		var x uint64
		var m int
		if len(v) >= 1 && v[0] < 0x80 {
			x, m = uint64(v[0]), 1
		} else if len(v) >= 2 && v[1] < 0x80 {
			x, m = uint64(v[0]&0x7f)|uint64(v[1])<<7, 2
		} else {
			x, m = consumeVarint(v)
		}
		if m <= 0 {
			*dst = s
			return errMalformed
		}
		s = append(s, uint32(x))
		v = v[m:]
	}
	*dst = s
	return nil
}

// appendPackedI64 appends a raw packed-varint payload to dst.
func appendPackedI64(v []byte, dst *[]int64) error {
	*dst = slices.Grow(*dst, countPackedVarints(v))
	s := *dst
	for len(v) > 0 {
		var x uint64
		var m int
		if len(v) >= 1 && v[0] < 0x80 {
			x, m = uint64(v[0]), 1
		} else if len(v) >= 2 && v[1] < 0x80 {
			x, m = uint64(v[0]&0x7f)|uint64(v[1])<<7, 2
		} else {
			x, m = consumeVarint(v)
		}
		if m <= 0 {
			*dst = s
			return errMalformed
		}
		s = append(s, int64(x))
		v = v[m:]
	}
	*dst = s
	return nil
}

// decodeRepeatedBytes: field 1 is repeated bytes (BytesArray / JSONArray).
func decodeRepeatedBytes(b []byte, dst *[][]byte, m proto.Message) error {
	full := b
	for len(b) > 0 {
		num, wtype, n := consumeTag(b)
		if n <= 0 {
			return errMalformed
		}
		b = b[n:]
		if num == 1 && wtype == 2 {
			v, n := consumeBytes(b)
			if n <= 0 {
				return errMalformed
			}
			*dst = append(*dst, cloneBytes(v))
			b = b[n:]
		} else {
			return fallbackUnmarshal(full, m) // field 1 with unexpected wire type → official
		}
	}
	return nil
}
