package fastpb

import (
	"encoding/binary"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

var (
	errMalformed   = errors.New("fastpb: malformed wire data")
	errInvalidUTF8 = errors.New("fastpb: invalid UTF-8 in string field")
	// errProto2 signals a proto2-only wire feature — a group, encoded with the
	// start-group (3) / end-group (4) wire types, which proto3 does not have.
	// fastpb only decodes canonical proto3; the public entry points catch this
	// and fall back to the official codec, which handles groups. Milvus itself
	// only ever produces proto3, so this never fires on trusted traffic — it is a
	// robustness path for non-Milvus / adversarial encoders.
	errProto2 = errors.New("fastpb: proto2 group wire type, fallback to official codec")
)

// isProto2Group reports whether wtype is a proto2 group delimiter (start=3,
// end=4). Encountering one means the whole message must be decoded by the
// official codec rather than fast-pathed.
func isProto2Group(wtype int) bool { return wtype == 3 || wtype == 4 }

// fallbackOnProto2 reports whether err means "fastpb hit a proto2 group". The
// public entry points use it to redo the decode with the official codec.
func fallbackOnProto2(err error) bool { return errors.Is(err, errProto2) }

// dec carries decode options down the recursive string-bearing decoders.
// utf8=true validates every decoded string (used on untrusted ingress, e.g.
// client InsertRequest) to match official proto3 behavior; internal/trusted
// result messages decode with utf8=false for maximum speed.
type dec struct {
	utf8 bool
}

// str converts a wire byte span to a string, validating UTF-8 if requested.
func (d dec) str(v []byte) (string, error) {
	if d.utf8 && !validUTF8(v) {
		return "", errInvalidUTF8
	}
	return string(v), nil
}

// UnmarshalFieldData decodes a wire-format schemapb.FieldData into fd
// (internal/trusted; no UTF-8 validation).
func UnmarshalFieldData(b []byte, fd *schemapb.FieldData) error {
	proto.Reset(fd) // match official proto.Unmarshal: clear target before decode
	if err := (dec{}).fieldData(b, fd); err != nil {
		if fallbackOnProto2(err) {
			proto.Reset(fd) // discard partial decode before the authoritative pass
			return proto.Unmarshal(b, fd)
		}
		return err
	}
	return nil
}

func (d dec) fieldData(b []byte, fd *schemapb.FieldData) error {
	full := b
	var rest []byte
	for len(b) > 0 {
		start := b
		num, wtype, n := consumeTag(b)
		if n <= 0 {
			return errMalformed
		}
		b = b[n:]
		if isProto2Group(wtype) {
			return errProto2
		}
		// Known field with an unexpected wire type → official codec, to match
		// proto.Unmarshal instead of misdecoding the wrong shape. Field 7 (ValidData)
		// is a packed repeated bool and legitimately accepts varint (unpacked) and
		// bytes (packed); any other wire type falls back like the rest.
		if (num == 1 || num == 5 || num == 6) && wtype != 0 {
			return fallbackUnmarshal(full, fd)
		}
		if (num == 2 || num == 3 || num == 4 || num == 8) && wtype != 2 {
			return fallbackUnmarshal(full, fd)
		}
		if num == 7 && wtype != 0 && wtype != 2 {
			return fallbackUnmarshal(full, fd)
		}
		switch num {
		case 1: // Type (enum, varint)
			v, n := consumeVarint(b)
			if n <= 0 {
				return errMalformed
			}
			fd.Type = schemapb.DataType(v)
			b = b[n:]
		case 2: // FieldName (string)
			v, n := consumeBytes(b)
			if n <= 0 {
				return errMalformed
			}
			s, err := d.str(v)
			if err != nil {
				return err
			}
			fd.FieldName = s
			b = b[n:]
		case 3: // Scalars (oneof, nested ScalarField)
			v, n := consumeBytes(b)
			if n <= 0 {
				return errMalformed
			}
			sf := &schemapb.ScalarField{}
			if err := d.scalarField(v, sf); err != nil {
				return err
			}
			fd.Field = &schemapb.FieldData_Scalars{Scalars: sf}
			b = b[n:]
		case 4: // Vectors (oneof, nested VectorField)
			v, n := consumeBytes(b)
			if n <= 0 {
				return errMalformed
			}
			vf := &schemapb.VectorField{}
			if err := unmarshalVectorField(v, vf); err != nil {
				return err
			}
			fd.Field = &schemapb.FieldData_Vectors{Vectors: vf}
			b = b[n:]
		case 5: // FieldId (varint)
			v, n := consumeVarint(b)
			if n <= 0 {
				return errMalformed
			}
			fd.FieldId = int64(v)
			b = b[n:]
		case 6: // IsDynamic (bool, varint)
			v, n := consumeVarint(b)
			if n <= 0 {
				return errMalformed
			}
			fd.IsDynamic = v != 0
			b = b[n:]
		case 7: // ValidData (repeated bool, packed)
			if wtype == 2 {
				v, n := consumeBytes(b)
				if n <= 0 {
					return errMalformed
				}
				if err := appendPackedBool(v, &fd.ValidData); err != nil {
					return err
				}
				b = b[n:]
			} else {
				v, n := consumeVarint(b)
				if n <= 0 {
					return errMalformed
				}
				fd.ValidData = append(fd.ValidData, v != 0)
				b = b[n:]
			}
		case 8: // StructArrays (oneof) — delegate to official; must stay in this
			// pass so oneof last-wins ordering matches official.
			v, n := consumeBytes(b)
			if n <= 0 {
				return errMalformed
			}
			m := &schemapb.StructArrayField{}
			if err := protoUnmarshal(v, m); err != nil {
				return err
			}
			fd.Field = &schemapb.FieldData_StructArrays{StructArrays: m}
			b = b[n:]
		default: // unhandled (future) non-oneof field → fold into official merge
			n2 := skipField(b, wtype)
			if n2 <= 0 {
				return errMalformed
			}
			rest = append(rest, start[:n+n2]...)
			b = b[n2:]
		}
	}
	if len(rest) > 0 {
		return protoMerge(rest, fd)
	}
	return nil
}

// scalarField decodes a wire-format schemapb.ScalarField into sf.
// Hot array types (bool/int/long/float/double/string/bytes/json) are hand-decoded;
// rarer variants (array/geometry/timestamptz/mol/...) fall back to the official codec.
func (d dec) scalarField(b []byte, sf *schemapb.ScalarField) error {
	var rest []byte
	for len(b) > 0 {
		start := b
		num, wtype, tn := consumeTag(b)
		if tn <= 0 {
			return errMalformed
		}
		b = b[tn:]
		if isProto2Group(wtype) {
			return errProto2
		}
		if wtype != 2 { // members are all length-delimited; anything else is unknown
			sn := skipField(b, wtype)
			if sn <= 0 {
				return errMalformed
			}
			rest = append(rest, start[:tn+sn]...)
			b = b[sn:]
			continue
		}
		v, vn := consumeBytes(b)
		if vn <= 0 {
			return errMalformed
		}
		b = b[vn:]
		switch num {
		case 1: // BoolData
			a := &schemapb.BoolArray{}
			if err := decodePackedBool(v, &a.Data, a); err != nil {
				return err
			}
			sf.Data = &schemapb.ScalarField_BoolData{BoolData: a}
		case 2: // IntData
			a := &schemapb.IntArray{}
			if err := decodePackedI32(v, &a.Data, a); err != nil {
				return err
			}
			sf.Data = &schemapb.ScalarField_IntData{IntData: a}
		case 3: // LongData
			a := &schemapb.LongArray{}
			if err := decodePackedI64(v, &a.Data, a); err != nil {
				return err
			}
			sf.Data = &schemapb.ScalarField_LongData{LongData: a}
		case 4: // FloatData
			a := &schemapb.FloatArray{}
			if err := decodePackedF32(v, &a.Data, a); err != nil {
				return err
			}
			sf.Data = &schemapb.ScalarField_FloatData{FloatData: a}
		case 5: // DoubleData
			a := &schemapb.DoubleArray{}
			if err := decodePackedF64(v, &a.Data, a); err != nil {
				return err
			}
			sf.Data = &schemapb.ScalarField_DoubleData{DoubleData: a}
		case 6: // StringData
			sa := &schemapb.StringArray{}
			if err := d.stringArray(v, sa); err != nil {
				return err
			}
			sf.Data = &schemapb.ScalarField_StringData{StringData: sa}
		case 7: // BytesData
			a := &schemapb.BytesArray{}
			if err := decodeRepeatedBytes(v, &a.Data, a); err != nil {
				return err
			}
			sf.Data = &schemapb.ScalarField_BytesData{BytesData: a}
		case 9: // JsonData
			a := &schemapb.JSONArray{}
			if err := decodeRepeatedBytes(v, &a.Data, a); err != nil {
				return err
			}
			sf.Data = &schemapb.ScalarField_JsonData{JsonData: a}
		default: // known cold oneof variants (array/geometry/mol/...) stay in-pass;
			// truly-unknown fields fold into official merge.
			handled, err := decodeScalarFallback(num, v, sf)
			if err != nil {
				return err
			}
			if !handled {
				rest = append(rest, start[:tn+vn]...)
			}
		}
	}
	if len(rest) > 0 {
		return protoMerge(rest, sf)
	}
	return nil
}

// stringArray decodes a wire-format schemapb.StringArray into sa
// (repeated string data = 1) via a single byte arena (N allocations → 2).
func (d dec) stringArray(b []byte, sa *schemapb.StringArray) error {
	strs, ok, err := d.strings(b)
	if err != nil {
		return err
	}
	if !ok {
		return fallbackUnmarshal(b, sa) // unknown field → official codec, preserves it
	}
	sa.Data = strs
	return nil
}

// unmarshalVectorField decodes a wire-format schemapb.VectorField into vf
// (no string fields, so no UTF-8 validation needed).
func unmarshalVectorField(b []byte, vf *schemapb.VectorField) error {
	full := b
	var rest []byte
	for len(b) > 0 {
		start := b
		num, wtype, n := consumeTag(b)
		if n <= 0 {
			return errMalformed
		}
		b = b[n:]
		if isProto2Group(wtype) {
			return errProto2
		}
		// Known field with an unexpected wire type (Dim=varint; 2..8=length-delimited)
		// → official codec, to match proto.Unmarshal instead of misdecoding it.
		if (num == 1 && wtype != 0) || (num >= 2 && num <= 8 && wtype != 2) {
			return fallbackUnmarshal(full, vf)
		}
		switch num {
		case 1: // Dim (varint)
			v, n := consumeVarint(b)
			if n <= 0 {
				return errMalformed
			}
			vf.Dim = int64(v)
			b = b[n:]
		case 2: // FloatVector (nested FloatArray)
			v, n := consumeBytes(b)
			if n <= 0 {
				return errMalformed
			}
			fa := &schemapb.FloatArray{}
			if err := unmarshalFloatArray(v, fa); err != nil {
				return err
			}
			vf.Data = &schemapb.VectorField_FloatVector{FloatVector: fa}
			b = b[n:]
		case 3: // BinaryVector (bytes)
			v, n := consumeBytes(b)
			if n <= 0 {
				return errMalformed
			}
			vf.Data = &schemapb.VectorField_BinaryVector{BinaryVector: cloneBytes(v)}
			b = b[n:]
		case 4: // Float16Vector (bytes)
			v, n := consumeBytes(b)
			if n <= 0 {
				return errMalformed
			}
			vf.Data = &schemapb.VectorField_Float16Vector{Float16Vector: cloneBytes(v)}
			b = b[n:]
		case 5: // Bfloat16Vector (bytes)
			v, n := consumeBytes(b)
			if n <= 0 {
				return errMalformed
			}
			vf.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: cloneBytes(v)}
			b = b[n:]
		case 6: // SparseFloatVector (nested SparseFloatArray)
			v, n := consumeBytes(b)
			if n <= 0 {
				return errMalformed
			}
			sa := &schemapb.SparseFloatArray{}
			if err := unmarshalSparseFloatArray(v, sa); err != nil {
				return err
			}
			vf.Data = &schemapb.VectorField_SparseFloatVector{SparseFloatVector: sa}
			b = b[n:]
		case 7: // Int8Vector (bytes)
			v, n := consumeBytes(b)
			if n <= 0 {
				return errMalformed
			}
			vf.Data = &schemapb.VectorField_Int8Vector{Int8Vector: cloneBytes(v)}
			b = b[n:]
		case 8: // VectorArray — delegate to official
			v, n := consumeBytes(b)
			if n <= 0 {
				return errMalformed
			}
			va := &schemapb.VectorArray{}
			if err := protoUnmarshal(v, va); err != nil {
				return err
			}
			vf.Data = &schemapb.VectorField_VectorArray{VectorArray: va}
			b = b[n:]
		default: // unhandled (future) field → fold into official merge
			n2 := skipField(b, wtype)
			if n2 <= 0 {
				return errMalformed
			}
			rest = append(rest, start[:n+n2]...)
			b = b[n2:]
		}
	}
	if len(rest) > 0 {
		return protoMerge(rest, vf)
	}
	return nil
}

// unmarshalSparseFloatArray: repeated bytes contents = 1; int64 dim = 2;
func unmarshalSparseFloatArray(b []byte, sa *schemapb.SparseFloatArray) error {
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
		// A known field with an unexpected wire type is not what proto3 produces;
		// hand the whole message to the official codec so the outcome matches
		// proto.Unmarshal exactly instead of blindly decoding the wrong shape.
		if (num == 1 && wtype != 2) || (num == 2 && wtype != 0) {
			return fallbackUnmarshal(full, sa)
		}
		switch num {
		case 1:
			v, n := consumeBytes(b)
			if n <= 0 {
				return errMalformed
			}
			sa.Contents = append(sa.Contents, cloneBytes(v))
			b = b[n:]
		case 2:
			v, n := consumeVarint(b)
			if n <= 0 {
				return errMalformed
			}
			sa.Dim = int64(v)
			b = b[n:]
		default:
			return fallbackUnmarshal(full, sa) // unknown field → official, preserves it
		}
	}
	return nil
}

// unmarshalFloatArray decodes a wire-format schemapb.FloatArray into fa
// (repeated float data = 1, packed fixed32).
func unmarshalFloatArray(b []byte, fa *schemapb.FloatArray) error {
	return decodePackedF32(b, &fa.Data, fa)
}

// --- wire primitives ---

func consumeVarint(b []byte) (uint64, int) {
	var x uint64
	var s uint
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c < 0x80 {
			if i > 9 || (i == 9 && c > 1) {
				return 0, -1 // overflow
			}
			return x | uint64(c)<<s, i + 1
		}
		x |= uint64(c&0x7f) << s
		s += 7
	}
	return 0, 0 // truncated
}

func consumeTag(b []byte) (num int, wtype int, n int) {
	v, n := consumeVarint(b)
	if n <= 0 {
		return 0, 0, n
	}
	return int(v >> 3), int(v & 7), n
}

func consumeBytes(b []byte) ([]byte, int) {
	l, n := consumeVarint(b)
	if n <= 0 {
		return nil, n
	}
	if uint64(len(b)-n) < l {
		return nil, 0 // truncated
	}
	return b[n : n+int(l)], n + int(l)
}

func skipField(b []byte, wtype int) int {
	switch wtype {
	case 0: // varint
		_, n := consumeVarint(b)
		return n
	case 1: // fixed64
		if len(b) < 8 {
			return 0
		}
		return 8
	case 2: // length-delimited
		l, n := consumeVarint(b)
		if n <= 0 {
			return n
		}
		if uint64(len(b)-n) < l {
			return 0
		}
		return n + int(l)
	case 5: // fixed32
		if len(b) < 4 {
			return 0
		}
		return 4
	default:
		return -1
	}
}

// little-endian fixed readers (used by packed vector/scalar decode single paths)
func le32(b []byte) uint32 { return binary.LittleEndian.Uint32(b) }
func le64(b []byte) uint64 { return binary.LittleEndian.Uint64(b) }
