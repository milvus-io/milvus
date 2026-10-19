package fastpb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	milvuspb "github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// TestWireTypeMismatchFallback: a known field carrying an unexpected wire type is
// not something proto3 produces (only malformed / adversarial encoders do). fastpb
// must hand such a message to the official codec and produce the exact same result
// (and the same error behavior) as proto.Unmarshal, rather than blindly decoding
// the wrong shape.
func TestWireTypeMismatchFallback(t *testing.T) {
	// FieldData with field 5 (FieldId, a varint field) wrongly length-delimited,
	// and field 1 (Type, varint) wrongly length-delimited.
	t.Run("FieldData varint field as length-delimited", func(t *testing.T) {
		var fd []byte
		fd = protowire.AppendTag(fd, 2, protowire.BytesType) // FieldName (legit)
		fd = protowire.AppendString(fd, "f")
		fd = protowire.AppendTag(fd, 5, protowire.BytesType) // FieldId: should be varint(0)
		fd = protowire.AppendBytes(fd, []byte{0x01, 0x02})

		want := &schemapb.FieldData{}
		wantErr := proto.Unmarshal(fd, want)
		got := &schemapb.FieldData{}
		gotErr := UnmarshalFieldData(fd, got)

		require.Equal(t, wantErr == nil, gotErr == nil, "error parity vs official (want=%v got=%v)", wantErr, gotErr)
		if wantErr == nil {
			assert.True(t, proto.Equal(got, want), "fallback must equal official decode")
		}
	})

	// VectorField with field 1 (Dim, varint) wrongly length-delimited.
	t.Run("VectorField Dim as length-delimited", func(t *testing.T) {
		var vf []byte
		vf = protowire.AppendTag(vf, 1, protowire.BytesType) // Dim: should be varint(0)
		vf = protowire.AppendBytes(vf, []byte{0x07})

		want := &schemapb.VectorField{}
		wantErr := proto.Unmarshal(vf, want)
		got := &schemapb.VectorField{}
		gotErr := unmarshalVectorField(vf, got)

		require.Equal(t, wantErr == nil, gotErr == nil, "error parity (want=%v got=%v)", wantErr, gotErr)
		if wantErr == nil {
			assert.True(t, proto.Equal(got, want))
		}
	})

	// SparseFloatArray with field 2 (dim, varint) wrongly length-delimited.
	t.Run("SparseFloatArray dim as length-delimited", func(t *testing.T) {
		var sa []byte
		sa = protowire.AppendTag(sa, 2, protowire.BytesType) // dim: should be varint(0)
		sa = protowire.AppendBytes(sa, []byte{0x05})

		want := &schemapb.SparseFloatArray{}
		wantErr := proto.Unmarshal(sa, want)
		got := &schemapb.SparseFloatArray{}
		gotErr := unmarshalSparseFloatArray(sa, got)

		require.Equal(t, wantErr == nil, gotErr == nil, "error parity (want=%v got=%v)", wantErr, gotErr)
		if wantErr == nil {
			assert.True(t, proto.Equal(got, want))
		}
	})

	// Same mismatch nested inside an InsertRequest's fields_data (exercises the
	// untrusted ingress entry point + nested propagation).
	t.Run("InsertRequest with mismatched nested FieldData", func(t *testing.T) {
		var fd []byte
		fd = protowire.AppendTag(fd, 1, protowire.BytesType) // Type: should be varint(0)
		fd = protowire.AppendBytes(fd, []byte{0x09})

		var wire []byte
		wire = protowire.AppendTag(wire, 3, protowire.BytesType) // collection_name
		wire = protowire.AppendString(wire, "c")
		wire = protowire.AppendTag(wire, 5, protowire.BytesType) // fields_data
		wire = protowire.AppendBytes(wire, fd)

		want := &milvuspb.InsertRequest{}
		wantErr := proto.Unmarshal(wire, want)
		got := &milvuspb.InsertRequest{}
		gotErr := UnmarshalInsertRequest(wire, got)

		require.Equal(t, wantErr == nil, gotErr == nil, "error parity (want=%v got=%v)", wantErr, gotErr)
		if wantErr == nil {
			assert.True(t, proto.Equal(got, want))
		}
	})
}

// TestVarintArrayWireTypeMismatch: field 1 of a varint-family repeated field
// (LongArray/IntArray/BoolArray) encoded with a fixed wire type is not valid
// proto3 output — only a malformed or adversarial encoder produces it. The
// varint decoders must hand such messages to the official codec (which treats
// the mismatched field as unknown) instead of consuming the fixed-width bytes
// as a varint and silently decoding garbage values.
func TestVarintArrayWireTypeMismatch(t *testing.T) {
	t.Run("LongArray data as fixed64", func(t *testing.T) {
		var w []byte
		w = protowire.AppendTag(w, 1, protowire.Fixed64Type)
		w = protowire.AppendFixed64(w, 42)

		want := &schemapb.LongArray{}
		wantErr := proto.Unmarshal(w, want)
		got := &schemapb.LongArray{}
		gotErr := decodePackedI64(w, &got.Data, got)

		require.Equal(t, wantErr == nil, gotErr == nil, "error parity vs official (want=%v got=%v)", wantErr, gotErr)
		if wantErr == nil {
			assert.True(t, proto.Equal(got, want), "mismatched wire type must not decode as data")
		}
	})

	t.Run("IntArray data as fixed32", func(t *testing.T) {
		var w []byte
		w = protowire.AppendTag(w, 1, protowire.Fixed32Type)
		w = protowire.AppendFixed32(w, 7)

		want := &schemapb.IntArray{}
		wantErr := proto.Unmarshal(w, want)
		got := &schemapb.IntArray{}
		gotErr := decodePackedI32(w, &got.Data, got)

		require.Equal(t, wantErr == nil, gotErr == nil, "error parity vs official (want=%v got=%v)", wantErr, gotErr)
		if wantErr == nil {
			assert.True(t, proto.Equal(got, want), "mismatched wire type must not decode as data")
		}
	})

	t.Run("BoolArray data as fixed64", func(t *testing.T) {
		var w []byte
		w = protowire.AppendTag(w, 1, protowire.Fixed64Type)
		w = protowire.AppendFixed64(w, 1)

		want := &schemapb.BoolArray{}
		wantErr := proto.Unmarshal(w, want)
		got := &schemapb.BoolArray{}
		gotErr := decodePackedBool(w, &got.Data, got)

		require.Equal(t, wantErr == nil, gotErr == nil, "error parity vs official (want=%v got=%v)", wantErr, gotErr)
		if wantErr == nil {
			assert.True(t, proto.Equal(got, want), "mismatched wire type must not decode as data")
		}
	})

	// The adversarial shape: an alternating fixed32-tag/byte pattern in which
	// every byte re-parses as a valid field-1 tag or varint. The pre-fix decoders
	// consumed this WITHOUT error as [8,8,8,8] — silently diverging from the
	// official codec, which consumes the fixed32 payload into unknown fields and
	// decodes a different, shorter array. This is the case the wire-type check
	// exists for; the simple vectors above happen to be rescued by the num!=1
	// fallback, this one is not.
	t.Run("LongArray alternating fixed32 tags decode divergence", func(t *testing.T) {
		w := []byte{0x0D, 0x08, 0x0D, 0x08, 0x0D, 0x08, 0x0D, 0x08} // tag(1,fixed32) interleaved

		want := &schemapb.LongArray{}
		wantErr := proto.Unmarshal(w, want)
		got := &schemapb.LongArray{}
		gotErr := decodePackedI64(w, &got.Data, got)

		require.Equal(t, wantErr == nil, gotErr == nil, "error parity vs official (want=%v got=%v)", wantErr, gotErr)
		if wantErr == nil {
			assert.True(t, proto.Equal(got, want),
				"want %v got %v: mismatched wire type must fall back, not decode as varints", want.Data, got.Data)
		}
	})

	t.Run("IntArray alternating fixed32 tags decode divergence", func(t *testing.T) {
		w := []byte{0x0D, 0x08, 0x0D, 0x08, 0x0D, 0x08, 0x0D, 0x08}

		want := &schemapb.IntArray{}
		wantErr := proto.Unmarshal(w, want)
		got := &schemapb.IntArray{}
		gotErr := decodePackedI32(w, &got.Data, got)

		require.Equal(t, wantErr == nil, gotErr == nil, "error parity vs official (want=%v got=%v)", wantErr, gotErr)
		if wantErr == nil {
			assert.True(t, proto.Equal(got, want),
				"want %v got %v: mismatched wire type must fall back, not decode as varints", want.Data, got.Data)
		}
	})

	t.Run("BoolArray alternating fixed32 tags decode divergence", func(t *testing.T) {
		w := []byte{0x0D, 0x08, 0x0D, 0x08, 0x0D, 0x08, 0x0D, 0x08}

		want := &schemapb.BoolArray{}
		wantErr := proto.Unmarshal(w, want)
		got := &schemapb.BoolArray{}
		gotErr := decodePackedBool(w, &got.Data, got)

		require.Equal(t, wantErr == nil, gotErr == nil, "error parity vs official (want=%v got=%v)", wantErr, gotErr)
		if wantErr == nil {
			assert.True(t, proto.Equal(got, want),
				"want %v got %v: mismatched wire type must fall back, not decode as varints", want.Data, got.Data)
		}
	})

	// Mixed: a legitimate packed chunk followed by a mismatched wire type on the
	// same field — the whole message must fall back, keeping parity with the
	// official codec rather than keeping the half-decoded prefix.
	t.Run("LongArray packed then fixed64", func(t *testing.T) {
		var packed []byte
		packed = protowire.AppendVarint(packed, 1)
		packed = protowire.AppendVarint(packed, 2)
		var w []byte
		w = protowire.AppendTag(w, 1, protowire.BytesType)
		w = protowire.AppendBytes(w, packed)
		w = protowire.AppendTag(w, 1, protowire.Fixed64Type)
		w = protowire.AppendFixed64(w, 42)

		want := &schemapb.LongArray{}
		wantErr := proto.Unmarshal(w, want)
		got := &schemapb.LongArray{}
		gotErr := decodePackedI64(w, &got.Data, got)

		require.Equal(t, wantErr == nil, gotErr == nil, "error parity vs official (want=%v got=%v)", wantErr, gotErr)
		if wantErr == nil {
			assert.True(t, proto.Equal(got, want), "fallback must equal official decode")
		}
	})
}
