package fastpb

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// --- tiny wire builders (each comment says which wire rule the bytes violate) ---

// wtag emits just a field tag (no value) — a value that should follow is missing.
func wtag(num int32, wt protowire.Type) []byte {
	return protowire.AppendTag(nil, protowire.Number(num), wt)
}

// wfield emits a well-formed length-delimited field wrapping payload.
func wfield(num int32, payload []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, protowire.Number(num), protowire.BytesType), payload)
}

func cat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func f32le(vals ...float32) []byte {
	var b []byte
	for _, v := range vals {
		b = protowire.AppendFixed32(b, math.Float32bits(v))
	}
	return b
}

func f64le(vals ...float64) []byte {
	var b []byte
	for _, v := range vals {
		b = protowire.AppendFixed64(b, math.Float64bits(v))
	}
	return b
}

// truncTag is a varint (tag or value) whose continuation bit never terminates.
var truncTag = []byte{0x80}

// TestMalformedWirePerField drives every decoder over hand-crafted wire bytes
// that violate one specific wire rule each (truncated varints, truncated
// length prefixes, length prefixes overrunning the buffer, bad packed payload
// sizes, invalid wire types, malformed delegated submessages). Every case is a
// differential assertion: fastpb must match the official codec both in error
// behavior and, when the official codec accepts (wire-type-mismatch fallbacks,
// unpacked encodings of packed fields), in the decoded message.
func TestMalformedWirePerField(t *testing.T) {
	cases := []struct {
		name  string
		wire  []byte
		fresh func() proto.Message
		fast  func([]byte, proto.Message) error
	}{
		// --- FieldData: one truncation per field ---
		{"FieldData/type-truncated-varint", cat(wtag(1, protowire.VarintType), truncTag), newFieldData, decFieldData},
		{"FieldData/fieldname-truncated-len-prefix", cat(wtag(2, protowire.BytesType), truncTag), newFieldData, decFieldData},
		{"FieldData/fieldname-as-varint-fallback", protowire.AppendVarint(wtag(2, protowire.VarintType), 5), newFieldData, decFieldData},
		{"FieldData/scalars-len-overruns-buffer", cat(wtag(3, protowire.BytesType), []byte{0x05, 0x01}), newFieldData, decFieldData},
		{"FieldData/scalars-as-varint-fallback", protowire.AppendVarint(wtag(3, protowire.VarintType), 5), newFieldData, decFieldData},
		{"FieldData/vectors-len-overruns-buffer", cat(wtag(4, protowire.BytesType), []byte{0x05}), newFieldData, decFieldData},
		{"FieldData/vectors-malformed-submsg", wfield(4, truncTag), newFieldData, decFieldData},
		{"FieldData/fieldid-truncated-varint", cat(wtag(5, protowire.VarintType), truncTag), newFieldData, decFieldData},
		{"FieldData/isdynamic-truncated-varint", cat(wtag(6, protowire.VarintType), truncTag), newFieldData, decFieldData},
		{"FieldData/validdata-truncated-len-prefix", cat(wtag(7, protowire.BytesType), truncTag), newFieldData, decFieldData},
		{"FieldData/validdata-truncated-packed-varint", wfield(7, truncTag), newFieldData, decFieldData},
		{"FieldData/validdata-single-varint-ok", protowire.AppendVarint(wtag(7, protowire.VarintType), 1), newFieldData, decFieldData},
		{"FieldData/validdata-single-varint-truncated", cat(wtag(7, protowire.VarintType), truncTag), newFieldData, decFieldData},
		{"FieldData/structarrays-len-overruns-buffer", cat(wtag(8, protowire.BytesType), []byte{0x05}), newFieldData, decFieldData},
		{"FieldData/structarrays-malformed-submsg", wfield(8, truncTag), newFieldData, decFieldData},
		{"FieldData/unknown-truncated-varint", cat(wtag(99, protowire.VarintType), truncTag), newFieldData, decFieldData},
		{"FieldData/unknown-fixed64-short", cat(wtag(99, protowire.Fixed64Type), []byte{1, 2, 3, 4}), newFieldData, decFieldData},
		{"FieldData/unknown-fixed32-short", cat(wtag(99, protowire.Fixed32Type), []byte{1, 2}), newFieldData, decFieldData},
		{"FieldData/unknown-lendelim-truncated-len", cat(wtag(99, protowire.BytesType), truncTag), newFieldData, decFieldData},
		{"FieldData/unknown-lendelim-overruns-buffer", cat(wtag(99, protowire.BytesType), []byte{0x05, 0x01}), newFieldData, decFieldData},
		{"FieldData/invalid-wire-type-6", wtag(99, protowire.Type(6)), newFieldData, decFieldData},

		// --- ScalarField: per-variant malformed payloads (leaf array decoders) ---
		{"Scalar/truncated-tag", truncTag, newScalarField, decScalarField},
		{"Scalar/unknown-varint-truncated", cat(wtag(20, protowire.VarintType), truncTag), newScalarField, decScalarField},
		{"Scalar/member-len-overruns-buffer", cat(wtag(1, protowire.BytesType), []byte{0x05}), newScalarField, decScalarField},
		// BoolArray (field 1): every decodePackedBool branch
		{"Scalar/bool-truncated-inner-tag", wfield(1, truncTag), newScalarField, decScalarField},
		{"Scalar/bool-truncated-packed-len", wfield(1, cat(wtag(1, protowire.BytesType), truncTag)), newScalarField, decScalarField},
		{"Scalar/bool-truncated-packed-varint", wfield(1, wfield(1, truncTag)), newScalarField, decScalarField},
		{"Scalar/bool-unpacked-varint-ok", wfield(1, protowire.AppendVarint(wtag(1, protowire.VarintType), 1)), newScalarField, decScalarField},
		{"Scalar/bool-unpacked-varint-truncated", wfield(1, wtag(1, protowire.VarintType)), newScalarField, decScalarField},
		// IntArray (field 2): decodePackedI32 branches
		{"Scalar/int-truncated-inner-tag", wfield(2, truncTag), newScalarField, decScalarField},
		{"Scalar/int-truncated-packed-len", wfield(2, cat(wtag(1, protowire.BytesType), truncTag)), newScalarField, decScalarField},
		{"Scalar/int-truncated-packed-varint", wfield(2, wfield(1, truncTag)), newScalarField, decScalarField},
		{"Scalar/int-unpacked-varint-ok", wfield(2, protowire.AppendVarint(wtag(1, protowire.VarintType), 7)), newScalarField, decScalarField},
		{"Scalar/int-unpacked-varint-truncated", wfield(2, wtag(1, protowire.VarintType)), newScalarField, decScalarField},
		// LongArray (field 3): decodePackedI64 branches
		{"Scalar/long-truncated-inner-tag", wfield(3, truncTag), newScalarField, decScalarField},
		{"Scalar/long-truncated-packed-len", wfield(3, cat(wtag(1, protowire.BytesType), truncTag)), newScalarField, decScalarField},
		{"Scalar/long-truncated-packed-varint", wfield(3, wfield(1, truncTag)), newScalarField, decScalarField},
		{"Scalar/long-unpacked-varint-ok", wfield(3, protowire.AppendVarint(wtag(1, protowire.VarintType), 9)), newScalarField, decScalarField},
		{"Scalar/long-unpacked-varint-truncated", wfield(3, wtag(1, protowire.VarintType)), newScalarField, decScalarField},
		// FloatArray (field 4): decodePackedF32 branches
		{"Scalar/float-truncated-inner-tag", wfield(4, truncTag), newScalarField, decScalarField},
		{"Scalar/float-truncated-packed-len", wfield(4, cat(wtag(1, protowire.BytesType), truncTag)), newScalarField, decScalarField},
		{"Scalar/float-packed-len-not-multiple-of-4", wfield(4, wfield(1, []byte{1, 2, 3})), newScalarField, decScalarField},
		{"Scalar/float-two-packed-chunks-ok", wfield(4, cat(wfield(1, f32le(1.5, 2.5)), wfield(1, f32le(3.5)))), newScalarField, decScalarField},
		{"Scalar/float-single-fixed32-short", wfield(4, cat(wtag(1, protowire.Fixed32Type), []byte{1, 2})), newScalarField, decScalarField},
		{"Scalar/float-data-as-varint-fallback", wfield(4, protowire.AppendVarint(wtag(1, protowire.VarintType), 7)), newScalarField, decScalarField},
		// DoubleArray (field 5): decodePackedF64 branches
		{"Scalar/double-truncated-inner-tag", wfield(5, truncTag), newScalarField, decScalarField},
		{"Scalar/double-truncated-packed-len", wfield(5, cat(wtag(1, protowire.BytesType), truncTag)), newScalarField, decScalarField},
		{"Scalar/double-packed-len-not-multiple-of-8", wfield(5, wfield(1, []byte{1, 2, 3})), newScalarField, decScalarField},
		{"Scalar/double-two-packed-chunks-ok", wfield(5, cat(wfield(1, f64le(1.5)), wfield(1, f64le(2.5)))), newScalarField, decScalarField},
		{"Scalar/double-single-fixed64-short", wfield(5, cat(wtag(1, protowire.Fixed64Type), []byte{1, 2})), newScalarField, decScalarField},
		{"Scalar/double-data-as-varint-fallback", wfield(5, protowire.AppendVarint(wtag(1, protowire.VarintType), 7)), newScalarField, decScalarField},
		// StringArray (field 6): strings arena pass-1 error branches
		{"Scalar/string-truncated-inner-tag", wfield(6, truncTag), newScalarField, decScalarField},
		{"Scalar/string-truncated-len-prefix", wfield(6, cat(wtag(1, protowire.BytesType), truncTag)), newScalarField, decScalarField},
		// BytesArray (field 7) / JSONArray (field 9): decodeRepeatedBytes branches
		{"Scalar/bytes-truncated-inner-tag", wfield(7, truncTag), newScalarField, decScalarField},
		{"Scalar/bytes-truncated-len-prefix", wfield(7, cat(wtag(1, protowire.BytesType), truncTag)), newScalarField, decScalarField},
		{"Scalar/json-truncated-inner-tag", wfield(9, truncTag), newScalarField, decScalarField},
		// cold oneof variants (decodeScalarFallback 8/10/11/12/13/14): malformed
		// submessage bytes must surface the official codec's decode error
		{"Scalar/array-data-malformed", wfield(8, truncTag), newScalarField, decScalarField},
		{"Scalar/geometry-data-malformed", wfield(10, truncTag), newScalarField, decScalarField},
		{"Scalar/timestamptz-data-malformed", wfield(11, truncTag), newScalarField, decScalarField},
		{"Scalar/geometry-wkt-data-malformed", wfield(12, truncTag), newScalarField, decScalarField},
		{"Scalar/mol-data-malformed", wfield(13, truncTag), newScalarField, decScalarField},
		{"Scalar/mol-smiles-data-malformed", wfield(14, truncTag), newScalarField, decScalarField},

		// --- VectorField: one malformed case per field ---
		{"Vector/truncated-tag", truncTag, newVectorField, decVectorField},
		{"Vector/dim-truncated-varint", cat(wtag(1, protowire.VarintType), truncTag), newVectorField, decVectorField},
		{"Vector/floatvector-len-overruns-buffer", cat(wtag(2, protowire.BytesType), []byte{0x05}), newVectorField, decVectorField},
		{"Vector/floatvector-malformed", wfield(2, truncTag), newVectorField, decVectorField},
		{"Vector/binary-truncated-len-prefix", cat(wtag(3, protowire.BytesType), truncTag), newVectorField, decVectorField},
		{"Vector/float16-len-overruns-buffer", cat(wtag(4, protowire.BytesType), []byte{0x05}), newVectorField, decVectorField},
		{"Vector/bfloat16-len-overruns-buffer", cat(wtag(5, protowire.BytesType), []byte{0x05}), newVectorField, decVectorField},
		{"Vector/sparse-len-overruns-buffer", cat(wtag(6, protowire.BytesType), []byte{0x05}), newVectorField, decVectorField},
		{"Vector/sparse-truncated-inner-tag", wfield(6, truncTag), newVectorField, decVectorField},
		{"Vector/sparse-contents-truncated-len", wfield(6, cat(wtag(1, protowire.BytesType), truncTag)), newVectorField, decVectorField},
		{"Vector/sparse-dim-truncated-varint", wfield(6, wtag(2, protowire.VarintType)), newVectorField, decVectorField},
		{"Vector/int8-truncated-len-prefix", cat(wtag(7, protowire.BytesType), truncTag), newVectorField, decVectorField},
		{"Vector/vectorarray-len-overruns-buffer", cat(wtag(8, protowire.BytesType), []byte{0x05}), newVectorField, decVectorField},
		{"Vector/vectorarray-malformed-submsg", wfield(8, truncTag), newVectorField, decVectorField},
		{"Vector/unknown-truncated-varint", cat(wtag(20, protowire.VarintType), truncTag), newVectorField, decVectorField},

		// --- IDs ---
		{"IDs/truncated-tag", truncTag, newIDs, decIDs},
		{"IDs/unknown-varint-truncated", cat(wtag(9, protowire.VarintType), truncTag), newIDs, decIDs},
		{"IDs/member-len-overruns-buffer", cat(wtag(1, protowire.BytesType), []byte{0x05}), newIDs, decIDs},
		{"IDs/intid-malformed", wfield(1, truncTag), newIDs, decIDs},
		{"IDs/strid-malformed", wfield(2, truncTag), newIDs, decIDs},

		// --- SearchResultData: error propagation per field ---
		{"SRD/numqueries-truncated-varint", cat(wtag(1, protowire.VarintType), truncTag), newSearchResult, decSearchResult},
		{"SRD/unknown-fixed64-short", cat(wtag(99, protowire.Fixed64Type), []byte{1, 2}), newSearchResult, decSearchResult},
		{"SRD/fieldsdata-malformed", wfield(3, truncTag), newSearchResult, decSearchResult},
		{"SRD/scores-len-not-multiple-of-4", wfield(4, []byte{1, 2, 3}), newSearchResult, decSearchResult},
		{"SRD/scores-two-packed-chunks-ok", cat(wfield(4, f32le(0.5)), wfield(4, f32le(1.5, 2.5))), newSearchResult, decSearchResult},
		{"SRD/ids-malformed", wfield(5, truncTag), newSearchResult, decSearchResult},
		{"SRD/topks-truncated-packed-varint", wfield(6, truncTag), newSearchResult, decSearchResult},
		{"SRD/groupby-malformed", wfield(8, truncTag), newSearchResult, decSearchResult},
		{"SRD/distances-len-not-multiple-of-4", wfield(10, []byte{1, 2, 3}), newSearchResult, decSearchResult},
		{"SRD/iterator-malformed", wfield(11, truncTag), newSearchResult, decSearchResult},
		{"SRD/recalls-len-not-multiple-of-4", wfield(12, []byte{1, 2, 3}), newSearchResult, decSearchResult},
		{"SRD/highlight-malformed", wfield(14, truncTag), newSearchResult, decSearchResult},
		{"SRD/elementindices-malformed", wfield(15, truncTag), newSearchResult, decSearchResult},
		{"SRD/groupbyvalues-malformed", wfield(17, truncTag), newSearchResult, decSearchResult},
		{"SRD/aggbuckets-malformed", wfield(18, truncTag), newSearchResult, decSearchResult},
		{"SRD/aggtopks-truncated-packed-varint", wfield(19, truncTag), newSearchResult, decSearchResult},

		// --- RetrieveResults: error propagation per field ---
		{"RR/reqid-truncated-varint", cat(wtag(3, protowire.VarintType), truncTag), newRetrieve, decRetrieve},
		{"RR/unknown-fixed32-short", cat(wtag(99, protowire.Fixed32Type), []byte{1}), newRetrieve, decRetrieve},
		{"RR/base-malformed", wfield(1, truncTag), newRetrieve, decRetrieve},
		{"RR/status-malformed", wfield(2, truncTag), newRetrieve, decRetrieve},
		{"RR/ids-malformed", wfield(4, truncTag), newRetrieve, decRetrieve},
		{"RR/fieldsdata-malformed", wfield(5, truncTag), newRetrieve, decRetrieve},
		{"RR/sealedsegids-truncated-packed-varint", wfield(6, truncTag), newRetrieve, decRetrieve},
		{"RR/globalsegids-truncated-packed-varint", wfield(8, truncTag), newRetrieve, decRetrieve},
		{"RR/cost-malformed", wfield(13, truncTag), newRetrieve, decRetrieve},
		{"RR/elementindices-malformed", wfield(19, truncTag), newRetrieve, decRetrieve},

		// --- InsertRequest: error propagation per field ---
		{"IR/numrows-truncated-varint", cat(wtag(7, protowire.VarintType), truncTag), newInsertRequest, decInsertRequest},
		{"IR/unknown-fixed64-short", cat(wtag(99, protowire.Fixed64Type), []byte{1, 2, 3}), newInsertRequest, decInsertRequest},
		{"IR/base-malformed", wfield(1, truncTag), newInsertRequest, decInsertRequest},
		{"IR/fieldsdata-malformed", wfield(5, truncTag), newInsertRequest, decInsertRequest},
		{"IR/hashkeys-truncated-packed-varint", wfield(6, truncTag), newInsertRequest, decInsertRequest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			diffDecode(t, c.wire, c.fresh, c.fast)
		})
	}
}

// TestNestedGroupFallbacks places a well-formed proto2 group (wire types 3/4,
// which proto3 never emits) inside every nested decoder that has its own
// errProto2 check. The error must propagate to the public entry point, which
// redoes the decode with the official codec — so the result must equal
// proto.Unmarshal exactly (the group is preserved as an unknown field).
func TestNestedGroupFallbacks(t *testing.T) {
	canonFD, err := proto.Marshal(&schemapb.FieldData{FieldName: "f", FieldId: 3})
	require.NoError(t, err)

	cases := []struct {
		name  string
		wire  []byte
		fresh func() proto.Message
		fast  func([]byte, proto.Message) error
	}{
		{"FieldData/top-level", appendGroup(append([]byte{}, canonFD...), 999), newFieldData, decFieldData},
		{"FieldData/in-scalarfield", wfield(3, appendGroup(nil, 20)), newFieldData, decFieldData},
		{"FieldData/in-boolarray", wfield(3, wfield(1, appendGroup(nil, 15))), newFieldData, decFieldData},
		{"FieldData/in-intarray", wfield(3, wfield(2, appendGroup(nil, 15))), newFieldData, decFieldData},
		{"FieldData/in-longarray", wfield(3, wfield(3, appendGroup(nil, 15))), newFieldData, decFieldData},
		{"FieldData/in-floatarray", wfield(3, wfield(4, appendGroup(nil, 15))), newFieldData, decFieldData},
		{"FieldData/in-doublearray", wfield(3, wfield(5, appendGroup(nil, 15))), newFieldData, decFieldData},
		{"FieldData/in-vectorfield", wfield(4, appendGroup(nil, 20)), newFieldData, decFieldData},
		{"FieldData/in-sparsefloatarray", wfield(4, wfield(6, appendGroup(nil, 9))), newFieldData, decFieldData},
		{"SearchResultData/in-ids", wfield(5, appendGroup(nil, 9)), newSearchResult, decSearchResult},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			diffDecode(t, c.wire, c.fresh, c.fast)
		})
	}
}

// TestUTF8ErrorBranches covers the remaining invalid-UTF-8 rejection branches.
// InsertRequest is untrusted ingress, so official proto3 behavior (reject) must
// be matched field by field.
func TestUTF8ErrorBranches(t *testing.T) {
	bad := []byte{0xff, 0xfe} // not a valid UTF-8 sequence
	cases := map[string][]byte{
		"collection-name":       wfield(3, bad),
		"partition-name":        wfield(4, bad),
		"namespace":             wfield(9, bad),
		"nested-fielddata-name": wfield(5, wfield(2, bad)), // fields_data → FieldData.field_name
	}
	for name, wire := range cases {
		t.Run("InsertRequest/"+name, func(t *testing.T) {
			diffDecode(t, wire, newInsertRequest, decInsertRequest)
		})
	}

	// The internal result decoders run with utf8=false via the public entry
	// points; exercise their validation branches directly with a utf8 dec so
	// the code stays correct if a validated entry point is ever added.
	t.Run("searchResultData/output-fields", func(t *testing.T) {
		srd := &schemapb.SearchResultData{}
		err := dec{utf8: true}.searchResultData(wfield(7, bad), srd)
		require.ErrorIs(t, err, errInvalidUTF8)
	})
	t.Run("searchResultData/primary-field-name", func(t *testing.T) {
		srd := &schemapb.SearchResultData{}
		err := dec{utf8: true}.searchResultData(wfield(13, bad), srd)
		require.ErrorIs(t, err, errInvalidUTF8)
	})
	t.Run("retrieveResults/channel-ids", func(t *testing.T) {
		rr := &internalpb.RetrieveResults{}
		err := dec{utf8: true}.retrieveResults(wfield(7, bad), rr)
		require.ErrorIs(t, err, errInvalidUTF8)
	})
}

// TestTrustedDecodersSkipUTF8Validation pins the intended divergence from the
// official codec: internal/trusted result decoders do NOT validate UTF-8 (the
// bytes were produced by Milvus itself), while official proto3 rejects them.
func TestTrustedDecodersSkipUTF8Validation(t *testing.T) {
	bad := []byte{0xff}

	srdWire := wfield(7, bad) // output_fields
	require.Error(t, proto.Unmarshal(srdWire, &schemapb.SearchResultData{}), "official proto3 rejects invalid UTF-8")
	srd := &schemapb.SearchResultData{}
	require.NoError(t, UnmarshalSearchResultData(srdWire, srd), "trusted fastpb path skips validation by design")
	require.Equal(t, []string{"\xff"}, srd.OutputFields)

	rrWire := wfield(7, bad) // channelIDs_retrieved
	require.Error(t, proto.Unmarshal(rrWire, &internalpb.RetrieveResults{}))
	rr := &internalpb.RetrieveResults{}
	require.NoError(t, UnmarshalRetrieveResults(rrWire, rr))
	require.Equal(t, []string{"\xff"}, rr.ChannelIDsRetrieved)
}
