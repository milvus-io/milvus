package fastpb

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	milvuspb "github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// --- wire helpers: append a single unknown (future) field of each wire type ---

func appendUnknownFixed64(b []byte, num int32) []byte {
	b = protowire.AppendTag(b, protowire.Number(num), protowire.Fixed64Type)
	return protowire.AppendFixed64(b, 0xDEADBEEFCAFEF00D)
}

func appendUnknownFixed32(b []byte, num int32) []byte {
	b = protowire.AppendTag(b, protowire.Number(num), protowire.Fixed32Type)
	return protowire.AppendFixed32(b, 0xCAFEF00D)
}

func appendUnknownLenDelim(b []byte, num int32) []byte {
	b = protowire.AppendTag(b, protowire.Number(num), protowire.BytesType)
	return protowire.AppendBytes(b, []byte("future-field-bytes"))
}

func appendUnknownVarint(b []byte, num int32) []byte {
	b = protowire.AppendTag(b, protowire.Number(num), protowire.VarintType)
	return protowire.AppendVarint(b, 0x7FFFFFFF)
}

// diffDecode asserts the fastpb decoder matches the official codec for wire,
// both in error behavior and (on success) in the decoded message.
func diffDecode(t *testing.T, wire []byte, fresh func() proto.Message, fast func([]byte, proto.Message) error) {
	t.Helper()
	want := fresh()
	wantErr := proto.Unmarshal(wire, want)
	got := fresh()
	gotErr := fast(wire, got)
	require.Equal(t, wantErr == nil, gotErr == nil, "error parity vs official (want=%v got=%v)", wantErr, gotErr)
	if wantErr == nil {
		require.True(t, proto.Equal(want, got), "mismatch:\n want=%v\n got=%v", want, got)
	}
}

// decoder adapters so each entry point shares diffDecode.
var (
	decFieldData    = func(b []byte, m proto.Message) error { return UnmarshalFieldData(b, m.(*schemapb.FieldData)) }
	decScalarField  = func(b []byte, m proto.Message) error { return dec{}.scalarField(b, m.(*schemapb.ScalarField)) }
	decVectorField  = func(b []byte, m proto.Message) error { return unmarshalVectorField(b, m.(*schemapb.VectorField)) }
	decIDs          = func(b []byte, m proto.Message) error { return dec{}.ids(b, m.(*schemapb.IDs)) }
	decSearchResult = func(b []byte, m proto.Message) error {
		return UnmarshalSearchResultData(b, m.(*schemapb.SearchResultData))
	}
	decRetrieve = func(b []byte, m proto.Message) error {
		return UnmarshalRetrieveResults(b, m.(*internalpb.RetrieveResults))
	}
	decInsertRequest = func(b []byte, m proto.Message) error { return UnmarshalInsertRequest(b, m.(*milvuspb.InsertRequest)) }
	newFieldData     = func() proto.Message { return &schemapb.FieldData{} }
	newScalarField   = func() proto.Message { return &schemapb.ScalarField{} }
	newVectorField   = func() proto.Message { return &schemapb.VectorField{} }
	newIDs           = func() proto.Message { return &schemapb.IDs{} }
	newSearchResult  = func() proto.Message { return &schemapb.SearchResultData{} }
	newRetrieve      = func() proto.Message { return &internalpb.RetrieveResults{} }
	newInsertRequest = func() proto.Message { return &milvuspb.InsertRequest{} }
)

// TestUnknownFieldSkipAndMerge feeds each entry point a canonical message with a
// trailing unknown (future) field of every wire type. This exercises skipField
// (all wire-type cases) plus the protoMerge "rest" tail that folds the unknown
// bytes back via the official codec, so the result must equal proto.Unmarshal.
func TestUnknownFieldSkipAndMerge(t *testing.T) {
	canonFieldData, err := proto.Marshal(&schemapb.FieldData{
		Type: schemapb.DataType_Int64, FieldName: "f", FieldId: 3,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}}}}},
	})
	require.NoError(t, err)
	canonScalar, err := proto.Marshal(&schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{4, 5}}}})
	require.NoError(t, err)
	canonVector, err := proto.Marshal(&schemapb.VectorField{Dim: 4, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}})
	require.NoError(t, err)
	canonIDs, err := proto.Marshal(&schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7, 8}}}})
	require.NoError(t, err)
	canonSRD, err := proto.Marshal(&schemapb.SearchResultData{NumQueries: 2, TopK: 5, Scores: []float32{0.1, 0.2}})
	require.NoError(t, err)
	canonRR, err := proto.Marshal(&internalpb.RetrieveResults{ReqID: 7, AllRetrieveCount: 9})
	require.NoError(t, err)
	canonIR, err := proto.Marshal(&milvuspb.InsertRequest{CollectionName: "c", DbName: "db", NumRows: 3})
	require.NoError(t, err)

	entries := []struct {
		name    string
		canon   []byte
		unknown int32 // an unused field number for this message
		fresh   func() proto.Message
		fast    func([]byte, proto.Message) error
	}{
		{"FieldData", canonFieldData, 20, newFieldData, decFieldData},
		{"ScalarField", canonScalar, 30, newScalarField, decScalarField},
		{"VectorField", canonVector, 20, newVectorField, decVectorField},
		{"IDs", canonIDs, 30, newIDs, decIDs},
		{"SearchResultData", canonSRD, 30, newSearchResult, decSearchResult},
		{"RetrieveResults", canonRR, 30, newRetrieve, decRetrieve},
		{"InsertRequest", canonIR, 30, newInsertRequest, decInsertRequest},
	}
	for _, e := range entries {
		for _, w := range []struct {
			name   string
			append func([]byte, int32) []byte
		}{
			{"fixed64", appendUnknownFixed64},
			{"fixed32", appendUnknownFixed32},
			{"lendelim", appendUnknownLenDelim},
			{"varint", appendUnknownVarint},
		} {
			t.Run(e.name+"/"+w.name, func(t *testing.T) {
				wire := w.append(append([]byte{}, e.canon...), e.unknown)
				diffDecode(t, wire, e.fresh, e.fast)
			})
		}
	}
}

// TestDecodePackedNonPackedFixed exercises the single-element (non-packed)
// fixed32/fixed64 paths (decodePackedF32 case 5 → le32, decodePackedF64 case 1
// → le64), which the packed-only fuzz tests never reach.
func TestDecodePackedNonPackedFixed(t *testing.T) {
	t.Run("float32 non-packed fixed32", func(t *testing.T) {
		var f []byte
		f = protowire.AppendTag(f, 1, protowire.Fixed32Type)
		f = protowire.AppendFixed32(f, math.Float32bits(1.5))
		f = protowire.AppendTag(f, 1, protowire.Fixed32Type)
		f = protowire.AppendFixed32(f, math.Float32bits(-2.25))

		want := &schemapb.FloatArray{}
		require.NoError(t, proto.Unmarshal(f, want))
		got := &schemapb.FloatArray{}
		require.NoError(t, decodePackedF32(f, &got.Data, got))
		require.True(t, proto.Equal(want, got))
	})
	t.Run("float64 non-packed fixed64", func(t *testing.T) {
		var d []byte
		d = protowire.AppendTag(d, 1, protowire.Fixed64Type)
		d = protowire.AppendFixed64(d, math.Float64bits(1.5))
		d = protowire.AppendTag(d, 1, protowire.Fixed64Type)
		d = protowire.AppendFixed64(d, math.Float64bits(-2.25))

		want := &schemapb.DoubleArray{}
		require.NoError(t, proto.Unmarshal(d, want))
		got := &schemapb.DoubleArray{}
		require.NoError(t, decodePackedF64(d, &got.Data, got))
		require.True(t, proto.Equal(want, got))
	})
}

// TestColdScalarVariants pins the rare ScalarField oneof variants that delegate
// to the official codec (decodeScalarFallback cases 8/10/11/12/13/14).
func TestColdScalarVariants(t *testing.T) {
	cases := map[string]*schemapb.ScalarField{
		"array":       {Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_Int64}}},
		"geometry":    {Data: &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{}}},
		"timestamptz": {Data: &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{}}},
		"geometrywkt": {Data: &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{}}},
		"mol":         {Data: &schemapb.ScalarField_MolData{MolData: &schemapb.MolArray{}}},
		"molsmiles":   {Data: &schemapb.ScalarField_MolSmilesData{MolSmilesData: &schemapb.MolSmilesArray{}}},
	}
	for name, sf := range cases {
		t.Run(name, func(t *testing.T) {
			roundTripFieldData(t, &schemapb.FieldData{
				FieldName: name, FieldId: 5,
				Field: &schemapb.FieldData_Scalars{Scalars: sf},
			})
		})
	}
}

// TestColdSearchResultFields pins the delegate / rarer SearchResultData fields
// that the common-case tests skip: iterator results (11), recalls (12),
// highlights (14), element_indices (15), group_by_field_values (17),
// agg_buckets (18), agg_topks (19).
func TestColdSearchResultFields(t *testing.T) {
	roundTripSRD(t, &schemapb.SearchResultData{
		NumQueries:              1,
		TopK:                    2,
		Recalls:                 []float32{0.95, 0.9},
		ElementIndices:          &schemapb.LongArray{Data: []int64{0, 1, 2}},
		AggTopks:                []int64{3, 4},
		SearchIteratorV2Results: &schemapb.SearchIteratorV2Results{Token: "tok", LastBound: 1.5},
		GroupByFieldValues: []*schemapb.FieldData{
			{FieldName: "g", FieldId: 9, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}}},
		},
	})
}

// TestRepeatedSingularMessageMerge: a singular message field encoded twice on the
// wire is merged by proto3 (last-wins per scalar, concatenated repeated). This hits
// the "already set → proto.Merge" branches for Base/Status/Ids/CostAggregation.
func TestRepeatedSingularMessageMerge(t *testing.T) {
	t.Run("RetrieveResults Base/Ids/Cost twice", func(t *testing.T) {
		// Build manually: field 1 (Base) twice, field 4 (Ids) twice, field 13 (Cost) twice.
		var wire []byte
		appendMsgField := func(num int32, m proto.Message) {
			bb, err := proto.Marshal(m)
			require.NoError(t, err)
			wire = protowire.AppendTag(wire, protowire.Number(num), protowire.BytesType)
			wire = protowire.AppendBytes(wire, bb)
		}
		appendMsgField(1, &commonpb.MsgBase{MsgID: 1, SourceID: 10})
		appendMsgField(1, &commonpb.MsgBase{TargetID: 2})
		appendMsgField(4, &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}})
		appendMsgField(4, &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{2}}}})
		appendMsgField(13, &internalpb.CostAggregation{ResponseTime: 5})
		appendMsgField(13, &internalpb.CostAggregation{TotalNQ: 7})
		diffDecode(t, wire, newRetrieve, decRetrieve)
	})
	t.Run("SearchResultData Ids/iterator twice", func(t *testing.T) {
		var wire []byte
		appendMsgField := func(num int32, m proto.Message) {
			bb, err := proto.Marshal(m)
			require.NoError(t, err)
			wire = protowire.AppendTag(wire, protowire.Number(num), protowire.BytesType)
			wire = protowire.AppendBytes(wire, bb)
		}
		appendMsgField(5, &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}})
		appendMsgField(5, &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{2}}}})
		appendMsgField(11, &schemapb.SearchIteratorV2Results{Token: "t1"})
		appendMsgField(11, &schemapb.SearchIteratorV2Results{LastBound: 2.0})
		diffDecode(t, wire, newSearchResult, decSearchResult)
	})
}

// TestPackedFieldsAsSingleVarint: proto allows a packed repeated field to also
// appear as a sequence of single varints. This hits the wtype==0 packed-field
// branches in searchResultData (6/19), retrieveResults (6/8), insertRequest (6/7/8).
func TestPackedFieldsAsSingleVarint(t *testing.T) {
	t.Run("SearchResultData topks/agg_topks", func(t *testing.T) {
		var wire []byte
		wire = protowire.AppendTag(wire, 6, protowire.VarintType) // topks
		wire = protowire.AppendVarint(wire, 3)
		wire = protowire.AppendTag(wire, 19, protowire.VarintType) // agg_topks
		wire = protowire.AppendVarint(wire, 4)
		diffDecode(t, wire, newSearchResult, decSearchResult)
	})
	t.Run("RetrieveResults sealed/global segIDs", func(t *testing.T) {
		var wire []byte
		wire = protowire.AppendTag(wire, 6, protowire.VarintType) // sealed_segmentIDs_retrieved
		wire = protowire.AppendVarint(wire, 11)
		wire = protowire.AppendTag(wire, 8, protowire.VarintType) // global_sealed_segmentIDs
		wire = protowire.AppendVarint(wire, 22)
		diffDecode(t, wire, newRetrieve, decRetrieve)
	})
	t.Run("InsertRequest hashkeys single", func(t *testing.T) {
		var wire []byte
		wire = protowire.AppendTag(wire, 6, protowire.VarintType) // hash_keys
		wire = protowire.AppendVarint(wire, 99)
		diffDecode(t, wire, newInsertRequest, decInsertRequest)
	})
}

// TestMalformedInputs feeds truncated / overflowing wire data to the public entry
// points; fastpb must report an error (matching the official codec), never panic.
func TestMalformedInputs(t *testing.T) {
	overflowVarint := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02} // 10th byte > 1
	truncatedVarint := []byte{0x80, 0x80}                                                // never terminates
	// field 2 (length-delimited) declaring length 10 but only 2 bytes follow
	truncatedBytes := append(protowire.AppendTag(nil, 2, protowire.BytesType), 0x0A, 0x01, 0x02)

	cases := map[string][]byte{
		"overflow-varint":  overflowVarint,
		"truncated-varint": truncatedVarint,
		"truncated-bytes":  truncatedBytes,
	}
	for name, wire := range cases {
		t.Run("FieldData/"+name, func(t *testing.T) { diffDecode(t, wire, newFieldData, decFieldData) })
		t.Run("SearchResultData/"+name, func(t *testing.T) { diffDecode(t, wire, newSearchResult, decSearchResult) })
		t.Run("RetrieveResults/"+name, func(t *testing.T) { diffDecode(t, wire, newRetrieve, decRetrieve) })
		t.Run("InsertRequest/"+name, func(t *testing.T) { diffDecode(t, wire, newInsertRequest, decInsertRequest) })
	}
}

// TestInvalidUTF8Ingress: InsertRequest is untrusted ingress, so strings are
// UTF-8 validated. An invalid byte sequence must be rejected, matching proto3's
// official decoder, which also rejects invalid UTF-8 in string fields.
func TestInvalidUTF8Ingress(t *testing.T) {
	t.Run("scalar string field (DbName)", func(t *testing.T) {
		var wire []byte
		wire = protowire.AppendTag(wire, 2, protowire.BytesType) // DbName
		wire = protowire.AppendBytes(wire, []byte{0xff, 0xfe})   // invalid UTF-8
		diffDecode(t, wire, newInsertRequest, decInsertRequest)
	})
	t.Run("string inside fields_data StringArray", func(t *testing.T) {
		// fields_data(5) → ScalarField(3) → StringData(6) → StringArray data(1) = invalid utf8
		var sa []byte
		sa = protowire.AppendTag(sa, 1, protowire.BytesType)
		sa = protowire.AppendBytes(sa, []byte{0xff})
		var sf []byte
		sf = protowire.AppendTag(sf, 6, protowire.BytesType)
		sf = protowire.AppendBytes(sf, sa)
		var fd []byte
		fd = protowire.AppendTag(fd, 3, protowire.BytesType) // Scalars
		fd = protowire.AppendBytes(fd, sf)
		var wire []byte
		wire = protowire.AppendTag(wire, 5, protowire.BytesType) // fields_data
		wire = protowire.AppendBytes(wire, fd)
		diffDecode(t, wire, newInsertRequest, decInsertRequest)
	})
}

// TestTryUnmarshalDispatch covers TryUnmarshal: the two hot types report handled,
// every other type reports (false, nil) so the caller uses the official codec.
func TestTryUnmarshalDispatch(t *testing.T) {
	rrWire, _ := proto.Marshal(&internalpb.RetrieveResults{ReqID: 1})
	irWire, _ := proto.Marshal(&milvuspb.InsertRequest{CollectionName: "c"})

	handled, err := TryUnmarshal(&internalpb.RetrieveResults{}, rrWire)
	require.True(t, handled)
	require.NoError(t, err)

	handled, err = TryUnmarshal(&milvuspb.InsertRequest{}, irWire)
	require.True(t, handled)
	require.NoError(t, err)

	// SearchResultData and any other proto are NOT top-level fast-pathed.
	handled, err = TryUnmarshal(&schemapb.SearchResultData{}, nil)
	require.False(t, handled)
	require.NoError(t, err)

	handled, err = TryUnmarshal(&milvuspb.SearchResults{}, nil)
	require.False(t, handled)
	require.NoError(t, err)
}
