package fastpb

import (
	"math"
	"math/rand"
	"testing"

	"google.golang.org/protobuf/proto"

	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// roundTripSRD pins SearchResultData decode equivalence against official proto.
func roundTripSRD(t *testing.T, src *schemapb.SearchResultData) {
	t.Helper()
	wire, err := proto.Marshal(src)
	if err != nil {
		t.Fatalf("official marshal: %v", err)
	}
	var got schemapb.SearchResultData
	if err := UnmarshalSearchResultData(wire, &got); err != nil {
		t.Fatalf("UnmarshalSearchResultData: %v", err)
	}
	if !proto.Equal(src, &got) {
		t.Fatalf("mismatch:\n src = %v\n got = %v", src, &got)
	}
}

// --- one explicit case per scalar data type ---

func TestEquiv_ScalarTypes(t *testing.T) {
	cases := map[string]*schemapb.ScalarField{
		"bool":   {Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true, false, true, true}}}},
		"int":    {Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{0, -1, 2147483647, -2147483648, 42}}}},
		"long":   {Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{0, -1, 9223372036854775807, -9223372036854775808}}}},
		"float":  {Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{1.5, -2.25, 0, 3e9}}}},
		"double": {Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{1.5, -2.25, 0, 3e300}}}},
		"string": {Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a", "", "世界", "long varchar value here"}}}},
		"bytes":  {Data: &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{Data: [][]byte{{1, 2, 3}, {}, {255, 0, 128}}}}},
		"json":   {Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte(`{"a":1}`), []byte(`[]`)}}}},
	}
	for name, sf := range cases {
		t.Run(name, func(t *testing.T) {
			roundTripFieldData(t, &schemapb.FieldData{
				FieldName: name, FieldId: 7,
				Field: &schemapb.FieldData_Scalars{Scalars: sf},
			})
		})
	}
}

func TestEquiv_VectorTypes(t *testing.T) {
	cases := map[string]*schemapb.VectorField{
		"float":  {Dim: 4, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6, 7, 8}}}},
		"binary": {Dim: 16, Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{0xAB, 0xCD, 0x00, 0xFF}}},
		"fp16":   {Dim: 2, Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{1, 2, 3, 4}}},
		"bf16":   {Dim: 2, Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{5, 6, 7, 8}}},
		"int8":   {Dim: 4, Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{250, 1, 0, 200}}},
		"sparse": {Dim: 100, Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Dim: 100, Contents: [][]byte{{1, 2}, {3, 4, 5}}}}},
	}
	for name, vf := range cases {
		t.Run(name, func(t *testing.T) {
			roundTripFieldData(t, &schemapb.FieldData{
				FieldName: name, FieldId: 9,
				Field: &schemapb.FieldData_Vectors{Vectors: vf},
			})
		})
	}
}

func TestEquiv_FieldData_ValidData(t *testing.T) {
	roundTripFieldData(t, &schemapb.FieldData{
		Type: schemapb.DataType_Int64, FieldName: "x", FieldId: 3, IsDynamic: true,
		ValidData: []bool{true, false, false, true},
		Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}}}}},
	})
}

func TestEquiv_IDs(t *testing.T) {
	roundTripSRD(t, &schemapb.SearchResultData{Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10, 20, 30}}}}})
	roundTripSRD(t, &schemapb.SearchResultData{Ids: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"k1", "k2"}}}}})
}

func TestEquiv_SearchResultData_Full(t *testing.T) {
	roundTripSRD(t, &schemapb.SearchResultData{
		NumQueries:       2,
		TopK:             3,
		Scores:           []float32{0.9, 0.8, 0.7, 0.6, 0.5, 0.4},
		Topks:            []int64{3, 3},
		OutputFields:     []string{"pk", "embedding", "title"},
		Distances:        []float32{1.1, 2.2, 3.3},
		PrimaryFieldName: "pk",
		AllSearchCount:   12345,
		Ids:              &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"a", "b", "c", "d", "e", "f"}}}},
		FieldsData: []*schemapb.FieldData{
			{Type: schemapb.DataType_VarChar, FieldName: "title", FieldId: 101, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"t1", "t2", "t3", "t4", "t5", "t6"}}}}}},
			{Type: schemapb.DataType_FloatVector, FieldName: "embedding", FieldId: 102, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}}}}}},
		},
	})
}

// --- randomized differential fuzz: the equivalence guarantee at volume ---

func randString(r *rand.Rand) string {
	n := r.Intn(12)
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + r.Intn(26))
	}
	return string(b)
}

func randScalarField(r *rand.Rand, rows int) *schemapb.ScalarField {
	switch r.Intn(7) {
	case 0:
		d := make([]bool, rows)
		for i := range d {
			d[i] = r.Intn(2) == 0
		}
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: d}}}
	case 1:
		d := make([]int32, rows)
		for i := range d {
			d[i] = int32(r.Uint32())
		}
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: d}}}
	case 2:
		d := make([]int64, rows)
		for i := range d {
			d[i] = int64(r.Uint64())
		}
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: d}}}
	case 3:
		d := make([]float32, rows)
		for i := range d {
			d[i] = math.Float32frombits(r.Uint32())
		}
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: d}}}
	case 4:
		d := make([]float64, rows)
		for i := range d {
			d[i] = math.Float64frombits(r.Uint64())
		}
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: d}}}
	case 5:
		d := make([]string, rows)
		for i := range d {
			d[i] = randString(r)
		}
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: d}}}
	default:
		d := make([][]byte, rows)
		for i := range d {
			d[i] = []byte(randString(r))
		}
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{Data: d}}}
	}
}

func randFieldData(r *rand.Rand, rows int) *schemapb.FieldData {
	fd := &schemapb.FieldData{FieldName: randString(r), FieldId: int64(r.Intn(1000))}
	if r.Intn(2) == 0 {
		fd.Field = &schemapb.FieldData_Scalars{Scalars: randScalarField(r, rows)}
	} else {
		dim := 1 + r.Intn(8)
		d := make([]float32, rows*dim)
		for i := range d {
			d[i] = math.Float32frombits(r.Uint32())
		}
		fd.Field = &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: int64(dim), Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: d}}}}
	}
	if r.Intn(2) == 0 {
		vd := make([]bool, rows)
		for i := range vd {
			vd[i] = r.Intn(2) == 0
		}
		fd.ValidData = vd
	}
	return fd
}

func randSRD(r *rand.Rand) *schemapb.SearchResultData {
	rows := r.Intn(20)
	srd := &schemapb.SearchResultData{
		NumQueries:       int64(r.Intn(5)),
		TopK:             int64(r.Intn(10)),
		PrimaryFieldName: randString(r),
		AllSearchCount:   int64(r.Uint32()),
	}
	srd.Scores = make([]float32, rows)
	for i := range srd.Scores {
		srd.Scores[i] = math.Float32frombits(r.Uint32())
	}
	if r.Intn(2) == 0 {
		ids := make([]int64, rows)
		for i := range ids {
			ids[i] = int64(r.Uint64())
		}
		srd.Ids = &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}}
	} else {
		ids := make([]string, rows)
		for i := range ids {
			ids[i] = randString(r)
		}
		srd.Ids = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: ids}}}
	}
	for i := 0; i < r.Intn(4); i++ {
		srd.FieldsData = append(srd.FieldsData, randFieldData(r, rows))
	}
	for i := 0; i < r.Intn(3); i++ {
		srd.OutputFields = append(srd.OutputFields, randString(r))
	}
	return srd
}

func TestEquiv_Fuzz(t *testing.T) {
	r := rand.New(rand.NewSource(0xC0FFEE))
	for i := 0; i < 5000; i++ {
		src := randSRD(r)
		wire, err := proto.Marshal(src)
		if err != nil {
			t.Fatalf("marshal iter %d: %v", i, err)
		}
		var got schemapb.SearchResultData
		if err := UnmarshalSearchResultData(wire, &got); err != nil {
			t.Fatalf("decode iter %d: %v", i, err)
		}
		if !proto.Equal(src, &got) {
			t.Fatalf("mismatch iter %d:\n src = %v\n got = %v", i, src, &got)
		}
	}
}
