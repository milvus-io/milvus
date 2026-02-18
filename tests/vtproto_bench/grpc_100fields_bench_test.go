package vtproto_bench

import (
	"crypto/rand"
	"fmt"
	"math"
	mrand "math/rand"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

// ---------------------------------------------------------------------------
// 100-field table schema
//
// Simulates a realistic Milvus collection with diverse field types:
//   field_0   : Int64 (PK)
//   field_1   : FloatVector 128-dim (search vector)
//   field_2-11  : Int64     (10 fields)
//   field_12-21 : Int32     (10 fields)
//   field_22-26 : Float     (5 fields)
//   field_27-31 : Double    (5 fields)
//   field_32-41 : VarChar   (10 fields, ~32 chars each)
//   field_42-51 : Bool      (10 fields)
//   field_52-61 : JSON      (10 fields, ~80 bytes each)
//   field_62-66 : Array<Int32> (5 fields, 10 elements each)
//   field_67-71 : Int16     (5 fields — stored as IntArray)
//   field_72-76 : Int8      (5 fields — stored as IntArray)
//   field_77-81 : BinaryVector 128-dim (5 fields)
//   field_82-86 : Float16Vector 128-dim (5 fields)
//   field_87-91 : VarChar long (~256 chars, 5 fields)
//   field_92-96 : Double    (5 fields)
//   field_97-99 : SparseFloatVector (3 fields)
//
// Total: 100 fields
// ---------------------------------------------------------------------------

const (
	numResultRows = 500 // nq=10, topK=50 → 500 rows
	vectorDim     = 128
)

// makeFieldsData builds 100 FieldData entries, each containing numResultRows of data.
func makeFieldsData(numRows int) []*schemapb.FieldData {
	fields := make([]*schemapb.FieldData, 0, 100)
	fieldID := int64(0)

	addField := func(fd *schemapb.FieldData) {
		fd.FieldId = fieldID
		fd.FieldName = fmt.Sprintf("field_%d", fieldID)
		fields = append(fields, fd)
		fieldID++
	}

	// --- field 0: Int64 PK ---
	addField(makeInt64Field(numRows))

	// --- field 1: FloatVector 128-dim ---
	addField(makeFloatVectorField(numRows, vectorDim))

	// --- field 2-11: Int64 ×10 ---
	for i := 0; i < 10; i++ {
		addField(makeInt64Field(numRows))
	}

	// --- field 12-21: Int32 ×10 ---
	for i := 0; i < 10; i++ {
		addField(makeInt32Field(numRows))
	}

	// --- field 22-26: Float ×5 ---
	for i := 0; i < 5; i++ {
		addField(makeFloatField(numRows))
	}

	// --- field 27-31: Double ×5 ---
	for i := 0; i < 5; i++ {
		addField(makeDoubleField(numRows))
	}

	// --- field 32-41: VarChar ×10 (short, ~32 chars) ---
	for i := 0; i < 10; i++ {
		addField(makeVarCharField(numRows, 32))
	}

	// --- field 42-51: Bool ×10 ---
	for i := 0; i < 10; i++ {
		addField(makeBoolField(numRows))
	}

	// --- field 52-61: JSON ×10 (~80 bytes each) ---
	for i := 0; i < 10; i++ {
		addField(makeJSONField(numRows, 80))
	}

	// --- field 62-66: Array<Int32> ×5 (10 elements per row) ---
	for i := 0; i < 5; i++ {
		addField(makeArrayInt32Field(numRows, 10))
	}

	// --- field 67-71: Int16 ×5 (stored as IntArray) ---
	for i := 0; i < 5; i++ {
		addField(makeInt16Field(numRows))
	}

	// --- field 72-76: Int8 ×5 (stored as IntArray) ---
	for i := 0; i < 5; i++ {
		addField(makeInt8Field(numRows))
	}

	// --- field 77-81: BinaryVector 128-dim ×5 ---
	for i := 0; i < 5; i++ {
		addField(makeBinaryVectorField(numRows, vectorDim))
	}

	// --- field 82-86: Float16Vector 128-dim ×5 ---
	for i := 0; i < 5; i++ {
		addField(makeFloat16VectorField(numRows, vectorDim))
	}

	// --- field 87-91: VarChar long ×5 (~256 chars) ---
	for i := 0; i < 5; i++ {
		addField(makeVarCharField(numRows, 256))
	}

	// --- field 92-96: Double ×5 ---
	for i := 0; i < 5; i++ {
		addField(makeDoubleField(numRows))
	}

	// --- field 97-99: SparseFloatVector ×3 ---
	for i := 0; i < 3; i++ {
		addField(makeSparseFloatVectorField(numRows, 20)) // 20 non-zero per row
	}

	return fields
}

// ---------------------------------------------------------------------------
// Field builders
// ---------------------------------------------------------------------------

func makeInt64Field(n int) *schemapb.FieldData {
	data := make([]int64, n)
	for i := range data {
		data[i] = mrand.Int63()
	}
	return &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}},
		}},
	}
}

func makeInt32Field(n int) *schemapb.FieldData {
	data := make([]int32, n)
	for i := range data {
		data[i] = mrand.Int31()
	}
	return &schemapb.FieldData{
		Type: schemapb.DataType_Int32,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: data}},
		}},
	}
}

func makeInt16Field(n int) *schemapb.FieldData {
	data := make([]int32, n)
	for i := range data {
		data[i] = int32(mrand.Intn(math.MaxInt16))
	}
	return &schemapb.FieldData{
		Type: schemapb.DataType_Int16,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: data}},
		}},
	}
}

func makeInt8Field(n int) *schemapb.FieldData {
	data := make([]int32, n)
	for i := range data {
		data[i] = int32(mrand.Intn(math.MaxInt8))
	}
	return &schemapb.FieldData{
		Type: schemapb.DataType_Int8,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: data}},
		}},
	}
}

func makeFloatField(n int) *schemapb.FieldData {
	data := make([]float32, n)
	for i := range data {
		data[i] = mrand.Float32()
	}
	return &schemapb.FieldData{
		Type: schemapb.DataType_Float,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: data}},
		}},
	}
}

func makeDoubleField(n int) *schemapb.FieldData {
	data := make([]float64, n)
	for i := range data {
		data[i] = mrand.Float64()
	}
	return &schemapb.FieldData{
		Type: schemapb.DataType_Double,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: data}},
		}},
	}
}

func makeBoolField(n int) *schemapb.FieldData {
	data := make([]bool, n)
	for i := range data {
		data[i] = mrand.Intn(2) == 1
	}
	return &schemapb.FieldData{
		Type: schemapb.DataType_Bool,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: data}},
		}},
	}
}

func makeVarCharField(n int, avgLen int) *schemapb.FieldData {
	data := make([]string, n)
	buf := make([]byte, avgLen)
	for i := range data {
		rand.Read(buf)
		// make printable ASCII
		for j := range buf {
			buf[j] = 'a' + buf[j]%26
		}
		data[i] = string(buf)
	}
	return &schemapb.FieldData{
		Type: schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: data}},
		}},
	}
}

func makeJSONField(n int, avgLen int) *schemapb.FieldData {
	data := make([][]byte, n)
	for i := range data {
		// realistic JSON: {"key_0": 12345, "key_1": "some_value", "active": true}
		data[i] = []byte(fmt.Sprintf(
			`{"id":%d,"name":"user_%d","score":%.4f,"active":%v,"tags":["t%d","t%d"]}`,
			i, i, mrand.Float64()*100, i%2 == 0, i%10, i%7,
		))
	}
	return &schemapb.FieldData{
		Type: schemapb.DataType_JSON,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: data}},
		}},
	}
}

func makeArrayInt32Field(n int, elemCount int) *schemapb.FieldData {
	arrays := make([]*schemapb.ScalarField, n)
	for i := range arrays {
		elems := make([]int32, elemCount)
		for j := range elems {
			elems[j] = mrand.Int31n(10000)
		}
		arrays[i] = &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: elems}},
		}
	}
	return &schemapb.FieldData{
		Type: schemapb.DataType_Array,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				Data:        arrays,
				ElementType: schemapb.DataType_Int32,
			}},
		}},
	}
}

func makeFloatVectorField(n int, dim int) *schemapb.FieldData {
	data := make([]float32, n*dim)
	for i := range data {
		data[i] = mrand.Float32()
	}
	return &schemapb.FieldData{
		Type: schemapb.DataType_FloatVector,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim:  int64(dim),
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: data}},
		}},
	}
}

func makeBinaryVectorField(n int, dim int) *schemapb.FieldData {
	bytesPerVec := dim / 8
	data := make([]byte, n*bytesPerVec)
	rand.Read(data)
	return &schemapb.FieldData{
		Type: schemapb.DataType_BinaryVector,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim:  int64(dim),
			Data: &schemapb.VectorField_BinaryVector{BinaryVector: data},
		}},
	}
}

func makeFloat16VectorField(n int, dim int) *schemapb.FieldData {
	bytesPerVec := dim * 2 // float16 = 2 bytes
	data := make([]byte, n*bytesPerVec)
	rand.Read(data)
	return &schemapb.FieldData{
		Type: schemapb.DataType_Float16Vector,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim:  int64(dim),
			Data: &schemapb.VectorField_Float16Vector{Float16Vector: data},
		}},
	}
}

func makeSparseFloatVectorField(n int, nnzPerRow int) *schemapb.FieldData {
	contents := make([][]byte, n)
	for i := range contents {
		// each element: uint32 index + float32 value = 8 bytes
		row := make([]byte, nnzPerRow*8)
		rand.Read(row)
		contents[i] = row
	}
	return &schemapb.FieldData{
		Type: schemapb.DataType_SparseFloatVector,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: 10000,
			Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{
				Contents: contents,
				Dim:      10000,
			}},
		}},
	}
}

// ---------------------------------------------------------------------------
// Top-level message constructors
// ---------------------------------------------------------------------------

// makeSearchResultData100Fields builds a SearchResultData with 100 output fields.
func makeSearchResultData100Fields(numRows int) *schemapb.SearchResultData {
	nq := int64(10)
	topk := int64(numRows) / nq

	scores := make([]float32, numRows)
	ids := make([]int64, numRows)
	for i := range scores {
		scores[i] = mrand.Float32()
		ids[i] = int64(i)
	}

	outputFields := make([]string, 100)
	for i := range outputFields {
		outputFields[i] = fmt.Sprintf("field_%d", i)
	}

	return &schemapb.SearchResultData{
		NumQueries:   nq,
		TopK:         topk,
		FieldsData:   makeFieldsData(numRows),
		Scores:       scores,
		Ids:          &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}},
		Topks:        []int64{topk},
		OutputFields: outputFields,
	}
}

// makeSearchResults100Fields wraps SearchResultData into the internal SearchResults
// (like the real gRPC path: SearchResultData is serialized into sliced_blob).
func makeSearchResults100Fields(numRows int) *internalpb.SearchResults {
	srd := makeSearchResultData100Fields(numRows)
	blob, _ := proto.Marshal(srd)
	return &internalpb.SearchResults{
		NumQueries:               10,
		TopK:                     int64(numRows) / 10,
		MetricType:               "L2",
		SlicedBlob:               blob,
		SealedSegmentIDsSearched: []int64{1, 2, 3, 4, 5},
		ChannelIDsSearched:       []string{"ch-0", "ch-1"},
	}
}

// ---------------------------------------------------------------------------
// Benchmark: Pure marshal/unmarshal of SearchResultData with 100 fields
// ---------------------------------------------------------------------------

func BenchmarkMarshal_100Fields_1000Rows_StdProto(b *testing.B) {
	msg := makeSearchResultData100Fields(numResultRows)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = proto.Marshal(msg)
	}
}

func BenchmarkMarshal_100Fields_1000Rows_VTProto(b *testing.B) {
	msg := makeSearchResultData100Fields(numResultRows)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = msg.MarshalVT()
	}
}

func BenchmarkUnmarshal_100Fields_1000Rows_StdProto(b *testing.B) {
	msg := makeSearchResultData100Fields(numResultRows)
	data, _ := proto.Marshal(msg)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := &schemapb.SearchResultData{}
		_ = proto.Unmarshal(data, out)
	}
}

func BenchmarkUnmarshal_100Fields_1000Rows_VTProto(b *testing.B) {
	msg := makeSearchResultData100Fields(numResultRows)
	data, _ := proto.Marshal(msg)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := &schemapb.SearchResultData{}
		_ = out.UnmarshalVT(data)
	}
}

// ---------------------------------------------------------------------------
// Benchmark: gRPC round-trip of SearchResultData with 100 fields
// ---------------------------------------------------------------------------

func BenchmarkGRPC_100Fields_1000Rows_StdProto(b *testing.B) {
	msg := makeSearchResultData100Fields(numResultRows)
	benchGRPCRoundTrip(b, "std-proto-bench", msg, func() interface{} { return &schemapb.SearchResultData{} })
}

func BenchmarkGRPC_100Fields_1000Rows_VTProto(b *testing.B) {
	msg := makeSearchResultData100Fields(numResultRows)
	benchGRPCRoundTrip(b, "vtproto-bench", msg, func() interface{} { return &schemapb.SearchResultData{} })
}

// ---------------------------------------------------------------------------
// Benchmark: gRPC round-trip of SearchResults wrapping 100-field blob
// (This is the real path: SearchResults.sliced_blob = Marshal(SearchResultData))
// ---------------------------------------------------------------------------

func BenchmarkGRPC_SearchResults_100Fields_Blob_StdProto(b *testing.B) {
	msg := makeSearchResults100Fields(numResultRows)
	benchGRPCRoundTrip(b, "std-proto-bench", msg, func() interface{} { return &internalpb.SearchResults{} })
}

func BenchmarkGRPC_SearchResults_100Fields_Blob_VTProto(b *testing.B) {
	msg := makeSearchResults100Fields(numResultRows)
	benchGRPCRoundTrip(b, "vtproto-bench", msg, func() interface{} { return &internalpb.SearchResults{} })
}

// ---------------------------------------------------------------------------
// Inspect helper
// ---------------------------------------------------------------------------

func TestInspect100FieldsMessage(t *testing.T) {
	srd := makeSearchResultData100Fields(numResultRows)
	srdSize := proto.Size(srd)

	sr := makeSearchResults100Fields(numResultRows)
	srSize := proto.Size(sr)

	fmt.Printf(`
=== 100-Field Table Benchmark Data ===
  Number of fields:         %d
  Number of result rows:    %d (nq=10, topK=100)

  SearchResultData size:    %d bytes (%.2f MB)
    - contains all 100 FieldData directly

  SearchResults size:       %d bytes (%.2f MB)
    - wraps SearchResultData as sliced_blob bytes

  Field breakdown:
    field_0       : Int64 PK
    field_1       : FloatVector 128-dim     (%.1f KB per field)
    field_2-11    : Int64 ×10               (%.1f KB per field)
    field_12-21   : Int32 ×10               (%.1f KB per field)
    field_22-26   : Float ×5                (%.1f KB per field)
    field_27-31   : Double ×5               (%.1f KB per field)
    field_32-41   : VarChar ~32ch ×10       (%.1f KB per field)
    field_42-51   : Bool ×10                (%.1f KB per field)
    field_52-61   : JSON ~80B ×10           (%.1f KB per field)
    field_62-66   : Array<Int32> ×5         (%.1f KB per field)
    field_67-71   : Int16 ×5                (%.1f KB per field)
    field_72-76   : Int8 ×5                 (%.1f KB per field)
    field_77-81   : BinaryVector 128-dim ×5 (%.1f KB per field)
    field_82-86   : Float16Vector 128-dim ×5(%.1f KB per field)
    field_87-91   : VarChar ~256ch ×5       (%.1f KB per field)
    field_92-96   : Double ×5               (%.1f KB per field)
    field_97-99   : SparseFloat ×3          (%.1f KB per field)
`,
		len(srd.FieldsData),
		numResultRows,
		srdSize, float64(srdSize)/1024/1024,
		srSize, float64(srSize)/1024/1024,
		float64(numResultRows*vectorDim*4)/1024,     // FloatVector
		float64(numResultRows*8)/1024,                // Int64
		float64(numResultRows*4)/1024,                // Int32
		float64(numResultRows*4)/1024,                // Float
		float64(numResultRows*8)/1024,                // Double
		float64(numResultRows*32)/1024,               // VarChar 32
		float64(numResultRows*1)/1024,                // Bool
		float64(numResultRows*80)/1024,               // JSON
		float64(numResultRows*10*4)/1024,             // Array<Int32>
		float64(numResultRows*4)/1024,                // Int16 (as int32)
		float64(numResultRows*4)/1024,                // Int8 (as int32)
		float64(numResultRows*vectorDim/8)/1024,      // BinaryVector
		float64(numResultRows*vectorDim*2)/1024,      // Float16Vector
		float64(numResultRows*256)/1024,              // VarChar 256
		float64(numResultRows*8)/1024,                // Double
		float64(numResultRows*20*8)/1024,             // SparseFloat
	)
}
