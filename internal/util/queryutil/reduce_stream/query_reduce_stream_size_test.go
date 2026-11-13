// Verify exact Query protobuf sizing without copying Units.
package reducestream

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func queryUnitReferenceSize(t testing.TB, unit queryReduceUnit) int {
	t.Helper()
	chunk, err := buildRetrieveChunk([]queryReduceUnit{unit})
	require.NoError(t, err)
	return proto.Size(&internalpb.RetrieveResults{Ids: chunk.GetIds(), FieldsData: chunk.GetFieldsData(), ComputedResults: chunk.GetComputedResults(), ElementIndices: chunk.GetElementIndices()})
}

func TestRetrieveUnitByteSizeProtobuf(t *testing.T) {
	values := []int64{0, 127, 128, -1, math.MinInt64, math.MaxInt64}
	result := newQueryTestChunk(values, values)
	result.FieldsData = append(result.FieldsData, &schemapb.FieldData{
		Type: schemapb.DataType_FloatVector, FieldId: 101, FieldName: "vector",
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: 768, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, len(values)*768)}},
		}},
	})
	calculator := typeutil.NewFieldDataIdxComputer(result.GetFieldsData())
	for row := range values {
		unit := queryReduceUnit{result: result, rowIndex: int64(row), fieldIndexComputer: calculator}
		got, err := unit.reducibleByteSize()
		require.NoError(t, err)
		require.Equal(t, queryUnitReferenceSize(t, unit), got)
		require.Zero(t, testing.AllocsPerRun(100, func() { _, err = unit.reducibleByteSize() }))
		require.NoError(t, err)
	}
	result.ReqID = 123
	result.AllRetrieveCount = math.MaxInt64
	unit := queryReduceUnit{result: result, fieldIndexComputer: calculator}
	got, err := unit.reducibleByteSize()
	require.NoError(t, err)
	require.Equal(t, queryUnitReferenceSize(t, unit), got)
	result.FieldsData[0].FieldName = "field descriptors count toward the encoded size"
	renamed, err := unit.reducibleByteSize()
	require.NoError(t, err)
	require.Greater(t, renamed, got)
	require.Equal(t, queryUnitReferenceSize(t, unit), renamed)
}

func TestRetrieveUnitComputedBytes(t *testing.T) {
	result := newQueryTestChunk([]int64{1, 2, 3}, []int64{10, 20, 30})
	result.ComputedResults = []*schemapb.FieldData{{
		FieldName: "scan.matches", Type: schemapb.DataType_JSON,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			ValidData: []bool{true, false, true},
			Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: [][]byte{
				[]byte(`[{"content":"foo","starts":[0],"ends":[3]}]`), nil, []byte(`{"text":"` + strings.Repeat("x", 1024) + `"}`),
			}}},
		}},
	}}
	for row := int64(0); row < 3; row++ {
		unit := queryReduceUnit{result: result, rowIndex: row, fieldIndexComputer: typeutil.NewFieldDataIdxComputer(result.FieldsData)}
		got, err := unit.reducibleByteSize()
		require.NoError(t, err)
		require.Equal(t, queryUnitReferenceSize(t, unit), got)
		require.Zero(t, testing.AllocsPerRun(100, func() { _, err = unit.reducibleByteSize() }))
		require.NoError(t, err)
	}
	threshold := queryChunkBytes(t, result, 2) - 1
	chunks, err := SplitRetrieveResult(result, threshold)
	require.NoError(t, err)
	require.Len(t, chunks, 2)
	require.Equal(t, []int64{1, 2}, queryTestIDs(chunks[0]))
	require.Equal(t, []int64{3}, queryTestIDs(chunks[1]))
	require.Greater(t, proto.Size(chunks[1]), threshold)
}

// TestRetrieveUnitByteSizeMixedFields covers variable fields and compact nullable vectors.
func TestRetrieveUnitByteSizeMixedFields(t *testing.T) {
	result := newQueryTestChunk([]int64{1, 2, 3}, []int64{10, 20, 30})
	result.FieldsData = append(result.FieldsData,
		&schemapb.FieldData{
			Type: schemapb.DataType_VarChar, FieldId: 102,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a", "", "longer"}}},
			}},
		},
		&schemapb.FieldData{
			Type: schemapb.DataType_JSON, FieldId: 103,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte("{}"), nil, []byte("[1]")}}},
			}},
		},
		&schemapb.FieldData{
			Type: schemapb.DataType_SparseFloatVector, FieldId: 104, ValidData: []bool{true, false, true},
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{
					Contents: [][]byte{make([]byte, 8), make([]byte, 16)},
				}},
			}},
		},
	)
	calculator := typeutil.NewFieldDataIdxComputer(result.GetFieldsData())
	for row := 0; row < 3; row++ {
		unit := queryReduceUnit{result: result, rowIndex: int64(row), fieldIndexComputer: calculator}
		got, err := unit.reducibleByteSize()
		require.NoError(t, err)
		require.Equal(t, queryUnitReferenceSize(t, unit), got)
		require.Zero(t, testing.AllocsPerRun(100, func() { _, err = unit.reducibleByteSize() }))
		require.NoError(t, err)
	}
	result.Ids = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"id", "", "long-id"}}}}
	unit := queryReduceUnit{result: result, rowIndex: 2, fieldIndexComputer: calculator}
	got, err := unit.reducibleByteSize()
	require.NoError(t, err)
	require.Equal(t, queryUnitReferenceSize(t, unit), got)
	result.Ids = nil                          // Grouped Query Units need not carry a PK.
	result.FieldsData = result.FieldsData[:3] // Group keys and aggregates are scalar fields.
	unit.fieldIndexComputer = typeutil.NewFieldDataIdxComputer(result.GetFieldsData())
	got, err = unit.reducibleByteSize()
	require.NoError(t, err)
	require.Equal(t, queryUnitReferenceSize(t, unit), got)
}

func TestBuildRetrieveChunkReusesSources(t *testing.T) {
	result := newQueryTestChunk([]int64{1, 2, 3}, []int64{10, 20, 30})
	result.FieldsData = append(result.FieldsData, &schemapb.FieldData{
		Type: schemapb.DataType_SparseFloatVector, FieldId: 104,
		ValidData: []bool{true, false, true}, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Contents: [][]byte{make([]byte, 8), make([]byte, 16)}}},
		}},
	})
	other := proto.Clone(result).(*internalpb.RetrieveResults)
	other.GetFieldsData()[1].GetVectors().GetSparseFloatVector().Contents[0][0] = 3
	for i := range other.GetIds().GetIntId().Data {
		other.GetIds().GetIntId().Data[i] += 100
	}
	units := make([]queryReduceUnit, 96)
	for i := range units {
		source := result
		if i%2 != 0 {
			source = other
		}
		units[i] = queryReduceUnit{result: source, rowIndex: int64(i % 3)}
	}
	var chunkErr error
	small := testing.AllocsPerRun(5, func() { _, chunkErr = buildRetrieveChunk(units[:2]) })
	require.NoError(t, chunkErr)
	large := testing.AllocsPerRun(5, func() { _, chunkErr = buildRetrieveChunk(units) })
	require.NoError(t, chunkErr)
	require.Less(t, large-small, float64(48), "repeated Units must not rebuild nullable indexes")
	chunk, err := buildRetrieveChunk(units)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 102, 3, 101}, queryTestIDs(chunk)[:4])
	require.Equal(t, []bool{true, false, true, true}, typeutil.GetFieldDataValidData(chunk.GetFieldsData()[1])[:4])
	require.Len(t, chunk.GetFieldsData()[1].GetVectors().GetSparseFloatVector().GetContents(), 64)
	contents := chunk.GetFieldsData()[1].GetVectors().GetSparseFloatVector().GetContents()
	require.Equal(t, byte(0), contents[0][0])
	require.Len(t, contents[1], 16)
	require.Equal(t, byte(3), contents[2][0])
}

func TestRetrieveUnitByteSizeVectorArray(t *testing.T) {
	for _, valid := range []bool{false, true} {
		result := newQueryTestChunk([]int64{1}, []int64{10})
		array := &schemapb.VectorArray{Dim: 2, ElementType: schemapb.DataType_FloatVector}
		if valid {
			array.Data = []*schemapb.VectorField{{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}}}
		}
		result.FieldsData = append(result.FieldsData, &schemapb.FieldData{
			Type: schemapb.DataType_ArrayOfVector, FieldId: 101,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{ValidData: []bool{valid}, Data: &schemapb.VectorField_VectorArray{VectorArray: array}}},
		})
		unit := queryReduceUnit{result: result}
		got, err := unit.reducibleByteSize()
		require.NoError(t, err)
		require.Equal(t, queryUnitReferenceSize(t, unit), got)
	}
}

// TestRetrieveUnitByteSizeCalculatorLifetime checks reuse and release with the child Chunk.
func TestRetrieveUnitByteSizeCalculatorLifetime(t *testing.T) {
	buffer := &orderedChildBuffer{}
	require.NoError(t, buffer.accept(newQueryTestChunk([]int64{1, 2}, []int64{10, 20})))
	calculator := buffer.fieldIndexComputer
	first, second := buffer.pop(), buffer.pop()
	require.Same(t, calculator, first.fieldIndexComputer)
	require.Same(t, calculator, second.fieldIndexComputer)
	require.Nil(t, buffer.fieldIndexComputer)
	for _, unit := range []queryReduceUnit{first, second} {
		got, err := unit.reducibleByteSize()
		require.NoError(t, err)
		require.Equal(t, queryUnitReferenceSize(t, unit), got)
	}
	_, err := (queryReduceUnit{result: first.result, rowIndex: -1}).reducibleByteSize()
	require.Error(t, err)
}

// BenchmarkRetrieveUnitByteSize compares exact arithmetic sizing with a one-row copy.
func BenchmarkRetrieveUnitByteSize(b *testing.B) {
	result := newQueryTestChunk([]int64{10000}, []int64{10000})
	result.FieldsData = append(result.FieldsData, &schemapb.FieldData{
		Type: schemapb.DataType_FloatVector, FieldId: 101,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: 768, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, 768)}},
		}},
	})
	unit := queryReduceUnit{result: result, fieldIndexComputer: typeutil.NewFieldDataIdxComputer(result.GetFieldsData())}
	for _, mode := range []string{"direct", "materialized"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if mode == "direct" {
					if _, err := unit.reducibleByteSize(); err != nil {
						b.Fatal(err)
					}
				} else {
					chunk, err := buildRetrieveChunk([]queryReduceUnit{unit})
					if err != nil {
						b.Fatal(err)
					}
					_ = proto.Size(&internalpb.RetrieveResults{Ids: chunk.GetIds(), FieldsData: chunk.GetFieldsData()})
				}
			}
		})
	}
}
