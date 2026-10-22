// Verify that Query Chunk sizing reuses row payload accounting without copying Units.
package queryutil

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// TestRetrieveUnitByteSizePayload checks payload widths independently of protobuf varints.
func TestRetrieveUnitByteSizePayload(t *testing.T) {
	values := []int64{0, 127, 128, -1, math.MinInt64, math.MaxInt64}
	result := newQueryTestChunk(values, values)
	result.FieldsData = append(result.FieldsData, &schemapb.FieldData{
		Type: schemapb.DataType_FloatVector, FieldId: 101, FieldName: "vector",
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: 768, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, len(values)*768)}},
		}},
	})
	calculator := newRowSizeCalculator(result)
	for row := range values {
		unit := retrieveUnit{result: result, rowIndex: int64(row), sizeCalculator: calculator}
		got, err := unit.reducibleByteSize()
		require.NoError(t, err)
		require.Equal(t, 8+8+768*4, got)
		require.Zero(t, testing.AllocsPerRun(100, func() { _, err = unit.reducibleByteSize() }))
		require.NoError(t, err)
	}
	result.ReqID = 123
	result.AllRetrieveCount = math.MaxInt64
	result.FieldsData[0].FieldName = "metadata does not affect the row payload"
	got, err := (retrieveUnit{result: result, sizeCalculator: calculator}).reducibleByteSize()
	require.NoError(t, err)
	require.Equal(t, 8+8+768*4, got)
}

// TestRetrieveUnitByteSizeMixedFields covers variable fields and compact nullable vectors.
func TestRetrieveUnitByteSizeMixedFields(t *testing.T) {
	result := newQueryTestChunk([]int64{1, 2, 3}, []int64{10, 20, 30})
	result.FieldsData = append(result.FieldsData,
		&schemapb.FieldData{Type: schemapb.DataType_VarChar, FieldId: 102,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a", "", "longer"}}},
			}}},
		&schemapb.FieldData{Type: schemapb.DataType_JSON, FieldId: 103,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte("{}"), nil, []byte("[1]")}}},
			}}},
		&schemapb.FieldData{Type: schemapb.DataType_SparseFloatVector, FieldId: 104, ValidData: []bool{true, false, true},
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{
					Contents: [][]byte{make([]byte, 8), make([]byte, 16)},
				}},
			}}},
	)
	calculator := newRowSizeCalculator(result)
	for row, want := range []int{16 + 1 + 2 + 8, 16, 16 + 6 + 3 + 16} {
		unit := retrieveUnit{result: result, rowIndex: int64(row), sizeCalculator: calculator}
		got, err := unit.reducibleByteSize()
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	result.Ids = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"id", "", "long-id"}}}}
	got, err := (retrieveUnit{result: result, rowIndex: 2, sizeCalculator: calculator}).reducibleByteSize()
	require.NoError(t, err)
	require.Equal(t, 7+8+6+3+16, got)
	result.Ids = nil // Grouped Query Units need not carry a PK.
	got, err = (retrieveUnit{result: result, rowIndex: 2, sizeCalculator: calculator}).reducibleByteSize()
	require.NoError(t, err)
	require.Equal(t, 8+6+3+16, got)
}

// TestRetrieveUnitByteSizeCalculatorLifetime checks reuse and release with the child Chunk.
func TestRetrieveUnitByteSizeCalculatorLifetime(t *testing.T) {
	buffer := &orderedChildBuffer{}
	require.NoError(t, buffer.accept(newQueryTestChunk([]int64{1, 2}, []int64{10, 20})))
	calculator := buffer.sizeCalculator
	first, second := buffer.pop(), buffer.pop()
	require.Same(t, calculator, first.sizeCalculator)
	require.Same(t, calculator, second.sizeCalculator)
	require.Nil(t, buffer.sizeCalculator)
	for _, unit := range []retrieveUnit{first, second} {
		got, err := unit.reducibleByteSize()
		require.NoError(t, err)
		require.Equal(t, 16, got)
	}
	_, err := (retrieveUnit{result: first.result, rowIndex: -1}).reducibleByteSize()
	require.Error(t, err)
}

// BenchmarkRetrieveUnitByteSize compares payload accounting with the former one-row copy.
func BenchmarkRetrieveUnitByteSize(b *testing.B) {
	result := newQueryTestChunk([]int64{10000}, []int64{10000})
	result.FieldsData = append(result.FieldsData, &schemapb.FieldData{
		Type: schemapb.DataType_FloatVector, FieldId: 101,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: 768, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, 768)}},
		}},
	})
	unit := retrieveUnit{result: result, sizeCalculator: newRowSizeCalculator(result)}
	for _, mode := range []string{"existing-helper", "materialized"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if mode == "existing-helper" {
					if _, err := unit.reducibleByteSize(); err != nil {
						b.Fatal(err)
					}
				} else {
					chunk, err := buildRetrieveChunk([]retrieveUnit{unit})
					if err != nil {
						b.Fatal(err)
					}
					_ = proto.Size(&internalpb.RetrieveResults{Ids: chunk.GetIds(), FieldsData: chunk.GetFieldsData()})
				}
			}
		})
	}
}
