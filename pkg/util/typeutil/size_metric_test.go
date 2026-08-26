package typeutil

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func TestEstimateMainIndexSizePerRecord(t *testing.T) {
	t.Run("nil schema", func(t *testing.T) {
		size, err := EstimateMainIndexSizePerRecord(nil)
		assert.Error(t, err)
		assert.Zero(t, size)
	})

	t.Run("no vector field", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 2, Name: "tag", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "64"}}},
			},
		}
		size, err := EstimateMainIndexSizePerRecord(schema)
		require.NoError(t, err)
		assert.Zero(t, size)
	})

	t.Run("widest dense vector selected", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 10, Name: "vec8", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}}},
				{FieldID: 11, Name: "vec128", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}}},
				{FieldID: 12, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
			},
		}
		size, err := EstimateMainIndexSizePerRecord(schema)
		require.NoError(t, err)
		assert.Equal(t, 128*4, size)
	})

	t.Run("element size per type", func(t *testing.T) {
		cases := []struct {
			dt     schemapb.DataType
			dim    int
			expect int
		}{
			{schemapb.DataType_FloatVector, 8, 32},
			{schemapb.DataType_Float16Vector, 8, 16},
			{schemapb.DataType_BFloat16Vector, 8, 16},
			{schemapb.DataType_Int8Vector, 8, 8},
			{schemapb.DataType_BinaryVector, 8, 1},
		}
		for _, tc := range cases {
			schema := &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 1, Name: "vec", DataType: tc.dt, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: strconv.Itoa(tc.dim)}}},
				},
			}
			size, err := EstimateMainIndexSizePerRecord(schema)
			require.NoError(t, err)
			assert.Equal(t, tc.expect, size, "type %s", tc.dt.String())
		}
	})

	t.Run("sparse only falls back to zero", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 1, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
			},
		}
		size, err := EstimateMainIndexSizePerRecord(schema)
		require.NoError(t, err)
		assert.Zero(t, size)
	})

	t.Run("ArrayOfVector only falls back to zero", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 1, Name: "vecarr", DataType: schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector,
					TypeParams:  []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}}},
			},
		}
		size, err := EstimateMainIndexSizePerRecord(schema)
		require.NoError(t, err)
		assert.Zero(t, size)
	})
}

func TestEstimateVectorColumnSize(t *testing.T) {
	t.Run("nil and non-vector", func(t *testing.T) {
		_, err := EstimateVectorColumnSize(nil)
		assert.Error(t, err)
		_, err = EstimateVectorColumnSize(&schemapb.FieldData{Type: schemapb.DataType_Int64})
		assert.Error(t, err)
	})

	t.Run("dense float vector", func(t *testing.T) {
		fd := &schemapb.FieldData{
			Type: schemapb.DataType_FloatVector,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: 8,
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{Data: make([]float32, 8*3)},
					},
				},
			},
		}
		size, err := EstimateVectorColumnSize(fd)
		require.NoError(t, err)
		assert.Equal(t, 8*3*4, size)
	})

	t.Run("binary vector", func(t *testing.T) {
		fd := &schemapb.FieldData{
			Type: schemapb.DataType_BinaryVector,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{Dim: 8, Data: &schemapb.VectorField_BinaryVector{BinaryVector: make([]byte, 3)}},
			},
		}
		size, err := EstimateVectorColumnSize(fd)
		require.NoError(t, err)
		assert.Equal(t, 3, size)
	})

	t.Run("sparse vector sums contents", func(t *testing.T) {
		fd := &schemapb.FieldData{
			Type: schemapb.DataType_SparseFloatVector,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_SparseFloatVector{
						SparseFloatVector: &schemapb.SparseFloatArray{Contents: [][]byte{{1, 2, 3}, {4, 5}}},
					},
				},
			},
		}
		size, err := EstimateVectorColumnSize(fd)
		require.NoError(t, err)
		assert.Equal(t, 5, size)
	})

	t.Run("ArrayOfVector sums inner vectors", func(t *testing.T) {
		fd := &schemapb.FieldData{
			Type: schemapb.DataType_ArrayOfVector,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_VectorArray{
						VectorArray: &schemapb.VectorArray{
							ElementType: schemapb.DataType_FloatVector,
							Data: []*schemapb.VectorField{
								{Dim: 8, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, 8)}}},
								{Dim: 8, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, 8*2)}}},
							},
						},
					},
				},
			},
		}
		size, err := EstimateVectorColumnSize(fd)
		require.NoError(t, err)
		assert.Equal(t, (8+16)*4, size)
	})
}

func TestSelectMainIndexField(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 10, Name: "vec_a", DataType: schemapb.DataType_FloatVector},
			{FieldID: 11, Name: "vec_b", DataType: schemapb.DataType_SparseFloatVector},
		},
	}

	t.Run("picks largest actual size", func(t *testing.T) {
		fieldID, ok := SelectMainIndexField(schema, map[int64]int{10: 100, 11: 200})
		require.True(t, ok)
		assert.Equal(t, int64(11), fieldID)
	})

	t.Run("false when no vector field has a measured size", func(t *testing.T) {
		_, ok := SelectMainIndexField(schema, map[int64]int{1: 100})
		assert.False(t, ok)
	})

	t.Run("false for nil schema", func(t *testing.T) {
		_, ok := SelectMainIndexField(nil, map[int64]int{})
		assert.False(t, ok)
	})

	t.Run("single vector field", func(t *testing.T) {
		fieldID, ok := SelectMainIndexField(schema, map[int64]int{10: 42})
		require.True(t, ok)
		assert.Equal(t, int64(10), fieldID)
	})
}

func TestHasVariableSizeVectorField(t *testing.T) {
	assert.False(t, HasVariableSizeVectorField(nil))
	assert.False(t, HasVariableSizeVectorField(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{{FieldID: 1, Name: "vec", DataType: schemapb.DataType_FloatVector}},
	}))
	assert.True(t, HasVariableSizeVectorField(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{{FieldID: 1, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector}},
	}))
	assert.True(t, HasVariableSizeVectorField(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{{FieldID: 1, Name: "vecarr", DataType: schemapb.DataType_ArrayOfVector}},
	}))
	assert.True(t, HasVariableSizeVectorField(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "vec", DataType: schemapb.DataType_FloatVector},
			{FieldID: 2, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
	}))
}
