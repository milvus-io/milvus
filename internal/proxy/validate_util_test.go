package proxy

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/testutils"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func Test_verifyLengthPerRow(t *testing.T) {
	maxLength := 16

	_, ok := verifyLengthPerRow[string](nil, int64(maxLength))
	assert.True(t, ok)

	_, ok = verifyLengthPerRow[string]([]string{"111111", "22222"}, int64(maxLength))
	assert.True(t, ok)

	row, ok := verifyLengthPerRow[string]([]string{strings.Repeat("1", 20)}, int64(maxLength))
	assert.False(t, ok)
	assert.Equal(t, 0, row)

	row, ok = verifyLengthPerRow[string]([]string{strings.Repeat("1", 20), "222"}, int64(maxLength))
	assert.False(t, ok)
	assert.Equal(t, 0, row)

	row, ok = verifyLengthPerRow[string]([]string{"11111", strings.Repeat("2", 20)}, int64(maxLength))
	assert.False(t, ok)
	assert.Equal(t, 1, row)
}

func Test_validateUtil_checkVarCharFieldData(t *testing.T) {
	t.Run("type mismatch", func(t *testing.T) {
		f := &schemapb.FieldData{}
		v := newValidateUtil()
		assert.Error(t, v.checkVarCharFieldData(f, nil))
	})

	t.Run("max length not found", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"111", "222"},
						},
					},
				},
			},
		}

		fs := &schemapb.FieldSchema{
			DataType: schemapb.DataType_VarChar,
		}

		v := newValidateUtil(withMaxLenCheck())

		err := v.checkVarCharFieldData(f, fs)
		assert.Error(t, err)
	})

	t.Run("length exceeds", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"111", "222"},
						},
					},
				},
			},
		}

		fs := &schemapb.FieldSchema{
			DataType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: "2",
				},
			},
		}

		v := newValidateUtil(withMaxLenCheck())

		err := v.checkVarCharFieldData(f, fs)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"111", "222"},
						},
					},
				},
			},
		}

		fs := &schemapb.FieldSchema{
			DataType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: "4",
				},
			},
		}

		v := newValidateUtil(withMaxLenCheck())

		err := v.checkVarCharFieldData(f, fs)
		assert.NoError(t, err)
	})

	t.Run("no check", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"111", "222"},
						},
					},
				},
			},
		}

		fs := &schemapb.FieldSchema{
			DataType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: "2",
				},
			},
		}

		v := newValidateUtil()

		err := v.checkVarCharFieldData(f, fs)
		assert.NoError(t, err)
	})

	t.Run("when autoID is true, no need to do max length check", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"111", "222"},
						},
					},
				},
			},
		}

		fs := &schemapb.FieldSchema{
			DataType:     schemapb.DataType_VarChar,
			IsPrimaryKey: true,
			AutoID:       true,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: "2",
				},
			},
		}

		v := newValidateUtil(withMaxLenCheck())

		err := v.checkVarCharFieldData(f, fs)
		assert.NoError(t, err)
	})
}

func Test_validateUtil_checkTextFieldData(t *testing.T) {
	t.Run("type mismatch", func(t *testing.T) {
		f := &schemapb.FieldData{}
		v := newValidateUtil()
		assert.Error(t, v.checkTextFieldData(f, nil))
	})

	t.Run("max length not found", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"111", "222"},
						},
					},
				},
			},
		}

		fs := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Text,
		}

		v := newValidateUtil(withMaxLenCheck())

		err := v.checkTextFieldData(f, fs)
		assert.Error(t, err)
	})

	t.Run("length exceeds", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"111", "222"},
						},
					},
				},
			},
		}

		fs := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Text,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: "2",
				},
			},
		}

		v := newValidateUtil(withMaxLenCheck())

		err := v.checkTextFieldData(f, fs)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"111", "222"},
						},
					},
				},
			},
		}

		fs := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Text,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: "4",
				},
			},
		}

		v := newValidateUtil(withMaxLenCheck())

		err := v.checkTextFieldData(f, fs)
		assert.NoError(t, err)
	})

	t.Run("no check", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"111", "222"},
						},
					},
				},
			},
		}

		fs := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Text,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: "2",
				},
			},
		}

		v := newValidateUtil()

		err := v.checkTextFieldData(f, fs)
		assert.NoError(t, err)
	})
}

func Test_validateUtil_checkBinaryVectorFieldData(t *testing.T) {
	t.Run("not binary vector", func(t *testing.T) {
		v := newValidateUtil()
		err := v.checkBinaryVectorFieldData(&schemapb.FieldData{Field: &schemapb.FieldData_Scalars{}}, nil)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		v := newValidateUtil()
		err := v.checkBinaryVectorFieldData(&schemapb.FieldData{Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 128,
				Data: &schemapb.VectorField_BinaryVector{
					BinaryVector: []byte(strings.Repeat("1", 128)),
				},
			},
		}}, nil)
		assert.NoError(t, err)
	})

	t.Run("nil vector not nullable", func(t *testing.T) {
		data := &schemapb.FieldData{
			FieldName: "vec",
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_BinaryVector{
						BinaryVector: nil,
					},
				},
			},
		}
		schema := &schemapb.FieldSchema{
			Name:     "vec",
			DataType: schemapb.DataType_BinaryVector,
			Nullable: false,
		}
		v := newValidateUtil()
		err := v.checkBinaryVectorFieldData(data, schema)
		assert.Error(t, err)
	})

	t.Run("nil vector nullable", func(t *testing.T) {
		data := &schemapb.FieldData{
			FieldName: "vec",
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_BinaryVector{
						BinaryVector: nil,
					},
				},
			},
		}
		schema := &schemapb.FieldSchema{
			Name:     "vec",
			DataType: schemapb.DataType_BinaryVector,
			Nullable: true,
		}
		v := newValidateUtil()
		err := v.checkBinaryVectorFieldData(data, schema)
		assert.NoError(t, err)
	})
}

func Test_validateUtil_checkFloatVectorFieldData(t *testing.T) {
	nb := 5
	dim := int64(8)
	data := testutils.GenerateFloatVectors(nb, int(dim))
	invalidData := testutils.GenerateFloatVectorsWithInvalidData(nb, int(dim))

	t.Run("not float vector", func(t *testing.T) {
		v := newValidateUtil()
		err := v.checkFloatVectorFieldData(&schemapb.FieldData{Field: &schemapb.FieldData_Scalars{}}, nil)
		assert.Error(t, err)
	})

	t.Run("no check", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: invalidData,
						},
					},
				},
			},
		}
		v := newValidateUtil()
		v.checkNAN = false
		err := v.checkFloatVectorFieldData(f, nil)
		assert.NoError(t, err)
	})

	t.Run("has nan", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: invalidData,
						},
					},
				},
			},
		}
		v := newValidateUtil(withNANCheck())
		err := v.checkFloatVectorFieldData(f, nil)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: data,
						},
					},
				},
			},
		}
		v := newValidateUtil(withNANCheck())
		err := v.checkFloatVectorFieldData(f, nil)
		assert.NoError(t, err)
	})

	t.Run("default", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldId:   100,
				FieldName: "vec",
				Type:      schemapb.DataType_FloatVector,
				Field:     &schemapb.FieldData_Vectors{},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "vec",
					DataType:     schemapb.DataType_FloatVector,
					DefaultValue: &schemapb.ValueField{},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()
		err = v.fillWithValue(data, h, 1)
		assert.Error(t, err)
	})

	t.Run("nil vector not nullable", func(t *testing.T) {
		data := &schemapb.FieldData{
			FieldName: "vec",
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: nil,
					},
				},
			},
		}
		schema := &schemapb.FieldSchema{
			Name:     "vec",
			DataType: schemapb.DataType_FloatVector,
			Nullable: false,
		}
		v := newValidateUtil()
		err := v.checkFloatVectorFieldData(data, schema)
		assert.Error(t, err)
	})

	t.Run("nil vector nullable", func(t *testing.T) {
		data := &schemapb.FieldData{
			FieldName: "vec",
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: nil,
					},
				},
			},
		}
		schema := &schemapb.FieldSchema{
			Name:     "vec",
			DataType: schemapb.DataType_FloatVector,
			Nullable: true,
		}

		v := newValidateUtil()
		err := v.checkFloatVectorFieldData(data, schema)
		assert.NoError(t, err)
	})
}

func Test_validateUtil_checkFloat16VectorFieldData(t *testing.T) {
	nb := 5
	dim := int64(8)
	data := testutils.GenerateFloat16Vectors(nb, int(dim))
	invalidData := testutils.GenerateFloat16VectorsWithInvalidData(nb, int(dim))

	t.Run("not float16 vector", func(t *testing.T) {
		v := newValidateUtil()
		err := v.checkFloat16VectorFieldData(&schemapb.FieldData{Field: &schemapb.FieldData_Scalars{}}, nil)
		assert.Error(t, err)
	})

	t.Run("no check", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Float16Vector{
						Float16Vector: invalidData,
					},
				},
			},
		}
		v := newValidateUtil()
		v.checkNAN = false
		err := v.checkFloat16VectorFieldData(f, nil)
		assert.NoError(t, err)
	})

	t.Run("has nan", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Float16Vector{
						Float16Vector: invalidData,
					},
				},
			},
		}
		v := newValidateUtil(withNANCheck())
		err := v.checkFloat16VectorFieldData(f, nil)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Float16Vector{
						Float16Vector: data,
					},
				},
			},
		}
		v := newValidateUtil(withNANCheck())
		err := v.checkFloat16VectorFieldData(f, nil)
		assert.NoError(t, err)
	})

	t.Run("default", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldId:   100,
				FieldName: "vec",
				Type:      schemapb.DataType_Float16Vector,
				Field:     &schemapb.FieldData_Vectors{},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "vec",
					DataType:     schemapb.DataType_Float16Vector,
					DefaultValue: &schemapb.ValueField{},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()
		err = v.fillWithValue(data, h, 1)
		assert.Error(t, err)
	})

	t.Run("nil vector not nullable", func(t *testing.T) {
		data := &schemapb.FieldData{
			FieldName: "vec",
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_Float16Vector{
						Float16Vector: nil,
					},
				},
			},
		}
		schema := &schemapb.FieldSchema{
			Name:     "vec",
			DataType: schemapb.DataType_Float16Vector,
			Nullable: false,
		}
		v := newValidateUtil()
		err := v.checkFloat16VectorFieldData(data, schema)
		assert.Error(t, err)
	})

	t.Run("nil vector nullable", func(t *testing.T) {
		data := &schemapb.FieldData{
			FieldName: "vec",
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_Float16Vector{
						Float16Vector: nil,
					},
				},
			},
		}
		schema := &schemapb.FieldSchema{
			Name:     "vec",
			DataType: schemapb.DataType_Float16Vector,
			Nullable: true,
		}

		v := newValidateUtil()
		err := v.checkFloat16VectorFieldData(data, schema)
		assert.NoError(t, err)
	})
}

func Test_validateUtil_checkBFloat16VectorFieldData(t *testing.T) {
	nb := 5
	dim := int64(8)
	data := testutils.GenerateBFloat16Vectors(nb, int(dim))
	invalidData := testutils.GenerateBFloat16VectorsWithInvalidData(nb, int(dim))

	t.Run("not bfloat16 vector", func(t *testing.T) {
		v := newValidateUtil()
		err := v.checkBFloat16VectorFieldData(&schemapb.FieldData{Field: &schemapb.FieldData_Scalars{}}, nil)
		assert.Error(t, err)
	})

	t.Run("no check", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Bfloat16Vector{
						Bfloat16Vector: invalidData,
					},
				},
			},
		}
		v := newValidateUtil()
		v.checkNAN = false
		err := v.checkBFloat16VectorFieldData(f, nil)
		assert.NoError(t, err)
	})

	t.Run("has nan", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Bfloat16Vector{
						Bfloat16Vector: invalidData,
					},
				},
			},
		}
		v := newValidateUtil(withNANCheck())
		err := v.checkBFloat16VectorFieldData(f, nil)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Bfloat16Vector{
						Bfloat16Vector: data,
					},
				},
			},
		}
		v := newValidateUtil(withNANCheck())
		err := v.checkBFloat16VectorFieldData(f, nil)
		assert.NoError(t, err)
	})

	t.Run("default", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldId:   100,
				FieldName: "vec",
				Type:      schemapb.DataType_BFloat16Vector,
				Field:     &schemapb.FieldData_Vectors{},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "vec",
					DataType:     schemapb.DataType_BFloat16Vector,
					DefaultValue: &schemapb.ValueField{},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()
		err = v.fillWithValue(data, h, 1)
		assert.Error(t, err)
	})

	t.Run("nil vector not nullable", func(t *testing.T) {
		data := &schemapb.FieldData{
			FieldName: "vec",
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_Bfloat16Vector{
						Bfloat16Vector: nil,
					},
				},
			},
		}
		schema := &schemapb.FieldSchema{
			Name:     "vec",
			DataType: schemapb.DataType_BFloat16Vector,
			Nullable: false,
		}
		v := newValidateUtil()
		err := v.checkBFloat16VectorFieldData(data, schema)
		assert.Error(t, err)
	})

	t.Run("nil vector nullable", func(t *testing.T) {
		data := &schemapb.FieldData{
			FieldName: "vec",
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_Bfloat16Vector{
						Bfloat16Vector: nil,
					},
				},
			},
		}
		schema := &schemapb.FieldSchema{
			Name:     "vec",
			DataType: schemapb.DataType_BFloat16Vector,
			Nullable: true,
		}

		v := newValidateUtil()
		err := v.checkBFloat16VectorFieldData(data, schema)
		assert.NoError(t, err)
	})
}

func Test_validateUtil_checkSparseFloatVectorFieldData(t *testing.T) {
	nb := 5
	sparseContents, dim := testutils.GenerateSparseFloatVectorsData(nb)

	t.Run("not sparse float vector", func(t *testing.T) {
		v := newValidateUtil()
		err := v.checkSparseFloatVectorFieldData(&schemapb.FieldData{Field: &schemapb.FieldData_Scalars{}}, nil)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		fieldData := &schemapb.FieldData{
			FieldName: "vec",
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_SparseFloatVector{
						SparseFloatVector: &schemapb.SparseFloatArray{
							Contents: sparseContents,
							Dim:      dim,
						},
					},
				},
			},
		}
		schema := &schemapb.FieldSchema{
			Name:     "vec",
			DataType: schemapb.DataType_SparseFloatVector,
		}
		v := newValidateUtil()
		err := v.checkSparseFloatVectorFieldData(fieldData, schema)
		assert.NoError(t, err)
	})

	t.Run("nil vector not nullable", func(t *testing.T) {
		data := &schemapb.FieldData{
			FieldName: "vec",
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_SparseFloatVector{
						SparseFloatVector: nil,
					},
				},
			},
		}
		schema := &schemapb.FieldSchema{
			Name:     "vec",
			DataType: schemapb.DataType_SparseFloatVector,
			Nullable: false,
		}
		v := newValidateUtil()
		err := v.checkSparseFloatVectorFieldData(data, schema)
		assert.Error(t, err)
	})

	t.Run("nil vector nullable", func(t *testing.T) {
		data := &schemapb.FieldData{
			FieldName: "vec",
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_SparseFloatVector{
						SparseFloatVector: nil,
					},
				},
			},
		}
		schema := &schemapb.FieldSchema{
			Name:     "vec",
			DataType: schemapb.DataType_SparseFloatVector,
			Nullable: true,
		}

		v := newValidateUtil()
		err := v.checkSparseFloatVectorFieldData(data, schema)
		assert.NoError(t, err)
	})
}

func Test_validateUtil_checkInt8VectorFieldData(t *testing.T) {
	nb := 5
	dim := int64(8)
	data := typeutil.Int8ArrayToBytes(testutils.GenerateInt8Vectors(nb, int(dim)))

	t.Run("not int8 vector", func(t *testing.T) {
		v := newValidateUtil()
		err := v.checkInt8VectorFieldData(&schemapb.FieldData{Field: &schemapb.FieldData_Scalars{}}, nil)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		fieldData := &schemapb.FieldData{
			FieldName: "vec",
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: 128,
					Data: &schemapb.VectorField_Int8Vector{
						Int8Vector: data,
					},
				},
			},
		}
		schema := &schemapb.FieldSchema{
			Name:     "vec",
			DataType: schemapb.DataType_Int8Vector,
		}
		v := newValidateUtil()
		err := v.checkInt8VectorFieldData(fieldData, schema)
		assert.NoError(t, err)
	})

	t.Run("nil vector not nullable", func(t *testing.T) {
		fieldData := &schemapb.FieldData{
			FieldName: "vec",
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_Int8Vector{
						Int8Vector: nil,
					},
				},
			},
		}
		schema := &schemapb.FieldSchema{
			Name:     "vec",
			DataType: schemapb.DataType_Int8Vector,
			Nullable: false,
		}
		v := newValidateUtil()
		err := v.checkInt8VectorFieldData(fieldData, schema)
		assert.Error(t, err)
	})

	t.Run("nil vector nullable", func(t *testing.T) {
		fieldData := &schemapb.FieldData{
			FieldName: "vec",
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_Int8Vector{
						Int8Vector: nil,
					},
				},
			},
		}
		schema := &schemapb.FieldSchema{
			Name:     "vec",
			DataType: schemapb.DataType_Int8Vector,
			Nullable: true,
		}

		v := newValidateUtil()
		err := v.checkInt8VectorFieldData(fieldData, schema)
		assert.NoError(t, err)
	})
}

func Test_validateUtil_checkAligned(t *testing.T) {
	t.Run("float vector column not found", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_FloatVector,
			},
		}

		schema := &schemapb.CollectionSchema{}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	t.Run("float vector column dimension not found", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_FloatVector,
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_FloatVector,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	t.Run("field_data dim not match schema dim", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: []float32{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8},
							},
						},
						Dim: 16,
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 1)

		assert.Error(t, err)
	})

	t.Run("invalid num rows", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: []float32{1.1, 2.2},
							},
						},
						Dim: 8,
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	t.Run("num rows mismatch", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: []float32{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8},
							},
						},
						Dim: 8,
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	//////////////////////////////////////////////////////////////////////

	t.Run("binary vector column not found", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_BinaryVector,
			},
		}

		schema := &schemapb.CollectionSchema{}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	t.Run("binary vector column dimension not found", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_BinaryVector,
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_BinaryVector,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	t.Run("field data dim not match schema dim", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_BinaryVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_BinaryVector{
							BinaryVector: []byte("66666666"),
						},
						Dim: 128,
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_BinaryVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	t.Run("invalid num rows", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_BinaryVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_BinaryVector{
							BinaryVector: []byte("not128"),
						},
						Dim: 128,
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_BinaryVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "128",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	t.Run("num rows mismatch", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_BinaryVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_BinaryVector{
							BinaryVector: []byte{'1', '2'},
						},
						Dim: 8,
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_BinaryVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	//////////////////////////////////////////////////////////////////////

	t.Run("float16 vector column not found", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Float16Vector,
			},
		}

		schema := &schemapb.CollectionSchema{}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	t.Run("float16 vector column dimension not found", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Float16Vector,
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Float16Vector,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	t.Run("invalid num rows", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Float16Vector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_Float16Vector{
							Float16Vector: []byte("not128"),
						},
						Dim: 128,
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Float16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "128",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	t.Run("num rows mismatch", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Float16Vector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_Float16Vector{
							Float16Vector: []byte{'1', '2'},
						},
						Dim: 2,
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Float16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "2",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	t.Run("field_data dim not match schema dim", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Float16Vector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_Float16Vector{
							Float16Vector: []byte{'1', '2', '3', '4', '5', '6'},
						},
						Dim: 16,
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Float16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "3",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 1)

		assert.Error(t, err)
	})

	//////////////////////////////////////////////////////////////////////

	t.Run("bfloat16 vector column not found", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_BFloat16Vector,
			},
		}

		schema := &schemapb.CollectionSchema{}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	t.Run("bfloat16 vector column dimension not found", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_BFloat16Vector,
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_BFloat16Vector,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	t.Run("field_data dim not match schema dim", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_BFloat16Vector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_Bfloat16Vector{
							Bfloat16Vector: []byte{'1', '2', '3', '4', '5', '6'},
						},
						Dim: 16,
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_BFloat16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "3",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 1)

		assert.Error(t, err)
	})

	t.Run("invalid num rows", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_BFloat16Vector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_Bfloat16Vector{
							Bfloat16Vector: []byte("not128"),
						},
						Dim: 128,
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_BFloat16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "128",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	t.Run("num rows mismatch", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_BFloat16Vector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_Bfloat16Vector{
							Bfloat16Vector: []byte{'1', '2'},
						},
						Dim: 2,
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_BFloat16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "2",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	//////////////////////////////////////////////////////////////////
	t.Run("int8 vector column not found", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int8Vector,
			},
		}

		schema := &schemapb.CollectionSchema{}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	t.Run("int8 vector column dimension not found", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int8Vector,
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int8Vector,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	t.Run("field_data dim not match schema dim", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int8Vector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_Int8Vector{
							Int8Vector: []byte{1, 2, 3, 4, 5, 6, 7, 8},
						},
						Dim: 16,
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int8Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 1)

		assert.Error(t, err)
	})

	t.Run("invalid num rows", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int8Vector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_Int8Vector{
							Int8Vector: []byte{1, 2},
						},
						Dim: 8,
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int8Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	t.Run("num rows mismatch", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int8Vector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_Int8Vector{
							Int8Vector: []byte{1, 2, 3, 4, 5, 6, 7, 8},
						},
						Dim: 8,
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int8Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	//////////////////////////////////////////////////////////////////

	t.Run("column not found", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{"111", "222"},
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	t.Run("mismatch", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{"111", "222"},
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxLengthKey,
							Value: "8",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 100)

		assert.Error(t, err)
	})

	/////////////////////////////////////////////////////////////////////

	t.Run("length of data is incorrect when nullable", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{"111", "222"},
							},
						},
					},
				},
				ValidData: []bool{false, false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxLengthKey,
							Value: "8",
						},
					},
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 3)

		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test1",
				Type:      schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: testutils.GenerateFloatVectors(10, 8),
							},
						},
						Dim: 8,
					},
				},
			},
			{
				FieldName: "test2",
				Type:      schemapb.DataType_BinaryVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_BinaryVector{
							BinaryVector: testutils.GenerateBinaryVectors(10, 8),
						},
						Dim: 8,
					},
				},
			},
			{
				FieldName: "test3",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: testutils.GenerateVarCharArray(10, 8),
							},
						},
					},
				},
			},
			{
				FieldName: "test4",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: testutils.GenerateVarCharArray(10, 8),
							},
						},
					},
				},
				ValidData: []bool{true, true, false, false, false, false, false, false, false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test1",
					FieldID:  101,
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
				{
					Name:     "test2",
					FieldID:  102,
					DataType: schemapb.DataType_BinaryVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
				{
					Name:     "test3",
					FieldID:  103,
					DataType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxLengthKey,
							Value: "8",
						},
					},
				},
				{
					Name:     "test4",
					FieldID:  104,
					DataType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxLengthKey,
							Value: "8",
						},
					},
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 10)

		assert.NoError(t, err)
	})

	t.Run("nullable float vector all null", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_FloatVector,
				ValidData: []bool{false, false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 3)

		assert.NoError(t, err)
	})

	t.Run("nullable float vector partial null", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, // 2 vectors of dim 8
							},
						},
						Dim: 8,
					},
				},
				ValidData: []bool{true, false, true, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 4)

		assert.NoError(t, err)
	})

	t.Run("nullable float vector mismatch valid count", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: []float32{1, 2, 3, 4, 5, 6, 7, 8}, // only 1 vector
							},
						},
						Dim: 8,
					},
				},
				ValidData: []bool{true, false, true, false}, // 2 valid
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 4)

		assert.Error(t, err)
	})

	t.Run("nullable binary vector all null", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_BinaryVector,
				ValidData: []bool{false, false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_BinaryVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 3)

		assert.NoError(t, err)
	})

	t.Run("nullable float16 vector all null", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Float16Vector,
				ValidData: []bool{false, false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Float16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 3)

		assert.NoError(t, err)
	})

	t.Run("nullable bfloat16 vector all null", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_BFloat16Vector,
				ValidData: []bool{false, false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_BFloat16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 3)

		assert.NoError(t, err)
	})

	t.Run("nullable int8 vector all null", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int8Vector,
				ValidData: []bool{false, false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int8Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 3)

		assert.NoError(t, err)
	})

	t.Run("nullable sparse vector all null", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_SparseFloatVector,
				ValidData: []bool{false, false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_SparseFloatVector,
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.checkAligned(data, h, 3)

		assert.NoError(t, err)
	})
}

func Test_validateUtil_Validate(t *testing.T) {
	paramtable.Init()

	t.Run("nil schema", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_FloatVector,
			},
		}

		v := newValidateUtil()

		err := v.Validate(data, nil, 100)

		assert.Error(t, err)
	})

	t.Run("not aligned", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{"111", "222"},
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxLengthKey,
							Value: "8",
						},
					},
				},
			},
		}

		v := newValidateUtil()
		helper, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		err = v.Validate(data, helper, 100)

		assert.Error(t, err)
	})

	t.Run("has nan", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test1",
				Type:      schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: []float32{float32(math.NaN()), float32(math.NaN())},
							},
						},
					},
				},
			},
			{
				FieldName: "test2",
				Type:      schemapb.DataType_BinaryVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_BinaryVector{
							BinaryVector: testutils.GenerateBinaryVectors(2, 8),
						},
					},
				},
			},
			{
				FieldName: "test3",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: testutils.GenerateVarCharArray(2, 8),
							},
						},
					},
				},
			},
			{
				FieldName: "test4",
				Type:      schemapb.DataType_Float16Vector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_Float16Vector{
							Float16Vector: testutils.GenerateFloat16Vectors(2, 8),
						},
					},
				},
			},
			{
				FieldName: "test5",
				Type:      schemapb.DataType_BFloat16Vector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_Bfloat16Vector{
							Bfloat16Vector: testutils.GenerateBFloat16Vectors(2, 8),
						},
					},
				},
			},
			{
				FieldName: "test6",
				Type:      schemapb.DataType_Int8Vector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_Int8Vector{
							Int8Vector: typeutil.Int8ArrayToBytes(testutils.GenerateInt8Vectors(2, 8)),
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test1",
					FieldID:  101,
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "1",
						},
					},
				},
				{
					Name:     "test2",
					FieldID:  102,
					DataType: schemapb.DataType_BinaryVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
				{
					Name:     "test3",
					FieldID:  103,
					DataType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxLengthKey,
							Value: "8",
						},
					},
				},
				{
					Name:     "test4",
					FieldID:  104,
					DataType: schemapb.DataType_Float16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
				{
					Name:     "test5",
					FieldID:  105,
					DataType: schemapb.DataType_BFloat16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
				{
					Name:     "test6",
					FieldID:  106,
					DataType: schemapb.DataType_Int8Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
			},
		}

		v := newValidateUtil(withNANCheck(), withMaxLenCheck())
		helper, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		err = v.Validate(data, helper, 2)

		assert.Error(t, err)
	})

	t.Run("length exceeds", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test1",
				Type:      schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: testutils.GenerateFloatVectors(2, 1),
							},
						},
					},
				},
			},
			{
				FieldName: "test2",
				Type:      schemapb.DataType_BinaryVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_BinaryVector{
							BinaryVector: testutils.GenerateBinaryVectors(2, 8),
						},
					},
				},
			},
			{
				FieldName: "test3",
				Type:      schemapb.DataType_Float16Vector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_Float16Vector{
							Float16Vector: testutils.GenerateFloat16Vectors(2, 8),
						},
					},
				},
			},
			{
				FieldName: "test4",
				Type:      schemapb.DataType_BFloat16Vector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_Bfloat16Vector{
							Bfloat16Vector: testutils.GenerateBFloat16Vectors(2, 8),
						},
					},
				},
			},
			{
				FieldName: "test5",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{"very_long", "very_very_long"},
							},
						},
					},
				},
			},
			{
				FieldName: "test6",
				Type:      schemapb.DataType_Int8Vector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_Int8Vector{
							Int8Vector: typeutil.Int8ArrayToBytes(testutils.GenerateInt8Vectors(2, 8)),
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test1",
					FieldID:  101,
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "1",
						},
					},
				},
				{
					Name:     "test2",
					FieldID:  102,
					DataType: schemapb.DataType_BinaryVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
				{
					Name:     "test3",
					FieldID:  103,
					DataType: schemapb.DataType_Float16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
				{
					Name:     "test4",
					FieldID:  104,
					DataType: schemapb.DataType_BFloat16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
				{
					Name:     "test5",
					FieldID:  105,
					DataType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxLengthKey,
							Value: "2",
						},
					},
				},
				{
					Name:     "test6",
					FieldID:  106,
					DataType: schemapb.DataType_Int8Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
			},
		}

		v := newValidateUtil(withNANCheck(), withMaxLenCheck())
		helper, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)
		err = v.Validate(data, helper, 2)
		assert.Error(t, err)

		// Validate JSON length
		longBytes := make([]byte, paramtable.Get().CommonCfg.JSONMaxLength.GetAsInt()+1)
		data = []*schemapb.FieldData{
			{
				FieldName: "json",
				Type:      schemapb.DataType_JSON,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: [][]byte{longBytes, longBytes},
							},
						},
					},
				},
			},
		}
		schema = &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "json",
					FieldID:  104,
					DataType: schemapb.DataType_JSON,
				},
			},
		}
		helper, err = typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)
		err = v.Validate(data, helper, 2)
		assert.Error(t, err)
	})

	t.Run("has overflow", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test1",
				Type:      schemapb.DataType_Int8,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: []int32{int32(math.MinInt8) - 1, int32(math.MaxInt8) + 1},
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test1",
					FieldID:  101,
					DataType: schemapb.DataType_Int8,
				},
			},
		}

		v := newValidateUtil(withOverflowCheck())
		helper, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)
		err = v.Validate(data, helper, 2)
		assert.Error(t, err)
	})

	t.Run("array data nil", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: nil,
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int64,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxCapacityKey,
							Value: "8",
						},
					},
				},
			},
		}

		v := newValidateUtil()
		helper, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		err = v.Validate(data, helper, 100)

		assert.Error(t, err)
	})

	t.Run("exceed max capacity", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								Data: []*schemapb.ScalarField{
									{
										Data: &schemapb.ScalarField_LongData{
											LongData: &schemapb.LongArray{
												Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int64,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxCapacityKey,
							Value: "2",
						},
					},
				},
			},
		}

		v := newValidateUtil(withMaxCapCheck())
		helper, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		err = v.Validate(data, helper, 1)

		assert.Error(t, err)
	})

	t.Run("string element exceed max length", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								Data: []*schemapb.ScalarField{
									{
										Data: &schemapb.ScalarField_StringData{
											StringData: &schemapb.StringArray{
												Data: []string{"abcdefghijkl", "ajsgfuioabaxyaefilagskjfhgka"},
											},
										},
									},
								},
								ElementType: schemapb.DataType_VarChar,
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxCapacityKey,
							Value: "10",
						},
						{
							Key:   common.MaxLengthKey,
							Value: "5",
						},
					},
				},
			},
		}

		v := newValidateUtil(withMaxCapCheck(), withMaxLenCheck())
		helper, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		err = v.Validate(data, helper, 1)

		assert.Error(t, err)
	})

	t.Run("no max capacity", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								Data: []*schemapb.ScalarField{
									{
										Data: &schemapb.ScalarField_LongData{
											LongData: &schemapb.LongArray{
												Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int64,
					TypeParams:  []*commonpb.KeyValuePair{},
				},
			},
		}

		v := newValidateUtil(withMaxCapCheck())
		helper, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		err = v.Validate(data, helper, 1)

		assert.Error(t, err)
	})

	t.Run("unsupported element type", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								Data: []*schemapb.ScalarField{
									{
										Data: &schemapb.ScalarField_LongData{
											LongData: &schemapb.LongArray{
												Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_JSON,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxCapacityKey,
							Value: "8",
						},
					},
				},
			},
		}

		v := newValidateUtil(withMaxCapCheck())
		helper, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		err = v.Validate(data, helper, 1)

		assert.Error(t, err)
	})

	t.Run("element type not match", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								Data: []*schemapb.ScalarField{
									{
										Data: &schemapb.ScalarField_BoolData{
											BoolData: &schemapb.BoolArray{
												Data: []bool{true, false},
											},
										},
									},
									{
										Data: &schemapb.ScalarField_LongData{
											LongData: &schemapb.LongArray{
												Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Bool,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxCapacityKey,
							Value: "100",
						},
					},
				},
			},
		}

		v := newValidateUtil(withMaxCapCheck())
		helper, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)
		err = v.Validate(data, helper, 1)
		assert.Error(t, err)

		data = []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								Data: []*schemapb.ScalarField{
									{
										Data: &schemapb.ScalarField_IntData{
											IntData: &schemapb.IntArray{
												Data: []int32{1, 2, 3},
											},
										},
									},
									{
										Data: &schemapb.ScalarField_LongData{
											LongData: &schemapb.LongArray{
												Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		schema = &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int8,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxCapacityKey,
							Value: "100",
						},
					},
				},
			},
		}
		helper, err = typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		err = newValidateUtil(withMaxCapCheck()).Validate(data, helper, 1)
		assert.Error(t, err)

		schema = &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int16,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxCapacityKey,
							Value: "100",
						},
					},
				},
			},
		}
		helper, err = typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		err = newValidateUtil(withMaxCapCheck()).Validate(data, helper, 1)
		assert.Error(t, err)

		schema = &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int32,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxCapacityKey,
							Value: "100",
						},
					},
				},
			},
		}

		helper, err = typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		err = newValidateUtil(withMaxCapCheck()).Validate(data, helper, 1)
		assert.Error(t, err)

		data = []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								Data: []*schemapb.ScalarField{
									{
										Data: &schemapb.ScalarField_FloatData{
											FloatData: &schemapb.FloatArray{
												Data: []float32{1, 2, 3},
											},
										},
									},
									{
										Data: &schemapb.ScalarField_LongData{
											LongData: &schemapb.LongArray{
												Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		schema = &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Float,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxCapacityKey,
							Value: "100",
						},
					},
				},
			},
		}
		helper, err = typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)
		err = newValidateUtil(withMaxCapCheck()).Validate(data, helper, 1)
		assert.Error(t, err)

		data = []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								Data: []*schemapb.ScalarField{
									{
										Data: &schemapb.ScalarField_DoubleData{
											DoubleData: &schemapb.DoubleArray{
												Data: []float64{1, 2, 3},
											},
										},
									},
									{
										Data: &schemapb.ScalarField_LongData{
											LongData: &schemapb.LongArray{
												Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		schema = &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Double,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxCapacityKey,
							Value: "100",
						},
					},
				},
			},
		}
		helper, err = typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		err = newValidateUtil(withMaxCapCheck()).Validate(data, helper, 1)
		assert.Error(t, err)

		data = []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								Data: []*schemapb.ScalarField{
									{
										Data: &schemapb.ScalarField_StringData{
											StringData: &schemapb.StringArray{
												Data: []string{"a", "b", "c"},
											},
										},
									},
									{
										Data: &schemapb.ScalarField_LongData{
											LongData: &schemapb.LongArray{
												Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		schema = &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxCapacityKey,
							Value: "100",
						},
					},
				},
			},
		}
		helper, err = typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		err = newValidateUtil(withMaxCapCheck()).Validate(data, helper, 1)
		assert.Error(t, err)

		data = []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								Data: []*schemapb.ScalarField{
									{
										Data: &schemapb.ScalarField_LongData{
											LongData: &schemapb.LongArray{
												Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
											},
										},
									},
									{
										Data: &schemapb.ScalarField_FloatData{
											FloatData: &schemapb.FloatArray{
												Data: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		schema = &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int64,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxCapacityKey,
							Value: "100",
						},
					},
				},
			},
		}
		helper, err = typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		err = newValidateUtil(withMaxCapCheck()).Validate(data, helper, 1)
		assert.Error(t, err)
	})

	t.Run("array element overflow", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								Data: []*schemapb.ScalarField{
									{
										Data: &schemapb.ScalarField_IntData{
											IntData: &schemapb.IntArray{
												Data: []int32{1, 2, 3, 1 << 9},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int8,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxCapacityKey,
							Value: "100",
						},
					},
				},
			},
		}
		helper, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)
		err = newValidateUtil(withMaxCapCheck(), withOverflowCheck()).Validate(data, helper, 1)
		assert.Error(t, err)

		data = []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								Data: []*schemapb.ScalarField{
									{
										Data: &schemapb.ScalarField_IntData{
											IntData: &schemapb.IntArray{
												Data: []int32{1, 2, 3, 1 << 9, 1 << 17},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		schema = &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int16,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxCapacityKey,
							Value: "100",
						},
					},
				},
			},
		}
		helper, err = typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		err = newValidateUtil(withMaxCapCheck(), withOverflowCheck()).Validate(data, helper, 1)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test1",
				Type:      schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: 8,
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: testutils.GenerateFloatVectors(2, 8),
							},
						},
					},
				},
			},
			{
				FieldName: "test2",
				Type:      schemapb.DataType_BinaryVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: 8,
						Data: &schemapb.VectorField_BinaryVector{
							BinaryVector: testutils.GenerateBinaryVectors(2, 8),
						},
					},
				},
			},
			{
				FieldName: "test3",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: testutils.GenerateVarCharArray(2, 8),
							},
						},
					},
				},
			},
			{
				FieldName: "test4",
				Type:      schemapb.DataType_JSON,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: [][]byte{[]byte("{}"), []byte("{}")},
							},
						},
					},
				},
			},
			{
				FieldName: "test5",
				Type:      schemapb.DataType_Int8,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: []int32{int32(math.MinInt8) + 1, int32(math.MaxInt8) - 1},
							},
						},
					},
				},
			},
			{
				FieldName: "bool_array",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								Data: []*schemapb.ScalarField{
									{
										Data: &schemapb.ScalarField_BoolData{
											BoolData: &schemapb.BoolArray{
												Data: []bool{true, true},
											},
										},
									},
									{
										Data: &schemapb.ScalarField_BoolData{
											BoolData: &schemapb.BoolArray{
												Data: []bool{false, false},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			{
				FieldName: "int_array",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								Data: []*schemapb.ScalarField{
									{
										Data: &schemapb.ScalarField_IntData{
											IntData: &schemapb.IntArray{
												Data: []int32{1, 2, 3},
											},
										},
									},
									{
										Data: &schemapb.ScalarField_IntData{
											IntData: &schemapb.IntArray{
												Data: []int32{4, 5, 6},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			{
				FieldName: "long_array",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								Data: []*schemapb.ScalarField{
									{
										Data: &schemapb.ScalarField_LongData{
											LongData: &schemapb.LongArray{
												Data: []int64{1, 2, 3},
											},
										},
									},
									{
										Data: &schemapb.ScalarField_LongData{
											LongData: &schemapb.LongArray{
												Data: []int64{4, 5, 6},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			{
				FieldName: "string_array",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								Data: []*schemapb.ScalarField{
									{
										Data: &schemapb.ScalarField_StringData{
											StringData: &schemapb.StringArray{
												Data: []string{"abc", "def"},
											},
										},
									},
									{
										Data: &schemapb.ScalarField_StringData{
											StringData: &schemapb.StringArray{
												Data: []string{"hij", "jkl"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			{
				FieldName: "float_array",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								Data: []*schemapb.ScalarField{
									{
										Data: &schemapb.ScalarField_FloatData{
											FloatData: &schemapb.FloatArray{
												Data: []float32{1.1, 2.2, 3.3},
											},
										},
									},
									{
										Data: &schemapb.ScalarField_FloatData{
											FloatData: &schemapb.FloatArray{
												Data: []float32{4.4, 5.5, 6.6},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			{
				FieldName: "double_array",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								Data: []*schemapb.ScalarField{
									{
										Data: &schemapb.ScalarField_DoubleData{
											DoubleData: &schemapb.DoubleArray{
												Data: []float64{1.2, 2.3, 3.4},
											},
										},
									},
									{
										Data: &schemapb.ScalarField_DoubleData{
											DoubleData: &schemapb.DoubleArray{
												Data: []float64{4.5, 5.6, 6.7},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			{
				FieldName: "test6",
				Type:      schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{(math.MinInt8) + 1, (math.MaxInt8) - 1},
							},
						},
					},
				},
			},
			{
				FieldName: "test7",
				Type:      schemapb.DataType_Float,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{
								Data: testutils.GenerateFloat32Array(2),
							},
						},
					},
				},
			},
			{
				FieldName: "test8",
				Type:      schemapb.DataType_Double,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: testutils.GenerateFloat64Array(2),
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test1",
					FieldID:  101,
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
				{
					Name:     "test2",
					FieldID:  102,
					DataType: schemapb.DataType_BinaryVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
				{
					Name:     "test3",
					FieldID:  103,
					DataType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxLengthKey,
							Value: "8",
						},
					},
				},
				{
					Name:     "test4",
					FieldID:  104,
					DataType: schemapb.DataType_JSON,
				},
				{
					Name:     "test5",
					FieldID:  105,
					DataType: schemapb.DataType_Int8,
				},
				{
					Name:        "bool_array",
					FieldID:     106,
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Bool,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxCapacityKey,
							Value: "10",
						},
					},
				},
				{
					Name:        "int_array",
					FieldID:     107,
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int16,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxCapacityKey,
							Value: "10",
						},
					},
				},
				{
					Name:        "long_array",
					FieldID:     108,
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int64,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxCapacityKey,
							Value: "10",
						},
					},
				},
				{
					Name:        "string_array",
					FieldID:     109,
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxCapacityKey,
							Value: "10",
						},
						{
							Key:   common.MaxLengthKey,
							Value: "10",
						},
					},
				},
				{
					Name:        "float_array",
					FieldID:     110,
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Float,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxCapacityKey,
							Value: "10",
						},
					},
				},
				{
					Name:        "double_array",
					FieldID:     111,
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Double,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxCapacityKey,
							Value: "10",
						},
					},
				},
				{
					Name:     "test6",
					FieldID:  112,
					DataType: schemapb.DataType_Int64,
				},
				{
					Name:     "test7",
					FieldID:  113,
					DataType: schemapb.DataType_Float,
				},
				{
					Name:     "test8",
					FieldID:  114,
					DataType: schemapb.DataType_Double,
				},
			},
		}

		v := newValidateUtil(withNANCheck(), withMaxLenCheck(), withOverflowCheck(), withMaxCapCheck())
		helper, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		err = v.Validate(data, helper, 2)

		assert.NoError(t, err)
	})
}

func checkfillWithValueData[T comparable](values []T, v T, length int) bool {
	if len(values) != length {
		return false
	}
	for i := 0; i < length; i++ {
		if values[i] != v {
			return false
		}
	}

	return true
}

func checkJsonfillWithValueData(values [][]byte, v []byte, length int) (bool, error) {
	if len(values) != length {
		return false, nil
	}
	var obj map[string]interface{}
	err := json.Unmarshal(v, &obj)
	if err != nil {
		return false, err
	}

	for i := 0; i < length; i++ {
		var value map[string]interface{}
		err := json.Unmarshal(values[i], &value)
		if err != nil {
			return false, err
		}
		if !reflect.DeepEqual(value, obj) {
			return false, nil
		}
	}

	return true, nil
}

func Test_validateUtil_fillWithValue(t *testing.T) {
	t.Run("bool scalars schema not found", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
			},
		}

		schema := &schemapb.CollectionSchema{}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.Error(t, err)
	})

	t.Run("the length of bool scalars is wrong when nullable", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: []bool{},
							},
						},
					},
				},
				ValidData: []bool{false, true},
			},
		}
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("the length of bool scalars is wrong when has default_value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: []bool{},
							},
						},
					},
				},
				ValidData: []bool{false, true},
			},
		}
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name: "test",
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_BoolData{
							BoolData: false,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("bool scalars has no data, will fill null value null value according to validData", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: []bool{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetBoolData().Data, false, 2)
		assert.True(t, flag)
	})

	t.Run("bool scalars has no data, and schema default value is legal", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: []bool{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}

		key := true
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name: "test",
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_BoolData{
							BoolData: key,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetBoolData().Data, schema.Fields[0].GetDefaultValue().GetBoolData(), 2)
		assert.Equal(t, 0, len(data[0].GetValidData()))
		assert.True(t, flag)
	})

	t.Run("bool scalars schema set both nullable==true and default value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: []bool{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}

		key := true
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name: "test",
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_BoolData{
							BoolData: key,
						},
					},
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetBoolData().Data, schema.Fields[0].GetDefaultValue().GetBoolData(), 2)
		assert.Equal(t, []bool{true, true}, data[0].GetValidData())
		assert.True(t, flag)
	})

	t.Run("bool scalars has no data, but validData length is wrong when fill default value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: []bool{},
							},
						},
					},
				},
				ValidData: []bool{true, true},
			},
		}

		key := true
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_BinaryVector,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_BoolData{
							BoolData: key,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 3)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("bool scalars has data, and schema default value is not set", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: []bool{true},
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_BinaryVector,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		flag := checkfillWithValueData(data[0].GetScalars().GetBoolData().Data, true, 1)
		assert.True(t, flag)

		assert.NoError(t, err)
	})

	t.Run("bool scalars has part of data, and schema default value is legal", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: []bool{true},
							},
						},
					},
				},
				ValidData: []bool{false, true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_BinaryVector,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_BoolData{
							BoolData: true,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetBoolData().Data, true, 2)
		assert.True(t, flag)
	})

	t.Run("bool scalars has data, and schema default value is legal", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: []bool{true},
							},
						},
					},
				},
				ValidData: []bool{true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_BinaryVector,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_BoolData{
							BoolData: false,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetBoolData().Data, true, 1)
		assert.True(t, flag)
	})

	t.Run("bool scalars has data, and no need to fill when nullable", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: []bool{true},
							},
						},
					},
				},
				ValidData: []bool{true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetBoolData().Data, true, 1)
		assert.True(t, flag)
	})

	////////////////////////////////////////////////////////////////////

	t.Run("int scalars schema not found", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int32,
			},
		}

		schema := &schemapb.CollectionSchema{}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.Error(t, err)
	})

	t.Run("the length of int scalars is wrong when nullable", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: []int32{},
							},
						},
					},
				},
				ValidData: []bool{false, true},
			},
		}
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("the length of int scalars is wrong when has default_value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: []int32{},
							},
						},
					},
				},
				ValidData: []bool{false, true},
			},
		}
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name: "test",
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_IntData{
							IntData: 1,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("int scalars has no data, will fill null value according to validData", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: []int32{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int32,
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetIntData().Data, 0, 2)
		assert.True(t, flag)
	})

	t.Run("int scalars has no data, and schema default value is legal", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: []int32{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int32,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_IntData{
							IntData: 1,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetIntData().Data, schema.Fields[0].GetDefaultValue().GetIntData(), 2)
		assert.Equal(t, 0, len(data[0].GetValidData()))
		assert.True(t, flag)
	})

	t.Run("int scalars schema set both nullable==true and default value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: []int32{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int32,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_IntData{
							IntData: 1,
						},
					},
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetIntData().Data, schema.Fields[0].GetDefaultValue().GetIntData(), 2)
		assert.Equal(t, []bool{true, true}, data[0].GetValidData())
		assert.True(t, flag)
	})

	t.Run("int scalars has no data, but validData length is wrong when fill default value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: []int32{},
							},
						},
					},
				},
				ValidData: []bool{true, true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int32,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_IntData{
							IntData: 1,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 3)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("int scalars has data, and schema default value is not set", func(t *testing.T) {
		intData := []int32{1}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: intData,
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int32,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetIntData().Data, intData[0], 1)
		assert.True(t, flag)
	})

	t.Run("int scalars has part of data, and schema default value is legal", func(t *testing.T) {
		intData := []int32{1}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: intData,
							},
						},
					},
				},
				ValidData: []bool{false, true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int32,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_IntData{
							IntData: 1,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetIntData().Data, intData[0], 2)
		assert.True(t, flag)
	})

	t.Run("int scalars has data, and schema default value is legal", func(t *testing.T) {
		intData := []int32{1}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: intData,
							},
						},
					},
				},
				ValidData: []bool{true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int32,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_IntData{
							IntData: 2,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetIntData().Data, intData[0], 1)
		assert.True(t, flag)
	})

	t.Run("int scalars has data, and no need to fill when nullable", func(t *testing.T) {
		intData := []int32{1}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: intData,
							},
						},
					},
				},
				ValidData: []bool{true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int32,
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetIntData().Data, intData[0], 1)
		assert.True(t, flag)
	})
	//////////////////////////////////////////////////////////////////

	t.Run("long scalars schema not found", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int64,
			},
		}

		schema := &schemapb.CollectionSchema{}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.Error(t, err)
	})

	t.Run("the length of long scalars is wrong when nullable", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{},
							},
						},
					},
				},
				ValidData: []bool{false, true},
			},
		}
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("the length of long scalars is wrong when has default_value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{},
							},
						},
					},
				},
				ValidData: []bool{false, true},
			},
		}
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name: "test",
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_LongData{
							LongData: 1,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("long scalars has no data, will fill null value according to validData", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int32,
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)
		flag := checkfillWithValueData(data[0].GetScalars().GetLongData().Data, 0, 2)
		assert.True(t, flag)
	})

	t.Run("long scalars has no data, and schema default value is legal", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int32,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_LongData{
							LongData: 1,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)
		flag := checkfillWithValueData(data[0].GetScalars().GetLongData().Data, schema.Fields[0].GetDefaultValue().GetLongData(), 2)
		assert.Equal(t, 0, len(data[0].GetValidData()))
		assert.True(t, flag)
	})

	t.Run("long scalars schema set both nullable==true and default value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int32,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_LongData{
							LongData: 1,
						},
					},
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)
		flag := checkfillWithValueData(data[0].GetScalars().GetLongData().Data, schema.Fields[0].GetDefaultValue().GetLongData(), 2)
		assert.Equal(t, []bool{true, true}, data[0].GetValidData())
		assert.True(t, flag)
	})

	t.Run("long scalars has no data, but validData length is wrong when fill default value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{1},
							},
						},
					},
				},
				ValidData: []bool{true, true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int32,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_LongData{
							LongData: 1,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 3)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("long scalars has data, and schema default value is not set", func(t *testing.T) {
		longData := []int64{1}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: longData,
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int64,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)
		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetLongData().Data, longData[0], 1)
		assert.True(t, flag)
	})

	t.Run("long scalars has part of data, and schema default value is legal", func(t *testing.T) {
		longData := []int64{1}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: longData,
							},
						},
					},
				},
				ValidData: []bool{false, true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int64,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_LongData{
							LongData: 1,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetLongData().Data, longData[0], 2)
		assert.True(t, flag)
	})

	t.Run("long scalars has data, and schema default value is legal", func(t *testing.T) {
		longData := []int64{1}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: longData,
							},
						},
					},
				},
				ValidData: []bool{true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int64,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_LongData{
							LongData: 2,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetLongData().Data, longData[0], 1)
		assert.True(t, flag)
	})

	t.Run("long scalars has data, and no need to fill when nullable", func(t *testing.T) {
		longData := []int64{1}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: longData,
							},
						},
					},
				},
				ValidData: []bool{true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int64,
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetLongData().Data, longData[0], 1)
		assert.True(t, flag)
	})

	////////////////////////////////////////////////////////////////////

	t.Run("float scalars schema not found", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Float,
			},
		}

		schema := &schemapb.CollectionSchema{}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.Error(t, err)
	})

	t.Run("the length of float scalars is wrong when nullable", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Float,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{
								Data: []float32{},
							},
						},
					},
				},
				ValidData: []bool{false, true},
			},
		}
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("the length of float scalars is wrong when has default_value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Float,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{
								Data: []float32{},
							},
						},
					},
				},
				ValidData: []bool{false, true},
			},
		}
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name: "test",
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_FloatData{
							FloatData: 1,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("float scalars has no data, will fill null value according to validData", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Float,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{
								Data: []float32{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Float,
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetFloatData().Data, 0, 2)
		assert.True(t, flag)
	})

	t.Run("float scalars has no data, and schema default value is legal", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Float,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{
								Data: []float32{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Float,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_FloatData{
							FloatData: 1,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetFloatData().Data, schema.Fields[0].GetDefaultValue().GetFloatData(), 2)
		assert.Equal(t, 0, len(data[0].GetValidData()))
		assert.True(t, flag)
	})

	t.Run("float scalars schema set both nullable==true and default value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Float,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{
								Data: []float32{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Float,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_FloatData{
							FloatData: 1,
						},
					},
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetFloatData().Data, schema.Fields[0].GetDefaultValue().GetFloatData(), 2)
		assert.Equal(t, []bool{true, true}, data[0].GetValidData())
		assert.True(t, flag)
	})

	t.Run("float scalars has no data, but validData length is wrong when fill default value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{
								Data: []float32{},
							},
						},
					},
				},
				ValidData: []bool{true, true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int32,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_FloatData{
							FloatData: 1,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 3)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("float scalars has data, and schema default value is not set", func(t *testing.T) {
		floatData := []float32{1}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Float,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{
								Data: floatData,
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Float,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)
		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetFloatData().Data, floatData[0], 1)
		assert.True(t, flag)
	})

	t.Run("float scalars has part of data, and schema default value is legal", func(t *testing.T) {
		floatData := []float32{1}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Float,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{
								Data: floatData,
							},
						},
					},
				},
				ValidData: []bool{false, true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Float,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_FloatData{
							FloatData: 1,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetFloatData().Data, floatData[0], 2)
		assert.True(t, flag)
	})

	t.Run("float scalars has data, and schema default value is legal", func(t *testing.T) {
		floatData := []float32{1}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Float,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{
								Data: floatData,
							},
						},
					},
				},
				ValidData: []bool{true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Float,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_FloatData{
							FloatData: 2,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetFloatData().Data, floatData[0], 1)
		assert.True(t, flag)
	})

	t.Run("float scalars has data, and no need to fill when nullable", func(t *testing.T) {
		floatData := []float32{1}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Float,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{
								Data: floatData,
							},
						},
					},
				},
				ValidData: []bool{true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Float,
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetFloatData().Data, floatData[0], 1)
		assert.True(t, flag)
	})
	////////////////////////////////////////////////////////////////////

	t.Run("double scalars schema not found", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Double,
			},
		}

		schema := &schemapb.CollectionSchema{}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.Error(t, err)
	})

	t.Run("the length of double scalars is wrong when nullable", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Double,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: []float64{},
							},
						},
					},
				},
				ValidData: []bool{false, true},
			},
		}
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("the length of double scalars is wrong when has default_value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Double,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: []float64{},
							},
						},
					},
				},
				ValidData: []bool{false, true},
			},
		}
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name: "test",
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_DoubleData{
							DoubleData: 1,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("double scalars has no data, will fill null value according to validData", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Double,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: []float64{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Double,
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetDoubleData().Data, 0, 2)
		assert.True(t, flag)
	})

	t.Run("double scalars has no data, and schema default value is legal", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Double,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: []float64{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Double,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_DoubleData{
							DoubleData: 1,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetDoubleData().Data, schema.Fields[0].GetDefaultValue().GetDoubleData(), 2)
		assert.Equal(t, 0, len(data[0].GetValidData()))
		assert.True(t, flag)
	})

	t.Run("double scalars schema set both nullable==true and default value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Double,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: []float64{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Double,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_DoubleData{
							DoubleData: 1,
						},
					},
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetDoubleData().Data, schema.Fields[0].GetDefaultValue().GetDoubleData(), 2)
		assert.Equal(t, []bool{true, true}, data[0].GetValidData())
		assert.True(t, flag)
	})

	t.Run("double scalars has no data, but validData length is wrong when fill default value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: []float64{},
							},
						},
					},
				},
				ValidData: []bool{true, true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int32,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_DoubleData{
							DoubleData: 1,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 3)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("double scalars has data, and schema default value is not set", func(t *testing.T) {
		doubleData := []float64{1}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Double,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: doubleData,
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Double,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)
		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetDoubleData().Data, doubleData[0], 1)
		assert.True(t, flag)
	})

	t.Run("double scalars has part of data, and schema default value is legal", func(t *testing.T) {
		doubleData := []float64{1}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Double,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: doubleData,
							},
						},
					},
				},
				ValidData: []bool{true, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Double,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_DoubleData{
							DoubleData: 1,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetDoubleData().Data, doubleData[0], 2)
		assert.True(t, flag)
	})

	t.Run("double scalars has data, and schema default value is legal", func(t *testing.T) {
		doubleData := []float64{1}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Double,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: doubleData,
							},
						},
					},
				},
				ValidData: []bool{true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Double,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_DoubleData{
							DoubleData: 2,
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetDoubleData().Data, doubleData[0], 1)
		assert.True(t, flag)
	})
	t.Run("double scalars has data, and no need to fill when nullable", func(t *testing.T) {
		doubleData := []float64{1}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Double,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: doubleData,
							},
						},
					},
				},
				ValidData: []bool{true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Double,
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetDoubleData().Data, doubleData[0], 1)
		assert.True(t, flag)
	})

	//////////////////////////////////////////////////////////////////

	t.Run("string scalars schema not found", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_VarChar,
			},
		}

		schema := &schemapb.CollectionSchema{}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.Error(t, err)
	})

	t.Run("the length of string scalars is wrong when has nullable", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{},
							},
						},
					},
				},
				ValidData: []bool{false, true},
			},
		}
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("the length of string scalars is wrong when has default_value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{},
							},
						},
					},
				},
				ValidData: []bool{false, true},
			},
		}
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name: "test",
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_StringData{
							StringData: "b",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("string scalars has no data, will fill null value according to validData", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_VarChar,
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetStringData().Data, "", 2)
		assert.True(t, flag)
	})

	t.Run("string scalars has no data, and schema default value is legal", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_VarChar,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_StringData{
							StringData: "b",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetStringData().Data, schema.Fields[0].GetDefaultValue().GetStringData(), 2)
		assert.Equal(t, 0, len(data[0].GetValidData()))
		assert.True(t, flag)
	})

	t.Run("string scalars schema set both nullable==true and default value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_VarChar,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_StringData{
							StringData: "b",
						},
					},
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetStringData().Data, schema.Fields[0].GetDefaultValue().GetStringData(), 2)
		assert.Equal(t, []bool{true, true}, data[0].GetValidData())
		assert.True(t, flag)
	})

	t.Run("string scalars has part of data, and schema default value is legal", func(t *testing.T) {
		stringData := []string{"a"}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: stringData,
							},
						},
					},
				},
				ValidData: []bool{true, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_VarChar,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_StringData{
							StringData: "a",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetStringData().Data, stringData[0], 2)
		assert.True(t, flag)
	})

	t.Run("string scalars has data, and schema default value is legal", func(t *testing.T) {
		stringData := []string{"a"}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: stringData,
							},
						},
					},
				},
				ValidData: []bool{true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_VarChar,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_StringData{
							StringData: "b",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetStringData().Data, stringData[0], 1)
		assert.True(t, flag)
	})

	t.Run("string scalars has no data, but validData length is wrong when fill default value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{"a"},
							},
						},
					},
				},
				ValidData: []bool{true, true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name: "test",
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_StringData{
							StringData: "b",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 3)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("string scalars has no data, and no need to fill when nullable", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{"a"},
							},
						},
					},
				},
				ValidData: []bool{true, true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 3)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("string scalars has data, and schema default value is not set", func(t *testing.T) {
		stringData := []string{"a"}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: stringData,
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_VarChar,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.NoError(t, err)

		flag := checkfillWithValueData(data[0].GetScalars().GetStringData().Data, stringData[0], 1)
		assert.True(t, flag)
	})

	t.Run("json scalars schema not found", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_JSON,
			},
		}

		schema := &schemapb.CollectionSchema{}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.Error(t, err)
	})

	t.Run("the length of json scalars is wrong when nullable", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_JSON,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: [][]byte{},
							},
						},
					},
				},
				ValidData: []bool{false, true},
			},
		}
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("the length of json scalars is wrong when has default_value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_JSON,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: [][]byte{},
							},
						},
					},
				},
				ValidData: []bool{false, true},
			},
		}
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name: "test",
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_BytesData{
							BytesData: []byte("{\"Hello\":\"world\"}"),
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("json scalars has no data, will fill null value according to validData", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_JSON,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: [][]byte{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_JSON,
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		assert.Equal(t, len(data[0].GetScalars().GetJsonData().Data), 2)
	})

	t.Run("json scalars has no data, and schema default value is legal", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_JSON,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: [][]byte{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_BinaryVector,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_BytesData{
							BytesData: []byte("{\"Hello\":\"world\"}"),
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag, err := checkJsonfillWithValueData(data[0].GetScalars().GetJsonData().Data, schema.Fields[0].GetDefaultValue().GetBytesData(), 2)
		assert.True(t, flag)
		assert.Equal(t, 0, len(data[0].GetValidData()))
		assert.NoError(t, err)
	})

	t.Run("json scalars schema set both nullable==true and default value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_JSON,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: [][]byte{},
							},
						},
					},
				},
				ValidData: []bool{false, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_BinaryVector,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_BytesData{
							BytesData: []byte("{\"Hello\":\"world\"}"),
						},
					},
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag, err := checkJsonfillWithValueData(data[0].GetScalars().GetJsonData().Data, schema.Fields[0].GetDefaultValue().GetBytesData(), 2)
		assert.True(t, flag)
		assert.Equal(t, []bool{true, true}, data[0].GetValidData())
		assert.NoError(t, err)
	})

	t.Run("json scalars has no data, but validData length is wrong when fill default value", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: [][]byte{},
							},
						},
					},
				},
				ValidData: []bool{true, true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_Int32,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_BytesData{
							BytesData: []byte("{\"Hello\":\"world\"}"),
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 3)

		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("json scalars has data, and schema default value is not set", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_JSON,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: [][]byte{[]byte("{\"Hello\":\"world\"}")},
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_BinaryVector,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)
		assert.NoError(t, err)
		flag, err := checkJsonfillWithValueData(data[0].GetScalars().GetJsonData().Data, []byte("{\"Hello\":\"world\"}"), 1)

		assert.True(t, flag)
		assert.NoError(t, err)
	})

	t.Run("json scalars has part of data, and schema default value is legal", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: [][]byte{[]byte("{\"Hello\":\"world\"}")},
							},
						},
					},
				},
				ValidData: []bool{true, false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_BinaryVector,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_BytesData{
							BytesData: []byte("{\"Hello\":\"world\"}"),
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)

		flag, err := checkJsonfillWithValueData(data[0].GetScalars().GetJsonData().Data, schema.Fields[0].GetDefaultValue().GetBytesData(), 2)
		assert.NoError(t, err)
		assert.True(t, flag)
	})

	t.Run("json scalars has data, and schema default value is legal", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: [][]byte{[]byte("{\"Hello\":\"world\"}")},
							},
						},
					},
				},
				ValidData: []bool{true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name: "test",
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_BytesData{
							BytesData: []byte("{\"hello\":\"world\"}"),
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.NoError(t, err)

		flag, err := checkJsonfillWithValueData(data[0].GetScalars().GetJsonData().Data, []byte("{\"Hello\":\"world\"}"), 1)
		assert.NoError(t, err)
		assert.True(t, flag)
	})

	t.Run("json scalars has data, and no need to fill when nullable", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: [][]byte{[]byte("{\"Hello\":\"world\"}")},
							},
						},
					},
				},
				ValidData: []bool{true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.NoError(t, err)

		flag, err := checkJsonfillWithValueData(data[0].GetScalars().GetJsonData().Data, []byte("{\"Hello\":\"world\"}"), 1)
		assert.NoError(t, err)
		assert.True(t, flag)
	})

	t.Run("all_valid_nullable_data_without_validdata", func(t *testing.T) {
		stringData := []string{"a"}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: stringData,
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_VarChar,
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.NoError(t, err)
	})

	t.Run("nullable_data_size_not_match", func(t *testing.T) {
		stringData := []string{"a"}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: stringData,
							},
						},
					},
				},
				ValidData: []bool{false},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_VarChar,
					Nullable: true,
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("default_value_field_data_without_validdata", func(t *testing.T) {
		// non-nullable field with defaultValue, data present, NO ValidData
		// Should auto-fill ValidData to all-true, then FillWithDefaultValue keeps data as-is
		stringData := []string{"actual_value"}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: stringData,
							},
						},
					},
				},
				// No ValidData set - this triggers the new auto-fill logic
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_VarChar,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_StringData{
							StringData: "default_val",
						},
					},
					// Nullable is false (default)
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)

		assert.NoError(t, err)
		// Data should remain "actual_value" since all ValidData was auto-filled to true
		assert.Equal(t, []string{"actual_value"}, data[0].GetScalars().GetStringData().GetData())
		// ValidData should be cleared for non-nullable field
		assert.Equal(t, 0, len(data[0].GetValidData()))
	})

	t.Run("default_value_field_no_data_without_validdata", func(t *testing.T) {
		// non-nullable field with defaultValue, empty data, NO ValidData
		// Should auto-fill ValidData to all-true, FillWithDefaultValue sees all valid so data stays empty...
		// BUT: fillWithDefaultValueImpl checks len(array)==getValidNumber(validData), so
		// data=[] with validData=[true,true] gives n=2 != len(array)=0 → error
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{},
							},
						},
					},
				},
				// No ValidData set
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_VarChar,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_StringData{
							StringData: "default_val",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		// Error because data length (0) doesn't match valid count (2)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("default_value_nullable_field_data_without_validdata", func(t *testing.T) {
		// nullable + defaultValue field, data present, NO ValidData
		// Should auto-fill ValidData to all-true, FillWithDefaultValue keeps data, ValidData set to all-true
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{"val1", "val2"},
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					DataType: schemapb.DataType_VarChar,
					Nullable: true,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_StringData{
							StringData: "default_val",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 2)

		assert.NoError(t, err)
		assert.Equal(t, []string{"val1", "val2"}, data[0].GetScalars().GetStringData().GetData())
		// ValidData should be all-true for nullable field
		assert.Equal(t, []bool{true, true}, data[0].GetValidData())
	})

	t.Run("check the length of ValidData when has default value", func(t *testing.T) {
		stringData := []string{"a"}
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				FieldId:   100,
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: stringData,
							},
						},
					},
				},
			},
			{
				FieldName: "test1",
				FieldId:   101,
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{},
							},
						},
					},
				},
				ValidData: []bool{true},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "test",
					FieldID:  100,
					DataType: schemapb.DataType_VarChar,
					Nullable: true,
				},
				{
					Name:     "test1",
					FieldID:  101,
					DataType: schemapb.DataType_VarChar,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_StringData{
							StringData: "b",
						},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()

		err = v.fillWithValue(data, h, 1)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})
}

func Test_verifyOverflowByRange(t *testing.T) {
	var err error

	err = verifyOverflowByRange(
		[]int32{int32(math.MinInt8 - 1)},
		math.MinInt8,
		math.MaxInt8,
	)
	assert.Error(t, err)

	err = verifyOverflowByRange(
		[]int32{int32(math.MaxInt8 + 1)},
		math.MinInt8,
		math.MaxInt8,
	)
	assert.Error(t, err)

	err = verifyOverflowByRange(
		[]int32{int32(math.MinInt8 - 1), int32(math.MaxInt8 + 1)},
		math.MinInt8,
		math.MaxInt8,
	)
	assert.Error(t, err)

	err = verifyOverflowByRange(
		[]int32{int32(math.MaxInt8 + 1), int32(math.MinInt8 - 1)},
		math.MinInt8,
		math.MaxInt8,
	)
	assert.Error(t, err)

	err = verifyOverflowByRange(
		[]int32{1, 2, 3, int32(math.MinInt8 - 1), int32(math.MaxInt8 + 1)},
		math.MinInt8,
		math.MaxInt8,
	)
	assert.Error(t, err)

	err = verifyOverflowByRange(
		[]int32{1, 2, 3, int32(math.MinInt8 + 1), int32(math.MaxInt8 - 1)},
		math.MinInt8,
		math.MaxInt8,
	)
	assert.NoError(t, err)
}

func Test_validateUtil_checkIntegerFieldData(t *testing.T) {
	t.Run("no check", func(t *testing.T) {
		v := newValidateUtil()
		assert.Error(t, v.checkIntegerFieldData(&schemapb.FieldData{Field: &schemapb.FieldData_Vectors{}}, nil))
		assert.NoError(t, v.checkIntegerFieldData(&schemapb.FieldData{Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: []int32{1, 2, 3, 4},
					},
				},
			},
		}}, nil))
	})

	t.Run("no int data but set nullable==true or set default_value", func(t *testing.T) {
		v := newValidateUtil(withOverflowCheck())

		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Int8,
			Nullable: true,
		}

		data := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: nil,
						},
					},
				},
			},
		}

		err := v.checkIntegerFieldData(data, f)
		assert.NoError(t, err)

		f = &schemapb.FieldSchema{
			DataType:     schemapb.DataType_Int8,
			DefaultValue: &schemapb.ValueField{},
		}

		data = &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: nil,
						},
					},
				},
			},
		}

		err = v.checkIntegerFieldData(data, f)
		assert.NoError(t, err)
	})

	t.Run("tiny int, type mismatch", func(t *testing.T) {
		v := newValidateUtil(withOverflowCheck())

		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Int8,
		}
		data := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{},
				},
			},
		}

		err := v.checkIntegerFieldData(data, f)
		assert.Error(t, err)
	})

	t.Run("tiny int, overflow", func(t *testing.T) {
		v := newValidateUtil(withOverflowCheck())

		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Int8,
		}
		data := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{int32(math.MinInt8 - 1)},
						},
					},
				},
			},
		}

		err := v.checkIntegerFieldData(data, f)
		assert.Error(t, err)
	})

	t.Run("tiny int, normal case", func(t *testing.T) {
		v := newValidateUtil(withOverflowCheck())

		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Int8,
		}
		data := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{int32(math.MinInt8 + 1), int32(math.MaxInt8 - 1)},
						},
					},
				},
			},
		}

		err := v.checkIntegerFieldData(data, f)
		assert.NoError(t, err)
	})

	t.Run("small int, overflow", func(t *testing.T) {
		v := newValidateUtil(withOverflowCheck())

		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Int16,
		}
		data := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{int32(math.MinInt16 - 1)},
						},
					},
				},
			},
		}

		err := v.checkIntegerFieldData(data, f)
		assert.Error(t, err)
	})

	t.Run("small int, normal case", func(t *testing.T) {
		v := newValidateUtil(withOverflowCheck())

		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Int16,
		}
		data := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{int32(math.MinInt16 + 1), int32(math.MaxInt16 - 1)},
						},
					},
				},
			},
		}

		err := v.checkIntegerFieldData(data, f)
		assert.NoError(t, err)
	})
}

func Test_validateUtil_checkJSONData(t *testing.T) {
	t.Run("no json data", func(t *testing.T) {
		v := newValidateUtil(withOverflowCheck())

		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_JSON,
		}
		data := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{int32(math.MinInt8 - 1)},
						},
					},
				},
			},
		}

		err := v.checkJSONFieldData(data, f)
		assert.Error(t, err)
	})

	t.Run("no json data but set nullable==true or set default_value", func(t *testing.T) {
		v := newValidateUtil(withOverflowCheck())

		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_JSON,
			Nullable: true,
		}
		data := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{
						JsonData: &schemapb.JSONArray{
							Data: nil,
						},
					},
				},
			},
		}

		err := v.checkJSONFieldData(data, f)
		assert.NoError(t, err)

		f = &schemapb.FieldSchema{
			DataType:     schemapb.DataType_JSON,
			DefaultValue: &schemapb.ValueField{},
		}
		data = &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{
						JsonData: &schemapb.JSONArray{
							Data: nil,
						},
					},
				},
			},
		}

		err = v.checkJSONFieldData(data, f)
		assert.NoError(t, err)
	})

	t.Run("json string exceed max length", func(t *testing.T) {
		v := newValidateUtil(withOverflowCheck(), withMaxLenCheck())
		jsonString := ""
		for i := 0; i < Params.CommonCfg.JSONMaxLength.GetAsInt(); i++ {
			jsonString += fmt.Sprintf("key: %d, value: %d", i, i)
		}
		jsonString = "{" + jsonString + "}"
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_JSON,
		}
		data := &schemapb.FieldData{
			FieldName: "json",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{
						JsonData: &schemapb.JSONArray{
							Data: [][]byte{[]byte(jsonString)},
						},
					},
				},
			},
		}

		err := v.checkJSONFieldData(data, f)
		assert.Error(t, err)
	})

	t.Run("dynamic field exceed max length", func(t *testing.T) {
		v := newValidateUtil(withOverflowCheck(), withMaxLenCheck())
		jsonString := ""
		for i := 0; i < Params.CommonCfg.JSONMaxLength.GetAsInt(); i++ {
			jsonString += fmt.Sprintf("key: %d, value: %d", i, i)
		}
		jsonString = "{" + jsonString + "}"
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_JSON,
		}
		data := &schemapb.FieldData{
			FieldName: "$meta",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{
						JsonData: &schemapb.JSONArray{
							Data: [][]byte{[]byte(jsonString)},
						},
					},
				},
			},
			IsDynamic: true,
		}

		err := v.checkJSONFieldData(data, f)
		assert.Error(t, err)
	})
}

func Test_validateUtil_checkLongFieldData(t *testing.T) {
	v := newValidateUtil()
	assert.Error(t, v.checkLongFieldData(&schemapb.FieldData{
		Field: &schemapb.FieldData_Vectors{},
	}, nil))
	assert.NoError(t, v.checkLongFieldData(&schemapb.FieldData{
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{1, 2, 3, 4},
					},
				},
			},
		},
	}, nil))

	t.Run("no long data but set nullable==true or set default_value", func(t *testing.T) {
		v := newValidateUtil(withOverflowCheck())

		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Int64,
			Nullable: true,
		}

		data := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: nil,
						},
					},
				},
			},
		}

		err := v.checkLongFieldData(data, f)
		assert.NoError(t, err)

		f = &schemapb.FieldSchema{
			DataType:     schemapb.DataType_Int64,
			DefaultValue: &schemapb.ValueField{},
		}

		data = &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: nil,
						},
					},
				},
			},
		}
		err = v.checkLongFieldData(data, f)
		assert.NoError(t, err)
	})
}

func Test_validateUtil_checkFloatFieldData(t *testing.T) {
	v := newValidateUtil(withNANCheck())
	assert.Error(t, v.checkFloatFieldData(&schemapb.FieldData{
		Field: &schemapb.FieldData_Vectors{},
	}, nil))
	assert.NoError(t, v.checkFloatFieldData(&schemapb.FieldData{
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: []float32{1, 2, 3, 4},
					},
				},
			},
		},
	}, nil))
	assert.Error(t, v.checkFloatFieldData(&schemapb.FieldData{
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: []float32{float32(math.NaN())},
					},
				},
			},
		},
	}, nil))

	t.Run("no float data but set nullable==true or set default_value", func(t *testing.T) {
		v := newValidateUtil(withOverflowCheck())

		f := &schemapb.FieldSchema{
			Nullable: true,
		}

		data := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{
							Data: nil,
						},
					},
				},
			},
		}

		err := v.checkFloatFieldData(data, f)
		assert.NoError(t, err)

		f = &schemapb.FieldSchema{
			DefaultValue: &schemapb.ValueField{},
		}

		data = &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{
							Data: nil,
						},
					},
				},
			},
		}
		err = v.checkFloatFieldData(data, f)
		assert.NoError(t, err)
	})
}

func Test_validateUtil_checkDoubleFieldData(t *testing.T) {
	v := newValidateUtil(withNANCheck())
	assert.Error(t, v.checkDoubleFieldData(&schemapb.FieldData{
		Field: &schemapb.FieldData_Vectors{},
	}, nil))
	assert.NoError(t, v.checkDoubleFieldData(&schemapb.FieldData{
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{
						Data: []float64{1, 2, 3, 4},
					},
				},
			},
		},
	}, nil))
	assert.Error(t, v.checkDoubleFieldData(&schemapb.FieldData{
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{
						Data: []float64{math.NaN()},
					},
				},
			},
		},
	}, nil))

	t.Run("no float data but set nullable==true or set default_value", func(t *testing.T) {
		v := newValidateUtil(withOverflowCheck())

		f := &schemapb.FieldSchema{
			Nullable: true,
		}

		data := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: nil,
						},
					},
				},
			},
		}

		err := v.checkDoubleFieldData(data, f)
		assert.NoError(t, err)

		f = &schemapb.FieldSchema{
			DefaultValue: &schemapb.ValueField{},
		}

		data = &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: nil,
						},
					},
				},
			},
		}
		err = v.checkDoubleFieldData(data, f)
		assert.NoError(t, err)
	})
}

func TestCheckArrayElementNilData(t *testing.T) {
	data := &schemapb.ArrayArray{
		Data: []*schemapb.ScalarField{nil},
	}

	fieldSchema := &schemapb.FieldSchema{
		Name:        "test",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int64,
	}

	v := newValidateUtil()
	err := v.checkArrayElement(data, fieldSchema)
	assert.True(t, merr.ErrParameterInvalid.Is(err))
}

func Test_validateUtil_checkArrayOfVectorFieldData(t *testing.T) {
	t.Run("nil data", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{},
			},
		}
		fieldSchema := &schemapb.FieldSchema{
			ElementType: schemapb.DataType_FloatVector,
		}
		v := newValidateUtil()
		err := v.checkArrayOfVectorFieldData(f, fieldSchema)
		assert.Error(t, err)
	})

	t.Run("nil vector in array", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_VectorArray{
						VectorArray: &schemapb.VectorArray{
							Data: []*schemapb.VectorField{{Data: nil}},
						},
					},
				},
			},
		}
		fieldSchema := &schemapb.FieldSchema{
			ElementType: schemapb.DataType_FloatVector,
		}
		v := newValidateUtil()
		err := v.checkArrayOfVectorFieldData(f, fieldSchema)
		assert.Error(t, err)
	})

	t.Run("no check", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_VectorArray{
						VectorArray: &schemapb.VectorArray{
							Data: []*schemapb.VectorField{
								{
									Data: &schemapb.VectorField_FloatVector{
										FloatVector: &schemapb.FloatArray{
											Data: []float32{1.0, 2.0, float32(math.NaN())},
										},
									},
								},
							},
						},
					},
				},
			},
		}
		fieldSchema := &schemapb.FieldSchema{
			ElementType: schemapb.DataType_FloatVector,
		}
		v := newValidateUtil()
		v.checkNAN = false
		err := v.checkArrayOfVectorFieldData(f, fieldSchema)
		assert.NoError(t, err)
	})

	t.Run("has nan", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_VectorArray{
						VectorArray: &schemapb.VectorArray{
							Data: []*schemapb.VectorField{
								{
									Data: &schemapb.VectorField_FloatVector{
										FloatVector: &schemapb.FloatArray{
											Data: []float32{float32(math.NaN())},
										},
									},
								},
							},
						},
					},
				},
			},
		}
		fieldSchema := &schemapb.FieldSchema{
			ElementType: schemapb.DataType_FloatVector,
		}
		v := newValidateUtil(withNANCheck())
		err := v.checkArrayOfVectorFieldData(f, fieldSchema)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_VectorArray{
						VectorArray: &schemapb.VectorArray{
							Data: []*schemapb.VectorField{
								{
									Data: &schemapb.VectorField_FloatVector{
										FloatVector: &schemapb.FloatArray{
											Data: []float32{1.0, 2.0},
										},
									},
								},
								{
									Data: &schemapb.VectorField_FloatVector{
										FloatVector: &schemapb.FloatArray{
											Data: []float32{3.0, 4.0},
										},
									},
								},
							},
						},
					},
				},
			},
		}
		fieldSchema := &schemapb.FieldSchema{
			ElementType: schemapb.DataType_FloatVector,
		}
		v := newValidateUtil(withNANCheck())
		err := v.checkArrayOfVectorFieldData(f, fieldSchema)
		assert.NoError(t, err)
	})
}

func Test_validateUtil_checkAligned_ArrayOfVector(t *testing.T) {
	t.Run("array of vector dim mismatch", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_ArrayOfVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_VectorArray{
							VectorArray: &schemapb.VectorArray{
								Dim: 16,
								Data: []*schemapb.VectorField{
									{
										Data: &schemapb.VectorField_FloatVector{
											FloatVector: &schemapb.FloatArray{
												Data: make([]float32, 16),
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test",
					DataType:    schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "8"},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()
		err = v.checkAligned(data, h, 1)
		assert.Error(t, err)
	})

	t.Run("array of vector row num mismatch", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test",
				Type:      schemapb.DataType_ArrayOfVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_VectorArray{
							VectorArray: &schemapb.VectorArray{
								Dim: 8,
								Data: []*schemapb.VectorField{
									{
										Data: &schemapb.VectorField_FloatVector{
											FloatVector: &schemapb.FloatArray{
												Data: make([]float32, 8),
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test",
					DataType:    schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "8"},
					},
				},
			},
		}
		h, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)

		v := newValidateUtil()
		err = v.checkAligned(data, h, 100)
		assert.Error(t, err)
	})
}

// Test FillWithNullValue for Geometry fields
func TestFillWithNullValue_Geometry(t *testing.T) {
	t.Run("geometry WKT with null values", func(t *testing.T) {
		field := &schemapb.FieldData{
			Type:      schemapb.DataType_Geometry,
			FieldName: "geo_field",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_GeometryWktData{
						GeometryWktData: &schemapb.GeometryWktArray{
							Data: []string{"POINT (1 2)", "POINT (3 4)"},
						},
					},
				},
			},
			ValidData: []bool{true, false, true, false},
		}

		fieldSchema := &schemapb.FieldSchema{
			Name:     "geo_field",
			DataType: schemapb.DataType_Geometry,
			Nullable: true,
		}

		numRows := 4
		err := FillWithNullValue(field, fieldSchema, numRows)

		assert.NoError(t, err)
		assert.Len(t, field.GetScalars().GetGeometryWktData().GetData(), numRows)
		assert.Equal(t, "POINT (1 2)", field.GetScalars().GetGeometryWktData().GetData()[0])
		assert.Equal(t, "", field.GetScalars().GetGeometryWktData().GetData()[1])
		assert.Equal(t, "POINT (3 4)", field.GetScalars().GetGeometryWktData().GetData()[2])
		assert.Equal(t, "", field.GetScalars().GetGeometryWktData().GetData()[3])
	})

	t.Run("geometry WKB with null values", func(t *testing.T) {
		field := &schemapb.FieldData{
			Type:      schemapb.DataType_Geometry,
			FieldName: "geo_field",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_GeometryData{
						GeometryData: &schemapb.GeometryArray{
							Data: [][]byte{{0x01, 0x02}, {0x03, 0x04}},
						},
					},
				},
			},
			ValidData: []bool{true, false, true, false},
		}

		fieldSchema := &schemapb.FieldSchema{
			Name:     "geo_field",
			DataType: schemapb.DataType_Geometry,
			Nullable: true,
		}

		numRows := 4
		err := FillWithNullValue(field, fieldSchema, numRows)

		assert.NoError(t, err)
		assert.Len(t, field.GetScalars().GetGeometryData().GetData(), numRows)
		assert.Equal(t, []byte{0x01, 0x02}, field.GetScalars().GetGeometryData().GetData()[0])
		assert.Nil(t, field.GetScalars().GetGeometryData().GetData()[1])
		assert.Equal(t, []byte{0x03, 0x04}, field.GetScalars().GetGeometryData().GetData()[2])
		assert.Nil(t, field.GetScalars().GetGeometryData().GetData()[3])
	})

	t.Run("ArrayOfVector expands to dense with null placeholder", func(t *testing.T) {
		field := &schemapb.FieldData{
			FieldName: "vec_array",
			Type:      schemapb.DataType_ArrayOfVector,
			ValidData: []bool{true, false, true},
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: 4,
					Data: &schemapb.VectorField_VectorArray{
						VectorArray: &schemapb.VectorArray{
							Data: []*schemapb.VectorField{
								{Dim: 4, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}},
								{Dim: 4, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{5, 6, 7, 8}}}},
							},
							Dim:         4,
							ElementType: schemapb.DataType_FloatVector,
						},
					},
				},
			},
		}

		fieldSchema := &schemapb.FieldSchema{
			Name:        "vec_array",
			DataType:    schemapb.DataType_ArrayOfVector,
			ElementType: schemapb.DataType_FloatVector,
			Nullable:    true,
		}
		err := FillWithNullValue(field, fieldSchema, 3)
		assert.NoError(t, err)

		vectorArray := field.GetVectors().GetVectorArray()
		require.NotNil(t, vectorArray)
		// Plan B: ArrayOfVector is expanded to dense; null row gets an empty per-row placeholder.
		assert.Equal(t, 3, len(vectorArray.Data))
		assert.Equal(t, []float32{1, 2, 3, 4}, vectorArray.Data[0].GetFloatVector().GetData())
		assert.Empty(t, vectorArray.Data[1].GetFloatVector().GetData())
		assert.Equal(t, int64(4), vectorArray.Data[1].GetDim())
		assert.Equal(t, []float32{5, 6, 7, 8}, vectorArray.Data[2].GetFloatVector().GetData())
	})
}

// Test_MetaNullableCompat_v25_vs_v26 verifies that insert and upsert Validate
// (specifically fillWithValue) works correctly for both 2.5-style $meta
// (Nullable=false, no DefaultValue) and 2.6-style $meta (Nullable=true,
// DefaultValue="{}").
//
// Scenario: after upgrading from 2.5 to 2.6, old collections retain their
// original $meta schema. The proxy code must handle both formats.
func Test_MetaNullableCompat_v25_vs_v26(t *testing.T) {
	numRows := 3

	// Build a minimal schema with PK + vector + $meta (dynamic field).
	// metaNullable/metaDefault control whether the $meta matches 2.5 or 2.6.
	buildSchema := func(metaNullable bool, metaDefault []byte) *schemapb.CollectionSchema {
		metaField := &schemapb.FieldSchema{
			FieldID:   101,
			Name:      common.MetaFieldName,
			DataType:  schemapb.DataType_JSON,
			IsDynamic: true,
			Nullable:  metaNullable,
		}
		if metaDefault != nil {
			metaField.DefaultValue = &schemapb.ValueField{
				Data: &schemapb.ValueField_BytesData{BytesData: metaDefault},
			}
		}
		return &schemapb.CollectionSchema{
			Name:               "compat_test",
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				metaField,
			},
		}
	}

	// Simulate SDK-provided $meta data WITHOUT ValidData (the common SDK behavior).
	sdkMetaFieldData := func() *schemapb.FieldData {
		jsonRows := make([][]byte, numRows)
		for i := range jsonRows {
			jsonRows[i] = []byte(`{"dyn_key":"value"}`)
		}
		return &schemapb.FieldData{
			FieldName: common.MetaFieldName,
			Type:      schemapb.DataType_JSON,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{
						JsonData: &schemapb.JSONArray{Data: jsonRows},
					},
				},
			},
			IsDynamic: true,
			// NOTE: no ValidData — this is what the SDK sends
		}
	}

	// Auto-generated $meta (when SDK sends no dynamic data) — mirrors autoGenDynamicFieldData.
	autoGenMetaFieldData := func(schema *schemapb.CollectionSchema) *schemapb.FieldData {
		defaultData := make([][]byte, numRows)
		for i := range defaultData {
			defaultData[i] = []byte("{}")
		}
		return autoGenDynamicFieldData(schema, defaultData)
	}

	t.Run("INSERT_v26_schema_sdk_provided_meta", func(t *testing.T) {
		schema := buildSchema(true, []byte("{}"))
		h, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		data := []*schemapb.FieldData{sdkMetaFieldData()}
		err = newValidateUtil().fillWithValue(data, h, numRows)
		assert.NoError(t, err, "2.6 schema + SDK-provided $meta should pass fillWithValue")
	})

	t.Run("INSERT_v25_schema_sdk_provided_meta", func(t *testing.T) {
		schema := buildSchema(false, nil) // 2.5 style
		h, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		data := []*schemapb.FieldData{sdkMetaFieldData()}
		err = newValidateUtil().fillWithValue(data, h, numRows)
		assert.NoError(t, err, "2.5 schema + SDK-provided $meta (no ValidData) should pass fillWithValue")
	})

	t.Run("INSERT_v26_schema_autogen_meta", func(t *testing.T) {
		schema := buildSchema(true, []byte("{}"))
		h, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		data := []*schemapb.FieldData{autoGenMetaFieldData(schema)}
		err = newValidateUtil().fillWithValue(data, h, numRows)
		assert.NoError(t, err, "2.6 schema + auto-generated $meta should pass fillWithValue")
	})

	t.Run("INSERT_v25_schema_autogen_meta", func(t *testing.T) {
		// autoGenDynamicFieldData checks schema: for 2.5 (non-nullable, no default),
		// it does NOT set ValidData. CheckValidData expects len(ValidData)==0.
		schema := buildSchema(false, nil) // 2.5 style
		h, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		data := []*schemapb.FieldData{autoGenMetaFieldData(schema)}
		err = newValidateUtil().fillWithValue(data, h, numRows)
		assert.NoError(t, err, "2.5 schema + auto-generated $meta should pass fillWithValue")
		// ValidData should remain empty for non-nullable field
		assert.Empty(t, data[0].GetValidData(), "non-nullable $meta should not have ValidData")
	})

	t.Run("UPSERT_queryPreExecute_v25_schema", func(t *testing.T) {
		// Simulates the fixed upsert queryPreExecute path where ValidData is
		// conditionally auto-filled only for nullable/default-value fields.
		schema := buildSchema(false, nil) // 2.5 style
		h, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		fieldData := sdkMetaFieldData()
		fieldSchema, _ := h.GetFieldFromName(common.MetaFieldName)

		// Simulate FIXED queryPreExecute auto-fill (with schema condition)
		if fieldData.GetIsDynamic() && len(fieldData.GetValidData()) == 0 &&
			(fieldSchema.GetNullable() || fieldSchema.GetDefaultValue() != nil) {
			validData := make([]bool, numRows)
			for i := range validData {
				validData[i] = true
			}
			fieldData.ValidData = validData
		}

		// For 2.5 schema: ValidData is NOT set, so this branch is skipped
		if len(fieldData.GetValidData()) != 0 {
			if fieldSchema.GetDefaultValue() != nil {
				err = FillWithDefaultValue(fieldData, fieldSchema, numRows)
			} else {
				err = FillWithNullValue(fieldData, fieldSchema, numRows)
			}
		}
		assert.NoError(t, err, "2.5 schema upsert queryPreExecute should not fail")
		assert.Empty(t, fieldData.GetValidData(), "non-nullable $meta should not have ValidData")
	})

	t.Run("UPSERT_queryPreExecute_v26_schema", func(t *testing.T) {
		schema := buildSchema(true, []byte("{}"))
		h, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		fieldData := sdkMetaFieldData()
		fieldSchema, _ := h.GetFieldFromName(common.MetaFieldName)

		// Simulate FIXED queryPreExecute auto-fill (with schema condition)
		if fieldData.GetIsDynamic() && len(fieldData.GetValidData()) == 0 &&
			(fieldSchema.GetNullable() || fieldSchema.GetDefaultValue() != nil) {
			validData := make([]bool, numRows)
			for i := range validData {
				validData[i] = true
			}
			fieldData.ValidData = validData
		}

		if len(fieldData.GetValidData()) != 0 {
			if fieldSchema.GetDefaultValue() != nil {
				err = FillWithDefaultValue(fieldData, fieldSchema, numRows)
			} else {
				err = FillWithNullValue(fieldData, fieldSchema, numRows)
			}
		}
		assert.NoError(t, err, "2.6 schema upsert queryPreExecute should pass")
		assert.Equal(t, numRows, len(fieldData.GetValidData()), "nullable $meta should have ValidData")
	})
}

func Test_newEmptyPerRowVectorField(t *testing.T) {
	cases := []struct {
		name  string
		elem  schemapb.DataType
		check func(t *testing.T, vf *schemapb.VectorField)
	}{
		{
			name: "FloatVector",
			elem: schemapb.DataType_FloatVector,
			check: func(t *testing.T, vf *schemapb.VectorField) {
				fv, ok := vf.GetData().(*schemapb.VectorField_FloatVector)
				require.True(t, ok)
				require.NotNil(t, fv.FloatVector)
				assert.Empty(t, fv.FloatVector.GetData())
			},
		},
		{
			name: "BinaryVector",
			elem: schemapb.DataType_BinaryVector,
			check: func(t *testing.T, vf *schemapb.VectorField) {
				bv, ok := vf.GetData().(*schemapb.VectorField_BinaryVector)
				require.True(t, ok)
				assert.Empty(t, bv.BinaryVector)
			},
		},
		{
			name: "Float16Vector",
			elem: schemapb.DataType_Float16Vector,
			check: func(t *testing.T, vf *schemapb.VectorField) {
				fv, ok := vf.GetData().(*schemapb.VectorField_Float16Vector)
				require.True(t, ok)
				assert.Empty(t, fv.Float16Vector)
			},
		},
		{
			name: "BFloat16Vector",
			elem: schemapb.DataType_BFloat16Vector,
			check: func(t *testing.T, vf *schemapb.VectorField) {
				bv, ok := vf.GetData().(*schemapb.VectorField_Bfloat16Vector)
				require.True(t, ok)
				assert.Empty(t, bv.Bfloat16Vector)
			},
		},
		{
			name: "Int8Vector",
			elem: schemapb.DataType_Int8Vector,
			check: func(t *testing.T, vf *schemapb.VectorField) {
				iv, ok := vf.GetData().(*schemapb.VectorField_Int8Vector)
				require.True(t, ok)
				assert.Empty(t, iv.Int8Vector)
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			vf := newEmptyPerRowVectorField(8, c.elem)
			require.NotNil(t, vf)
			assert.EqualValues(t, 8, vf.Dim)
			c.check(t, vf)
		})
	}
}

func Test_fillVectorArrayNullValueImpl(t *testing.T) {
	makeCompact := func(k int) []*schemapb.VectorField {
		out := make([]*schemapb.VectorField, k)
		for i := 0; i < k; i++ {
			out[i] = &schemapb.VectorField{
				Dim: 8,
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{Data: []float32{float32(i + 1)}},
				},
			}
		}
		return out
	}

	t.Run("partial null expansion", func(t *testing.T) {
		compact := makeCompact(2)
		validData := []bool{true, false, true, false}
		res, err := fillVectorArrayNullValueImpl(compact, validData, 8, schemapb.DataType_FloatVector)
		require.NoError(t, err)
		require.Len(t, res, 4)
		assert.Equal(t, float32(1), res[0].GetFloatVector().GetData()[0])
		assert.Equal(t, float32(2), res[2].GetFloatVector().GetData()[0])
		assert.Empty(t, res[1].GetFloatVector().GetData())
		assert.EqualValues(t, 8, res[1].GetDim())
		assert.Empty(t, res[3].GetFloatVector().GetData())
		assert.EqualValues(t, 8, res[3].GetDim())
	})

	t.Run("all valid short-circuits", func(t *testing.T) {
		compact := makeCompact(3)
		validData := []bool{true, true, true}
		res, err := fillVectorArrayNullValueImpl(compact, validData, 8, schemapb.DataType_FloatVector)
		require.NoError(t, err)
		require.Len(t, res, 3)
		assert.Equal(t, float32(1), res[0].GetFloatVector().GetData()[0])
		assert.Equal(t, float32(2), res[1].GetFloatVector().GetData()[0])
		assert.Equal(t, float32(3), res[2].GetFloatVector().GetData()[0])
	})

	t.Run("all null expansion", func(t *testing.T) {
		compact := makeCompact(0)
		validData := []bool{false, false, false}
		res, err := fillVectorArrayNullValueImpl(compact, validData, 8, schemapb.DataType_FloatVector)
		require.NoError(t, err)
		require.Len(t, res, 3)
		for i := 0; i < 3; i++ {
			assert.Empty(t, res[i].GetFloatVector().GetData(), "row %d", i)
			assert.EqualValues(t, 8, res[i].GetDim(), "row %d", i)
		}
	})

	t.Run("length mismatch returns error", func(t *testing.T) {
		compact := makeCompact(3)
		validData := []bool{true, false, false, false}
		res, err := fillVectorArrayNullValueImpl(compact, validData, 8, schemapb.DataType_FloatVector)
		assert.Error(t, err)
		assert.Nil(t, res)
	})
}
