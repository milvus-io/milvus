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
	v := newValidateUtil()
	assert.Error(t, v.checkBinaryVectorFieldData(&schemapb.FieldData{Field: &schemapb.FieldData_Scalars{}}, nil))
	assert.NoError(t, v.checkBinaryVectorFieldData(&schemapb.FieldData{Field: &schemapb.FieldData_Vectors{
		Vectors: &schemapb.VectorField{
			Dim: 128,
			Data: &schemapb.VectorField_BinaryVector{
				BinaryVector: []byte(strings.Repeat("1", 128)),
			},
		},
	}}, nil))
}

func Test_validateUtil_checkFloatVectorFieldData(t *testing.T) {
	t.Run("not float vector", func(t *testing.T) {
		f := &schemapb.FieldData{}
		v := newValidateUtil()
		err := v.checkFloatVectorFieldData(f, nil)
		assert.Error(t, err)
	})

	t.Run("no check", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: []float32{1.1, 2.2},
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
							Data: []float32{float32(math.NaN())},
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
							Data: []float32{1.1, 2.2},
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
}

func Test_validateUtil_checkFloat16VectorFieldData(t *testing.T) {
	nb := 5
	dim := int64(8)
	data := testutils.GenerateFloat16Vectors(nb, int(dim))
	invalidData := testutils.GenerateFloat16VectorsWithInvalidData(nb, int(dim))

	t.Run("not float16 vector", func(t *testing.T) {
		f := &schemapb.FieldData{}
		v := newValidateUtil()
		err := v.checkFloat16VectorFieldData(f, nil)
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
}

func Test_validateUtil_checkBfloatVectorFieldData(t *testing.T) {
	nb := 5
	dim := int64(8)
	data := testutils.GenerateFloat16Vectors(nb, int(dim))
	invalidData := testutils.GenerateBFloat16VectorsWithInvalidData(nb, int(dim))
	t.Run("not float vector", func(t *testing.T) {
		f := &schemapb.FieldData{}
		v := newValidateUtil()
		err := v.checkBFloat16VectorFieldData(f, nil)
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

	t.Run("check the length of ValidData when not has default value", func(t *testing.T) {
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

		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
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
