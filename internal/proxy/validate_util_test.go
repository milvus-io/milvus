package proxy

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func Test_verifyLengthPerRow(t *testing.T) {
	maxLength := 16

	assert.NoError(t, verifyLengthPerRow[string](nil, int64(maxLength)))

	assert.NoError(t, verifyLengthPerRow([]string{"111111", "22222"}, int64(maxLength)))

	assert.Error(t, verifyLengthPerRow([]string{"11111111111111111"}, int64(maxLength)))

	assert.Error(t, verifyLengthPerRow([]string{"11111111111111111", "222"}, int64(maxLength)))

	assert.Error(t, verifyLengthPerRow([]string{"11111", "22222222222222222"}, int64(maxLength)))
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
}

func Test_validateUtil_checkBinaryVectorFieldData(t *testing.T) {
	assert.NoError(t, newValidateUtil().checkBinaryVectorFieldData(nil, nil))
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

	//////////////////////////////////////////////////////////////////

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

	t.Run("normal case", func(t *testing.T) {
		data := []*schemapb.FieldData{
			{
				FieldName: "test1",
				Type:      schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: generateFloatVectors(10, 8),
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
							BinaryVector: generateBinaryVectors(10, 8),
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
								Data: generateVarCharArray(10, 8),
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

		err := v.Validate(data, schema, 100)

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
							BinaryVector: generateBinaryVectors(2, 8),
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
								Data: generateVarCharArray(2, 8),
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
			},
		}

		v := newValidateUtil(withNANCheck(), withMaxLenCheck())

		err := v.Validate(data, schema, 2)

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
								Data: generateFloatVectors(2, 1),
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
							BinaryVector: generateBinaryVectors(2, 8),
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
								Data: []string{"very_long", "very_very_long"},
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
							Value: "2",
						},
					},
				},
			},
		}

		v := newValidateUtil(withNANCheck(), withMaxLenCheck())
		err := v.Validate(data, schema, 2)
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
		err = v.Validate(data, schema, 2)
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

		err := v.Validate(data, schema, 2)
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
								Data: generateFloatVectors(2, 8),
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
							BinaryVector: generateBinaryVectors(2, 8),
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
								Data: generateVarCharArray(2, 8),
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
			},
		}

		v := newValidateUtil(withNANCheck(), withMaxLenCheck(), withOverflowCheck())

		err := v.Validate(data, schema, 2)

		assert.NoError(t, err)
	})
}

func checkFillWithDefaultValueData[T comparable](values []T, v T, length int) bool {
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

func Test_validateUtil_fillWithDefaultValue(t *testing.T) {
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

		err = v.fillWithDefaultValue(data, h, 1)

		assert.Error(t, err)
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
			},
		}

		var key bool
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

		err = v.fillWithDefaultValue(data, h, 10)

		assert.NoError(t, err)

		flag := checkFillWithDefaultValueData(data[0].GetScalars().GetBoolData().Data, schema.Fields[0].GetDefaultValue().GetBoolData(), 10)
		assert.True(t, flag)
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

		err = v.fillWithDefaultValue(data, h, 10)

		assert.NoError(t, err)
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

		err = v.fillWithDefaultValue(data, h, 10)

		assert.NoError(t, err)

		flag := checkFillWithDefaultValueData(data[0].GetScalars().GetBoolData().Data, true, 1)
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

		err = v.fillWithDefaultValue(data, h, 1)

		assert.Error(t, err)
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

		err = v.fillWithDefaultValue(data, h, 10)

		assert.NoError(t, err)

		flag := checkFillWithDefaultValueData(data[0].GetScalars().GetIntData().Data, schema.Fields[0].GetDefaultValue().GetIntData(), 10)
		assert.True(t, flag)
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

		err = v.fillWithDefaultValue(data, h, 10)

		assert.NoError(t, err)
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

		err = v.fillWithDefaultValue(data, h, 10)

		assert.NoError(t, err)

		flag := checkFillWithDefaultValueData(data[0].GetScalars().GetIntData().Data, intData[0], 1)
		assert.True(t, flag)
	})
	////////////////////////////////////////////////////////////////////

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

		err = v.fillWithDefaultValue(data, h, 1)

		assert.Error(t, err)
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

		err = v.fillWithDefaultValue(data, h, 10)

		assert.NoError(t, err)
		flag := checkFillWithDefaultValueData(data[0].GetScalars().GetLongData().Data, schema.Fields[0].GetDefaultValue().GetLongData(), 10)
		assert.True(t, flag)
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

		err = v.fillWithDefaultValue(data, h, 10)

		assert.NoError(t, err)
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

		err = v.fillWithDefaultValue(data, h, 10)

		assert.NoError(t, err)

		flag := checkFillWithDefaultValueData(data[0].GetScalars().GetLongData().Data, longData[0], 1)
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

		err = v.fillWithDefaultValue(data, h, 1)

		assert.Error(t, err)
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

		err = v.fillWithDefaultValue(data, h, 10)

		assert.NoError(t, err)

		flag := checkFillWithDefaultValueData(data[0].GetScalars().GetFloatData().Data, schema.Fields[0].GetDefaultValue().GetFloatData(), 10)
		assert.True(t, flag)
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

		err = v.fillWithDefaultValue(data, h, 10)

		assert.NoError(t, err)
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

		err = v.fillWithDefaultValue(data, h, 10)

		assert.NoError(t, err)

		flag := checkFillWithDefaultValueData(data[0].GetScalars().GetFloatData().Data, floatData[0], 1)
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

		err = v.fillWithDefaultValue(data, h, 1)

		assert.Error(t, err)
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

		err = v.fillWithDefaultValue(data, h, 10)

		assert.NoError(t, err)

		flag := checkFillWithDefaultValueData(data[0].GetScalars().GetDoubleData().Data, schema.Fields[0].GetDefaultValue().GetDoubleData(), 10)
		assert.True(t, flag)
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

		err = v.fillWithDefaultValue(data, h, 10)

		assert.NoError(t, err)
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

		err = v.fillWithDefaultValue(data, h, 10)

		assert.NoError(t, err)

		flag := checkFillWithDefaultValueData(data[0].GetScalars().GetDoubleData().Data, doubleData[0], 1)
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

		err = v.fillWithDefaultValue(data, h, 1)

		assert.Error(t, err)
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

		err = v.fillWithDefaultValue(data, h, 10)

		assert.NoError(t, err)

		flag := checkFillWithDefaultValueData(data[0].GetScalars().GetStringData().Data, schema.Fields[0].GetDefaultValue().GetStringData(), 10)
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

		err = v.fillWithDefaultValue(data, h, 10)

		assert.NoError(t, err)

		flag := checkFillWithDefaultValueData(data[0].GetScalars().GetStringData().Data, stringData[0], 1)
		assert.True(t, flag)
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

		err = v.fillWithDefaultValue(data, h, 10)

		assert.NoError(t, err)

		flag := checkFillWithDefaultValueData(data[0].GetScalars().GetStringData().Data, stringData[0], 1)
		assert.True(t, flag)
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
		assert.NoError(t, v.checkIntegerFieldData(nil, nil))
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
