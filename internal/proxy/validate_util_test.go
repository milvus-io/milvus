package proxy

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func Test_verifyLengthPerRow(t *testing.T) {
	maxLength := 16

	assert.NoError(t, verifyLengthPerRow(nil, int64(maxLength)))

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
					Key:   "max_length",
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
					Key:   "max_length",
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
					Key:   "max_length",
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
							Key:   "dim",
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
							Key:   "dim",
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
							Key:   "dim",
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
							Key:   "dim",
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
							Key:   "max_length",
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
							Key:   "dim",
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
							Key:   "dim",
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
							Key:   "max_length",
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
							Key:   "max_length",
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
							Key:   "dim",
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
							Key:   "dim",
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
							Key:   "max_length",
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
							Key:   "dim",
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
							Key:   "dim",
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
							Key:   "max_length",
							Value: "2",
						},
					},
				},
			},
		}

		v := newValidateUtil(withNANCheck(), withMaxLenCheck())

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
							Key:   "dim",
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
							Key:   "dim",
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
							Key:   "max_length",
							Value: "8",
						},
					},
				},
			},
		}

		v := newValidateUtil(withNANCheck(), withMaxLenCheck())

		err := v.Validate(data, schema, 10)

		assert.NoError(t, err)
	})
}
