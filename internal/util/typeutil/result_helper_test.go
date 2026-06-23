package typeutil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func fieldDataEmpty(data *schemapb.FieldData) bool {
	if data == nil {
		return true
	}
	switch realData := data.Field.(type) {
	case *schemapb.FieldData_Scalars:
		switch realScalars := realData.Scalars.Data.(type) {
		case *schemapb.ScalarField_BoolData:
			return len(realScalars.BoolData.GetData()) <= 0
		case *schemapb.ScalarField_LongData:
			return len(realScalars.LongData.GetData()) <= 0
		case *schemapb.ScalarField_FloatData:
			return len(realScalars.FloatData.GetData()) <= 0
		case *schemapb.ScalarField_DoubleData:
			return len(realScalars.DoubleData.GetData()) <= 0
		case *schemapb.ScalarField_StringData:
			return len(realScalars.StringData.GetData()) <= 0
		}
	case *schemapb.FieldData_Vectors:
		switch realVectors := realData.Vectors.Data.(type) {
		case *schemapb.VectorField_BinaryVector:
			return len(realVectors.BinaryVector) <= 0
		case *schemapb.VectorField_FloatVector:
			return len(realVectors.FloatVector.Data) <= 0
		}
	}
	return true
}

func TestGenEmptyFieldData(t *testing.T) {
	allTypes := []schemapb.DataType{
		schemapb.DataType_Bool,
		schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_Float,
		schemapb.DataType_Double,
		schemapb.DataType_VarChar,
		schemapb.DataType_Array,
		schemapb.DataType_JSON,
	}
	allUnsupportedTypes := []schemapb.DataType{
		schemapb.DataType_String,
		schemapb.DataType_None,
	}
	vectorTypes := []schemapb.DataType{
		schemapb.DataType_BinaryVector,
		schemapb.DataType_FloatVector,
		schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector,
		schemapb.DataType_Int8Vector,
	}

	field := &schemapb.FieldSchema{Name: "field_name", FieldID: 100}
	for _, dataType := range allTypes {
		field.DataType = dataType
		fieldData, err := typeutil.GenEmptyFieldData(field)
		assert.NoError(t, err)
		assert.Equal(t, dataType, fieldData.GetType())
		assert.Equal(t, field.GetName(), fieldData.GetFieldName())
		assert.True(t, fieldDataEmpty(fieldData))
		assert.Equal(t, field.GetFieldID(), fieldData.GetFieldId())
	}

	for _, dataType := range allUnsupportedTypes {
		field.DataType = dataType
		_, err := typeutil.GenEmptyFieldData(field)
		assert.Error(t, err)
	}

	// dim not found
	for _, dataType := range vectorTypes {
		field.DataType = dataType
		_, err := typeutil.GenEmptyFieldData(field)
		assert.Error(t, err)
	}

	field.TypeParams = []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}}
	for _, dataType := range vectorTypes {
		field.DataType = dataType
		fieldData, err := typeutil.GenEmptyFieldData(field)
		assert.NoError(t, err)
		assert.Equal(t, dataType, fieldData.GetType())
		assert.Equal(t, field.GetName(), fieldData.GetFieldName())
		assert.True(t, fieldDataEmpty(fieldData))
		assert.Equal(t, field.GetFieldID(), fieldData.GetFieldId())
	}
}

func TestFillIfEmpty(t *testing.T) {
	t.Run("not empty, do nothing", func(t *testing.T) {
		result := &segcorepb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: []int64{1, 2},
					},
				},
			},
		}
		err := FillRetrieveResultIfEmpty(NewSegcoreResults(result), []int64{100, 101}, nil)
		assert.NoError(t, err)
	})

	t.Run("invalid schema", func(t *testing.T) {
		result := &segcorepb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: nil,
					},
				},
			},
		}
		err := FillRetrieveResultIfEmpty(NewSegcoreResults(result), []int64{100, 101}, nil)
		assert.Error(t, err)
	})

	t.Run("field not found", func(t *testing.T) {
		result := &segcorepb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: nil,
					},
				},
			},
		}
		schema := &schemapb.CollectionSchema{
			Name:        "collection",
			Description: "description",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  100,
					DataType: schemapb.DataType_Int64,
				},
			},
		}
		err := FillRetrieveResultIfEmpty(NewSegcoreResults(result), []int64{101}, schema)
		assert.Error(t, err)
	})

	t.Run("unsupported data type", func(t *testing.T) {
		result := &segcorepb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: nil,
					},
				},
			},
		}
		schema := &schemapb.CollectionSchema{
			Name:        "collection",
			Description: "description",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  100,
					DataType: schemapb.DataType_String,
				},
			},
		}
		err := FillRetrieveResultIfEmpty(NewSegcoreResults(result), []int64{100}, schema)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		result := &segcorepb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: nil,
					},
				},
			},
		}
		schema := &schemapb.CollectionSchema{
			Name:        "collection",
			Description: "description",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  100,
					Name:     "field100",
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:  101,
					Name:     "field101",
					DataType: schemapb.DataType_VarChar,
				},
			},
		}
		err := FillRetrieveResultIfEmpty(NewSegcoreResults(result), []int64{100, 101}, schema)
		assert.NoError(t, err)
		assert.Nil(t, result.GetOffset())
		assert.Equal(t, 2, len(result.GetFieldsData()))
		for _, fieldData := range result.GetFieldsData() {
			assert.True(t, fieldDataEmpty(fieldData))
		}
	})

	t.Run("normal case", func(t *testing.T) {
		result := &internalpb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: nil,
					},
				},
			},
		}
		schema := &schemapb.CollectionSchema{
			Name:        "collection",
			Description: "description",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  100,
					Name:     "field100",
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:  101,
					Name:     "field101",
					DataType: schemapb.DataType_VarChar,
				},
			},
		}
		err := FillRetrieveResultIfEmpty(NewInternalResult(result), []int64{100, 101}, schema)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(result.GetFieldsData()))
		for _, fieldData := range result.GetFieldsData() {
			assert.True(t, fieldDataEmpty(fieldData))
		}
	})

	t.Run("normal case", func(t *testing.T) {
		result := &milvuspb.QueryResults{
			FieldsData: nil,
		}
		schema := &schemapb.CollectionSchema{
			Name:        "collection",
			Description: "description",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  100,
					Name:     "field100",
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:  101,
					Name:     "field101",
					DataType: schemapb.DataType_VarChar,
				},
			},
		}
		err := FillRetrieveResultIfEmpty(NewMilvusResult(result), []int64{100, 101}, schema)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(result.GetFieldsData()))
		for _, fieldData := range result.GetFieldsData() {
			assert.True(t, fieldDataEmpty(fieldData))
		}
	})

	// System fields (RowID=0, Timestamp=1) are never part of the user/collection
	// schema, but the proxy appends common.TimeStampField to OutputFieldsId for
	// MVCC dedup. For external collections the segcore synthesizes these as Int64
	// columns, so the empty-fill path must emit them as empty Int64 columns too,
	// instead of failing the schema lookup with "fieldID(1) not found".
	// See https://github.com/milvus-io/milvus/issues/50188.
	t.Run("system field with external collection schema", func(t *testing.T) {
		result := &segcorepb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: nil,
					},
				},
			},
		}
		// External collection schema: virtual PK + user fields, no system fields.
		schema := &schemapb.CollectionSchema{
			Name: "external_collection",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         common.VirtualPKFieldName,
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
					AutoID:       true,
				},
				{
					FieldID:  101,
					Name:     "vc_tag",
					DataType: schemapb.DataType_VarChar,
				},
			},
		}
		// OutputFieldsId carries the system timestamp field appended by the proxy.
		err := FillRetrieveResultIfEmpty(NewSegcoreResults(result),
			[]int64{101, common.TimeStampField}, schema)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(result.GetFieldsData()))

		var tsFieldData *schemapb.FieldData
		for _, fieldData := range result.GetFieldsData() {
			assert.True(t, fieldDataEmpty(fieldData))
			if fieldData.GetFieldId() == common.TimeStampField {
				tsFieldData = fieldData
			}
		}
		assert.NotNil(t, tsFieldData)
		assert.Equal(t, schemapb.DataType_Int64, tsFieldData.GetType())
	})

	t.Run("row id system field", func(t *testing.T) {
		result := &segcorepb.RetrieveResults{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: nil}},
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: "external_collection",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: common.VirtualPKFieldName, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			},
		}
		err := FillRetrieveResultIfEmpty(NewSegcoreResults(result),
			[]int64{common.RowIDField, common.TimeStampField}, schema)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(result.GetFieldsData()))
		for _, fieldData := range result.GetFieldsData() {
			assert.True(t, fieldDataEmpty(fieldData))
			assert.Equal(t, schemapb.DataType_Int64, fieldData.GetType())
		}
	})
}
