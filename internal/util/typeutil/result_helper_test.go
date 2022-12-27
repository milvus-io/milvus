package typeutil

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"

	"github.com/milvus-io/milvus-proto/go-api/milvuspb"

	"github.com/milvus-io/milvus/internal/proto/internalpb"

	"github.com/milvus-io/milvus/internal/proto/segcorepb"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
)

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
	}
	allUnsupportedTypes := []schemapb.DataType{
		schemapb.DataType_String,
		schemapb.DataType_None,
	}
	vectorTypes := []schemapb.DataType{
		schemapb.DataType_BinaryVector,
		schemapb.DataType_FloatVector,
	}

	field := &schemapb.FieldSchema{Name: "field_name", FieldID: 100}
	for _, dataType := range allTypes {
		field.DataType = dataType
		fieldData, err := GenEmptyFieldData(field)
		assert.NoError(t, err)
		assert.Equal(t, dataType, fieldData.GetType())
		assert.Equal(t, field.GetName(), fieldData.GetFieldName())
		assert.True(t, fieldDataEmpty(fieldData))
		assert.Equal(t, field.GetFieldID(), fieldData.GetFieldId())
	}

	for _, dataType := range allUnsupportedTypes {
		field.DataType = dataType
		_, err := GenEmptyFieldData(field)
		assert.Error(t, err)
	}

	// dim not found
	for _, dataType := range vectorTypes {
		field.DataType = dataType
		_, err := GenEmptyFieldData(field)
		assert.Error(t, err)
	}

	field.TypeParams = []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}
	for _, dataType := range vectorTypes {
		field.DataType = dataType
		fieldData, err := GenEmptyFieldData(field)
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
}
