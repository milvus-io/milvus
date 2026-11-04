package typeutil

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func TestSchemaHelperUserFieldLookup(t *testing.T) {
	for _, dynamic := range []bool{false, true} {
		schema := &schemapb.CollectionSchema{EnableDynamicField: dynamic, Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		}}
		if dynamic {
			schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: 101, Name: common.MetaFieldName, DataType: schemapb.DataType_JSON, IsDynamic: true})
		}
		helper, err := CreateSchemaHelper(schema)
		require.NoError(t, err)
		for _, name := range []string{common.RowIDFieldName, common.TimeStampFieldName} {
			field, err := helper.GetFieldFromName(name)
			require.NoError(t, err)
			require.True(t, common.IsSystemField(field.GetFieldID()))
			_, err = helper.GetUserFieldFromName(name)
			require.Error(t, err)
			field, err = helper.GetUserFieldFromNameDefaultJSON(name)
			if dynamic {
				require.NoError(t, err)
				require.Same(t, schema.Fields[3], field)
			} else {
				require.Error(t, err)
			}
		}
		field, err := helper.GetUserFieldFromName("id")
		require.NoError(t, err)
		require.Same(t, schema.Fields[2], field)
		field, err = helper.GetUserFieldFromNameDefaultJSON("id")
		require.NoError(t, err)
		require.Same(t, schema.Fields[2], field)
		_, err = helper.GetUserFieldFromName("missing")
		require.Error(t, err)
	}
}
