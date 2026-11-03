package dql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/util/schemautil"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func TestUserQueriesWithSystemFields(t *testing.T) {
	for _, dynamic := range []bool{false, true} {
		raw := &schemapb.CollectionSchema{EnableDynamicField: dynamic, Fields: []*schemapb.FieldSchema{
			{FieldID: 0, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: 1, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		}}
		if dynamic {
			raw.Fields = append(raw.Fields, &schemapb.FieldSchema{FieldID: 101, Name: common.MetaFieldName, DataType: schemapb.DataType_JSON, IsDynamic: true})
		}
		schema, err := schemautil.NewSchemaInfo(raw)
		require.NoError(t, err)
		outputs, _, _, _, _, err := translateOutputFields([]string{"*"}, schema, false)
		require.NoError(t, err)
		require.Contains(t, outputs, "id")
		for _, name := range []string{common.RowIDFieldName, common.TimeStampFieldName} {
			require.NotContains(t, outputs, name)
			_, err := translateGroupByFieldIds([]string{name}, raw)
			require.Error(t, err)
			_, err = getOutputFieldIDs(schema, []string{name})
			require.Error(t, err)
			_, _, _, _, _, outputErr := translateOutputFields([]string{name}, schema, false)
			plan, exprErr := planparserv2.CreateRetrievePlan(schema.SchemaHelper, name+" > 0", nil)
			groupID, _, groupErr := parseGroupByField(name, raw)
			if dynamic {
				require.NoError(t, outputErr)
				require.NoError(t, exprErr)
				column := plan.GetQuery().GetPredicates().GetUnaryRangeExpr().GetColumnInfo()
				require.EqualValues(t, 101, column.GetFieldId())
				require.Equal(t, []string{name}, column.GetNestedPath())
				require.NoError(t, groupErr)
				require.EqualValues(t, 101, groupID)
			} else {
				require.Error(t, outputErr)
				require.Error(t, exprErr)
				require.Error(t, groupErr)
			}
		}
	}
}
