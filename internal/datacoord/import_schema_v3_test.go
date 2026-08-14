package datacoord

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func importV3ProjectionSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:        "collection",
		Description: "old description",
		Version:     1,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "body", DataType: schemapb.DataType_VarChar},
		},
		Properties: []*commonpb.KeyValuePair{
			{Key: common.CollectionTTLConfigKey, Value: "3600"},
			{Key: "presentation.only", Value: "old"},
		},
	}
}

func TestCompareImportSchemaProjectionIgnoresPresentationChanges(t *testing.T) {
	frozen := importV3ProjectionSchema()
	current := proto.Clone(frozen).(*schemapb.CollectionSchema)
	current.Version = 2
	current.Name = "renamed"
	current.Description = "new description"
	current.Fields[1].Name = "renamed_body"
	current.Fields[1].Description = "new field description"
	current.Properties[1].Value = "new"

	equal, difference := compareImportSchemaProjection(frozen, current, nil)
	require.True(t, equal)
	require.Empty(t, difference)
}

func TestCompareImportSchemaProjectionRejectsPhysicalChanges(t *testing.T) {
	tests := map[string]func(*schemapb.CollectionSchema){
		"field type": func(schema *schemapb.CollectionSchema) {
			schema.Fields[1].DataType = schemapb.DataType_JSON
		},
		"ttl": func(schema *schemapb.CollectionSchema) {
			schema.Properties[0].Value = "7200"
		},
		"function": func(schema *schemapb.CollectionSchema) {
			schema.Functions = []*schemapb.FunctionSchema{{
				Id: 1, Type: schemapb.FunctionType_BM25,
				InputFieldIds: []int64{101}, OutputFieldIds: []int64{102},
			}}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			frozen := importV3ProjectionSchema()
			current := proto.Clone(frozen).(*schemapb.CollectionSchema)
			current.Version = 2
			mutate(current)

			equal, difference := compareImportSchemaProjection(frozen, current, nil)
			require.False(t, equal)
			require.NotEmpty(t, difference)
		})
	}
}
