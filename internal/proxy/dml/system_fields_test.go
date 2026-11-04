package dml

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func TestWriteValidationWithSystemFields(t *testing.T) {
	schema, err := newSchemaInfo(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 0, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
		{FieldID: 1, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}})
	require.NoError(t, err)
	columns := []*schemapb.FieldData{{FieldName: "id", FieldId: 100, Type: schemapb.DataType_Int64}}
	msg := &BaseInsertTask{InsertRequest: &msgpb.InsertRequest{FieldsData: columns}}
	require.NoError(t, checkFieldsDataBySchema(t.Context(), schema.Fields, schema.CollectionSchema, msg, true))
	require.NoError(t, validateFieldDataColumns(columns, schema))
	require.NoError(t, LackOfFieldsDataBySchema(schema.CollectionSchema, columns, false, false))
	require.Error(t, LackOfFieldsDataBySchema(schema.CollectionSchema, nil, false, false))
	for _, name := range []string{common.RowIDFieldName, common.TimeStampFieldName} {
		msg.FieldsData = append(columns[:len(columns):len(columns)], &schemapb.FieldData{FieldName: name})
		require.ErrorContains(t, checkFieldsDataBySchema(t.Context(), schema.Fields, schema.CollectionSchema, msg, true), "cannot write system field")
		_, err := findFieldSchemaByName(schema.CollectionSchema, name)
		require.Error(t, err)
	}
	require.Len(t, schema.Fields, 3)
}
