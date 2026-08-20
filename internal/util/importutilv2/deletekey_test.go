package importutilv2

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestBuildDeleteKeySchema(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "coll",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "doc_id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector},
			{FieldID: 102, Name: "text", DataType: schemapb.DataType_VarChar},
		},
	}

	got, err := BuildDeleteKeySchema(schema)
	assert.NoError(t, err)
	assert.Len(t, got.GetFields(), 1)
	assert.Equal(t, "doc_id", got.GetFields()[0].GetName())
	assert.Equal(t, int64(100), got.GetFields()[0].GetFieldID())
	assert.True(t, got.GetFields()[0].GetIsPrimaryKey())
	// AutoID must be cleared or isSchemaEqual will skip the column entirely.
	assert.False(t, got.GetFields()[0].GetAutoID())
	assert.Empty(t, got.GetStructArrayFields())
	// the source schema must not be mutated
	assert.True(t, schema.GetFields()[0].GetAutoID())
}

func TestBuildDeleteKeySchema_NoPrimaryKey(t *testing.T) {
	_, err := BuildDeleteKeySchema(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{{FieldID: 101, Name: "vec"}},
	})
	assert.Error(t, err)
}

func TestBuildDeleteKeySchema_VarCharPrimaryKey(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "coll",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "doc_id",
				DataType:     schemapb.DataType_VarChar,
				IsPrimaryKey: true,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "max_length", Value: "128"},
				},
			},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector},
		},
	}

	got, err := BuildDeleteKeySchema(schema)
	assert.NoError(t, err)
	assert.Len(t, got.GetFields(), 1)
	pkField := got.GetFields()[0]
	assert.Equal(t, "doc_id", pkField.GetName())
	assert.Equal(t, schemapb.DataType_VarChar, pkField.GetDataType())
	assert.True(t, pkField.GetIsPrimaryKey())
	assert.False(t, pkField.GetAutoID())
	require.Len(t, pkField.GetTypeParams(), 1)
	assert.Equal(t, "max_length", pkField.GetTypeParams()[0].GetKey())
	assert.Equal(t, "128", pkField.GetTypeParams()[0].GetValue())
}
