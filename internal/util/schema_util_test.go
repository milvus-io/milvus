package util

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
)

func TestSchemaDiff_BasicFieldChanges(t *testing.T) {
	// Create old schema with basic fields
	oldSchema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      1,
				Name:         "id",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  2,
				Name:     "name",
				DataType: schemapb.DataType_VarChar,
			},
			{
				FieldID:  3,
				Name:     "vector",
				DataType: schemapb.DataType_FloatVector,
			},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Id:   1,
				Name: "bm25_function",
				Type: schemapb.FunctionType_BM25,
			},
		},
	}

	// Create new schema with some changes
	newSchema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      1,
				Name:         "id",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  2,
				Name:     "name",
				DataType: schemapb.DataType_VarChar,
				Nullable: true, // Modified: added nullable
			},
			{
				FieldID:  4, // Added: new field
				Name:     "description",
				DataType: schemapb.DataType_VarChar,
			},
			// Removed: vector field (FieldID 3)
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Id:              1,
				Name:            "bm25_function",
				Type:            schemapb.FunctionType_BM25,
				InputFieldNames: []string{"name"}, // Modified: added input field names
			},
			{
				Id:   2, // Added: new function
				Name: "embedding_function",
				Type: schemapb.FunctionType_Unknown,
			},
		},
	}

	fieldDiff, funcDiff, err := SchemaDiff(oldSchema, newSchema)
	assert.NoError(t, err)
	assert.NotNil(t, fieldDiff)
	assert.NotNil(t, funcDiff)

	// Check field differences - only Added fields
	assert.Len(t, fieldDiff.Added, 1)
	assert.Equal(t, int64(4), fieldDiff.Added[0].GetFieldID())
	assert.Equal(t, "description", fieldDiff.Added[0].GetName())

	// Check function differences - only Added functions
	assert.Len(t, funcDiff.Added, 1)
	assert.Equal(t, int64(2), funcDiff.Added[0].GetId())
	assert.Equal(t, "embedding_function", funcDiff.Added[0].GetName())
}

func TestSchemaDiff_NilSchemas(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "id", DataType: schemapb.DataType_Int64},
		},
	}

	// Test nil old schema
	_, _, err := SchemaDiff(nil, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "old_schema cannot be nil")

	// Test nil new schema
	_, _, err = SchemaDiff(schema, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "new_schema cannot be nil")
}

func TestSchemaDiff_IdenticalSchemas(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      1,
				Name:         "id",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Id:   1,
				Name: "test_function",
				Type: schemapb.FunctionType_BM25,
			},
		},
	}

	fieldDiff, funcDiff, err := SchemaDiff(schema, schema)
	assert.NoError(t, err)
	assert.NotNil(t, fieldDiff)
	assert.NotNil(t, funcDiff)

	// No differences should be found
	assert.Len(t, fieldDiff.Added, 0)
	assert.Len(t, funcDiff.Added, 0)
}

func TestSchemaDiff_OnlyAddedFields(t *testing.T) {
	oldSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  1,
				Name:     "existing_field",
				DataType: schemapb.DataType_Int64,
			},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Id:   1,
				Name: "existing_function",
				Type: schemapb.FunctionType_BM25,
			},
		},
	}

	newSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  1,
				Name:     "existing_field",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:  2,
				Name:     "new_field_1",
				DataType: schemapb.DataType_VarChar,
			},
			{
				FieldID:  3,
				Name:     "new_field_2",
				DataType: schemapb.DataType_FloatVector,
			},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Id:   1,
				Name: "existing_function",
				Type: schemapb.FunctionType_BM25,
			},
			{
				Id:   2,
				Name: "new_function_1",
				Type: schemapb.FunctionType_Unknown,
			},
		},
	}

	fieldDiff, funcDiff, err := SchemaDiff(oldSchema, newSchema)
	assert.NoError(t, err)
	assert.NotNil(t, fieldDiff)
	assert.NotNil(t, funcDiff)

	// Should find 2 added fields
	assert.Len(t, fieldDiff.Added, 2)
	assert.Equal(t, int64(2), fieldDiff.Added[0].GetFieldID())
	assert.Equal(t, "new_field_1", fieldDiff.Added[0].GetName())
	assert.Equal(t, int64(3), fieldDiff.Added[1].GetFieldID())
	assert.Equal(t, "new_field_2", fieldDiff.Added[1].GetName())

	// Should find 1 added function
	assert.Len(t, funcDiff.Added, 1)
	assert.Equal(t, int64(2), funcDiff.Added[0].GetId())
	assert.Equal(t, "new_function_1", funcDiff.Added[0].GetName())
}
