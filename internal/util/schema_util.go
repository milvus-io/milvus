package util

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// FieldDiff represents the differences in fields between two schemas
type FieldDiff struct {
	Added []*schemapb.FieldSchema // Fields present in new_schema but not in old_schema
}

// FuncDiff represents the differences in functions between two schemas
type FuncDiff struct {
	Added []*schemapb.FunctionSchema // Functions present in new_schema but not in old_schema
}

// schema_diff compares two schemas and returns the differences in fields and functions
func SchemaDiff(oldSchema, newSchema *schemapb.CollectionSchema) (*FieldDiff, *FuncDiff, error) {
	if oldSchema == nil {
		return nil, nil, fmt.Errorf("old_schema cannot be nil")
	}
	if newSchema == nil {
		return nil, nil, fmt.Errorf("new_schema cannot be nil")
	}
	fieldDiff := compareFields(oldSchema.GetFields(), newSchema.GetFields())
	funcDiff := compareFunctions(oldSchema.GetFunctions(), newSchema.GetFunctions())
	return fieldDiff, funcDiff, nil
}

// compareFields compares field arrays and returns the differences
func compareFields(oldFields, newFields []*schemapb.FieldSchema) *FieldDiff {
	diff := &FieldDiff{
		Added: make([]*schemapb.FieldSchema, 0),
	}

	// Create map for efficient lookup by FieldID
	oldFieldMap := make(map[int64]*schemapb.FieldSchema)

	for _, field := range oldFields {
		if field != nil {
			oldFieldMap[field.GetFieldID()] = field
		}
	}

	// Find added fields
	for _, newField := range newFields {
		if newField != nil {
			if _, exists := oldFieldMap[newField.GetFieldID()]; !exists {
				// Field only exists in new schema
				diff.Added = append(diff.Added, newField)
			}
		}
	}

	return diff
}

// compareFunctions compares function arrays and returns the differences
func compareFunctions(oldFunctions, newFunctions []*schemapb.FunctionSchema) *FuncDiff {
	diff := &FuncDiff{
		Added: make([]*schemapb.FunctionSchema, 0),
	}
	// Create map for efficient lookup by function ID
	oldFuncMap := make(map[int64]*schemapb.FunctionSchema)

	for _, function := range oldFunctions {
		if function != nil {
			oldFuncMap[function.GetId()] = function
		}
	}

	// Find added functions
	for _, newFunc := range newFunctions {
		if newFunc != nil {
			if _, exists := oldFuncMap[newFunc.GetId()]; !exists {
				// Function only exists in new schema
				diff.Added = append(diff.Added, newFunc)
			}
		}
	}

	return diff
}
