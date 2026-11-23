package typeutil

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func SupportsPKUpdateType(field *schemapb.FieldSchema) bool {
	switch field.GetDataType() {
	case schemapb.DataType_Bool, schemapb.DataType_Int8, schemapb.DataType_Int16,
		schemapb.DataType_Int32, schemapb.DataType_Int64, schemapb.DataType_VarChar,
		schemapb.DataType_Timestamptz:
		return true
	case schemapb.DataType_Array:
		switch field.GetElementType() {
		case schemapb.DataType_Bool, schemapb.DataType_Int8, schemapb.DataType_Int16,
			schemapb.DataType_Int32, schemapb.DataType_Int64, schemapb.DataType_Float,
			schemapb.DataType_Double, schemapb.DataType_VarChar:
			return true
		}
	}
	return false
}

// PKUpdateFieldRestriction resolves function and TTL dependencies once for a schema.
func PKUpdateFieldRestriction(schema *schemapb.CollectionSchema) func(*schemapb.FieldSchema) bool {
	restrictedNames := map[string]bool{}
	restrictedIDs := map[int64]bool{}
	for _, parent := range schema.GetStructArrayFields() {
		for _, child := range parent.GetFields() {
			restrictedIDs[child.GetFieldID()] = true
		}
	}
	for _, f := range schema.GetFunctions() {
		for _, name := range append(append([]string{}, f.GetInputFieldNames()...), f.GetOutputFieldNames()...) {
			restrictedNames[name] = true
		}
		for _, id := range append(append([]int64{}, f.GetInputFieldIds()...), f.GetOutputFieldIds()...) {
			restrictedIDs[id] = true
		}
	}
	for _, p := range schema.GetProperties() {
		if p.GetKey() == common.CollectionTTLFieldKey {
			restrictedNames[p.GetValue()] = true
		}
	}
	return func(field *schemapb.FieldSchema) bool {
		if field.GetDataType() == schemapb.DataType_VarChar && CreateFieldSchemaHelper(field).EnableMatch() {
			return true
		}
		return field.GetFieldID() < common.StartOfUserFieldID || field.GetIsPrimaryKey() || field.GetIsPartitionKey() || field.GetIsClusteringKey() || field.GetIsFunctionOutput() || restrictedNames[field.GetName()] || restrictedIDs[field.GetFieldID()]
	}
}
