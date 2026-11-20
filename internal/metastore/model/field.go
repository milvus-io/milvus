package model

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

// Field is the in-memory field schema held by model.Collection. It is the
// protobuf FieldSchema itself, so the model and the collection view can
// share it.
type Field = schemapb.FieldSchema

// CloneField returns a deep copy of the field.
func CloneField(f *Field) *Field {
	if f == nil {
		return nil
	}
	return proto.Clone(f).(*Field)
}

// CloneFields returns a deep copy of each field; nil stays nil.
func CloneFields(fields []*Field) []*Field {
	if fields == nil {
		return nil
	}
	clone := make([]*Field, len(fields))
	for i, field := range fields {
		clone[i] = CloneField(field)
	}
	return clone
}

func checkParamsEqual(paramsA, paramsB []*commonpb.KeyValuePair) bool {
	var A common.KeyValuePairs = paramsA
	return A.Equal(paramsB)
}

// FieldEqual reports whether two fields are equal, ignoring State.
func FieldEqual(a, b *Field) bool {
	return a.FieldID == b.FieldID &&
		a.Name == b.Name &&
		a.IsPrimaryKey == b.IsPrimaryKey &&
		a.Description == b.Description &&
		a.DataType == b.DataType &&
		checkParamsEqual(a.TypeParams, b.TypeParams) &&
		checkParamsEqual(a.IndexParams, b.IndexParams) &&
		a.AutoID == b.AutoID &&
		a.IsPartitionKey == b.IsPartitionKey &&
		a.IsDynamic == b.IsDynamic &&
		a.IsClusteringKey == b.IsClusteringKey &&
		proto.Equal(a.DefaultValue, b.DefaultValue) &&
		a.ElementType == b.ElementType &&
		a.ElementNullable == b.ElementNullable &&
		proto.Equal(a.TypeSchema, b.TypeSchema) &&
		a.IsFunctionOutput == b.IsFunctionOutput &&
		a.Nullable == b.Nullable &&
		a.ExternalField == b.ExternalField
}

func CheckFieldsEqual(fieldsA, fieldsB []*Field) bool {
	if len(fieldsA) != len(fieldsB) {
		return false
	}
	mapA := make(map[int64]*Field)
	for _, f := range fieldsA {
		mapA[f.FieldID] = f
	}

	for _, f := range fieldsB {
		if other, exists := mapA[f.FieldID]; !exists || !FieldEqual(f, other) {
			return false
		}
	}
	return true
}
