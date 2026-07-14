// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schemautil

import (
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ValidateSchemaEvolution validates that a proposed schema can safely replace
// the committed schema without reinterpreting existing rows or leaving an
// inconsistent field graph.
func ValidateSchemaEvolution(oldSchema, newSchema *schemapb.CollectionSchema) error {
	if oldSchema == nil || newSchema == nil {
		return merr.WrapErrParameterInvalidMsg("old and new collection schemas must not be nil")
	}

	oldFields, err := buildEvolutionFieldMaps(oldSchema)
	if err != nil {
		return err
	}
	newFields, err := buildEvolutionFieldMaps(newSchema)
	if err != nil {
		return err
	}

	if err := validateKeptEvolutionFields(oldFields, newFields); err != nil {
		return err
	}
	if err := validateAddedEvolutionFields(oldFields, newFields, newSchema); err != nil {
		return err
	}
	if err := validateDroppedEvolutionFields(oldFields, newFields, newSchema); err != nil {
		return err
	}
	if err := validateEvolutionFunctions(oldSchema, newSchema, oldFields, newFields); err != nil {
		return err
	}
	if err := validateEvolutionDynamicGraph(newSchema); err != nil {
		return err
	}
	return validateMaxFieldIDEvolution(oldSchema, newSchema, oldFields, newFields)
}

type evolutionFieldMaps struct {
	fields          map[int64]*schemapb.FieldSchema
	structFields    map[int64]*schemapb.StructArrayFieldSchema
	structSubFields map[int64]struct{}
	structParents   map[int64]int64
	allIDs          map[int64]string
}

func buildEvolutionFieldMaps(schema *schemapb.CollectionSchema) (*evolutionFieldMaps, error) {
	result := &evolutionFieldMaps{
		fields:          make(map[int64]*schemapb.FieldSchema),
		structFields:    make(map[int64]*schemapb.StructArrayFieldSchema),
		structSubFields: make(map[int64]struct{}),
		structParents:   make(map[int64]int64),
		allIDs:          make(map[int64]string),
	}
	addID := func(id int64, kind string) error {
		if previous, ok := result.allIDs[id]; ok {
			return merr.WrapErrParameterInvalidMsg("duplicate field id %d is used by both %s and %s", id, previous, kind)
		}
		result.allIDs[id] = kind
		return nil
	}

	for _, field := range schema.GetFields() {
		if field == nil {
			return nil, merr.WrapErrParameterInvalidMsg("collection schema contains a nil field")
		}
		if err := addID(field.GetFieldID(), "field"); err != nil {
			return nil, err
		}
		result.fields[field.GetFieldID()] = field
	}
	for _, structField := range schema.GetStructArrayFields() {
		if structField == nil {
			return nil, merr.WrapErrParameterInvalidMsg("collection schema contains a nil struct field")
		}
		if err := addID(structField.GetFieldID(), "struct field"); err != nil {
			return nil, err
		}
		result.structFields[structField.GetFieldID()] = structField
		for _, field := range structField.GetFields() {
			if field == nil {
				return nil, merr.WrapErrParameterInvalidMsg("struct field %q contains a nil sub-field", structField.GetName())
			}
			if err := addID(field.GetFieldID(), "struct sub-field"); err != nil {
				return nil, err
			}
			result.fields[field.GetFieldID()] = field
			result.structSubFields[field.GetFieldID()] = struct{}{}
			result.structParents[field.GetFieldID()] = structField.GetFieldID()
		}
	}
	return result, nil
}

func validateKeptEvolutionFields(oldFields, newFields *evolutionFieldMaps) error {
	for id, oldKind := range oldFields.allIDs {
		if newKind, kept := newFields.allIDs[id]; kept && oldKind != newKind {
			return merr.WrapErrParameterInvalidMsg("cannot change field id %d from %s to %s", id, oldKind, newKind)
		}
	}
	for id, oldParent := range oldFields.structParents {
		if newParent, kept := newFields.structParents[id]; kept && oldParent != newParent {
			return merr.WrapErrParameterInvalidMsg("cannot move struct sub-field id %d from parent %d to parent %d", id, oldParent, newParent)
		}
	}
	for id, oldField := range oldFields.fields {
		newField, kept := newFields.fields[id]
		if !kept {
			continue
		}
		if oldFields.allIDs[id] != newFields.allIDs[id] {
			return merr.WrapErrParameterInvalidMsg("cannot change the kind of field id %d in place", id)
		}
		if err := validateKeptEvolutionField(oldField, newField); err != nil {
			return err
		}
	}
	for id, oldField := range oldFields.structFields {
		newField, kept := newFields.structFields[id]
		if !kept {
			continue
		}
		if oldField.GetName() != newField.GetName() {
			return merr.WrapErrParameterInvalidMsg("cannot rename struct field id %d from %q to %q", id, oldField.GetName(), newField.GetName())
		}
		if oldField.GetNullable() != newField.GetNullable() {
			return merr.WrapErrParameterInvalidMsg("cannot change the nullability of struct field %q in place", oldField.GetName())
		}
		oldChildren := make(map[int64]struct{}, len(oldField.GetFields()))
		for _, child := range oldField.GetFields() {
			oldChildren[child.GetFieldID()] = struct{}{}
		}
		newChildren := make(map[int64]struct{}, len(newField.GetFields()))
		for _, child := range newField.GetFields() {
			newChildren[child.GetFieldID()] = struct{}{}
		}
		for childID := range oldChildren {
			if _, kept := newChildren[childID]; !kept {
				return merr.WrapErrParameterInvalidMsg("cannot drop sub-field id %d from kept struct field %q", childID, oldField.GetName())
			}
		}
		for childID := range newChildren {
			if _, existed := oldChildren[childID]; !existed {
				return merr.WrapErrParameterInvalidMsg("cannot add sub-field id %d to kept struct field %q", childID, oldField.GetName())
			}
		}
		if err := validateMonotonicNumericBounds(oldField.GetName(), oldField.GetTypeParams(), newField.GetTypeParams()); err != nil {
			return err
		}
	}
	return nil
}

func validateKeptEvolutionField(oldField, newField *schemapb.FieldSchema) error {
	name := oldField.GetName()
	switch {
	case name != newField.GetName():
		return merr.WrapErrParameterInvalidMsg("cannot rename field id %d from %q to %q", oldField.GetFieldID(), name, newField.GetName())
	case oldField.GetDataType() != newField.GetDataType():
		return merr.WrapErrParameterInvalidMsg("cannot change the data type of field %q in place", name)
	case oldField.GetElementType() != newField.GetElementType():
		return merr.WrapErrParameterInvalidMsg("cannot change the element type of field %q in place", name)
	case oldField.GetNullable() != newField.GetNullable():
		return merr.WrapErrParameterInvalidMsg("cannot change the nullability of field %q in place", name)
	case !oldField.GetIsFunctionOutput() && newField.GetIsFunctionOutput():
		return merr.WrapErrParameterInvalidMsg("cannot repurpose existing field %q as a function output", name)
	case oldField.GetIsPrimaryKey() != newField.GetIsPrimaryKey(),
		oldField.GetIsPartitionKey() != newField.GetIsPartitionKey(),
		oldField.GetIsClusteringKey() != newField.GetIsClusteringKey(),
		oldField.GetAutoID() != newField.GetAutoID(),
		oldField.GetIsDynamic() != newField.GetIsDynamic():
		return merr.WrapErrParameterInvalidMsg("cannot change the structural role of field %q in place", name)
	}
	return validateMonotonicNumericBounds(name, oldField.GetTypeParams(), newField.GetTypeParams())
}

func validateAddedEvolutionFields(oldFields, newFields *evolutionFieldMaps, newSchema *schemapb.CollectionSchema) error {
	for id, field := range newFields.fields {
		if _, existed := oldFields.allIDs[id]; existed {
			continue
		}
		if _, isSubField := newFields.structSubFields[id]; isSubField {
			parentID := newFields.structParents[id]
			if _, parentExisted := oldFields.structFields[parentID]; parentExisted {
				return merr.WrapErrParameterInvalidMsg("cannot add sub-field %q to kept struct field id %d", field.GetName(), parentID)
			}
			if field.GetIsPrimaryKey() || field.GetAutoID() || field.GetIsPartitionKey() || field.GetIsClusteringKey() || field.GetIsFunctionOutput() || field.GetIsDynamic() {
				return merr.WrapErrParameterInvalidMsg("cannot add struct sub-field %q with a protected role", field.GetName())
			}
			if !field.GetNullable() {
				return merr.WrapErrParameterInvalidMsg("cannot add non-nullable sub-field %q in a new struct field", field.GetName())
			}
			continue
		}
		if err := validateAddedEvolutionField(field, newSchema.GetEnableDynamicField()); err != nil {
			return err
		}
	}
	for id, structField := range newFields.structFields {
		if _, existed := oldFields.allIDs[id]; existed {
			continue
		}
		if structField.GetFieldID() < common.StartOfUserFieldID || isReservedEvolutionFieldName(structField.GetName()) {
			return merr.WrapErrParameterInvalidMsg("cannot add system struct field %q online", structField.GetName())
		}
		if !structField.GetNullable() {
			return merr.WrapErrParameterInvalidMsg("cannot add non-nullable struct field %q online", structField.GetName())
		}
		if len(structField.GetFields()) == 0 {
			return merr.WrapErrParameterInvalidMsg("new struct field %q must contain at least one sub-field", structField.GetName())
		}
	}
	return nil
}

func validateAddedEvolutionField(field *schemapb.FieldSchema, dynamicEnabled bool) error {
	name := field.GetName()
	if field.GetIsPrimaryKey() {
		return merr.WrapErrParameterInvalidMsg("cannot add primary key field %q online", name)
	}
	if field.GetAutoID() {
		return merr.WrapErrParameterInvalidMsg("cannot add auto-ID field %q online", name)
	}
	if field.GetIsPartitionKey() {
		return merr.WrapErrParameterInvalidMsg("cannot add partition key field %q online", name)
	}
	if field.GetFieldID() < common.StartOfUserFieldID {
		return merr.WrapErrParameterInvalidMsg("cannot add system field %q online", name)
	}
	if isReservedEvolutionFieldName(name) && !(dynamicEnabled && field.GetIsDynamic() && name == common.MetaFieldName) {
		return merr.WrapErrParameterInvalidMsg("cannot add system field %q online", name)
	}
	if field.GetIsFunctionOutput() || field.GetIsDynamic() {
		return nil
	}
	if !field.GetNullable() && field.GetDefaultValue() == nil {
		return merr.WrapErrParameterInvalidMsg("cannot add non-nullable field %q without a default value", name)
	}
	return nil
}

func validateDroppedEvolutionFields(oldFields, newFields *evolutionFieldMaps, newSchema *schemapb.CollectionSchema) error {
	for id, field := range oldFields.fields {
		if _, kept := newFields.allIDs[id]; kept {
			continue
		}
		if parentID, isSubField := oldFields.structParents[id]; isSubField {
			if _, parentKept := newFields.structFields[parentID]; parentKept {
				return merr.WrapErrParameterInvalidMsg("cannot drop sub-field %q from kept struct field id %d", field.GetName(), parentID)
			}
		}
		if err := validateDroppedEvolutionField(field, newSchema); err != nil {
			return err
		}
	}
	return nil
}

func validateDroppedEvolutionField(field *schemapb.FieldSchema, newSchema *schemapb.CollectionSchema) error {
	name := field.GetName()
	switch {
	case field.GetIsPrimaryKey():
		return merr.WrapErrParameterInvalidMsg("cannot drop primary key field %q", name)
	case field.GetIsPartitionKey():
		return merr.WrapErrParameterInvalidMsg("cannot drop partition key field %q", name)
	case field.GetIsClusteringKey():
		return merr.WrapErrParameterInvalidMsg("cannot drop clustering key field %q", name)
	case field.GetFieldID() < common.StartOfUserFieldID:
		return merr.WrapErrParameterInvalidMsg("cannot drop system field %q", name)
	case isReservedEvolutionFieldName(name) && !(field.GetIsDynamic() && !newSchema.GetEnableDynamicField()):
		return merr.WrapErrParameterInvalidMsg("cannot drop system field %q", name)
	}
	if function := evolutionFunctionReferencing(newSchema, field.GetFieldID()); function != "" {
		return merr.WrapErrParameterInvalidMsg("cannot drop field %q while function %q still references it", name, function)
	}
	if typeutil.IsVectorType(field.GetDataType()) && !evolutionHasVectorField(newSchema) {
		return merr.WrapErrParameterInvalidMsg("cannot drop the last vector field %q", name)
	}
	return nil
}

func validateEvolutionFunctions(oldSchema, newSchema *schemapb.CollectionSchema, oldFields, newFields *evolutionFieldMaps) error {
	oldFunctions := make(map[int64]*schemapb.FunctionSchema)
	for _, function := range oldSchema.GetFunctions() {
		if function == nil {
			return merr.WrapErrParameterInvalidMsg("old schema contains a nil function")
		}
		if _, duplicate := oldFunctions[function.GetId()]; duplicate {
			return merr.WrapErrParameterInvalidMsg("old schema contains duplicate function id %d", function.GetId())
		}
		oldFunctions[function.GetId()] = function
	}

	newFunctions := make(map[int64]*schemapb.FunctionSchema)
	functionNames := make(map[string]struct{})
	outputOwners := make(map[int64]string)
	outputNameOwners := make(map[string]string)
	for _, function := range newSchema.GetFunctions() {
		if function == nil {
			return merr.WrapErrParameterInvalidMsg("new schema contains a nil function")
		}
		if _, duplicate := newFunctions[function.GetId()]; duplicate {
			return merr.WrapErrParameterInvalidMsg("new schema contains duplicate function id %d", function.GetId())
		}
		newFunctions[function.GetId()] = function
		if _, duplicate := functionNames[function.GetName()]; duplicate {
			return merr.WrapErrParameterInvalidMsg("duplicate function name: %s", function.GetName())
		}
		functionNames[function.GetName()] = struct{}{}

		if len(function.GetInputFieldIds()) != len(function.GetInputFieldNames()) || len(function.GetOutputFieldIds()) != len(function.GetOutputFieldNames()) {
			return merr.WrapErrParameterInvalidMsg("function %q has mismatched field id and name lists", function.GetName())
		}
		if len(function.GetOutputFieldIds()) == 0 {
			return merr.WrapErrParameterInvalidMsg("function %q has no output field", function.GetName())
		}
		inputFields := make([]*schemapb.FieldSchema, 0, len(function.GetInputFieldIds()))
		inputIDs := make(map[int64]struct{}, len(function.GetInputFieldIds()))
		for index, id := range function.GetInputFieldIds() {
			field, ok := newFields.fields[id]
			if !ok || field.GetName() != function.GetInputFieldNames()[index] {
				return merr.WrapErrParameterInvalidMsg("function %q references a missing input field", function.GetName())
			}
			inputFields = append(inputFields, field)
			inputIDs[id] = struct{}{}
		}
		outputFields := make([]*schemapb.FieldSchema, 0, len(function.GetOutputFieldIds()))
		for index, id := range function.GetOutputFieldIds() {
			field, ok := newFields.fields[id]
			if !ok || field.GetName() != function.GetOutputFieldNames()[index] {
				return merr.WrapErrParameterInvalidMsg("function %q references a missing output field", function.GetName())
			}
			if !field.GetIsFunctionOutput() {
				return merr.WrapErrParameterInvalidMsg("output field %q of function %q is not marked as a function output", field.GetName(), function.GetName())
			}
			if _, isInput := inputIDs[id]; isInput {
				return merr.WrapErrParameterInvalidMsg("a single field cannot be both input and output in the same function, function: %s, field: %s", function.GetName(), field.GetName())
			}
			if field.GetNullable() {
				return merr.WrapErrParameterInvalidMsg("function output field cannot be nullable: function %s, field %s", function.GetName(), field.GetName())
			}
			if owner, duplicate := outputOwners[id]; duplicate {
				return merr.WrapErrParameterInvalidMsg("output field %q is produced by both %q and %q", field.GetName(), owner, function.GetName())
			}
			if _, duplicate := outputNameOwners[field.GetName()]; duplicate {
				return merr.WrapErrParameterInvalidMsg("duplicate function output field: function %s, field %s", function.GetName(), field.GetName())
			}
			outputOwners[id] = function.GetName()
			outputNameOwners[field.GetName()] = function.GetName()
			outputFields = append(outputFields, field)
		}
		if err := validateEvolutionFunctionStaticInvariants(function, inputFields, outputFields); err != nil {
			return err
		}

		if oldFunction, existed := oldFunctions[function.GetId()]; existed {
			if !proto.Equal(oldFunction, function) {
				return merr.WrapErrParameterInvalidMsg("cannot alter existing function %q in place", oldFunction.GetName())
			}
			continue
		}
		for _, outputID := range function.GetOutputFieldIds() {
			if _, existed := oldFields.allIDs[outputID]; existed {
				return merr.WrapErrParameterInvalidMsg("new function %q reuses an existing output field id %d", function.GetName(), outputID)
			}
		}
	}

	for _, field := range newFields.fields {
		if field.GetIsFunctionOutput() {
			if _, produced := outputOwners[field.GetFieldID()]; !produced {
				return merr.WrapErrParameterInvalidMsg("function output field %q has no producer", field.GetName())
			}
		}
	}
	return nil
}

// validateEvolutionFunctionStaticInvariants mirrors the authoritative static
// checks in internal/util/function/validator without importing that package.
// Importing it would pull the function runtime and libmilvus_core into this
// low-level validation-only package.
func validateEvolutionFunctionStaticInvariants(function *schemapb.FunctionSchema, inputFields, outputFields []*schemapb.FieldSchema) error {
	if function.GetName() == "" {
		return merr.WrapErrParameterMissingMsg("function name cannot be empty")
	}
	if len(inputFields) == 0 {
		return merr.WrapErrParameterMissingMsg("function input field names cannot be empty, function: %s", function.GetName())
	}
	seenInputs := make(map[string]struct{}, len(function.GetInputFieldNames()))
	for _, name := range function.GetInputFieldNames() {
		if name == "" {
			return merr.WrapErrParameterMissingMsg("function input field name cannot be empty string, function: %s", function.GetName())
		}
		if _, duplicate := seenInputs[name]; duplicate {
			return merr.WrapErrParameterInvalidMsg("each function input field should be used exactly once in the same function, function: %s, input field: %s", function.GetName(), name)
		}
		seenInputs[name] = struct{}{}
	}
	seenOutputs := make(map[string]struct{}, len(function.GetOutputFieldNames()))
	for _, name := range function.GetOutputFieldNames() {
		if name == "" {
			return merr.WrapErrParameterMissingMsg("function output field name cannot be empty string, function: %s", function.GetName())
		}
		if _, isInput := seenInputs[name]; isInput {
			return merr.WrapErrParameterInvalidMsg("a single field cannot be both input and output in the same function, function: %s, field: %s", function.GetName(), name)
		}
		if _, duplicate := seenOutputs[name]; duplicate {
			return merr.WrapErrParameterInvalidMsg("each function output field should be used exactly once in the same function, function: %s, output field: %s", function.GetName(), name)
		}
		seenOutputs[name] = struct{}{}
	}

	for _, field := range outputFields {
		if field.GetIsPrimaryKey() {
			return merr.WrapErrParameterInvalidMsg("function output field cannot be primary key: function %s, field %s", function.GetName(), field.GetName())
		}
		if field.GetIsPartitionKey() || field.GetIsClusteringKey() {
			return merr.WrapErrParameterInvalidMsg("function output field cannot be partition key or clustering key: function %s, field %s", function.GetName(), field.GetName())
		}
	}

	switch function.GetType() {
	case schemapb.FunctionType_BM25:
		if len(function.GetParams()) != 0 {
			return merr.WrapErrParameterInvalidMsg("BM25 function accepts no params")
		}
		if len(inputFields) != 1 || !isEvolutionFunctionStringInput(inputFields[0].GetDataType()) {
			return merr.WrapErrParameterInvalidMsg("BM25 function input field must be a VARCHAR/TEXT field")
		}
		if !typeutil.CreateFieldSchemaHelper(inputFields[0]).EnableAnalyzer() {
			return merr.WrapErrParameterInvalidMsg("BM25 function input field must set enable_analyzer to true")
		}
		if len(outputFields) != 1 || !typeutil.IsSparseFloatVectorType(outputFields[0].GetDataType()) {
			return merr.WrapErrParameterInvalidMsg("BM25 function output field must be a SparseFloatVector field, but got %s", outputFields[0].GetDataType().String())
		}
	case schemapb.FunctionType_TextEmbedding:
		if len(function.GetParams()) == 0 {
			return merr.WrapErrParameterInvalidMsg("TextEmbedding function accepts no params")
		}
		if len(inputFields) != 1 || !isEvolutionFunctionStringInput(inputFields[0].GetDataType()) {
			return merr.WrapErrParameterInvalidMsg("TextEmbedding function input field must be a VARCHAR/TEXT field")
		}
		if inputFields[0].GetNullable() {
			return merr.WrapErrParameterInvalidMsg("function input field cannot be nullable: function %s, field %s", function.GetName(), inputFields[0].GetName())
		}
		if len(outputFields) != 1 || (outputFields[0].GetDataType() != schemapb.DataType_FloatVector && outputFields[0].GetDataType() != schemapb.DataType_Int8Vector) {
			return merr.WrapErrParameterInvalidMsg("TextEmbedding function output field must be a FloatVector or Int8Vector field")
		}
	case schemapb.FunctionType_MinHash:
		if len(inputFields) != 1 || !isEvolutionFunctionStringInput(inputFields[0].GetDataType()) {
			return merr.WrapErrParameterInvalidMsg("MinHash function input field must be a VARCHAR/TEXT field")
		}
		if len(outputFields) != 1 || outputFields[0].GetDataType() != schemapb.DataType_BinaryVector {
			return merr.WrapErrParameterInvalidMsg("MinHash function output field must be a BinaryVector field, but got %s", outputFields[0].GetDataType().String())
		}
	default:
		return merr.WrapErrParameterInvalidMsg("check function params with unknown function type")
	}
	return nil
}

func isEvolutionFunctionStringInput(dataType schemapb.DataType) bool {
	return dataType == schemapb.DataType_VarChar || dataType == schemapb.DataType_Text
}

func validateEvolutionDynamicGraph(schema *schemapb.CollectionSchema) error {
	var dynamicField *schemapb.FieldSchema
	for _, field := range schema.GetFields() {
		if !field.GetIsDynamic() {
			continue
		}
		if dynamicField != nil {
			return merr.WrapErrParameterInvalidMsg("collection schema contains more than one dynamic field")
		}
		dynamicField = field
	}
	if !schema.GetEnableDynamicField() {
		if dynamicField != nil {
			return merr.WrapErrParameterInvalidMsg("dynamic field %q exists while dynamic fields are disabled", dynamicField.GetName())
		}
		return nil
	}
	if dynamicField == nil {
		return merr.WrapErrParameterInvalidMsg("dynamic fields are enabled but the dynamic field is missing")
	}
	if dynamicField.GetName() != common.MetaFieldName || dynamicField.GetDataType() != schemapb.DataType_JSON {
		return merr.WrapErrParameterInvalidMsg("dynamic field must be the JSON field %q", common.MetaFieldName)
	}
	if dynamicField.GetIsFunctionOutput() {
		return merr.WrapErrParameterInvalidMsg("dynamic field %q cannot be a function output", dynamicField.GetName())
	}
	return nil
}

func validateMonotonicNumericBounds(fieldName string, oldParams, newParams []*commonpb.KeyValuePair) error {
	for _, key := range []string{common.MaxLengthKey, common.MaxCapacityKey} {
		oldValue, existed, err := evolutionNumericValue(oldParams, key)
		if err != nil {
			return merr.WrapErrParameterInvalidMsg("field %q has an invalid old %s bound", fieldName, key)
		}
		if !existed {
			continue
		}
		newValue, kept, err := evolutionNumericValue(newParams, key)
		if err != nil || !kept {
			return merr.WrapErrParameterInvalidMsg("cannot remove or invalidate %s of field %q", key, fieldName)
		}
		if newValue < oldValue {
			return merr.WrapErrParameterInvalidMsg("cannot shrink %s of field %q", key, fieldName)
		}
	}

	oldDim, existed, err := evolutionNumericValue(oldParams, common.DimKey)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("field %q has an invalid old dimension", fieldName)
	}
	if !existed {
		return nil
	}
	newDim, kept, err := evolutionNumericValue(newParams, common.DimKey)
	if err != nil || !kept || oldDim != newDim {
		return merr.WrapErrParameterInvalidMsg("cannot change or remove the dimension of field %q", fieldName)
	}
	return nil
}

func evolutionNumericValue(params []*commonpb.KeyValuePair, key string) (int64, bool, error) {
	for _, param := range params {
		if param.GetKey() != key {
			continue
		}
		value, err := strconv.ParseInt(param.GetValue(), 10, 64)
		return value, true, err
	}
	return 0, false, nil
}

func validateMaxFieldIDEvolution(oldSchema, newSchema *schemapb.CollectionSchema, oldFields, newFields *evolutionFieldMaps) error {
	oldProperty, err := evolutionSchemaIntProperty(oldSchema, common.MaxFieldIDKey)
	if err != nil {
		return err
	}
	newProperty, err := evolutionSchemaIntProperty(newSchema, common.MaxFieldIDKey)
	if err != nil {
		return err
	}
	if proto.Equal(oldSchema, newSchema) {
		return nil
	}

	oldFloor := maxEvolutionFieldID(oldFields)
	if oldProperty.valid && oldProperty.value > oldFloor {
		oldFloor = oldProperty.value
	}
	expectedMax := oldFloor
	if newLiveMax := maxEvolutionFieldID(newFields); newLiveMax > expectedMax {
		expectedMax = newLiveMax
	}
	if !newProperty.present || !newProperty.valid {
		return merr.WrapErrParameterInvalidMsg("schema evolution must contain a valid %s property", common.MaxFieldIDKey)
	}
	if newProperty.value != expectedMax {
		return merr.WrapErrParameterInvalidMsg("%s must be %d after this evolution, got %d", common.MaxFieldIDKey, expectedMax, newProperty.value)
	}

	for id := range newFields.allIDs {
		if _, existed := oldFields.allIDs[id]; !existed && id <= oldFloor {
			return merr.WrapErrParameterInvalidMsg("new field id %d reuses an already allocated field id", id)
		}
	}
	return nil
}

type evolutionIntProperty struct {
	value   int64
	present bool
	valid   bool
}

func evolutionSchemaIntProperty(schema *schemapb.CollectionSchema, key string) (evolutionIntProperty, error) {
	result := evolutionIntProperty{}
	for _, property := range schema.GetProperties() {
		if property.GetKey() != key {
			continue
		}
		if result.present {
			return evolutionIntProperty{}, merr.WrapErrParameterInvalidMsg("schema contains duplicate property %q", key)
		}
		result.present = true
		parsed, err := strconv.ParseInt(property.GetValue(), 10, 64)
		if err != nil {
			continue
		}
		result.value = parsed
		result.valid = true
	}
	return result, nil
}

func maxEvolutionFieldID(fields *evolutionFieldMaps) int64 {
	maxID := int64(common.StartOfUserFieldID)
	for id := range fields.allIDs {
		if id > maxID {
			maxID = id
		}
	}
	return maxID
}

func isReservedEvolutionFieldName(name string) bool {
	switch name {
	case common.RowIDFieldName, common.TimeStampFieldName, common.MetaFieldName, common.NamespaceFieldName, common.VirtualPKFieldName:
		return true
	default:
		return false
	}
}

func evolutionFunctionReferencing(schema *schemapb.CollectionSchema, fieldID int64) string {
	for _, function := range schema.GetFunctions() {
		for _, inputID := range function.GetInputFieldIds() {
			if inputID == fieldID {
				return function.GetName()
			}
		}
		for _, outputID := range function.GetOutputFieldIds() {
			if outputID == fieldID {
				return function.GetName()
			}
		}
	}
	return ""
}

func evolutionHasVectorField(schema *schemapb.CollectionSchema) bool {
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		if typeutil.IsVectorType(field.GetDataType()) {
			return true
		}
	}
	return false
}
