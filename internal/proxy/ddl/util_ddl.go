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

package ddl

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/fieldvalidator"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var (
	// enableMultipleVectorFields indicates whether to enable multiple vector fields.
	enableMultipleVectorFields = true

	// defaultVecIndexDataTypeCheck is the vec-index data-type compatibility
	// check used when a task is built without the composition-root injection
	// (white-box tests). Production always injects the cgo-backed check via the
	// constructors; the permissive default only affects tests.
	defaultVecIndexDataTypeCheck = func(string, schemapb.DataType, schemapb.DataType) bool {
		return true
	}
)

// restoreStructFieldNames restores original field names from structName[fieldName] format
// This is used when returning schema information to users (e.g., in describe collection)
func restoreStructFieldNames(schema *schemapb.CollectionSchema) error {
	for _, structArrayField := range schema.StructArrayFields {
		structName := structArrayField.Name
		expectedPrefix := structName + "["

		for _, field := range structArrayField.Fields {
			if strings.HasPrefix(field.Name, expectedPrefix) && strings.HasSuffix(field.Name, "]") {
				// Extract fieldName: remove "structName[" prefix and "]" suffix
				field.Name = field.Name[len(expectedPrefix) : len(field.Name)-1]
			}
		}
	}
	return nil
}

func transformStructFieldNames(schema *schemapb.CollectionSchema) error {
	for _, structArrayField := range schema.StructArrayFields {
		structName := structArrayField.Name
		for _, field := range structArrayField.Fields {
			// Create transformed name: structName[fieldName]
			newName := typeutil.ConcatStructFieldName(structName, field.Name)
			field.Name = newName
		}
	}

	return nil
}

// isAlpha check if c is alpha.
func isAlpha(c uint8) bool {
	if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') {
		return false
	}
	return true
}

// isNumber check if c is a number.
func isNumber(c uint8) bool {
	if c < '0' || c > '9' {
		return false
	}
	return true
}

func validateCollectionNameOrAlias(entity, entityType string) error {
	if entity == "" {
		return merr.WrapErrParameterInvalidMsg("collection %s should not be empty", entityType)
	}

	invalidMsg := fmt.Sprintf("Invalid collection %s: %s. ", entityType, entity)
	if len(entity) > paramtable.Get().ProxyCfg.MaxNameLength.GetAsInt() {
		return merr.WrapErrParameterInvalidMsg("%s the length of a collection %s must be less than %s characters", invalidMsg, entityType,
			paramtable.Get().ProxyCfg.MaxNameLength.GetValue())
	}

	firstChar := entity[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		return merr.WrapErrParameterInvalidMsg("%s the first character of a collection %s must be an underscore or letter", invalidMsg, entityType)
	}

	for i := 1; i < len(entity); i++ {
		c := entity[i]
		if c != '_' && !isAlpha(c) && !isNumber(c) {
			return merr.WrapErrParameterInvalidMsg("%s collection %s can only contain numbers, letters and underscores", invalidMsg, entityType)
		}
	}
	return nil
}

func validateCollectionName(collName string) error {
	return validateCollectionNameOrAlias(collName, "name")
}

func ValidateDatabaseName(dbName string) error {
	if dbName == "" {
		return merr.WrapErrDatabaseNameInvalid(dbName, "database name couldn't be empty")
	}

	if len(dbName) > paramtable.Get().ProxyCfg.MaxNameLength.GetAsInt() {
		return merr.WrapErrDatabaseNameInvalid(dbName,
			fmt.Sprintf("the length of a database name must be less than %d characters", paramtable.Get().ProxyCfg.MaxNameLength.GetAsInt()))
	}

	firstChar := dbName[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		return merr.WrapErrDatabaseNameInvalid(dbName,
			"the first character of a database name must be an underscore or letter")
	}

	for i := 1; i < len(dbName); i++ {
		c := dbName[i]
		if c != '_' && !isAlpha(c) && !isNumber(c) {
			return merr.WrapErrDatabaseNameInvalid(dbName,
				"database name can only contain numbers, letters and underscores")
		}
	}
	return nil
}

func validateCollectionDescription(description string) error {
	if len(description) > paramtable.Get().ProxyCfg.MaxCollectionDescriptionLength.GetAsInt() {
		return merr.WrapErrParameterInvalidMsg(
			"the length of a collection description must not exceed %s bytes",
			paramtable.Get().ProxyCfg.MaxCollectionDescriptionLength.GetValue())
	}
	return nil
}

// ValidateCollectionAlias returns true if collAlias is a valid alias name for collection, otherwise returns false.
func ValidateCollectionAlias(collAlias string) error {
	return validateCollectionNameOrAlias(collAlias, "alias")
}

func validatePartitionTag(partitionTag string, strictCheck bool) error {
	partitionTag = strings.TrimSpace(partitionTag)

	invalidMsg := "Invalid partition name: " + partitionTag + ". "
	if partitionTag == "" {
		msg := invalidMsg + "Partition name should not be empty."
		return merr.WrapErrParameterInvalidMsg("%s", msg)
	}
	if len(partitionTag) > paramtable.Get().ProxyCfg.MaxNameLength.GetAsInt() {
		msg := invalidMsg + "The length of a partition name must be less than " + paramtable.Get().ProxyCfg.MaxNameLength.GetValue() + " characters."
		return merr.WrapErrParameterInvalidMsg("%s", msg)
	}

	if strictCheck {
		firstChar := partitionTag[0]
		if firstChar != '_' && !isAlpha(firstChar) && !isNumber(firstChar) {
			msg := invalidMsg + "The first character of a partition name must be an underscore or letter."
			return merr.WrapErrParameterInvalidMsg("%s", msg)
		}

		tagSize := len(partitionTag)
		for i := 1; i < tagSize; i++ {
			c := partitionTag[i]
			if c != '_' && !isAlpha(c) && !isNumber(c) && c != '-' {
				msg := invalidMsg + "Partition name can only contain numbers, letters and underscores."
				return merr.WrapErrParameterInvalidMsg("%s", msg)
			}
		}
	}

	return nil
}

func validateFieldName(fieldName string) error {
	fieldName = strings.TrimSpace(fieldName)

	if fieldName == "" {
		return merr.WrapErrFieldNameInvalid(fieldName, "field name should not be empty")
	}

	invalidMsg := "Invalid field name: " + fieldName + ". "
	if len(fieldName) > paramtable.Get().ProxyCfg.MaxNameLength.GetAsInt() {
		msg := invalidMsg + "The length of a field name must be less than " + paramtable.Get().ProxyCfg.MaxNameLength.GetValue() + " characters."
		return merr.WrapErrFieldNameInvalid(fieldName, msg)
	}

	firstChar := fieldName[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		msg := invalidMsg + "The first character of a field name must be an underscore or letter."
		return merr.WrapErrFieldNameInvalid(fieldName, msg)
	}

	fieldNameSize := len(fieldName)
	for i := 1; i < fieldNameSize; i++ {
		c := fieldName[i]
		if c != '_' && !isAlpha(c) && !isNumber(c) {
			msg := invalidMsg + "Field name can only contain numbers, letters, and underscores."
			return merr.WrapErrFieldNameInvalid(fieldName, msg)
		}
	}
	if common.IsFieldNameKeyword(fieldName) {
		msg := invalidMsg + fmt.Sprintf("%s is keyword in milvus.", fieldName)
		return merr.WrapErrFieldNameInvalid(fieldName, msg)
	}
	return nil
}

func validateDimension(field *schemapb.FieldSchema) error {
	exist := false
	var dim int64
	for _, param := range field.TypeParams {
		if param.Key == common.DimKey {
			exist = true
			tmp, err := strconv.ParseInt(param.Value, 10, 64)
			if err != nil {
				return err
			}
			dim = tmp
			break
		}
	}
	// for sparse vector field, dim should not be specified
	if typeutil.IsSparseFloatVectorType(field.DataType) {
		if exist {
			return merr.WrapErrParameterInvalidMsg("dim should not be specified for sparse vector field %s(%d)", field.GetName(), field.FieldID)
		}
		return nil
	}
	if !exist {
		return merr.WrapErrParameterInvalidMsg("dimension is not defined in field type params of field %s, check type param `dim` for vector field", field.GetName())
	}

	if dim <= 1 {
		return merr.WrapErrParameterInvalidMsg("invalid dimension: %d. should be in range 2 ~ %d", dim, paramtable.Get().ProxyCfg.MaxDimension.GetAsInt())
	}

	// for dense vector field, dim will be limited by max_dimension
	isBinaryDimension := typeutil.IsBinaryVectorType(field.DataType) ||
		(field.GetDataType() == schemapb.DataType_ArrayOfVector && typeutil.IsBinaryVectorType(field.GetElementType()))
	if isBinaryDimension {
		if dim%8 != 0 {
			return merr.WrapErrParameterInvalidMsg("invalid dimension: %d of field %s. binary vector dimension should be multiple of 8. ", dim, field.GetName())
		}
		if dim > paramtable.Get().ProxyCfg.MaxDimension.GetAsInt64()*8 {
			return merr.WrapErrParameterInvalidMsg("invalid dimension: %d of field %s. binary vector dimension should be in range 2 ~ %d", dim, field.GetName(), paramtable.Get().ProxyCfg.MaxDimension.GetAsInt()*8)
		}
	} else {
		if dim > paramtable.Get().ProxyCfg.MaxDimension.GetAsInt64() {
			return merr.WrapErrParameterInvalidMsg("invalid dimension: %d of field %s. float vector dimension should be in range 2 ~ %d", dim, field.GetName(), paramtable.Get().ProxyCfg.MaxDimension.GetAsInt())
		}
	}
	return nil
}

func validateMaxLengthPerRow(collectionName string, fieldName string, dataType schemapb.DataType, typeParams []*commonpb.KeyValuePair) error {
	exist := false
	for _, param := range typeParams {
		if param.Key != common.MaxLengthKey {
			continue
		}

		maxLengthPerRow, err := strconv.ParseInt(param.Value, 10, 64)
		if err != nil {
			return err
		}

		var defaultMaxLength int64
		if dataType == schemapb.DataType_Text {
			defaultMaxLength = paramtable.Get().ProxyCfg.MaxTextLength.GetAsInt64()
		} else {
			defaultMaxLength = paramtable.Get().ProxyCfg.MaxVarCharLength.GetAsInt64()
		}

		if maxLengthPerRow > defaultMaxLength || maxLengthPerRow <= 0 {
			return merr.WrapErrParameterInvalidMsg("the maximum length specified for the field(%s) should be in (0, %d], but got %d instead", fieldName, defaultMaxLength, maxLengthPerRow)
		}
		exist = true
	}
	// if not exist type params max_length, return error
	if !exist {
		return merr.WrapErrParameterMissingMsg("type param(max_length) should be specified for the field(%s) of collection %s", fieldName, collectionName)
	}

	return nil
}

func getMaxCapacityPerRow(collectionName string, fieldName string, typeParams []*commonpb.KeyValuePair) (int64, error) {
	maxArrayCapacity := paramtable.Get().ProxyCfg.MaxArrayCapacity.GetAsInt64()
	exist := false
	var maxCapacityPerRow int64
	for _, param := range typeParams {
		if param.Key != common.MaxCapacityKey {
			continue
		}

		var err error
		maxCapacityPerRow, err = strconv.ParseInt(param.Value, 10, 64)
		if err != nil {
			return 0, merr.WrapErrParameterInvalidMsg("the value for %s of field %s must be an integer", common.MaxCapacityKey, fieldName)
		}
		if maxCapacityPerRow > maxArrayCapacity || maxCapacityPerRow <= 0 {
			return 0, merr.WrapErrParameterInvalidMsg("the maximum capacity specified for a Array should be in (0, %d]", maxArrayCapacity)
		}
		exist = true
	}
	// if not exist type params max_capacity, return error
	if !exist {
		return 0, merr.WrapErrParameterMissingMsg("type param(max_capacity) should be specified for array field %s of collection %s", fieldName, collectionName)
	}
	return maxCapacityPerRow, nil
}

func validateMaxCapacityPerRow(collectionName string, field *schemapb.FieldSchema) error {
	_, err := getMaxCapacityPerRow(collectionName, field.GetName(), field.GetTypeParams())
	if err != nil {
		return err
	}
	return nil
}

func validateNestedArrayTypeParams(collectionName string, fieldName string, typeSchema *schemapb.TypeSchema) (int64, error) {
	if typeSchema == nil {
		return 0, merr.WrapErrParameterMissingMsg("type_schema should be specified for nested array field %s", fieldName)
	}
	if typeSchema.GetNullable() {
		return 0, merr.WrapErrParameterInvalidMsg("nullable nested array elements are not supported for field %s", fieldName)
	}

	switch kind := typeSchema.GetKind().(type) {
	case *schemapb.TypeSchema_ArrayElement:
		if kind.ArrayElement == nil {
			return 0, merr.WrapErrParameterMissingMsg("array element should be specified for nested array field %s", fieldName)
		}
		maxCapacity, err := getMaxCapacityPerRow(collectionName, fieldName, typeSchema.GetTypeParams())
		if err != nil {
			return 0, err
		}
		if _, err := validateNestedArrayTypeParams(collectionName, fieldName, kind.ArrayElement); err != nil {
			return 0, err
		}
		return maxCapacity, nil
	case *schemapb.TypeSchema_LeafType:
		if kind.LeafType == schemapb.DataType_VarChar {
			if err := validateMaxLengthPerRow(collectionName, fieldName, kind.LeafType, typeSchema.GetTypeParams()); err != nil {
				return 0, err
			}
		}
		return 0, nil
	default:
		return 0, merr.WrapErrParameterMissingMsg("type should be specified for nested array field %s", fieldName)
	}
}

func validateArrayOfVectorFieldSchema(collectionName string, field *schemapb.FieldSchema) error {
	if field.GetElementType() == schemapb.DataType_ArrayOfVector {
		return merr.WrapErrParameterInvalidMsg("nested ArrayOfVector is not supported for field %s", field.GetName())
	}
	// ArrayOfVector: support FloatVector, Float16Vector, BFloat16Vector, Int8Vector, BinaryVector
	if !typeutil.IsFixDimVectorType(field.GetElementType()) {
		return merr.WrapErrParameterInvalidMsg("Unsupported element type %s of ArrayOfVector field %s, only fixed dimension vector types are supported", field.GetElementType().String(), field.Name)
	}
	if err := validateDimension(field); err != nil {
		return err
	}
	if err := validateMaxCapacityPerRow(collectionName, field); err != nil {
		return err
	}
	return nil
}

func validateArrayFieldSchema(collectionName string, field *schemapb.FieldSchema) error {
	if field.GetTypeSchema() != nil {
		rootCapacity, err := validateNestedArrayTypeParams(
			collectionName, field.GetName(), field.GetTypeSchema())
		if err != nil {
			return err
		}

		for _, param := range field.GetTypeParams() {
			if param.GetKey() != common.MaxCapacityKey {
				continue
			}
			mirrorCapacity, err := getMaxCapacityPerRow(
				collectionName, field.GetName(), field.GetTypeParams())
			if err != nil {
				return err
			}
			if mirrorCapacity != rootCapacity {
				return merr.WrapErrParameterInvalidMsg(
					"type param %s of nested array field %s must match type_schema root capacity %d",
					common.MaxCapacityKey, field.GetName(), rootCapacity)
			}
			break
		}
		return nil
	}

	if field.GetElementType() == schemapb.DataType_Array {
		return merr.WrapErrParameterMissingMsg("type_schema should be specified for nested array field %s", field.GetName())
	}
	if err := typeutil.ValidateArrayElementType(field.GetElementType()); err != nil {
		return err
	}
	if field.GetElementType() == schemapb.DataType_VarChar {
		if err := validateMaxLengthPerRow(collectionName, field.GetName(), field.GetDataType(), field.GetTypeParams()); err != nil {
			return err
		}
	}
	if err := validateMaxCapacityPerRow(collectionName, field); err != nil {
		return err
	}
	return nil
}

func validateElementNullable(field *schemapb.FieldSchema) error {
	if !field.GetElementNullable() {
		return nil
	}
	if field.GetDataType() != schemapb.DataType_Array && field.GetDataType() != schemapb.DataType_ArrayOfVector {
		return merr.WrapErrParameterInvalidMsg("element_nullable is only valid for Array and ArrayOfVector fields, field name = %s", field.GetName())
	}
	if typeutil.IsNestedArrayTypeSchema(field.GetTypeSchema()) {
		return merr.WrapErrParameterInvalidMsg("element_nullable is not supported for nested Array field %s", field.GetName())
	}
	// TODO: temporarily disable element nullable until all parts ready
	return merr.WrapErrParameterInvalidMsg("element_nullable is not supported yet, field name = %s", field.GetName())
}

func validateFieldType(schema *schemapb.CollectionSchema) error {
	for _, field := range schema.GetFields() {
		if err := typeutil.ValidateFieldTypeSchema(field); err != nil {
			return err
		}
		switch field.GetDataType() {
		case schemapb.DataType_String:
			return merr.WrapErrParameterInvalidMsg("string data type not supported yet, please use VarChar type instead")
		case schemapb.DataType_None:
			return merr.WrapErrParameterInvalidMsg("data type None is not valid")
		case schemapb.DataType_Array:
			if typeutil.IsNestedArrayTypeSchema(field.GetTypeSchema()) {
				return merr.WrapErrParameterInvalidMsg(
					"nested array can only be in a struct array field, field name: %s",
					field.GetName())
			}
			if field.GetTypeSchema() == nil {
				if err := typeutil.ValidateArrayElementType(field.GetElementType()); err != nil {
					return err
				}
			}
		case schemapb.DataType_ArrayOfVector:
			return merr.WrapErrParameterInvalidMsg("array of vector can only be in the struct array field, field name: %s", field.Name)
		}
	}
	for _, structArrayField := range schema.StructArrayFields {
		for _, field := range structArrayField.Fields {
			if err := typeutil.ValidateFieldTypeSchema(field); err != nil {
				return err
			}
			if field.GetDataType() != schemapb.DataType_Array && field.GetDataType() != schemapb.DataType_ArrayOfVector {
				return merr.WrapErrParameterInvalidMsg("fields in StructArrayField must be Array or ArrayOfVector, field name = %s, field type = %s",
					field.GetName(), field.GetDataType().String())
			}
		}
	}
	return nil
}

func validateDuplicatedFieldName(schema *schemapb.CollectionSchema) error {
	names := make(map[string]bool)
	validateFieldNames := func(name string) error {
		_, ok := names[name]
		if ok {
			return merr.WrapErrParameterInvalidMsg("duplicated field name %s found", name)
		}
		names[name] = true
		return nil
	}
	for _, field := range schema.Fields {
		if err := validateFieldNames(field.Name); err != nil {
			return err
		}
	}
	for _, structArrayField := range schema.StructArrayFields {
		if err := validateFieldNames(structArrayField.Name); err != nil {
			return err
		}

		for _, field := range structArrayField.Fields {
			if err := validateFieldNames(field.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

// ValidateFieldAutoID call after validatePrimaryKey
func ValidateFieldAutoID(coll *schemapb.CollectionSchema) error {
	idx := -1
	for i, field := range coll.Fields {
		if field.AutoID {
			if idx != -1 {
				return merr.WrapErrParameterInvalidMsg("only one field can speficy AutoID with true, field name = %s, %s", coll.Fields[idx].Name, field.Name)
			}
			idx = i
			if !field.IsPrimaryKey {
				return merr.WrapErrParameterInvalidMsg("only primary field can speficy AutoID with true, field name = %s", field.Name)
			}
		}
	}
	for _, structArrayField := range coll.StructArrayFields {
		for _, field := range structArrayField.Fields {
			if field.AutoID {
				return merr.WrapErrParameterInvalidMsg("autoID is not supported for struct field, field name = %s", field.Name)
			}
		}
	}
	return nil
}

func ValidateField(field *schemapb.FieldSchema, schema *schemapb.CollectionSchema) error {
	// validate field name
	var err error
	if err := validateFieldName(field.Name); err != nil {
		return err
	}
	if err := validateElementNullable(field); err != nil {
		return err
	}
	if err := typeutil.ValidateFieldTypeSchema(field); err != nil {
		return err
	}
	if typeutil.IsNestedArrayTypeSchema(field.GetTypeSchema()) {
		return merr.WrapErrParameterInvalidMsg(
			"nested array can only be in a struct array field, field name: %s",
			field.GetName())
	}
	// validate dense vector field type parameters
	isVectorType := typeutil.IsVectorType(field.DataType)
	if isVectorType {
		err = validateDimension(field)
		if err != nil {
			return err
		}
	}
	// valid max length per row parameters
	// if max_length not specified, return error
	if field.DataType == schemapb.DataType_VarChar {
		err = validateMaxLengthPerRow(schema.Name, field.GetName(), field.GetDataType(), field.GetTypeParams())
		if err != nil {
			return err
		}
	}
	// valid max capacity for array per row parameters
	// if max_capacity not specified, return error
	if field.DataType == schemapb.DataType_Array {
		if err = validateArrayFieldSchema(schema.Name, field); err != nil {
			return err
		}
	}

	if field.DataType == schemapb.DataType_ArrayOfVector {
		return merr.WrapErrParameterInvalidMsg("array of vector can only be in the struct array field, field name: %s", field.Name)
	}

	// TODO should remove the index params in the field schema
	indexParams := funcutil.KeyValuePair2Map(field.GetIndexParams())
	if err = fieldvalidator.ValidateAutoIndexMmapConfig(isVectorType, indexParams); err != nil {
		return err
	}

	// Validate warmup policy if specified in field TypeParams
	if warmupPolicy, exist := common.GetWarmupPolicy(field.GetTypeParams()...); exist {
		if err = common.ValidateWarmupPolicy(warmupPolicy); err != nil {
			return merr.WrapErrParameterInvalidMsg("invalid warmup policy for field %s: %s", field.Name, err.Error())
		}
	}

	return nil
}

func ValidateFieldsInStruct(field *schemapb.FieldSchema, schema *schemapb.CollectionSchema) error {
	// validate field name
	var err error
	if err := validateFieldName(field.Name); err != nil {
		return err
	}
	if err := validateElementNullable(field); err != nil {
		return err
	}
	if err := typeutil.ValidateFieldTypeSchema(field); err != nil {
		return err
	}
	if typeutil.IsNestedArrayTypeSchema(field.GetTypeSchema()) {
		leafSchema := field.GetTypeSchema().GetArrayElement().GetArrayElement()
		if _, ok := leafSchema.GetKind().(*schemapb.TypeSchema_LeafType); !ok {
			return merr.WrapErrParameterInvalidMsg(
				"nested array field %s supports exactly one nested array level",
				field.GetName())
		}
	}

	if field.DataType != schemapb.DataType_Array && field.DataType != schemapb.DataType_ArrayOfVector {
		return merr.WrapErrParameterInvalidMsg("fields in StructArrayField can only be array or array of struct, but field %s is %s", field.Name, field.DataType.String())
	}
	switch field.GetElementType() {
	case schemapb.DataType_ArrayOfVector:
		return merr.WrapErrParameterInvalidMsg("nested ArrayOfVector is not supported for field %s", field.GetName())
	case schemapb.DataType_ArrayOfStruct:
		return merr.WrapErrParameterInvalidMsg("nested ArrayOfStruct is not supported for field %s", field.GetName())
	}

	if field.DataType == schemapb.DataType_Array {
		err = validateArrayFieldSchema(schema.Name, field)
	} else {
		err = validateArrayOfVectorFieldSchema(schema.Name, field)
	}
	if err != nil {
		return err
	}

	// Validate warmup policy if specified in field TypeParams
	if warmupPolicy, exist := common.GetWarmupPolicy(field.GetTypeParams()...); exist {
		if err = common.ValidateWarmupPolicy(warmupPolicy); err != nil {
			return merr.WrapErrParameterInvalidMsg("invalid warmup policy for field %s: %s", field.Name, err.Error())
		}
	}

	return nil
}

func validateStructArrayFieldMaxCapacity(structArrayField *schemapb.StructArrayFieldSchema, collectionName string) error {
	var expectedMaxCapacity int64
	hasExpectedMaxCapacity := false
	for _, subField := range structArrayField.Fields {
		typeParams := subField.GetTypeParams()
		if typeutil.IsNestedArrayTypeSchema(subField.GetTypeSchema()) {
			typeParams = subField.GetTypeSchema().GetTypeParams()
		}
		maxCapacity, err := getMaxCapacityPerRow(collectionName, subField.GetName(), typeParams)
		if err != nil {
			return err
		}
		if !hasExpectedMaxCapacity {
			expectedMaxCapacity = maxCapacity
			hasExpectedMaxCapacity = true
			continue
		}
		if maxCapacity != expectedMaxCapacity {
			return merr.WrapErrParameterInvalidMsg("all sub-fields in struct array field must have the same max_capacity: structName=%s, subFieldName=%s, max_capacity=%d, expected=%d",
				structArrayField.Name, subField.Name, maxCapacity, expectedMaxCapacity)
		}
	}
	return nil
}

// ValidateStructArrayField validates the struct array field schema.
// When the struct is nullable, sub-field schemas are mutated in-place to set Nullable=true.
func ValidateStructArrayField(structArrayField *schemapb.StructArrayFieldSchema, schema *schemapb.CollectionSchema) error {
	if len(structArrayField.Fields) == 0 {
		return merr.WrapErrParameterInvalidMsg("struct array field %s has no sub-fields", structArrayField.Name)
	}

	// Validate warmup policy if specified in struct field TypeParams
	if warmupPolicy, exist := common.GetWarmupPolicy(structArrayField.GetTypeParams()...); exist {
		if err := common.ValidateWarmupPolicy(warmupPolicy); err != nil {
			return merr.WrapErrParameterInvalidMsg("invalid warmup policy for struct field %s: %s", structArrayField.Name, err.Error())
		}
	}

	for _, subField := range structArrayField.Fields {
		if err := ValidateFieldsInStruct(subField, schema); err != nil {
			return err
		}
	}
	if err := validateStructArrayFieldMaxCapacity(structArrayField, schema.Name); err != nil {
		return err
	}

	// If struct is nullable, propagate nullable to all sub-fields
	if structArrayField.GetNullable() {
		for _, subField := range structArrayField.Fields {
			subField.Nullable = true
		}
	} else {
		// If struct is not nullable, sub-fields must not be individually nullable
		for _, subField := range structArrayField.Fields {
			if subField.GetNullable() {
				return merr.WrapErrParameterInvalidMsg("sub-field in non-nullable struct cannot be nullable individually, set nullable on the struct instead: structName=%s, subFieldName=%s",
					structArrayField.Name, subField.Name)
			}
		}
	}

	return nil
}

func validatePrimaryKey(coll *schemapb.CollectionSchema) error {
	idx := -1
	for i, field := range coll.Fields {
		if field.IsPrimaryKey {
			if idx != -1 {
				return merr.WrapErrParameterInvalidMsg("there are more than one primary key, field name = %s, %s", coll.Fields[idx].Name, field.Name)
			}

			// The type of the primary key field can only be int64 and varchar
			if field.DataType != schemapb.DataType_Int64 && field.DataType != schemapb.DataType_VarChar {
				return merr.WrapErrParameterInvalidMsg("the data type of primary key should be Int64 or VarChar")
			}

			idx = i
		}
	}
	if idx == -1 {
		// External collections may not have a primary key
		if !typeutil.IsExternalCollection(coll) {
			return merr.WrapErrParameterMissingMsg("primary key is not specified")
		}
	}

	for _, structArrayField := range coll.StructArrayFields {
		for _, field := range structArrayField.Fields {
			if field.IsPrimaryKey {
				return merr.WrapErrParameterInvalidMsg("primary key is not supported for struct field, field name = %s", field.Name)
			}
		}
	}

	return nil
}

// validateReservedFieldNames rejects user-supplied schema fields whose name
// collides with a system-reserved identifier (RowID, Timestamp,
// __virtual_pk__). Must be called BEFORE server-side injection of the
// virtual PK so the check only applies to user input. Applies to regular
// and struct-array fields alike. Fix for issue #49314.
func validateReservedFieldNames(schema *schemapb.CollectionSchema) error {
	reserved := map[string]struct{}{
		common.RowIDFieldName:     {},
		common.TimeStampFieldName: {},
		common.VirtualPKFieldName: {},
	}
	check := func(name string) error {
		if _, ok := reserved[name]; ok {
			return merr.WrapErrFieldNameInvalid(name,
				fmt.Sprintf("field name %q is reserved for internal use and cannot be used in user schemas", name))
		}
		return nil
	}
	for _, f := range schema.GetFields() {
		if err := check(f.GetName()); err != nil {
			return err
		}
	}
	for _, saf := range schema.GetStructArrayFields() {
		if err := check(saf.GetName()); err != nil {
			return err
		}
		for _, f := range saf.GetFields() {
			if err := check(f.GetName()); err != nil {
				return err
			}
		}
	}
	return nil
}

// injectVirtualPKForExternalCollection adds a virtual PK field for external collections
// if no primary key field exists. External collections use virtual PKs in the format:
// (segmentID << 32) | offset
func injectVirtualPKForExternalCollection(schema *schemapb.CollectionSchema) error {
	// Check if a primary key already exists
	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			// PK already exists, nothing to inject
			return nil
		}
	}

	// Create virtual PK field with FieldID=0; RootCoord's assignFieldAndFunctionID
	// will assign the actual field ID during collection creation.
	virtualPKField := &schemapb.FieldSchema{
		Name:         common.VirtualPKFieldName,
		Description:  "auto-generated primary key for external collection",
		DataType:     schemapb.DataType_Int64,
		IsPrimaryKey: true,
		AutoID:       true, // Virtual PKs are auto-generated
	}

	// Prepend virtual PK field to the schema fields
	schema.Fields = append([]*schemapb.FieldSchema{virtualPKField}, schema.Fields...)

	return nil
}

func validateDynamicField(coll *schemapb.CollectionSchema) error {
	for _, field := range coll.Fields {
		if field.IsDynamic {
			return merr.WrapErrParameterInvalidMsg("cannot explicitly set a field as a dynamic field")
		}
	}
	return nil
}

// validateMultipleVectorFields check if schema has multiple vector fields.
func validateMultipleVectorFields(schema *schemapb.CollectionSchema) error {
	vecExist := false
	var vecName string

	for i := range schema.Fields {
		name := schema.Fields[i].Name
		dType := schema.Fields[i].DataType
		isVec := typeutil.IsVectorType(dType)
		if isVec && vecExist && !enableMultipleVectorFields {
			return merr.WrapErrParameterInvalidMsg(
				"multiple vector fields is not supported, fields name: %s, %s",
				vecName,
				name,
			)
		} else if isVec {
			vecExist = true
			vecName = name
		}
	}

	return nil
}

func validateLoadFieldsList(schema *schemapb.CollectionSchema) error {
	var vectorCnt int
	for _, field := range schema.Fields {
		shouldLoad, err := common.ShouldFieldBeLoaded(field.GetTypeParams())
		if err != nil {
			return err
		}
		// shoud load field, skip other check
		if shouldLoad {
			if typeutil.IsVectorType(field.GetDataType()) {
				vectorCnt++
			}
			continue
		}

		if field.IsPrimaryKey {
			return merr.WrapErrParameterInvalidMsg("Primary key field %s cannot skip loading", field.GetName())
		}

		if field.IsPartitionKey {
			return merr.WrapErrParameterInvalidMsg("Partition Key field %s cannot skip loading", field.GetName())
		}

		if field.IsClusteringKey {
			return merr.WrapErrParameterInvalidMsg("Clustering Key field %s cannot skip loading", field.GetName())
		}
	}

	for _, structArrayField := range schema.StructArrayFields {
		for _, field := range structArrayField.Fields {
			shouldLoad, err := common.ShouldFieldBeLoaded(field.GetTypeParams())
			if err != nil {
				return err
			}
			if shouldLoad {
				if typeutil.IsVectorType(field.ElementType) {
					vectorCnt++
				}
				continue
			}
		}
	}

	if vectorCnt == 0 {
		return merr.WrapErrParameterInvalidMsg("cannot config all vector field(s) skip loading")
	}

	return nil
}

func validateName(entity string, nameType string) error {
	return validateNameWithCustomChars(entity, nameType, paramtable.Get().ProxyCfg.NameValidationAllowedChars.GetValue())
}

func validateNameWithCustomChars(entity string, nameType string, allowedChars string) error {
	entity = strings.TrimSpace(entity)

	if entity == "" {
		return merr.WrapErrParameterInvalid("not empty", entity, nameType+" should be not empty")
	}

	if len(entity) > paramtable.Get().ProxyCfg.MaxNameLength.GetAsInt() {
		return merr.WrapErrParameterInvalidRange(0,
			paramtable.Get().ProxyCfg.MaxNameLength.GetAsInt(),
			len(entity),
			fmt.Sprintf("the length of %s must be not greater than limit", nameType))
	}

	firstChar := entity[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		return merr.WrapErrParameterInvalid('_',
			firstChar,
			fmt.Sprintf("the first character of %s must be an underscore or letter", nameType))
	}

	for i := 1; i < len(entity); i++ {
		c := entity[i]
		if c != '_' && !isAlpha(c) && !isNumber(c) && !strings.ContainsRune(allowedChars, rune(c)) {
			return merr.WrapErrParameterInvalidMsg("%s can only contain numbers, letters, underscores, and allowed characters (%s), found %c at %d", nameType, allowedChars, c, i)
		}
	}
	return nil
}

// ValidateSnapshotName validates snapshot name using standard naming rules.
func ValidateSnapshotName(snapshotName string) error {
	return validateName(snapshotName, "snapshot name")
}

func ValidateCollectionName(entity string) error {
	if util.IsAnyWord(entity) {
		return nil
	}
	return validateName(entity, "collection name")
}

func ReplaceID2Name(oldStr string, id int64, name string) string {
	return strings.ReplaceAll(oldStr, strconv.FormatInt(id, 10), name)
}

func GetCurUserFromContext(ctx context.Context) (string, error) {
	return contextutil.GetCurUserFromContext(ctx)
}

func GetCurDBNameFromContextOrDefault(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return util.DefaultDBName
	}
	dbNameData := md[strings.ToLower(util.HeaderDBName)]
	if len(dbNameData) < 1 || dbNameData[0] == "" {
		return util.DefaultDBName
	}
	return dbNameData[0]
}

func AppendUserInfoForRPC(ctx context.Context) context.Context {
	curUser, _ := GetCurUserFromContext(ctx)
	if curUser != "" {
		originValue := fmt.Sprintf("%s%s%s", curUser, util.CredentialSeparator, curUser)
		authKey := strings.ToLower(util.HeaderAuthorize)
		authValue := crypto.Base64Encode(originValue)
		ctx = metadata.AppendToOutgoingContext(ctx, authKey, authValue)
	}
	return ctx
}

func validateIndexName(indexName string) error {
	// Shared with rootcoord's bound-index prepare (indexparamcheck).
	return indexparamcheck.ValidateIndexName(indexName)
}

func isCollectionLoaded(ctx context.Context, mc types.MixCoordClient, collID int64) (bool, error) {
	// get all loading collections
	resp, err := mc.ShowLoadCollections(ctx, &querypb.ShowCollectionsRequest{
		CollectionIDs: nil,
	})
	if err != nil {
		return false, err
	}
	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return false, merr.Error(resp.GetStatus())
	}

	for _, loadedCollID := range resp.GetCollectionIDs() {
		if collID == loadedCollID {
			return true, nil
		}
	}
	return false, nil
}

func isPartitionLoaded(ctx context.Context, mc types.MixCoordClient, collID int64, partID int64) (bool, error) {
	// get all loading collections
	resp, err := mc.ShowLoadPartitions(ctx, &querypb.ShowPartitionsRequest{
		CollectionID: collID,
		PartitionIDs: []int64{partID},
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		// qc returns error if partition not loaded
		if errors.Is(err, merr.ErrPartitionNotLoaded) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func isPartitionKeyMode(ctx context.Context, metaCache Cache, dbName string, colName string) (bool, error) {
	colSchema, err := metaCache.GetCollectionSchema(ctx, dbName, colName)
	if err != nil {
		return false, err
	}

	for _, fieldSchema := range colSchema.GetFields() {
		if fieldSchema.IsPartitionKey {
			return true, nil
		}
	}

	return false, nil
}

func hasPartitionKeyModeField(schema *schemapb.CollectionSchema) bool {
	for _, fieldSchema := range schema.GetFields() {
		if fieldSchema.IsPartitionKey {
			return true
		}
	}
	return false
}

// getDefaultPartitionsInPartitionKeyMode only used in partition key mode
func getDefaultPartitionsInPartitionKeyMode(ctx context.Context, metaCache Cache, dbName string, collectionName string) ([]string, error) {
	partitions, err := metaCache.GetPartitions(ctx, dbName, collectionName)
	if err != nil {
		return nil, err
	}

	// Make sure the order of the partition names got every time is the same
	partitionNames, _, err := typeutil.RearrangePartitionsForPartitionKey(partitions)
	if err != nil {
		return nil, err
	}

	return partitionNames, nil
}

// resolveCollectionAlias resolves an alias to its actual collection name
func resolveCollectionAlias(ctx context.Context, metaCache Cache, dbName, nameOrAlias string) (string, error) {
	if metaCache == nil {
		return nameOrAlias, merr.WrapErrServiceInternal("meta cache not initialized")
	}
	return metaCache.ResolveCollectionAlias(ctx, dbName, nameOrAlias)
}
