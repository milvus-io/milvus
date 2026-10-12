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

package dml

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/dql"
	"github.com/milvus-io/milvus/internal/proxy/fieldvalidator"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ErrWithLog wraps err with msg and logs the failure.
func ErrWithLog(logger *mlog.Logger, msg string, err error) error {
	wrapErr := errors.Wrap(err, msg)
	if logger != nil {
		logger.Warn(context.TODO(), msg, mlog.Err(err))
		return wrapErr
	}
	mlog.Warn(context.TODO(), msg, mlog.Err(err))
	return wrapErr
}

// resolveNamespacePartitionName resolves the partition name for a namespace
// when partition-mode namespacing is active.
func resolveNamespacePartitionName(schema *schemapb.CollectionSchema, namespace *string, partitionName string) (string, bool, error) {
	if err := common.CheckNamespace(schema, namespace); err != nil {
		return "", false, err
	}
	if !dql.NamespacePartitionModeEnabled(schema) {
		return partitionName, false, nil
	}

	namespacePartitionName := *namespace
	if err := dql.ValidatePartitionTag(namespacePartitionName, true); err != nil {
		return "", true, err
	}
	if partitionName != "" && partitionName != namespacePartitionName {
		return "", true, merr.WrapErrParameterInvalidMsg("partition name %q mismatches namespace %q", partitionName, namespacePartitionName)
	}
	return namespacePartitionName, true, nil
}

// parsePrimaryFieldData2IDs get IDs to fill grpc result, for example insert request, delete request etc.
func parsePrimaryFieldData2IDs(fieldData *schemapb.FieldData) (*schemapb.IDs, error) {
	primaryData := &schemapb.IDs{}
	switch fieldData.Field.(type) {
	case *schemapb.FieldData_Scalars:
		scalarField := fieldData.GetScalars()
		switch scalarField.Data.(type) {
		case *schemapb.ScalarField_LongData:
			primaryData.IdField = &schemapb.IDs_IntId{
				IntId: scalarField.GetLongData(),
			}
		case *schemapb.ScalarField_StringData:
			primaryData.IdField = &schemapb.IDs_StrId{
				StrId: scalarField.GetStringData(),
			}
		default:
			return nil, merr.WrapErrParameterInvalidMsg("currently only support DataType Int64 or VarChar as PrimaryField")
		}
	default:
		return nil, merr.WrapErrParameterInvalidMsg("currently not support vector field as PrimaryField")
	}

	return primaryData, nil
}

// autoGenPrimaryFieldData generate primary data when autoID == true
func autoGenPrimaryFieldData(fieldSchema *schemapb.FieldSchema, data interface{}) (*schemapb.FieldData, error) {
	var fieldData schemapb.FieldData
	fieldData.FieldName = fieldSchema.Name
	fieldData.Type = fieldSchema.DataType
	switch data := data.(type) {
	case []int64:
		switch fieldData.Type {
		case schemapb.DataType_Int64:
			fieldData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: data,
						},
					},
				},
			}
		case schemapb.DataType_VarChar:
			strIDs := make([]string, len(data))
			for i, v := range data {
				strIDs[i] = strconv.FormatInt(v, 10)
			}
			fieldData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: strIDs,
						},
					},
				},
			}
		default:
			return nil, merr.WrapErrParameterInvalidMsg("currently only support autoID for int64 and varchar PrimaryField")
		}
	default:
		return nil, merr.WrapErrParameterInvalidMsg("currently only int64 is supported as the data source for the autoID of a PrimaryField")
	}

	return &fieldData, nil
}

// validateFieldDataColumns validates that all required fields are present and no unknown fields exist.
// It checks:
// 1. The number of columns matches the expected count (excluding BM25 output fields)
// 2. All field names exist in the schema
// Returns detailed error message listing expected and provided fields if validation fails.
func validateFieldDataColumns(columns []*schemapb.FieldData, schema *schemaInfo) error {
	expectColumnNum := 0

	// Count expected columns
	for _, field := range schema.GetFields() {
		if !typeutil.IsBM25FunctionOutputField(field, schema.CollectionSchema) && !typeutil.IsMinHashFunctionOutputField(field, schema.CollectionSchema) {
			expectColumnNum++
		}
	}
	for _, structField := range schema.GetStructArrayFields() {
		expectColumnNum += len(structField.GetFields())
	}

	// Validate column count
	if len(columns) != expectColumnNum {
		return merr.WrapErrParameterInvalidMsg("len(columns) mismatch the expectColumnNum, expectColumnNum: %d, len(columns): %d",
			expectColumnNum, len(columns))
	}

	// Validate field existence using schemaHelper
	for _, fieldData := range columns {
		_, err := schema.SchemaHelper.GetFieldFromNameDefaultJSON(fieldData.FieldName)
		if err != nil {
			return merr.WrapErrParameterInvalidMsg("fieldName %v not exist in collection schema", fieldData.FieldName)
		}
	}

	return nil
}

// validateAndNormalizeFieldDataValidData validates compatibility fields once
// when a user payload enters the proxy, then keeps only the current
// field-specific representation for internal processing.
func validateAndNormalizeFieldDataValidData(fields []*schemapb.FieldData) error {
	for _, field := range fields {
		if !typeutil.ValidateAndNormalizeFieldDataValidData(field) {
			return merr.WrapErrParameterInvalidMsg(
				"field %s has different legacy and field-specific valid_data",
				field.GetFieldName(),
			)
		}
	}
	return nil
}

// fillFieldPropertiesOnly fills field properties (FieldId, Type, ElementType) from schema.
// It assumes that columns have been validated and does not perform validation.
// Use validateFieldDataColumns before calling this function if validation is needed.
func fillFieldPropertiesOnly(columns []*schemapb.FieldData, schema *schemaInfo) error {
	for _, fieldData := range columns {
		// Use schemaHelper to get field schema, automatically handles dynamic fields
		fieldSchema, err := schema.SchemaHelper.GetFieldFromNameDefaultJSON(fieldData.FieldName)
		if err != nil {
			return merr.WrapErrParameterInvalidMsg("fieldName %v not exist in collection schema", fieldData.FieldName)
		}

		fieldData.FieldId = fieldSchema.FieldID
		fieldData.Type = fieldSchema.DataType

		// Set the ElementType because it may not be set in the insert request.
		switch fieldData.Type {
		case schemapb.DataType_Array:
			fd, ok := fieldData.Field.(*schemapb.FieldData_Scalars)
			if !ok || fd.Scalars.GetArrayData() == nil {
				return merr.WrapErrParameterInvalidMsg("field convert FieldData_Scalars fail in fieldData, fieldName: %s, collectionName: %s",
					fieldData.FieldName, schema.Name)
			}
			fd.Scalars.GetArrayData().ElementType = fieldSchema.ElementType
		case schemapb.DataType_ArrayOfVector:
			fd, ok := fieldData.Field.(*schemapb.FieldData_Vectors)
			if !ok || fd.Vectors.GetVectorArray() == nil {
				return merr.WrapErrParameterInvalidMsg("field convert FieldData_Vectors fail in fieldData, fieldName: %s, collectionName: %s",
					fieldData.FieldName, schema.Name)
			}
			fd.Vectors.GetVectorArray().ElementType = fieldSchema.ElementType
		}
	}

	return nil
}

func translatePkOutputFields(schema *schemapb.CollectionSchema) ([]string, []int64) {
	pkNames := []string{}
	fieldIDs := []int64{}
	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			pkNames = append(pkNames, field.GetName())
			fieldIDs = append(fieldIDs, field.GetFieldID())
		}
	}
	return pkNames, fieldIDs
}

func checkFieldsDataBySchema(ctx context.Context, allFields []*schemapb.FieldSchema, schema *schemapb.CollectionSchema, insertMsg *msgstream.InsertMsg, inInsert bool) error {
	log := mlog.With(mlog.String("collection", schema.GetName()))
	primaryKeyNum := 0
	autoGenFieldNum := 0

	dataNameSet := typeutil.NewSet[string]()
	for _, data := range insertMsg.FieldsData {
		fieldName := data.GetFieldName()
		if dataNameSet.Contain(fieldName) {
			return merr.WrapErrParameterInvalidMsg("duplicated field %s found", fieldName)
		}
		dataNameSet.Insert(fieldName)
	}

	allowInsertAutoID, _ := common.IsAllowInsertAutoID(schema.GetProperties()...)
	hasPkData := false
	needAutoGenPk := false

	for _, fieldSchema := range allFields {
		if fieldSchema.AutoID && !fieldSchema.IsPrimaryKey {
			log.Warn(ctx, "not primary key field, but set autoID true", mlog.String("field", fieldSchema.GetName()))
			return merr.WrapErrParameterInvalidMsg("only primary key could be with AutoID enabled")
		}

		if fieldSchema.IsPrimaryKey {
			primaryKeyNum++
			hasPkData = dataNameSet.Contain(fieldSchema.GetName())
			needAutoGenPk = fieldSchema.AutoID && (!allowInsertAutoID || !hasPkData)
		}
		if fieldSchema.GetDefaultValue() != nil && fieldSchema.IsPrimaryKey {
			return merr.WrapErrParameterInvalidMsg("primary key can't be with default value")
		}
		if (fieldSchema.IsPrimaryKey && fieldSchema.AutoID && !paramtable.Get().ProxyCfg.SkipAutoIDCheck.GetAsBool() && needAutoGenPk && inInsert) || typeutil.IsBM25FunctionOutputField(fieldSchema, schema) || typeutil.IsMinHashFunctionOutputField(fieldSchema, schema) {
			// when inInsert, no need to pass when pk is autoid and SkipAutoIDCheck is false
			autoGenFieldNum++
		}
		if _, ok := dataNameSet[fieldSchema.GetName()]; !ok {
			if (fieldSchema.IsPrimaryKey && fieldSchema.AutoID && !paramtable.Get().ProxyCfg.SkipAutoIDCheck.GetAsBool() && needAutoGenPk && inInsert) || typeutil.IsBM25FunctionOutputField(fieldSchema, schema) || typeutil.IsMinHashFunctionOutputField(fieldSchema, schema) {
				// autoGenField
				continue
			}

			if fieldSchema.GetDefaultValue() == nil && !fieldSchema.GetNullable() {
				log.Warn(ctx, "no corresponding fieldData pass in", mlog.String("fieldSchema", fieldSchema.GetName()))
				return merr.WrapErrParameterInvalidMsg("fieldSchema(%s) has no corresponding fieldData pass in", fieldSchema.GetName())
			}
			// when use default_value or has set Nullable
			// it's ok that no corresponding fieldData found
			dataToAppend, err := typeutil.GenEmptyFieldData(fieldSchema)
			if err != nil {
				return err
			}
			typeutil.SetFieldDataValidData(dataToAppend, make([]bool, insertMsg.GetNumRows()))
			insertMsg.FieldsData = append(insertMsg.FieldsData, dataToAppend)
		}
	}

	if primaryKeyNum > 1 {
		log.Warn(ctx, "more than 1 primary keys not supported",
			mlog.Int64("primaryKeyNum", int64(primaryKeyNum)))
		return merr.WrapErrParameterInvalidMsg("more than 1 primary keys not supported, got %d", primaryKeyNum)
	}
	expectedNum := len(allFields)
	actualNum := len(insertMsg.FieldsData) + autoGenFieldNum

	if expectedNum != actualNum {
		log.Warn(ctx, "the number of fields is not the same as needed", mlog.Int("expected", expectedNum), mlog.Int("actual", actualNum))
		return merr.WrapErrParameterInvalid(expectedNum, actualNum, "more fieldData has pass in")
	}

	return nil
}

// subFieldHasData checks whether a struct sub-field contains actual data content,
// not just a protobuf-initialized empty wrapper (e.g. some SDKs may initialize the
// Vectors oneof by accessing .vectors.dim, making Field non-nil without real data).
func subFieldHasData(subField *schemapb.FieldData) bool {
	switch fd := subField.Field.(type) {
	case *schemapb.FieldData_Scalars:
		return fd.Scalars.GetData() != nil
	case *schemapb.FieldData_Vectors:
		return fd.Vectors.GetData() != nil
	default:
		return false
	}
}

// vectorArrayElementWidth counts underlying slice entries per vector: float32
// entries for FloatVector, bytes for the other supported vector types.
func vectorArrayElementWidth(elementType schemapb.DataType, dim int64) (int, error) {
	if dim <= 0 {
		return 0, merr.WrapErrParameterInvalidMsg("invalid dim %d", dim)
	}
	switch elementType {
	case schemapb.DataType_FloatVector, schemapb.DataType_Int8Vector:
		return int(dim), nil
	case schemapb.DataType_BinaryVector:
		return int((dim + 7) / 8), nil
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		return int(dim * 2), nil
	default:
		return 0, merr.WrapErrParameterInvalidMsg("unsupported array-of-vector element type %s", elementType.String())
	}
}

// checkAndFlattenStructFieldData verifies the array length of the struct array field data in the insert message
// and then flattens the data so that data node and query node have not to handle the struct array field data.
func checkAndFlattenStructFieldData(schema *schemapb.CollectionSchema, insertMsg *msgstream.InsertMsg) error {
	structSchemaMap := make(map[string]*schemapb.StructArrayFieldSchema, len(schema.GetStructArrayFields()))
	for _, structField := range schema.GetStructArrayFields() {
		structSchemaMap[structField.Name] = structField
	}

	fieldSchemaMap := make(map[string]*schemapb.FieldSchema, len(schema.GetFields()))
	for _, fieldSchema := range schema.GetFields() {
		fieldSchemaMap[fieldSchema.Name] = fieldSchema
	}

	structFieldCount := 0
	flattenedFields := make([]*schemapb.FieldData, 0, len(insertMsg.GetFieldsData())+5)

	for _, fieldData := range insertMsg.GetFieldsData() {
		if _, ok := fieldSchemaMap[fieldData.FieldName]; ok {
			flattenedFields = append(flattenedFields, fieldData)
			continue
		}

		structName := fieldData.FieldName
		structSchema, ok := structSchemaMap[structName]
		if !ok {
			return merr.WrapErrParameterInvalidMsg("fieldName %v not exist in collection schema, fieldType %v, fieldId %v", fieldData.FieldName, fieldData.Type, fieldData.FieldId)
		}

		structFieldCount++
		structArrays, ok := fieldData.Field.(*schemapb.FieldData_StructArrays)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("field convert FieldData_StructArrays fail in fieldData, fieldName: %s,"+
				" collectionName:%s", structName, schema.Name)
		}

		if len(structArrays.StructArrays.Fields) != len(structSchema.GetFields()) {
			return merr.WrapErrParameterInvalidMsg("length of fields of struct field mismatch length of the fields in schema, fieldName: %s,"+
				" collectionName:%s, fieldData fields length:%d, schema fields length:%d",
				structName, schema.Name, len(structArrays.StructArrays.Fields), len(structSchema.GetFields()))
		}

		// Check sub-field data consistency: within the same struct, all sub-fields must
		// either have data or all be empty. Partial presence is invalid.
		hasDataCount := 0
		for _, subField := range structArrays.StructArrays.Fields {
			if !typeutil.ValidateAndNormalizeFieldDataValidData(subField) {
				return merr.WrapErrParameterInvalidMsg("sub-field '%s' in struct '%s' has different legacy and field-specific valid_data",
					subField.GetFieldName(), structName)
			}
			if subFieldHasData(subField) {
				hasDataCount++
			}
		}
		totalSubFields := len(structArrays.StructArrays.Fields)
		if hasDataCount == 0 {
			// All sub-fields have empty payload — equivalent to the struct being
			// omitted entirely. Reject illegal ValidData first: when no payload is
			// provided, any ValidData[i]==true contradicts itself.
			for _, subField := range structArrays.StructArrays.Fields {
				for j, v := range typeutil.GetFieldDataValidData(subField) {
					if v {
						return merr.WrapErrParameterInvalidMsg("sub-field '%s' in struct '%s' claims row %d is valid but no payload is provided",
							subField.FieldName, structName, j)
					}
				}
			}
			// Skip flatten and let checkFieldsDataBySchema backfill missing sub-fields
			// uniformly, so scenario "struct omitted" and scenario "struct present but
			// empty" share one code path downstream.
			continue
		}
		if hasDataCount != totalSubFields {
			return merr.WrapErrParameterInvalidMsg("inconsistent sub-field data in struct '%s': %d of %d sub-fields have data, all must be present or all absent",
				structName, hasDataCount, totalSubFields)
		}

		// Validate that all sub-fields share the same ValidData mask.
		// Nullable is a struct-level concept: a row is either entirely null or entirely present.
		if structSchema.GetNullable() {
			var refValidData []bool
			var refFieldName string
			refInitialized := false
			for _, subField := range structArrays.StructArrays.Fields {
				validData := typeutil.GetFieldDataValidData(subField)
				if !refInitialized {
					refValidData = validData
					refFieldName = subField.FieldName
					refInitialized = true
					continue
				}
				if len(validData) != len(refValidData) {
					return merr.WrapErrParameterInvalidMsg("sub-field ValidData length mismatch in struct '%s': '%s' has %d, '%s' has %d",
						structName, refFieldName, len(refValidData), subField.FieldName, len(validData))
				}
				for j := range refValidData {
					if validData[j] != refValidData[j] {
						return merr.WrapErrParameterInvalidMsg("sub-field ValidData mismatch in struct '%s' at row %d: '%s'=%v, '%s'=%v",
							structName, j, refFieldName, refValidData[j], subField.FieldName, validData[j])
					}
				}
			}
		}

		subFieldSchemaByName := make(map[string]*schemapb.FieldSchema, len(structSchema.GetFields())*2)
		for _, subFieldSchema := range structSchema.GetFields() {
			subFieldSchemaByName[subFieldSchema.GetName()] = subFieldSchema
			subFieldSchemaByName[storedStructSubFieldName(structName, subFieldSchema.GetName())] = subFieldSchema
			if typeutil.IsStructSubField(subFieldSchema.GetName()) {
				rawName, err := typeutil.ExtractStructFieldName(subFieldSchema.GetName())
				if err != nil {
					return err
				}
				subFieldSchemaByName[rawName] = subFieldSchema
			}
		}

		vectorElementWidth := func(subField *schemapb.FieldData, subFieldSchema *schemapb.FieldSchema) (int, error) {
			dim, err := typeutil.GetDim(subFieldSchema)
			if err != nil {
				return 0, merr.WrapErrParameterInvalidErr(err, "sub-field '%s' in struct '%s'", subField.GetFieldName(), structName)
			}
			width, err := vectorArrayElementWidth(subFieldSchema.GetElementType(), dim)
			if err != nil {
				return 0, merr.Wrapf(err, "sub-field '%s' in struct '%s'", subField.GetFieldName(), structName)
			}
			return width, nil
		}

		// Check the payload row count and, while those rows are in hand, verify the
		// per-row struct element count. The outer row count only proves that every
		// sub-field has the same number of physical payload rows. For each such row,
		// every sub-field must also describe the same number of struct elements.
		expectedArrayLen := -1
		var firstValidData []bool
		type rowElementCounter struct {
			name  string
			count func(physicalRow int) (int, error)
		}
		rowElementCounters := make([]rowElementCounter, 0, totalSubFields)
		for _, subField := range structArrays.StructArrays.Fields {
			subFieldSchema := subFieldSchemaByName[subField.GetFieldName()]
			if subFieldSchema == nil {
				return merr.WrapErrParameterInvalidMsg("sub-field '%s' not found in struct schema '%s'", subField.GetFieldName(), structName)
			}

			var currentArrayLen int

			switch subFieldData := subField.Field.(type) {
			case *schemapb.FieldData_Scalars:
				if scalarArray := subFieldData.Scalars.GetArrayData(); scalarArray != nil {
					currentArrayLen = len(scalarArray.Data)
					if totalSubFields > 1 {
						rowElementCounters = append(rowElementCounters, rowElementCounter{
							name: subField.GetFieldName(),
							count: func(physicalRow int) (int, error) {
								row := scalarArray.GetData()[physicalRow]
								if row.GetData() == nil {
									return 0, merr.WrapErrParameterInvalidMsg("nil array data")
								}
								if subFieldSchema.GetElementNullable() {
									return len(typeutil.GetArrayElementValidData(row)), nil
								}
								if typeutil.IsNestedArrayTypeSchema(subFieldSchema.GetTypeSchema()) {
									return len(row.GetArrayData().GetData()), nil
								}
								switch subFieldSchema.GetElementType() {
								case schemapb.DataType_Bool:
									return len(row.GetBoolData().GetData()), nil
								case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
									return len(row.GetIntData().GetData()), nil
								case schemapb.DataType_Int64:
									return len(row.GetLongData().GetData()), nil
								case schemapb.DataType_Float:
									return len(row.GetFloatData().GetData()), nil
								case schemapb.DataType_Double:
									return len(row.GetDoubleData().GetData()), nil
								case schemapb.DataType_VarChar, schemapb.DataType_String:
									return len(row.GetStringData().GetData()), nil
								default:
									return 0, merr.WrapErrParameterInvalidMsg("unsupported array element type %s", subFieldSchema.GetElementType().String())
								}
							},
						})
					}
				} else {
					return merr.WrapErrParameterInvalidMsg("scalar array data is nil in struct field '%s', sub-field '%s'",
						structName, subField.FieldName)
				}
			case *schemapb.FieldData_Vectors:
				if vectorArray := subFieldData.Vectors.GetVectorArray(); vectorArray != nil {
					currentArrayLen = len(vectorArray.Data)
					if totalSubFields > 1 {
						var vectorWidth int
						rowElementCounters = append(rowElementCounters, rowElementCounter{
							name: subField.GetFieldName(),
							count: func(physicalRow int) (int, error) {
								if vectorWidth == 0 {
									var err error
									vectorWidth, err = vectorElementWidth(subField, subFieldSchema)
									if err != nil {
										return 0, err
									}
								}
								row := vectorArray.GetData()[physicalRow]
								if row.GetData() == nil {
									return 0, merr.WrapErrParameterInvalidMsg("nil vector array data")
								}
								var payloadLen int
								switch subFieldSchema.GetElementType() {
								case schemapb.DataType_FloatVector:
									payloadLen = len(row.GetFloatVector().GetData())
								case schemapb.DataType_BinaryVector:
									payloadLen = len(row.GetBinaryVector())
								case schemapb.DataType_Float16Vector:
									payloadLen = len(row.GetFloat16Vector())
								case schemapb.DataType_BFloat16Vector:
									payloadLen = len(row.GetBfloat16Vector())
								case schemapb.DataType_Int8Vector:
									payloadLen = len(row.GetInt8Vector())
								}
								if payloadLen%vectorWidth != 0 {
									return 0, merr.WrapErrParameterInvalidMsg("payload length %d is not divisible by vector width %d", payloadLen, vectorWidth)
								}
								if subFieldSchema.GetElementNullable() {
									return len(typeutil.GetVectorArrayElementValidData(row)), nil
								}
								return payloadLen / vectorWidth, nil
							},
						})
					}
				} else {
					return merr.WrapErrParameterInvalidMsg("vector array data is nil in struct field '%s', sub-field '%s'",
						structName, subField.FieldName)
				}
			default:
				return merr.WrapErrParameterInvalidMsg("unexpected field data type in struct array field, fieldName: %s", structName)
			}

			if expectedArrayLen == -1 {
				expectedArrayLen = currentArrayLen
				firstValidData = typeutil.GetFieldDataValidData(subField)
			} else if currentArrayLen != expectedArrayLen {
				return merr.WrapErrParameterInvalidMsg("inconsistent array length in struct field '%s': expected %d, got %d for sub-field '%s'",
					structName, expectedArrayLen, currentArrayLen, subField.FieldName)
			}
		}

		if totalSubFields > 1 && expectedArrayLen > 0 {
			var physicalToLogical []int
			if len(firstValidData) > 0 {
				physicalToLogical = make([]int, 0, expectedArrayLen)
				for logicalRow, valid := range firstValidData {
					if valid {
						physicalToLogical = append(physicalToLogical, logicalRow)
					}
				}
				if len(physicalToLogical) != expectedArrayLen {
					return merr.WrapErrParameterInvalidMsg("invalid ValidData for struct '%s': true count %d does not match payload row count %d",
						structName, len(physicalToLogical), expectedArrayLen)
				}
			}
			logicalRow := func(physicalRow int) int {
				if len(physicalToLogical) == 0 {
					return physicalRow
				}
				return physicalToLogical[physicalRow]
			}

			refCounter := rowElementCounters[0]
			refElementCounts := make([]int, expectedArrayLen)
			for physicalRow := 0; physicalRow < expectedArrayLen; physicalRow++ {
				count, err := refCounter.count(physicalRow)
				if err != nil {
					return merr.WrapErrParameterInvalidErr(err, "struct '%s' row %d sub-field '%s'",
						structName, logicalRow(physicalRow), refCounter.name)
				}
				refElementCounts[physicalRow] = count
			}
			for _, counter := range rowElementCounters[1:] {
				for physicalRow := 0; physicalRow < expectedArrayLen; physicalRow++ {
					count, err := counter.count(physicalRow)
					if err != nil {
						return merr.WrapErrParameterInvalidErr(err, "struct '%s' row %d sub-field '%s'",
							structName, logicalRow(physicalRow), counter.name)
					}
					if count != refElementCounts[physicalRow] {
						return merr.WrapErrParameterInvalidMsg("inconsistent struct element count in struct '%s' at row %d: '%s' has %d, '%s' has %d",
							structName, logicalRow(physicalRow), refCounter.name, refElementCounts[physicalRow], counter.name, count)
					}
				}
			}
		}

		for _, subField := range structArrays.StructArrays.Fields {
			transformedFieldName := storedStructSubFieldName(structName, subField.FieldName)
			validData := typeutil.GetFieldDataValidData(subField)
			// Field is shared by the flattened copy. Normalize validity before
			// sharing it so the source and flattened field cannot conflict.
			typeutil.SetFieldDataValidData(subField, validData)
			subFieldCopy := &schemapb.FieldData{
				FieldName: transformedFieldName,
				FieldId:   subField.FieldId,
				Type:      subField.Type,
				Field:     subField.Field,
				IsDynamic: subField.IsDynamic,
			}

			flattenedFields = append(flattenedFields, subFieldCopy)
		}
	}

	// Verify all required (non-nullable) struct array fields are provided
	seenStructs := make(map[string]bool, structFieldCount)
	for _, fieldData := range insertMsg.GetFieldsData() {
		if _, ok := structSchemaMap[fieldData.FieldName]; ok {
			seenStructs[fieldData.FieldName] = true
		}
	}
	for _, sf := range schema.GetStructArrayFields() {
		if !sf.GetNullable() && !seenStructs[sf.Name] {
			return merr.WrapErrParameterInvalidMsg("required struct array field '%s' is missing in insert data", sf.Name)
		}
	}

	insertMsg.FieldsData = flattenedFields
	return nil
}

func checkPrimaryFieldData(ctx context.Context, allFields []*schemapb.FieldSchema, schema *schemapb.CollectionSchema, insertMsg *msgstream.InsertMsg) (*schemapb.IDs, error) {
	log := mlog.With(mlog.String("collectionName", insertMsg.CollectionName))
	rowNums := uint32(insertMsg.NRows())
	// TODO(dragondriver): in fact, NumRows is not trustable, we should check all input fields
	if insertMsg.NRows() <= 0 {
		return nil, merr.WrapErrParameterInvalid("invalid num_rows", fmt.Sprint(rowNums), "num_rows should be greater than 0")
	}

	if err := checkFieldsDataBySchema(ctx, allFields, schema, insertMsg, true); err != nil {
		return nil, err
	}

	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		log.Error(ctx, "get primary field schema failed", mlog.FieldSchema(schema), mlog.Err(err))
		return nil, err
	}
	if primaryFieldSchema.GetNullable() {
		return nil, merr.WrapErrParameterInvalidMsg("primary field not support null")
	}
	var primaryFieldData *schemapb.FieldData
	// when checkPrimaryFieldData in insert

	allowInsertAutoID, _ := common.IsAllowInsertAutoID(schema.GetProperties()...)
	skipAutoIDCheck := primaryFieldSchema.AutoID &&
		typeutil.IsPrimaryFieldDataExist(insertMsg.GetFieldsData(), primaryFieldSchema) && (paramtable.Get().ProxyCfg.SkipAutoIDCheck.GetAsBool() || allowInsertAutoID)

	if !primaryFieldSchema.AutoID || skipAutoIDCheck {
		primaryFieldData, err = typeutil.GetPrimaryFieldData(insertMsg.GetFieldsData(), primaryFieldSchema)
		if err != nil {
			log.Info(ctx, "get primary field data failed", mlog.Err(err))
			return nil, err
		}
	} else {
		// check primary key data not exist
		if typeutil.IsPrimaryFieldDataExist(insertMsg.GetFieldsData(), primaryFieldSchema) {
			return nil, merr.WrapErrParameterInvalidMsg("can not assign primary field data when auto id enabled and allow_insert_auto_id is false %v", primaryFieldSchema.Name)
		}
		// if autoID == true, currently support autoID for int64 and varchar PrimaryField
		primaryFieldData, err = autoGenPrimaryFieldData(primaryFieldSchema, insertMsg.GetRowIDs())
		if err != nil {
			log.Info(ctx, "generate primary field data failed when autoID == true", mlog.Err(err))
			return nil, err
		}
		// if autoID == true, set the primary field data
		// insertMsg.fieldsData need append primaryFieldData
		insertMsg.FieldsData = append(insertMsg.FieldsData, primaryFieldData)
	}

	// parse primaryFieldData to result.IDs, and as returned primary keys
	ids, err := parsePrimaryFieldData2IDs(primaryFieldData)
	if err != nil {
		log.Warn(ctx, "parse primary field data to IDs failed", mlog.Err(err))
		return nil, err
	}

	return ids, nil
}

// for some varchar with analzyer
// we need check char format before insert it to message queue
// now only support utf-8
func checkInputUtf8Compatiable(allFields []*schemapb.FieldSchema, insertMsg *msgstream.InsertMsg) error {
	checkeFields := lo.FilterMap(allFields, func(field *schemapb.FieldSchema, _ int) (int64, bool) {
		if field.DataType == schemapb.DataType_VarChar {
			return field.GetFieldID(), true
		}

		if field.DataType != schemapb.DataType_Text {
			return 0, false
		}

		for _, kv := range field.GetTypeParams() {
			if kv.Key == common.EnableAnalyzerKey {
				return field.GetFieldID(), true
			}
		}
		return 0, false
	})

	if len(checkeFields) == 0 {
		return nil
	}

	for _, fieldData := range insertMsg.FieldsData {
		if !lo.Contains(checkeFields, fieldData.GetFieldId()) {
			continue
		}

		strData := fieldData.GetScalars().GetStringData()
		for row, data := range strData.GetData() {
			ok := utf8.ValidString(data)
			if !ok {
				mlog.Warn(context.TODO(), "string field data not utf-8 format", mlog.String("messageVersion", strData.ProtoReflect().Descriptor().Syntax().GoString()))
				return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("input with analyzer should be utf-8 format, but row: %d not utf-8 format. data: %s", row, data))
			}
		}
	}
	return nil
}

// doCheckDynamicFieldData is the shared implementation for dynamic field validation.
// When skipStaticFieldNameCheck is true, $meta keys matching static field names are
// allowed — this is needed for partial updates after schema evolution, where $meta
// may legitimately contain keys that now correspond to static columns.
func doCheckDynamicFieldData(schema *schemapb.CollectionSchema, insertMsg *msgstream.InsertMsg, skipStaticFieldNameCheck bool) error {
	for _, data := range insertMsg.FieldsData {
		if data.IsDynamic {
			data.FieldName = common.MetaFieldName
			return verifyDynamicFieldData(schema, insertMsg, skipStaticFieldNameCheck)
		}
	}
	defaultData := make([][]byte, insertMsg.NRows())
	for i := range defaultData {
		defaultData[i] = []byte("{}")
	}
	dynamicData := autoGenDynamicFieldData(schema, defaultData)
	insertMsg.FieldsData = append(insertMsg.FieldsData, dynamicData)
	return nil
}

func checkDynamicFieldData(schema *schemapb.CollectionSchema, insertMsg *msgstream.InsertMsg) error {
	return doCheckDynamicFieldData(schema, insertMsg, false)
}

func addNamespaceData(schema *schemapb.CollectionSchema, insertMsg *msgstream.InsertMsg) error {
	partitionName, namespaceAsPartition, err := resolveNamespacePartitionName(schema, insertMsg.Namespace, insertMsg.GetPartitionName())
	if err != nil {
		return err
	}
	if !schema.GetEnableNamespace() {
		return nil
	}
	if namespaceAsPartition {
		insertMsg.PartitionName = partitionName
		return nil
	}

	// check namespace field exists
	namespaceField := typeutil.GetFieldByName(schema, common.NamespaceFieldName)
	if namespaceField == nil {
		return merr.WrapErrParameterInvalidMsg("namespace field not found")
	}

	// If namespace field data is already present, validate it instead of rejecting outright.
	for _, fieldData := range insertMsg.FieldsData {
		if fieldData.FieldId == namespaceField.FieldID {
			ns := ""
			if insertMsg.Namespace != nil {
				ns = *insertMsg.Namespace
			}
			scalars := fieldData.GetScalars()
			if scalars == nil {
				return merr.WrapErrParameterInvalidMsg("invalid namespace field data layout")
			}
			strData := scalars.GetStringData()
			if strData == nil {
				return merr.WrapErrParameterInvalidMsg("invalid namespace field data layout")
			}
			for _, v := range strData.GetData() {
				if v != ns {
					return merr.WrapErrParameterInvalidMsg("namespace field value %q mismatches namespace %q", v, ns)
				}
			}
			// Values are consistent with the namespace; nothing more to do.
			return nil
		}
	}

	// set namespace field data
	namespaceData := make([]string, insertMsg.NRows())
	namespace := *insertMsg.Namespace
	for i := range namespaceData {
		namespaceData[i] = namespace
	}
	insertMsg.FieldsData = append(insertMsg.FieldsData, &schemapb.FieldData{
		FieldName: namespaceField.Name,
		FieldId:   namespaceField.FieldID,
		Type:      namespaceField.DataType,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: namespaceData,
					},
				},
			},
		},
	})
	return nil
}

func genFunctionFields(ctx context.Context, insertMsg *msgstream.InsertMsg, schema *schemaInfo, partialUpdate bool) error {
	functions := schema.GetFunctions()
	allowNonBM25Outputs := common.GetCollectionAllowInsertNonBM25FunctionOutputs(schema.Properties)
	fieldIDs := lo.Map(insertMsg.FieldsData, func(fieldData *schemapb.FieldData, _ int) int64 {
		id, _ := schema.MapFieldID(fieldData.FieldName)
		return id
	})

	// Since PartialUpdate is supported, the field_data here may not be complete
	needProcessFunctions, err := typeutil.GetNeedProcessFunctions(fieldIDs, functions, allowNonBM25Outputs, partialUpdate)
	if err != nil {
		mlog.Warn(context.TODO(), "Check upsert field error,", mlog.String("collectionName", schema.Name), mlog.Err(err))
		return err
	}

	if embedding.HasNonBM25AndMinHashFunctions(functions, []int64{}) {
		ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-genFunctionFields-call-function-udf")
		defer sp.End()
		exec, err := embedding.NewFunctionExecutor(schema.CollectionSchema, needProcessFunctions, &models.ModelExtraInfo{ClusterID: paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), DBName: insertMsg.GetDbName()})
		if err != nil {
			return err
		}
		sp.AddEvent("Create-function-udf")
		if err := exec.ProcessInsert(ctx, insertMsg); err != nil {
			return err
		}
		sp.AddEvent("Call-function-udf")
	}
	return nil
}

func storedStructSubFieldName(structName string, fieldName string) string {
	if typeutil.IsStructSubField(fieldName) {
		return fieldName
	}
	return typeutil.ConcatStructFieldName(structName, fieldName)
}

func getPartitionKeyFieldData(fieldSchema *schemapb.FieldSchema, insertMsg *msgstream.InsertMsg) (*schemapb.FieldData, error) {
	if len(insertMsg.GetPartitionName()) > 0 && !paramtable.Get().ProxyCfg.SkipPartitionKeyCheck.GetAsBool() {
		return nil, merr.WrapErrParameterInvalidMsg("not support manually specifying the partition names if partition key mode is used")
	}

	for _, fieldData := range insertMsg.GetFieldsData() {
		if fieldData.GetFieldId() == fieldSchema.GetFieldID() {
			return fieldData, nil
		}
	}

	return nil, merr.WrapErrParameterInvalidMsg("partition key not specify when insert")
}

func assignChannelsByPK(pks *schemapb.IDs, channelNames []string, insertMsg *msgstream.InsertMsg) (map[string][]int, error) {
	hashValues, err := typeutil.HashPK2Channels(pks, channelNames)
	if err != nil {
		return nil, err
	}
	insertMsg.HashValues = hashValues

	numChannels := len(channelNames)
	if numChannels == 0 {
		return nil, nil
	}

	numRows := len(insertMsg.HashValues)
	avgCapacity := (numRows / numChannels) + 1

	channel2RowOffsets := make(map[string][]int, numChannels)

	for offset, channelID := range insertMsg.HashValues {
		idx := int(channelID)
		if idx >= numChannels {
			continue
		}

		channelName := channelNames[idx]

		if _, ok := channel2RowOffsets[channelName]; !ok {
			channel2RowOffsets[channelName] = make([]int, 0, avgCapacity)
		}
		channel2RowOffsets[channelName] = append(channel2RowOffsets[channelName], offset)
	}

	return channel2RowOffsets, nil
}

func assignChannelsByNamespace(namespace string, channelNames []string, insertMsg *msgstream.InsertMsg) (map[string][]int, error) {
	if len(channelNames) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("no virtual channels available for namespace sharding")
	}
	channelID := typeutil.HashNamespace2Channels(namespace, channelNames)
	return assignChannelsByChannel(channelID, channelNames, insertMsg), nil
}

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

// check whether insertMsg has all fields in schema
func LackOfFieldsDataBySchema(schema *schemapb.CollectionSchema, fieldsData []*schemapb.FieldData, skipPkFieldCheck bool, skipDynamicFieldCheck bool) error {
	log := mlog.With(mlog.String("collection", schema.GetName()))

	// find bm25 generated fields
	bm25Fields := typeutil.NewSet[string](GetFunctionOutputFields(schema)...)
	dataNameMap := make(map[string]*schemapb.FieldData)
	for _, data := range fieldsData {
		dataNameMap[data.GetFieldName()] = data
	}

	for _, fieldSchema := range schema.Fields {
		if bm25Fields.Contain(fieldSchema.GetName()) {
			continue
		}

		if fieldSchema.GetNullable() || fieldSchema.GetDefaultValue() != nil {
			continue
		}

		if _, ok := dataNameMap[fieldSchema.GetName()]; !ok {
			if (fieldSchema.IsPrimaryKey && fieldSchema.AutoID && !paramtable.Get().ProxyCfg.SkipAutoIDCheck.GetAsBool() && skipPkFieldCheck) ||
				typeutil.IsBM25FunctionOutputField(fieldSchema, schema) || typeutil.IsMinHashFunctionOutputField(fieldSchema, schema) ||
				(skipDynamicFieldCheck && fieldSchema.GetIsDynamic()) {
				// autoGenField
				continue
			}

			log.Info(context.TODO(), "no corresponding fieldData pass in", mlog.String("fieldSchema", fieldSchema.GetName()))
			return merr.WrapErrParameterInvalidMsg("missing required field %q", fieldSchema.GetName())
		}
	}
	for _, structSchema := range schema.GetStructArrayFields() {
		if structSchema.GetNullable() {
			continue
		}
		if _, ok := dataNameMap[structSchema.GetName()]; !ok {
			log.Info(context.TODO(), "no corresponding struct fieldData pass in", mlog.String("structFieldSchema", structSchema.GetName()))
			return merr.WrapErrParameterInvalidMsg("missing required struct field %q", structSchema.GetName())
		}
	}

	return nil
}

// checkDynamicFieldDataForPartialUpdate is a relaxed version of checkDynamicFieldData
// for partial updates. After schema evolution, $meta may legitimately contain keys
// matching static field names (e.g., a dynamic field "end_timestamp" that was later
// added as a static column). This function validates JSON format and rejects the
// reserved $meta key, but skips the static field name conflict check so that
// existing dynamic field data is preserved.
func checkDynamicFieldDataForPartialUpdate(schema *schemapb.CollectionSchema, insertMsg *msgstream.InsertMsg) error {
	return doCheckDynamicFieldData(schema, insertMsg, true)
}

// checkUpsertPrimaryFieldData validates and returns the PKs, applying only
// caller-allocated AutoIDs. allocatedIDs maps zero-based row offsets in the
// supplied field data to IDs; a nil or empty map leaves the input fields unchanged.
// A PK column is required; non-PK fields are neither validated nor filled, so
// partial patches are accepted. When IDs are supplied, the PK column is replaced
// only after validation, collision checking, and parsing succeed. Allocation and
// retry state remain the caller's responsibility.
func checkUpsertPrimaryFieldData(
	schema *schemaInfo,
	fields []*schemapb.FieldData,
	numRows uint64,
	allocatedIDs map[int]int64,
) (*schemapb.IDs, error) {
	if numRows == 0 {
		return nil, merr.WrapErrParameterInvalid("invalid num_rows", fmt.Sprint(numRows), "num_rows should be greater than 0")
	}
	pkSchema, err := typeutil.GetPrimaryFieldSchema(schema.CollectionSchema)
	if err != nil {
		return nil, err
	}
	if pkSchema.GetNullable() {
		return nil, merr.WrapErrParameterInvalidMsg("primary field not support null")
	}
	primaryField, err := typeutil.GetPrimaryFieldData(fields, pkSchema)
	if err != nil {
		return nil, err
	}
	pk := proto.Clone(primaryField).(*schemapb.FieldData)
	if err := fieldvalidator.NewValidateUtil().Validate([]*schemapb.FieldData{pk}, schema.SchemaHelper, numRows); err != nil {
		return nil, err
	}
	if len(allocatedIDs) > 0 {
		ids := make([]int64, 0, len(allocatedIDs))
		rows := make([]int64, 0, len(allocatedIDs))
		indices := make([]int64, 0, len(allocatedIDs))
		for row, id := range allocatedIDs {
			if row < 0 || row >= typeutil.GetPKSize(pk) {
				return nil, merr.WrapErrServiceInternalMsg("upsert allocated AutoID row %d is out of range", row)
			}
			indices = append(indices, int64(len(ids)))
			ids = append(ids, id)
			rows = append(rows, int64(row))
		}
		generated, err := autoGenPrimaryFieldData(pkSchema, ids)
		if err != nil {
			return nil, err
		}
		if err := typeutil.UpdateFieldDataByColumn(pk, generated, rows, indices); err != nil {
			return nil, err
		}
		// Supplied PKs can contain arbitrary values, including a generated ID.
		duplicate, err := CheckDuplicatePkExist(pkSchema, []*schemapb.FieldData{pk})
		if err != nil {
			return nil, err
		}
		if duplicate {
			return nil, merr.WrapErrServiceInternalMsg("upsert: duplicate primary keys after applying allocated AutoIDs")
		}
	}
	ids, err := parsePrimaryFieldData2IDs(pk)
	if err != nil {
		return nil, err
	}
	if len(allocatedIDs) > 0 {
		for index, field := range fields {
			if field == primaryField {
				fields[index] = pk
				break
			}
		}
	}
	return ids, nil
}

func GetBM25FunctionOutputFields(collSchema *schemapb.CollectionSchema) []string {
	fields := make([]string, 0)
	for _, fSchema := range collSchema.Functions {
		if fSchema.Type == schemapb.FunctionType_BM25 {
			fields = append(fields, fSchema.OutputFieldNames...)
		}
	}
	return fields
}

func GetMinHashFunctionOutputFields(collSchema *schemapb.CollectionSchema) []string {
	fields := make([]string, 0)
	for _, fSchema := range collSchema.Functions {
		if fSchema.Type == schemapb.FunctionType_MinHash {
			fields = append(fields, fSchema.OutputFieldNames...)
		}
	}
	return fields
}

func autoGenDynamicFieldData(schema *schemapb.CollectionSchema, data [][]byte) *schemapb.FieldData {
	fd := &schemapb.FieldData{
		FieldName: common.MetaFieldName,
		Type:      schemapb.DataType_JSON,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{
						Data: data,
					},
				},
			},
		},
		IsDynamic: true,
	}

	// Only set ValidData when the $meta field is nullable or has a default value.
	// For 2.5 collections (non-nullable, no default), CheckValidData expects
	// len(ValidData)==0, so we must NOT set it.
	for _, f := range schema.Fields {
		if f.GetIsDynamic() && (f.GetNullable() || f.GetDefaultValue() != nil) {
			validData := make([]bool, len(data))
			for i := range validData {
				validData[i] = true
			}
			typeutil.SetFieldDataValidData(fd, validData)
			break
		}
	}

	return fd
}

func verifyDynamicFieldData(schema *schemapb.CollectionSchema, insertMsg *msgstream.InsertMsg, skipStaticFieldNameCheck bool) error {
	for _, field := range insertMsg.FieldsData {
		if field.GetFieldName() == common.MetaFieldName {
			if !schema.EnableDynamicField {
				return merr.WrapErrParameterInvalidMsg("without dynamic schema enabled, the field name cannot be set to %s", common.MetaFieldName)
			}
			for _, rowData := range field.GetScalars().GetJsonData().GetData() {
				jsonData := make(map[string]interface{})
				if err := json.Unmarshal(rowData, &jsonData); err != nil {
					mlog.Info(context.TODO(), "insert invalid dynamic data, milvus only support json map",
						mlog.ByteString("data", rowData),
						mlog.Err(err),
					)
					return merr.WrapErrParameterInvalidMsg("invalid dynamic field data, only json map is supported: %s", err.Error())
				}
				if _, ok := jsonData[common.MetaFieldName]; ok {
					return merr.WrapErrParameterInvalidMsg("cannot set json key to: %s", common.MetaFieldName)
				}
				if !skipStaticFieldNameCheck {
					for _, f := range schema.GetFields() {
						if _, ok := jsonData[f.GetName()]; ok {
							mlog.Info(context.TODO(), "dynamic field name include the static field name", mlog.String("fieldName", f.GetName()))
							return merr.WrapErrParameterInvalidMsg("dynamic field name cannot include the static field name: %s", f.GetName())
						}
					}
				}
			}
		}
	}
	return nil
}

// CheckDuplicatePkExist checks if there are duplicate primary keys in the field data.
// Returns (true, nil) if duplicates exist, (false, nil) if no duplicates.
// Returns (false, error) if there's an error during checking.
func CheckDuplicatePkExist(primaryFieldSchema *schemapb.FieldSchema, fieldsData []*schemapb.FieldData) (bool, error) {
	if len(fieldsData) == 0 {
		return false, nil
	}

	// find primary field data
	var primaryFieldData *schemapb.FieldData
	for _, field := range fieldsData {
		if field.GetFieldName() == primaryFieldSchema.GetName() {
			primaryFieldData = field
			break
		}
	}

	if primaryFieldData == nil {
		return false, merr.WrapErrParameterInvalidMsg("must assign pk when upsert, primary field: %v", primaryFieldSchema.GetName())
	}

	// check for duplicates based on primary key type
	switch primaryFieldData.Field.(type) {
	case *schemapb.FieldData_Scalars:
		scalarField := primaryFieldData.GetScalars()
		switch scalarField.Data.(type) {
		case *schemapb.ScalarField_LongData:
			intIDs := scalarField.GetLongData().GetData()
			return hasDuplicates(intIDs), nil
		case *schemapb.ScalarField_StringData:
			strIDs := scalarField.GetStringData().GetData()
			return hasDuplicates(strIDs), nil
		default:
			return false, merr.WrapErrParameterInvalidMsg("unsupported primary key type")
		}
	default:
		return false, merr.WrapErrParameterInvalidMsg("primary field must be scalar type")
	}
}

func GetFunctionOutputFields(collSchema *schemapb.CollectionSchema) []string {
	fields := make([]string, 0)
	for _, fSchema := range collSchema.Functions {
		fields = append(fields, fSchema.OutputFieldNames...)
	}
	return fields
}

func assignChannelsByChannel(channelID uint32, channelNames []string, insertMsg *msgstream.InsertMsg) map[string][]int {
	insertMsg.HashValues = make([]uint32, insertMsg.NumRows)
	for i := range insertMsg.HashValues {
		insertMsg.HashValues[i] = channelID
	}

	channelName := channelNames[channelID]
	channel2RowOffsets := map[string][]int{
		channelName: make([]int, 0, insertMsg.NRows()),
	}
	for i := range insertMsg.HashValues {
		channel2RowOffsets[channelName] = append(channel2RowOffsets[channelName], i)
	}
	return channel2RowOffsets
}

func hasDuplicates[T comparable](ids []T) bool {
	seen := make(map[T]struct{}, len(ids))
	for _, id := range ids {
		if _, exists := seen[id]; exists {
			return true
		}
		seen[id] = struct{}{}
	}
	return false
}
