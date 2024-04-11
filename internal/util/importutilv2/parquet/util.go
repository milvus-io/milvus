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

package parquet

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func WrapTypeErr(expect string, actual string, field *schemapb.FieldSchema) error {
	return merr.WrapErrImportFailed(
		fmt.Sprintf("expect '%s' type for field '%s', but got '%s' type",
			expect, field.GetName(), actual))
}

func calcBufferSize(blockSize int, schema *schemapb.CollectionSchema) int {
	if len(schema.GetFields()) <= 0 {
		return blockSize
	}
	return blockSize / len(schema.GetFields())
}

func CreateFieldReaders(ctx context.Context, fileReader *pqarrow.FileReader, schema *schemapb.CollectionSchema) (map[int64]*FieldReader, error) {
	nameToField := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) string {
		return field.GetName()
	})

	pqSchema, err := fileReader.Schema()
	if err != nil {
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("get parquet schema failed, err=%v", err))
	}

	crs := make(map[int64]*FieldReader)
	for i, pqField := range pqSchema.Fields() {
		field, ok := nameToField[pqField.Name]
		if !ok {
			// TODO @cai.zhang: handle dynamic field
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the field: %s is not in schema, "+
				"if it's a dynamic field, please reformat data by bulk_writer", pqField.Name))
		}
		if field.GetIsPrimaryKey() && field.GetAutoID() {
			return nil, merr.WrapErrImportFailed(
				fmt.Sprintf("the primary key '%s' is auto-generated, no need to provide", field.GetName()))
		}

		arrowType, isList := convertArrowSchemaToDataType(pqField, false)
		dataType := field.GetDataType()
		if isList {
			if !typeutil.IsVectorType(dataType) && dataType != schemapb.DataType_Array {
				return nil, WrapTypeErr(dataType.String(), pqField.Type.Name(), field)
			}
			if dataType == schemapb.DataType_Array {
				dataType = field.GetElementType()
			}
		}
		if !isConvertible(arrowType, dataType, isList) {
			if isList {
				return nil, WrapTypeErr(dataType.String(), pqField.Type.(*arrow.ListType).ElemField().Type.Name(), field)
			}
			return nil, WrapTypeErr(dataType.String(), pqField.Type.Name(), field)
		}

		cr, err := NewFieldReader(ctx, fileReader, i, field)
		if err != nil {
			return nil, err
		}
		if _, ok = crs[field.GetFieldID()]; ok {
			return nil, merr.WrapErrImportFailed(
				fmt.Sprintf("there is multi field with name: %s", field.GetName()))
		}
		crs[field.GetFieldID()] = cr
	}

	for _, field := range nameToField {
		if (field.GetIsPrimaryKey() && field.GetAutoID()) || field.GetIsDynamic() {
			continue
		}
		if _, ok := crs[field.GetFieldID()]; !ok {
			return nil, merr.WrapErrImportFailed(
				fmt.Sprintf("no parquet field for milvus file '%s'", field.GetName()))
		}
	}
	return crs, nil
}

func convertArrowSchemaToDataType(field arrow.Field, isList bool) (schemapb.DataType, bool) {
	switch field.Type.ID() {
	case arrow.BOOL:
		return schemapb.DataType_Bool, false
	case arrow.UINT8:
		if isList {
			return schemapb.DataType_BinaryVector, false
		}
		return schemapb.DataType_None, false
	case arrow.INT8:
		return schemapb.DataType_Int8, false
	case arrow.INT16:
		return schemapb.DataType_Int16, false
	case arrow.INT32:
		return schemapb.DataType_Int32, false
	case arrow.INT64:
		return schemapb.DataType_Int64, false
	case arrow.FLOAT16:
		if isList {
			return schemapb.DataType_Float16Vector, false
		}
		return schemapb.DataType_None, false
	case arrow.FLOAT32:
		return schemapb.DataType_Float, false
	case arrow.FLOAT64:
		return schemapb.DataType_Double, false
	case arrow.STRING:
		return schemapb.DataType_VarChar, false
	case arrow.BINARY:
		return schemapb.DataType_BinaryVector, false
	case arrow.LIST:
		elementType, _ := convertArrowSchemaToDataType(field.Type.(*arrow.ListType).ElemField(), true)
		return elementType, true
	default:
		return schemapb.DataType_None, false
	}
}

func isConvertible(src, dst schemapb.DataType, isList bool) bool {
	switch src {
	case schemapb.DataType_Bool:
		return typeutil.IsBoolType(dst)
	case schemapb.DataType_Int8:
		return typeutil.IsArithmetic(dst)
	case schemapb.DataType_Int16:
		return typeutil.IsArithmetic(dst) && dst != schemapb.DataType_Int8
	case schemapb.DataType_Int32:
		return typeutil.IsArithmetic(dst) && dst != schemapb.DataType_Int8 && dst != schemapb.DataType_Int16
	case schemapb.DataType_Int64:
		return typeutil.IsFloatingType(dst) || dst == schemapb.DataType_Int64
	case schemapb.DataType_Float:
		if isList && dst == schemapb.DataType_FloatVector {
			return true
		}
		return typeutil.IsFloatingType(dst)
	case schemapb.DataType_Double:
		if isList && dst == schemapb.DataType_FloatVector {
			return true
		}
		return dst == schemapb.DataType_Double
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return typeutil.IsStringType(dst) || typeutil.IsJSONType(dst)
	case schemapb.DataType_JSON:
		return typeutil.IsJSONType(dst)
	case schemapb.DataType_BinaryVector:
		return dst == schemapb.DataType_BinaryVector
	case schemapb.DataType_Float16Vector:
		return dst == schemapb.DataType_Float16Vector
	default:
		return false
	}
}

func estimateReadCountPerBatch(bufferSize int, schema *schemapb.CollectionSchema) (int64, error) {
	sizePerRecord, err := typeutil.EstimateMaxSizePerRecord(schema)
	if err != nil {
		return 0, err
	}
	if 1000*sizePerRecord <= bufferSize {
		return 1000, nil
	}
	return int64(bufferSize) / int64(sizePerRecord), nil
}
