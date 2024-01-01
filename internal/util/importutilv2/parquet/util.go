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
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

func CreateColumnReaders(fileReader *pqarrow.FileReader, schema *schemapb.CollectionSchema) (map[int64]*ColumnReader, error) {
	pqSchema, err := fileReader.Schema()
	if err != nil {
		log.Warn("can't schema from file", zap.Error(err))
		return nil, err
	}
	fields := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) string {
		return field.GetName()
	})
	pqFields := pqSchema.Fields()
	crs := make(map[int64]*ColumnReader)
	fmt.Println("dyh debug, CreateColumnReaders, pqFields:", pqFields, ", fields:", fields)
	for i, pqField := range pqFields {
		field, ok := fields[pqField.Name]
		if !ok {
			// TODO @cai.zhang: handle dynamic field
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the field: %s is not in schema, "+
				"if it's a dynamic field, please reformat data by bulk_writer", pqField.Name))
		}
		if field.GetIsPrimaryKey() && field.GetAutoID() {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the field: %s is primary key, "+
				"and autoID is true, please remove it from file", field.GetName()))
		}
		arrowType, isList := convertArrowSchemaToDataType(pqField, false)
		dataType := field.GetDataType()
		fmt.Println("dyh, debug, aaaaaa, arrowType:", arrowType, ", isList:", isList, ", field:", field)
		if isList { // TODO: dyh, refine error
			if !typeutil.IsVectorType(dataType) && dataType != schemapb.DataType_Array {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("list field schema is not match, "+
					"collection field dataType: %s, file field dataType: %s", dataType.String(), pqField.Type.Name()))
			}
			if dataType == schemapb.DataType_Array {
				dataType = field.GetElementType()
			}
		}
		if !isConvertible(arrowType, dataType, isList) {
			//return nil, merr.WrapErrImportFailed(fmt.Sprintf("field schema is not match, "+
			//	"collection field dataType: %s, file field dataType: %s", dataType.String(), pqField.Type.Name()))
		}
		cr, err := NewColumnReader(fileReader, i, field)
		if err != nil {
			return nil, err
		}
		if _, ok = crs[field.GetFieldID()]; ok {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("there is multi field of fieldName: %s", field.GetName()))
		}
		crs[field.GetFieldID()] = cr
	}
	for _, field := range fields {
		if (field.GetIsPrimaryKey() && field.GetAutoID()) || field.GetIsDynamic() {
			continue
		}
		if _, ok := crs[field.GetFieldID()]; !ok {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("there is no field "+
				"in parquet file of name: %s", field.GetName()))
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
