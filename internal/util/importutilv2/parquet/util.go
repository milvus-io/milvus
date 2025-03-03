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

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

	err = isSchemaEqual(schema, pqSchema)
	if err != nil {
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("schema not equal, err=%v", err))
	}

	crs := make(map[int64]*FieldReader)
	for i, pqField := range pqSchema.Fields() {
		field, ok := nameToField[pqField.Name]
		if !ok {
			// TODO @cai.zhang: handle dynamic field
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the field: %s is not in schema, "+
				"if it's a dynamic field, please reformat data by bulk_writer", pqField.Name))
		}
		if typeutil.IsAutoPKField(field) {
			return nil, merr.WrapErrImportFailed(
				fmt.Sprintf("the primary key '%s' is auto-generated, no need to provide", field.GetName()))
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
		if typeutil.IsAutoPKField(field) || field.GetIsDynamic() || field.GetIsFunctionOutput() {
			continue
		}
		if _, ok := crs[field.GetFieldID()]; !ok {
			return nil, merr.WrapErrImportFailed(
				fmt.Sprintf("no parquet field for milvus file '%s'", field.GetName()))
		}
	}
	return crs, nil
}

func isArrowIntegerType(dataType arrow.Type) bool {
	switch dataType {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		return true
	default:
		return false
	}
}

func isArrowFloatingType(dataType arrow.Type) bool {
	switch dataType {
	case arrow.FLOAT32, arrow.FLOAT64:
		return true
	default:
		return false
	}
}

func isArrowArithmeticType(dataType arrow.Type) bool {
	return isArrowIntegerType(dataType) || isArrowFloatingType(dataType)
}

func isArrowDataTypeConvertible(src arrow.DataType, dst arrow.DataType, field *schemapb.FieldSchema) bool {
	srcType := src.ID()
	dstType := dst.ID()
	switch srcType {
	case arrow.BOOL:
		return dstType == arrow.BOOL
	case arrow.UINT8:
		return dstType == arrow.UINT8
	case arrow.INT8:
		return isArrowArithmeticType(dstType)
	case arrow.INT16:
		return isArrowArithmeticType(dstType) && dstType != arrow.INT8
	case arrow.INT32:
		return isArrowArithmeticType(dstType) && dstType != arrow.INT8 && dstType != arrow.INT16
	case arrow.INT64:
		return isArrowFloatingType(dstType) || dstType == arrow.INT64
	case arrow.FLOAT32:
		return isArrowFloatingType(dstType)
	case arrow.FLOAT64:
		// TODO caiyd: need do strict type check
		// return dstType == arrow.FLOAT64
		return isArrowFloatingType(dstType)
	case arrow.STRING:
		return dstType == arrow.STRING
	case arrow.BINARY:
		return dstType == arrow.LIST && dst.(*arrow.ListType).Elem().ID() == arrow.UINT8
	case arrow.LIST:
		return dstType == arrow.LIST && isArrowDataTypeConvertible(src.(*arrow.ListType).Elem(), dst.(*arrow.ListType).Elem(), field)
	case arrow.NULL:
		// if nullable==true or has set default_value, can use null type
		return field.GetNullable() || field.GetDefaultValue() != nil
	default:
		return false
	}
}

func convertToArrowDataType(field *schemapb.FieldSchema, isArray bool) (arrow.DataType, error) {
	dataType := field.GetDataType()
	if isArray {
		dataType = field.GetElementType()
	}
	switch dataType {
	case schemapb.DataType_Bool:
		return &arrow.BooleanType{}, nil
	case schemapb.DataType_Int8:
		return &arrow.Int8Type{}, nil
	case schemapb.DataType_Int16:
		return &arrow.Int16Type{}, nil
	case schemapb.DataType_Int32:
		return &arrow.Int32Type{}, nil
	case schemapb.DataType_Int64:
		return &arrow.Int64Type{}, nil
	case schemapb.DataType_Float:
		return &arrow.Float32Type{}, nil
	case schemapb.DataType_Double:
		return &arrow.Float64Type{}, nil
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		return &arrow.StringType{}, nil
	case schemapb.DataType_JSON:
		return &arrow.StringType{}, nil
	case schemapb.DataType_Array:
		elemType, err := convertToArrowDataType(field, true)
		if err != nil {
			return nil, err
		}
		return arrow.ListOfField(arrow.Field{
			Name:     "item",
			Type:     elemType,
			Nullable: true,
			Metadata: arrow.Metadata{},
		}), nil
	case schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		return arrow.ListOfField(arrow.Field{
			Name:     "item",
			Type:     &arrow.Uint8Type{},
			Nullable: true,
			Metadata: arrow.Metadata{},
		}), nil
	case schemapb.DataType_FloatVector:
		return arrow.ListOfField(arrow.Field{
			Name:     "item",
			Type:     &arrow.Float32Type{},
			Nullable: true,
			Metadata: arrow.Metadata{},
		}), nil
	case schemapb.DataType_SparseFloatVector:
		return &arrow.StringType{}, nil
	case schemapb.DataType_Int8Vector:
		return arrow.ListOfField(arrow.Field{
			Name:     "item",
			Type:     &arrow.Int8Type{},
			Nullable: true,
			Metadata: arrow.Metadata{},
		}), nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported data type %v", dataType.String())
	}
}

// This method is used only by import util and related tests. Returned arrow.Schema
// doesn't include function output fields.
func ConvertToArrowSchema(schema *schemapb.CollectionSchema, useNullType bool) (*arrow.Schema, error) {
	arrFields := make([]arrow.Field, 0)
	for _, field := range schema.GetFields() {
		if typeutil.IsAutoPKField(field) || field.GetIsFunctionOutput() {
			continue
		}
		arrDataType, err := convertToArrowDataType(field, false)
		if err != nil {
			return nil, err
		}
		nullable := field.GetNullable()
		if field.GetNullable() && useNullType {
			arrDataType = arrow.Null
		}
		if field.GetDefaultValue() != nil && useNullType {
			arrDataType = arrow.Null
			nullable = true
		}
		arrFields = append(arrFields, arrow.Field{
			Name:     field.GetName(),
			Type:     arrDataType,
			Nullable: nullable,
			Metadata: arrow.Metadata{},
		})
	}
	return arrow.NewSchema(arrFields, nil), nil
}

func isSchemaEqual(schema *schemapb.CollectionSchema, arrSchema *arrow.Schema) error {
	arrNameToField := lo.KeyBy(arrSchema.Fields(), func(field arrow.Field) string {
		return field.Name
	})
	for _, field := range schema.GetFields() {
		if typeutil.IsAutoPKField(field) || field.GetIsFunctionOutput() {
			continue
		}
		arrField, ok := arrNameToField[field.GetName()]
		if !ok {
			if field.GetIsDynamic() {
				continue
			}
			return merr.WrapErrImportFailed(fmt.Sprintf("field '%s' not in arrow schema", field.GetName()))
		}
		toArrDataType, err := convertToArrowDataType(field, false)
		if err != nil {
			return err
		}
		if !isArrowDataTypeConvertible(arrField.Type, toArrDataType, field) {
			return merr.WrapErrImportFailed(fmt.Sprintf("field '%s' type mis-match, milvus data type '%s', arrow data type get '%s'",
				field.Name, field.DataType.String(), arrField.Type.String()))
		}
	}
	return nil
}

// todo(smellthemoon): use byte to store valid_data
func bytesToValidData(length int, bytes []byte) []bool {
	bools := make([]bool, 0, length)
	if len(bytes) == 0 {
		// parquet field is "optional" or "required"
		// for "required" field, the arrow.array.NullBitmapBytes() returns an empty byte list
		// which means all the elements are valid. In this case, we simply construct an all-true bool array
		for i := 0; i < length; i++ {
			bools = append(bools, true)
		}
		return bools
	}

	// for "optional" field, the arrow.array.NullBitmapBytes() returns a non-empty byte list
	// with each bit representing the existence of an element
	for i := 0; i < length; i++ {
		bit := (bytes[uint(i)/8] & BitMask[byte(i)%8]) != 0
		bools = append(bools, bit)
	}

	return bools
}

var (
	BitMask        = [8]byte{1, 2, 4, 8, 16, 32, 64, 128}
	FlippedBitMask = [8]byte{254, 253, 251, 247, 239, 223, 191, 127}
)
