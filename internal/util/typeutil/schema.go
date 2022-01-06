// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package typeutil

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"go.uber.org/zap"
)

// EstimateSizePerRecord returns the estimate size of a record in a collection
func EstimateSizePerRecord(schema *schemapb.CollectionSchema) (int, error) {
	res := 0
	for _, fs := range schema.Fields {
		switch fs.DataType {
		case schemapb.DataType_Bool, schemapb.DataType_Int8:
			res++
		case schemapb.DataType_Int16:
			res += 2
		case schemapb.DataType_Int32, schemapb.DataType_Float:
			res += 4
		case schemapb.DataType_Int64, schemapb.DataType_Double:
			res += 8
		case schemapb.DataType_String:
			res += 125 // todo find a better way to estimate string type
		case schemapb.DataType_BinaryVector:
			for _, kv := range fs.TypeParams {
				if kv.Key == "dim" {
					v, err := strconv.Atoi(kv.Value)
					if err != nil {
						return -1, err
					}
					res += v / 8
					break
				}
			}
		case schemapb.DataType_FloatVector:
			for _, kv := range fs.TypeParams {
				if kv.Key == "dim" {
					v, err := strconv.Atoi(kv.Value)
					if err != nil {
						return -1, err
					}
					res += v * 4
					break
				}
			}
		}
	}
	return res, nil
}

// SchemaHelper provides methods to get the schema of fields
type SchemaHelper struct {
	schema           *schemapb.CollectionSchema
	nameOffset       map[string]int
	idOffset         map[int64]int
	primaryKeyOffset int
}

// CreateSchemaHelper returns a new SchemaHelper object
func CreateSchemaHelper(schema *schemapb.CollectionSchema) (*SchemaHelper, error) {
	if schema == nil {
		return nil, errors.New("schema is nil")
	}
	schemaHelper := SchemaHelper{schema: schema, nameOffset: make(map[string]int), idOffset: make(map[int64]int), primaryKeyOffset: -1}
	for offset, field := range schema.Fields {
		if _, ok := schemaHelper.nameOffset[field.Name]; ok {
			return nil, errors.New("duplicated fieldName: " + field.Name)
		}
		if _, ok := schemaHelper.idOffset[field.FieldID]; ok {
			return nil, errors.New("duplicated fieldID: " + strconv.FormatInt(field.FieldID, 10))
		}
		schemaHelper.nameOffset[field.Name] = offset
		schemaHelper.idOffset[field.FieldID] = offset
		if field.IsPrimaryKey {
			if schemaHelper.primaryKeyOffset != -1 {
				return nil, errors.New("primary key is not unique")
			}
			schemaHelper.primaryKeyOffset = offset
		}
	}
	return &schemaHelper, nil
}

// GetPrimaryKeyField returns the schema of the primary key
func (helper *SchemaHelper) GetPrimaryKeyField() (*schemapb.FieldSchema, error) {
	if helper.primaryKeyOffset == -1 {
		return nil, fmt.Errorf("failed to get primary key field: no primary in schema")
	}
	return helper.schema.Fields[helper.primaryKeyOffset], nil
}

// GetFieldFromName is used to find the schema by field name
func (helper *SchemaHelper) GetFieldFromName(fieldName string) (*schemapb.FieldSchema, error) {
	offset, ok := helper.nameOffset[fieldName]
	if !ok {
		return nil, fmt.Errorf("failed to get field schema by name: fieldName(%s) not found", fieldName)
	}
	return helper.schema.Fields[offset], nil
}

// GetFieldFromID returns the schema of specified field
func (helper *SchemaHelper) GetFieldFromID(fieldID int64) (*schemapb.FieldSchema, error) {
	offset, ok := helper.idOffset[fieldID]
	if !ok {
		return nil, fmt.Errorf("fieldID(%d) not found", fieldID)
	}
	return helper.schema.Fields[offset], nil
}

// GetVectorDimFromID returns the dimension of specified field
func (helper *SchemaHelper) GetVectorDimFromID(fieldID int64) (int, error) {
	sch, err := helper.GetFieldFromID(fieldID)
	if err != nil {
		return 0, err
	}
	if !IsVectorType(sch.DataType) {
		return 0, fmt.Errorf("field type = %s not has dim", schemapb.DataType_name[int32(sch.DataType)])
	}
	for _, kv := range sch.TypeParams {
		if kv.Key == "dim" {
			dim, err := strconv.Atoi(kv.Value)
			if err != nil {
				return 0, err
			}
			return dim, nil
		}
	}
	return 0, fmt.Errorf("fieldID(%d) not has dim", fieldID)
}

// IsVectorType returns true if input is a vector type, otherwise false
func IsVectorType(dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector:
		return true
	default:
		return false
	}
}

// IsIntegerType returns true if input is an integer type, otherwise false
func IsIntegerType(dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_Int8, schemapb.DataType_Int16,
		schemapb.DataType_Int32, schemapb.DataType_Int64:
		return true
	default:
		return false
	}
}

// IsFloatingType returns true if input is a floating type, otherwise false
func IsFloatingType(dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_Float, schemapb.DataType_Double:
		return true
	default:
		return false
	}
}

// IsBoolType returns true if input is a bool type, otherwise false
func IsBoolType(dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_Bool:
		return true
	default:
		return false
	}
}

// AppendFieldData appends fields data of specified index from src to dst
func AppendFieldData(dst []*schemapb.FieldData, src []*schemapb.FieldData, idx int64) {
	for i, fieldData := range src {
		switch fieldType := fieldData.Field.(type) {
		case *schemapb.FieldData_Scalars:
			if dst[i] == nil || dst[i].GetScalars() == nil {
				dst[i] = &schemapb.FieldData{
					FieldName: fieldData.FieldName,
					FieldId:   fieldData.FieldId,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{},
					},
				}
			}
			dstScalar := dst[i].GetScalars()
			switch srcScalar := fieldType.Scalars.Data.(type) {
			case *schemapb.ScalarField_BoolData:
				if dstScalar.GetBoolData() == nil {
					dstScalar.Data = &schemapb.ScalarField_BoolData{
						BoolData: &schemapb.BoolArray{
							Data: []bool{srcScalar.BoolData.Data[idx]},
						},
					}
				} else {
					dstScalar.GetBoolData().Data = append(dstScalar.GetBoolData().Data, srcScalar.BoolData.Data[idx])
				}
			case *schemapb.ScalarField_IntData:
				if dstScalar.GetIntData() == nil {
					dstScalar.Data = &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{srcScalar.IntData.Data[idx]},
						},
					}
				} else {
					dstScalar.GetIntData().Data = append(dstScalar.GetIntData().Data, srcScalar.IntData.Data[idx])
				}
			case *schemapb.ScalarField_LongData:
				if dstScalar.GetLongData() == nil {
					dstScalar.Data = &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{srcScalar.LongData.Data[idx]},
						},
					}
				} else {
					dstScalar.GetLongData().Data = append(dstScalar.GetLongData().Data, srcScalar.LongData.Data[idx])
				}
			case *schemapb.ScalarField_FloatData:
				if dstScalar.GetFloatData() == nil {
					dstScalar.Data = &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{
							Data: []float32{srcScalar.FloatData.Data[idx]},
						},
					}
				} else {
					dstScalar.GetFloatData().Data = append(dstScalar.GetFloatData().Data, srcScalar.FloatData.Data[idx])
				}
			case *schemapb.ScalarField_DoubleData:
				if dstScalar.GetDoubleData() == nil {
					dstScalar.Data = &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: []float64{srcScalar.DoubleData.Data[idx]},
						},
					}
				} else {
					dstScalar.GetDoubleData().Data = append(dstScalar.GetDoubleData().Data, srcScalar.DoubleData.Data[idx])
				}
			default:
				log.Error("Not supported field type", zap.String("field type", fieldData.Type.String()))
			}
		case *schemapb.FieldData_Vectors:
			dim := fieldType.Vectors.Dim
			if dst[i] == nil || dst[i].GetVectors() == nil {
				dst[i] = &schemapb.FieldData{
					FieldName: fieldData.FieldName,
					FieldId:   fieldData.FieldId,
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim: dim,
						},
					},
				}
			}
			dstVector := dst[i].GetVectors()
			switch srcVector := fieldType.Vectors.Data.(type) {
			case *schemapb.VectorField_BinaryVector:
				if dstVector.GetBinaryVector() == nil {
					dstVector.Data = &schemapb.VectorField_BinaryVector{
						BinaryVector: srcVector.BinaryVector[idx*(dim/8) : (idx+1)*(dim/8)],
					}
				} else {
					dstBinaryVector := dstVector.Data.(*schemapb.VectorField_BinaryVector)
					dstBinaryVector.BinaryVector = append(dstBinaryVector.BinaryVector, srcVector.BinaryVector[idx*(dim/8):(idx+1)*(dim/8)]...)
				}
			case *schemapb.VectorField_FloatVector:
				if dstVector.GetFloatVector() == nil {
					dstVector.Data = &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: srcVector.FloatVector.Data[idx*dim : (idx+1)*dim],
						},
					}
				} else {
					dstVector.GetFloatVector().Data = append(dstVector.GetFloatVector().Data, srcVector.FloatVector.Data[idx*dim:(idx+1)*dim]...)
				}
			default:
				log.Error("Not supported field type", zap.String("field type", fieldData.Type.String()))
			}
		}
	}
}
