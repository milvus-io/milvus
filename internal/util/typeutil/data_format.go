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

package typeutil

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"go.uber.org/zap"
)

type rowsHelper = [][]interface{}

func appendScalarField(datas *rowsHelper, rowNum *int, getDataFunc func() interface{}) error {
	fieldDatas := reflect.ValueOf(getDataFunc())
	if *rowNum != 0 && *rowNum != fieldDatas.Len() {
		return errors.New("the row num of different column is not equal")
	}
	*rowNum = fieldDatas.Len()
	*datas = append(*datas, make([]interface{}, 0, *rowNum))
	idx := len(*datas) - 1
	for i := 0; i < *rowNum; i++ {
		(*datas)[idx] = append((*datas)[idx], fieldDatas.Index(i).Interface())
	}

	return nil
}

func appendFloatVectorField(datas *rowsHelper, rowNum *int, fDatas []float32, dim int64) error {
	l := len(fDatas)
	if int64(l)%dim != 0 {
		return errors.New("invalid vectors")
	}
	r := int64(l) / dim
	if *rowNum != 0 && *rowNum != int(r) {
		return errors.New("the row num of different column is not equal")
	}
	*rowNum = int(r)
	*datas = append(*datas, make([]interface{}, 0, *rowNum))
	idx := len(*datas) - 1
	vector := make([]float32, 0, dim)
	for i := 0; i < l; i++ {
		vector = append(vector, fDatas[i])
		if int64(i+1)%dim == 0 {
			(*datas)[idx] = append((*datas)[idx], vector)
			vector = make([]float32, 0, dim)
		}
	}

	return nil
}

func appendBinaryVectorField(datas *rowsHelper, rowNum *int, bDatas []byte, dim int64) error {
	l := len(bDatas)
	if dim%8 != 0 {
		return errors.New("invalid dim")
	}
	if (8*int64(l))%dim != 0 {
		return errors.New("invalid vectors")
	}
	r := (8 * int64(l)) / dim
	if *rowNum != 0 && *rowNum != int(r) {
		return errors.New("the row num of different column is not equal")
	}
	*rowNum = int(r)
	*datas = append(*datas, make([]interface{}, 0, *rowNum))
	idx := len(*datas) - 1
	vector := make([]byte, 0, dim)
	for i := 0; i < l; i++ {
		vector = append(vector, bDatas[i])
		if (8*int64(i+1))%dim == 0 {
			(*datas)[idx] = append((*datas)[idx], vector)
			vector = make([]byte, 0, dim)
		}
	}

	return nil
}

func writeToBuffer(w io.Writer, endian binary.ByteOrder, d interface{}) error {
	return binary.Write(w, endian, d)
}

func TransferColumnBasedDataToRowBasedData(schema *schemapb.CollectionSchema, columns []*schemapb.FieldData) (rows []*commonpb.Blob, err error) {
	dTypes := make([]schemapb.DataType, 0, len(columns))
	datas := make([][]interface{}, 0, len(columns))
	rowNum := 0

	fieldID2FieldData := make(map[int64]schemapb.FieldData)
	for _, field := range columns {
		fieldID2FieldData[field.FieldId] = *field
	}

	// reorder field data by schema field orider
	for _, field := range schema.Fields {
		if field.FieldID == common.RowIDField || field.FieldID == common.TimeStampField {
			continue
		}
		fieldData, ok := fieldID2FieldData[field.FieldID]
		if !ok {
			return nil, fmt.Errorf("field %s data not exist", field.Name)
		}

		switch fieldData.Field.(type) {
		case *schemapb.FieldData_Scalars:
			scalarField := fieldData.GetScalars()
			switch scalarField.Data.(type) {
			case *schemapb.ScalarField_BoolData:
				err := appendScalarField(&datas, &rowNum, func() interface{} {
					return scalarField.GetBoolData().Data
				})
				if err != nil {
					return nil, err
				}
			case *schemapb.ScalarField_IntData:
				err := appendScalarField(&datas, &rowNum, func() interface{} {
					return scalarField.GetIntData().Data
				})
				if err != nil {
					return nil, err
				}
			case *schemapb.ScalarField_LongData:
				err := appendScalarField(&datas, &rowNum, func() interface{} {
					return scalarField.GetLongData().Data
				})
				if err != nil {
					return nil, err
				}
			case *schemapb.ScalarField_FloatData:
				err := appendScalarField(&datas, &rowNum, func() interface{} {
					return scalarField.GetFloatData().Data
				})
				if err != nil {
					return nil, err
				}
			case *schemapb.ScalarField_DoubleData:
				err := appendScalarField(&datas, &rowNum, func() interface{} {
					return scalarField.GetDoubleData().Data
				})
				if err != nil {
					return nil, err
				}
			case *schemapb.ScalarField_BytesData:
				return nil, errors.New("bytes field is not supported now")
			case *schemapb.ScalarField_StringData:
				return nil, errors.New("string field is not supported now")
			case nil:
				continue
			default:
				continue
			}
		case *schemapb.FieldData_Vectors:
			vectorField := fieldData.GetVectors()
			switch vectorField.Data.(type) {
			case *schemapb.VectorField_FloatVector:
				floatVectorFieldData := vectorField.GetFloatVector().Data
				dim := vectorField.GetDim()
				err := appendFloatVectorField(&datas, &rowNum, floatVectorFieldData, dim)
				if err != nil {
					return nil, err
				}
			case *schemapb.VectorField_BinaryVector:
				binaryVectorFieldData := vectorField.GetBinaryVector()
				dim := vectorField.GetDim()
				err := appendBinaryVectorField(&datas, &rowNum, binaryVectorFieldData, dim)
				if err != nil {
					return nil, err
				}
			case nil:
				continue
			default:
				continue
			}
		case nil:
			continue
		default:
			continue
		}

		dTypes = append(dTypes, field.DataType)
	}

	rows = make([]*commonpb.Blob, 0, rowNum)
	l := len(dTypes)
	// TODO(dragondriver): big endian or little endian?
	endian := common.Endian
	for i := 0; i < rowNum; i++ {
		blob := &commonpb.Blob{
			Value: make([]byte, 0),
		}

		for j := 0; j < l; j++ {
			var buffer bytes.Buffer
			var err error
			switch dTypes[j] {
			case schemapb.DataType_Bool:
				d := datas[j][i].(bool)
				err = writeToBuffer(&buffer, endian, d)
			case schemapb.DataType_Int8:
				d := int8(datas[j][i].(int32))
				err = writeToBuffer(&buffer, endian, d)
			case schemapb.DataType_Int16:
				d := int16(datas[j][i].(int32))
				err = writeToBuffer(&buffer, endian, d)
			case schemapb.DataType_Int32:
				d := datas[j][i].(int32)
				err = writeToBuffer(&buffer, endian, d)
			case schemapb.DataType_Int64:
				d := datas[j][i].(int64)
				err = writeToBuffer(&buffer, endian, d)
			case schemapb.DataType_Float:
				d := datas[j][i].(float32)
				err = writeToBuffer(&buffer, endian, d)
			case schemapb.DataType_Double:
				d := datas[j][i].(float64)
				err = writeToBuffer(&buffer, endian, d)
			case schemapb.DataType_FloatVector:
				d := datas[j][i].([]float32)
				err = writeToBuffer(&buffer, endian, d)
			case schemapb.DataType_BinaryVector:
				d := datas[j][i].([]byte)
				err = writeToBuffer(&buffer, endian, d)
			default:
				log.Warn("unsupported data type", zap.String("type", dTypes[j].String()))
			}
			if err != nil {
				log.Error("failed to write to buffer", zap.Error(err))
				return nil, err
			}
			blob.Value = append(blob.Value, buffer.Bytes()...)
		}
		rows = append(rows, blob)
	}

	return rows, nil
}
