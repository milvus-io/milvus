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

package util

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/cdc/core/pb"
	"go.uber.org/zap"
)

func GetChannelStartPosition(vchannel string, startPositions []*commonpb.KeyDataPair) (*pb.MsgPosition, error) {
	for _, sp := range startPositions {
		if sp.GetKey() != ToPhysicalChannel(vchannel) {
			continue
		}
		return &pb.MsgPosition{
			ChannelName: vchannel,
			MsgID:       sp.GetData(),
		}, nil
	}
	return nil, errors.New("not found channel start position, channel:" + vchannel)
}

func getNumRowsOfScalarField(datas interface{}) uint64 {
	realTypeDatas := reflect.ValueOf(datas)
	return uint64(realTypeDatas.Len())
}

func getNumRowsOfFloatVectorField(fDatas []float32, dim int64) (uint64, error) {
	if dim <= 0 {
		return 0, fmt.Errorf("dim(%d) should be greater than 0", dim)
	}
	l := len(fDatas)
	if int64(l)%dim != 0 {
		return 0, fmt.Errorf("the length(%d) of float data should divide the dim(%d)", l, dim)
	}
	return uint64(int64(l) / dim), nil
}

func getNumRowsOfBinaryVectorField(bDatas []byte, dim int64) (uint64, error) {
	if dim <= 0 {
		return 0, fmt.Errorf("dim(%d) should be greater than 0", dim)
	}
	if dim%8 != 0 {
		return 0, fmt.Errorf("dim(%d) should divide 8", dim)
	}
	l := len(bDatas)
	if (8*int64(l))%dim != 0 {
		return 0, fmt.Errorf("the num(%d) of all bits should divide the dim(%d)", 8*l, dim)
	}
	return uint64((8 * int64(l)) / dim), nil
}

// GetNumRowOfFieldData return num rows of the field data
func GetNumRowOfFieldData(fieldData *schemapb.FieldData) (uint64, error) {
	var fieldNumRows uint64
	var err error
	switch fieldType := fieldData.Field.(type) {
	case *schemapb.FieldData_Scalars:
		scalarField := fieldData.GetScalars()
		switch scalarType := scalarField.Data.(type) {
		case *schemapb.ScalarField_BoolData:
			fieldNumRows = getNumRowsOfScalarField(scalarField.GetBoolData().Data)
		case *schemapb.ScalarField_IntData:
			fieldNumRows = getNumRowsOfScalarField(scalarField.GetIntData().Data)
		case *schemapb.ScalarField_LongData:
			fieldNumRows = getNumRowsOfScalarField(scalarField.GetLongData().Data)
		case *schemapb.ScalarField_FloatData:
			fieldNumRows = getNumRowsOfScalarField(scalarField.GetFloatData().Data)
		case *schemapb.ScalarField_DoubleData:
			fieldNumRows = getNumRowsOfScalarField(scalarField.GetDoubleData().Data)
		case *schemapb.ScalarField_StringData:
			fieldNumRows = getNumRowsOfScalarField(scalarField.GetStringData().Data)
		default:
			return 0, fmt.Errorf("%s is not supported now", scalarType)
		}
	case *schemapb.FieldData_Vectors:
		vectorField := fieldData.GetVectors()
		switch vectorFieldType := vectorField.Data.(type) {
		case *schemapb.VectorField_FloatVector:
			dim := vectorField.GetDim()
			fieldNumRows, err = getNumRowsOfFloatVectorField(vectorField.GetFloatVector().Data, dim)
			if err != nil {
				return 0, err
			}
		case *schemapb.VectorField_BinaryVector:
			dim := vectorField.GetDim()
			fieldNumRows, err = getNumRowsOfBinaryVectorField(vectorField.GetBinaryVector(), dim)
			if err != nil {
				return 0, err
			}
		default:
			return 0, fmt.Errorf("%s is not supported now", vectorFieldType)
		}
	default:
		return 0, fmt.Errorf("%s is not supported now", fieldType)
	}

	return fieldNumRows, nil
}

// AppendFieldData appends fields data of specified index from src to dst
func AppendFieldData(dst []*schemapb.FieldData, src []*schemapb.FieldData, idx int64) {
	for i, fieldData := range src {
		switch fieldType := fieldData.Field.(type) {
		case *schemapb.FieldData_Scalars:
			if dst[i] == nil || dst[i].GetScalars() == nil {
				dst[i] = &schemapb.FieldData{
					Type:      fieldData.Type,
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
			case *schemapb.ScalarField_StringData:
				if dstScalar.GetStringData() == nil {
					dstScalar.Data = &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{srcScalar.StringData.Data[idx]},
						},
					}
				} else {
					dstScalar.GetStringData().Data = append(dstScalar.GetStringData().Data, srcScalar.StringData.Data[idx])
				}
			default:
				Log.Error("Not supported field type", zap.String("field type", fieldData.Type.String()))
			}
		case *schemapb.FieldData_Vectors:
			dim := fieldType.Vectors.Dim
			if dst[i] == nil || dst[i].GetVectors() == nil {
				dst[i] = &schemapb.FieldData{
					Type:      fieldData.Type,
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
					srcToCopy := srcVector.BinaryVector[idx*(dim/8) : (idx+1)*(dim/8)]
					dstVector.Data = &schemapb.VectorField_BinaryVector{
						BinaryVector: make([]byte, len(srcToCopy)),
					}
					copy(dstVector.Data.(*schemapb.VectorField_BinaryVector).BinaryVector, srcToCopy)
				} else {
					dstBinaryVector := dstVector.Data.(*schemapb.VectorField_BinaryVector)
					dstBinaryVector.BinaryVector = append(dstBinaryVector.BinaryVector, srcVector.BinaryVector[idx*(dim/8):(idx+1)*(dim/8)]...)
				}
			case *schemapb.VectorField_FloatVector:
				if dstVector.GetFloatVector() == nil {
					srcToCopy := srcVector.FloatVector.Data[idx*dim : (idx+1)*dim]
					dstVector.Data = &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: make([]float32, len(srcToCopy)),
						},
					}
					copy(dstVector.Data.(*schemapb.VectorField_FloatVector).FloatVector.Data, srcToCopy)
				} else {
					dstVector.GetFloatVector().Data = append(dstVector.GetFloatVector().Data, srcVector.FloatVector.Data[idx*dim:(idx+1)*dim]...)
				}
			default:
				Log.Error("Not supported field type", zap.String("field type", fieldData.Type.String()))
			}
		}
	}
}

// DeleteFieldData delete fields data appended last time
func DeleteFieldData(dst []*schemapb.FieldData) {
	for i, fieldData := range dst {
		switch fieldType := fieldData.Field.(type) {
		case *schemapb.FieldData_Scalars:
			if dst[i] == nil || dst[i].GetScalars() == nil {
				Log.Info("empty field data can't be deleted")
				return
			}
			dstScalar := dst[i].GetScalars()
			switch fieldType.Scalars.Data.(type) {
			case *schemapb.ScalarField_BoolData:
				dstScalar.GetBoolData().Data = dstScalar.GetBoolData().Data[:len(dstScalar.GetBoolData().Data)-1]
			case *schemapb.ScalarField_IntData:
				dstScalar.GetIntData().Data = dstScalar.GetIntData().Data[:len(dstScalar.GetIntData().Data)-1]
			case *schemapb.ScalarField_LongData:
				dstScalar.GetLongData().Data = dstScalar.GetLongData().Data[:len(dstScalar.GetLongData().Data)-1]
			case *schemapb.ScalarField_FloatData:
				dstScalar.GetFloatData().Data = dstScalar.GetFloatData().Data[:len(dstScalar.GetFloatData().Data)-1]
			case *schemapb.ScalarField_DoubleData:
				dstScalar.GetDoubleData().Data = dstScalar.GetDoubleData().Data[:len(dstScalar.GetDoubleData().Data)-1]
			case *schemapb.ScalarField_StringData:
				dstScalar.GetStringData().Data = dstScalar.GetStringData().Data[:len(dstScalar.GetStringData().Data)-1]
			default:
				Log.Error("wrong field type added", zap.String("field type", fieldData.Type.String()))
			}
		case *schemapb.FieldData_Vectors:
			if dst[i] == nil || dst[i].GetVectors() == nil {
				Log.Info("empty field data can't be deleted")
				return
			}
			dim := fieldType.Vectors.Dim
			dstVector := dst[i].GetVectors()
			switch fieldType.Vectors.Data.(type) {
			case *schemapb.VectorField_BinaryVector:
				dstBinaryVector := dstVector.Data.(*schemapb.VectorField_BinaryVector)
				dstBinaryVector.BinaryVector = dstBinaryVector.BinaryVector[:len(dstBinaryVector.BinaryVector)-int(dim/8)]
			case *schemapb.VectorField_FloatVector:
				dstVector.GetFloatVector().Data = dstVector.GetFloatVector().Data[:len(dstVector.GetFloatVector().Data)-int(dim)]
			default:
				Log.Error("wrong field type added", zap.String("field type", fieldData.Type.String()))
			}
		}
	}
}

func GetSizeOfIDs(data *schemapb.IDs) int {
	result := 0
	if data.IdField == nil {
		return result
	}

	switch data.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		result = len(data.GetIntId().GetData())
	case *schemapb.IDs_StrId:
		result = len(data.GetStrId().GetData())
	default:
		//TODO::
	}

	return result
}
