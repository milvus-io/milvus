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

package storage

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

// DataSorter sorts insert data
type DataSorter struct {
	InsertCodec *InsertCodec
	InsertData  *InsertData
}

// getRowIDFieldData returns auto generated row id Field
func (ds *DataSorter) getRowIDFieldData() FieldData {
	if data, ok := ds.InsertData.Data[common.RowIDField]; ok {
		return data
	}
	return nil
}

// Len returns length of the insert data
func (ds *DataSorter) Len() int {
	idField := ds.getRowIDFieldData()
	if idField == nil {
		return 0
	}
	fieldData, ok := idField.(*Int64FieldData)
	if !ok {
		return 0
	}
	return len(fieldData.Data)
}

// Swap swaps each field's i-th and j-th element
func (ds *DataSorter) Swap(i, j int) {
	for _, field := range ds.InsertCodec.Schema.Schema.Fields {
		singleData, has := ds.InsertData.Data[field.FieldID]
		if !has {
			continue
		}
		switch field.DataType {
		case schemapb.DataType_Bool:
			data := singleData.(*BoolFieldData).Data
			data[i], data[j] = data[j], data[i]
		case schemapb.DataType_Int8:
			data := singleData.(*Int8FieldData).Data
			data[i], data[j] = data[j], data[i]
		case schemapb.DataType_Int16:
			data := singleData.(*Int16FieldData).Data
			data[i], data[j] = data[j], data[i]
		case schemapb.DataType_Int32:
			data := singleData.(*Int32FieldData).Data
			data[i], data[j] = data[j], data[i]
		case schemapb.DataType_Int64:
			data := singleData.(*Int64FieldData).Data
			data[i], data[j] = data[j], data[i]
		case schemapb.DataType_Float:
			data := singleData.(*FloatFieldData).Data
			data[i], data[j] = data[j], data[i]
		case schemapb.DataType_Double:
			data := singleData.(*DoubleFieldData).Data
			data[i], data[j] = data[j], data[i]
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			data := singleData.(*StringFieldData).Data
			data[i], data[j] = data[j], data[i]
		case schemapb.DataType_BinaryVector:
			data := singleData.(*BinaryVectorFieldData).Data
			dim := singleData.(*BinaryVectorFieldData).Dim
			// dim for binary vector must be multiplier of 8, simple verion for swapping:
			steps := dim / 8
			for idx := 0; idx < steps; idx++ {
				data[i*steps+idx], data[j*steps+idx] = data[j*steps+idx], data[i*steps+idx]
			}
		case schemapb.DataType_FloatVector:
			data := singleData.(*FloatVectorFieldData).Data
			dim := singleData.(*FloatVectorFieldData).Dim
			for idx := 0; idx < dim; idx++ {
				data[i*dim+idx], data[j*dim+idx] = data[j*dim+idx], data[i*dim+idx]
			}
		case schemapb.DataType_Float16Vector:
			data := singleData.(*Float16VectorFieldData).Data
			dim := singleData.(*Float16VectorFieldData).Dim
			steps := dim * 2
			for idx := 0; idx < steps; idx++ {
				data[i*steps+idx], data[j*steps+idx] = data[j*steps+idx], data[i*steps+idx]
			}
		case schemapb.DataType_BFloat16Vector:
			data := singleData.(*BFloat16VectorFieldData).Data
			dim := singleData.(*BFloat16VectorFieldData).Dim
			steps := dim * 2
			for idx := 0; idx < steps; idx++ {
				data[i*steps+idx], data[j*steps+idx] = data[j*steps+idx], data[i*steps+idx]
			}
		case schemapb.DataType_Array:
			data := singleData.(*ArrayFieldData).Data
			data[i], data[j] = data[j], data[i]
		case schemapb.DataType_JSON:
			data := singleData.(*JSONFieldData).Data
			data[i], data[j] = data[j], data[i]
		case schemapb.DataType_SparseFloatVector:
			fieldData := singleData.(*SparseFloatVectorFieldData)
			fieldData.Contents[i], fieldData.Contents[j] = fieldData.Contents[j], fieldData.Contents[i]
		default:
			errMsg := "undefined data type " + string(field.DataType)
			panic(errMsg)
		}
	}
}

// Less returns whether i-th entry is less than j-th entry, using ID field comparison result
func (ds *DataSorter) Less(i, j int) bool {
	idField := ds.getRowIDFieldData()
	if idField == nil {
		return true // to skip swap
	}
	data, ok := idField.(*Int64FieldData)
	if !ok || data.Data == nil {
		return true // to skip swap
	}
	l := len(data.Data)
	// i,j range check
	if i < 0 || i >= l || j < 0 || j > l {
		return true // to skip swap
	}
	ids := data.Data
	return ids[i] < ids[j]
}
