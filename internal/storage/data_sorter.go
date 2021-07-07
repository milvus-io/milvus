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

package storage

import (
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/rootcoord"
)

type DataSorter struct {
	InsertCodec *InsertCodec
	InsertData  *InsertData
}

func (ds *DataSorter) getRowIDFieldData() FieldData {
	if data, ok := ds.InsertData.Data[rootcoord.RowIDField]; ok {
		return data
	}
	return nil
}

func (ds *DataSorter) Len() int {
	return len(ds.getRowIDFieldData().(*Int64FieldData).Data)
}

func (ds *DataSorter) Swap(i, j int) {
	for _, field := range ds.InsertCodec.Schema.Schema.Fields {
		singleData := ds.InsertData.Data[field.FieldID]
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
		case schemapb.DataType_String:
			data := singleData.(*StringFieldData).Data
			data[i], data[j] = data[j], data[i]
		case schemapb.DataType_BinaryVector:
			data := singleData.(*BinaryVectorFieldData).Data
			dim := singleData.(*BinaryVectorFieldData).Dim
			for i := 0; i < dim/8; i++ {
				data[i], data[i+dim/8] = data[i+dim/8], data[i]
			}
		case schemapb.DataType_FloatVector:
			data := singleData.(*FloatVectorFieldData).Data
			dim := singleData.(*FloatVectorFieldData).Dim
			for i := 0; i < dim; i++ {
				data[i], data[i+dim] = data[i+dim], data[i]
			}
		default:
			errMsg := "undefined data type " + string(field.DataType)
			panic(errMsg)
		}
	}
}

func (ds *DataSorter) Less(i, j int) bool {
	ids := ds.getRowIDFieldData().(*Int64FieldData).Data
	return ids[i] < ids[j]
}
