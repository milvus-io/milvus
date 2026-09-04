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
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// DataSorter sorts insert data
type DataSorter struct {
	InsertCodec             *InsertCodec
	InsertData              *InsertData
	AllFields               []*schemapb.FieldSchema
	nullableVectorDataRanks map[int64]*nullableVectorRanks
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

func swapValidData(validData []bool, i, j int) {
	if len(validData) == 0 {
		return
	}
	validData[i], validData[j] = validData[j], validData[i]
}

func swapFixedRows[T any](data []T, i, j, rowWidth int) {
	if i == j || rowWidth == 0 {
		return
	}
	for idx := 0; idx < rowWidth; idx++ {
		data[i*rowWidth+idx], data[j*rowWidth+idx] = data[j*rowWidth+idx], data[i*rowWidth+idx]
	}
}

func moveFixedRow[T any](data []T, from, to, rowWidth int) {
	if from == to || rowWidth == 0 {
		return
	}
	row := make([]T, rowWidth)
	copy(row, data[from*rowWidth:(from+1)*rowWidth])
	if from < to {
		copy(data[from*rowWidth:to*rowWidth], data[(from+1)*rowWidth:(to+1)*rowWidth])
	} else {
		copy(data[(to+1)*rowWidth:(from+1)*rowWidth], data[to*rowWidth:from*rowWidth])
	}
	copy(data[to*rowWidth:(to+1)*rowWidth], row)
}

type nullableVectorRanks struct {
	tree []int
}

func newNullableVectorRanks(validData []bool) *nullableVectorRanks {
	ranks := &nullableVectorRanks{tree: make([]int, len(validData)+1)}
	for i, valid := range validData {
		if valid {
			ranks.add(i, 1)
		}
	}
	return ranks
}

func (r *nullableVectorRanks) add(logicalIdx, delta int) {
	for idx := logicalIdx + 1; idx < len(r.tree); idx += idx & -idx {
		r.tree[idx] += delta
	}
}

func (r *nullableVectorRanks) rankBefore(logicalIdx int) int {
	sum := 0
	for idx := logicalIdx; idx > 0; idx -= idx & -idx {
		sum += r.tree[idx]
	}
	return sum
}

func (r *nullableVectorRanks) rankOfValid(logicalIdx int) int {
	return r.rankBefore(logicalIdx+1) - 1
}

func invalidateNullableVectorMapping(mapping *LogicalToPhysicalMapping) {
	if mapping == nil {
		return
	}
	mapping.validCount = 0
	mapping.l2pMap = nil
}

func (ds *DataSorter) getNullableVectorDataRanks(fieldID int64, validData []bool) *nullableVectorRanks {
	if ds.nullableVectorDataRanks == nil {
		ds.nullableVectorDataRanks = make(map[int64]*nullableVectorRanks)
	}
	if ranks, ok := ds.nullableVectorDataRanks[fieldID]; ok && len(ranks.tree) == len(validData)+1 {
		return ranks
	}
	ranks := newNullableVectorRanks(validData)
	ds.nullableVectorDataRanks[fieldID] = ranks
	return ranks
}

func moveNullableVectorPhysicalIndex(validData []bool, from, to int, ranks *nullableVectorRanks) (int, int) {
	srcPhysical := ranks.rankOfValid(from)
	ranks.add(from, -1)
	ranks.add(to, 1)
	dstPhysical := ranks.rankBefore(to)
	validData[from] = false
	validData[to] = true
	return srcPhysical, dstPhysical
}

func swapCompactNullableFixedRows[T any](data []T, validData []bool, i, j, rowWidth int, mapping *LogicalToPhysicalMapping, ranks *nullableVectorRanks) {
	if i == j {
		return
	}
	invalidateNullableVectorMapping(mapping)

	validI, validJ := validData[i], validData[j]
	switch {
	case validI && validJ:
		swapFixedRows(data, ranks.rankOfValid(i), ranks.rankOfValid(j), rowWidth)
	case validI != validJ:
		fromLogical, toLogical := i, j
		if !validI {
			fromLogical, toLogical = j, i
		}
		srcPhysical, dstPhysical := moveNullableVectorPhysicalIndex(validData, fromLogical, toLogical, ranks)
		moveFixedRow(data, srcPhysical, dstPhysical, rowWidth)
	}
}

func moveSparseRow(contents [][]byte, from, to int) {
	if from == to {
		return
	}
	row := contents[from]
	if from < to {
		copy(contents[from:to], contents[from+1:to+1])
	} else {
		copy(contents[to+1:from+1], contents[to:from])
	}
	contents[to] = row
}

func swapCompactNullableSparseRows(contents [][]byte, validData []bool, i, j int, mapping *LogicalToPhysicalMapping, ranks *nullableVectorRanks) {
	if i == j {
		return
	}
	invalidateNullableVectorMapping(mapping)

	validI, validJ := validData[i], validData[j]
	switch {
	case validI && validJ:
		physicalI, physicalJ := ranks.rankOfValid(i), ranks.rankOfValid(j)
		contents[physicalI], contents[physicalJ] = contents[physicalJ], contents[physicalI]
	case validI != validJ:
		fromLogical, toLogical := i, j
		if !validI {
			fromLogical, toLogical = j, i
		}
		srcPhysical, dstPhysical := moveNullableVectorPhysicalIndex(validData, fromLogical, toLogical, ranks)
		moveSparseRow(contents, srcPhysical, dstPhysical)
	}
}

// Swap swaps each field's i-th and j-th element
func (ds *DataSorter) Swap(i, j int) {
	if ds.AllFields == nil {
		ds.AllFields = typeutil.GetAllFieldSchemas(ds.InsertCodec.Schema.Schema)
	}
	for _, field := range ds.AllFields {
		singleData, has := ds.InsertData.Data[field.FieldID]
		if !has {
			continue
		}
		switch field.DataType {
		case schemapb.DataType_Bool:
			fd := singleData.(*BoolFieldData)
			data := fd.Data
			data[i], data[j] = data[j], data[i]
			swapValidData(fd.ValidData, i, j)
		case schemapb.DataType_Int8:
			fd := singleData.(*Int8FieldData)
			data := fd.Data
			data[i], data[j] = data[j], data[i]
			swapValidData(fd.ValidData, i, j)
		case schemapb.DataType_Int16:
			fd := singleData.(*Int16FieldData)
			data := fd.Data
			data[i], data[j] = data[j], data[i]
			swapValidData(fd.ValidData, i, j)
		case schemapb.DataType_Int32:
			fd := singleData.(*Int32FieldData)
			data := fd.Data
			data[i], data[j] = data[j], data[i]
			swapValidData(fd.ValidData, i, j)
		case schemapb.DataType_Int64:
			fd := singleData.(*Int64FieldData)
			data := fd.Data
			data[i], data[j] = data[j], data[i]
			swapValidData(fd.ValidData, i, j)
		case schemapb.DataType_Float:
			fd := singleData.(*FloatFieldData)
			data := fd.Data
			data[i], data[j] = data[j], data[i]
			swapValidData(fd.ValidData, i, j)
		case schemapb.DataType_Double:
			fd := singleData.(*DoubleFieldData)
			data := fd.Data
			data[i], data[j] = data[j], data[i]
			swapValidData(fd.ValidData, i, j)
		case schemapb.DataType_Timestamptz:
			fd := singleData.(*TimestamptzFieldData)
			data := fd.Data
			data[i], data[j] = data[j], data[i]
			swapValidData(fd.ValidData, i, j)
		case schemapb.DataType_Decimal:
			fd := singleData.(*DecimalFieldData)
			data := fd.Data
			data[i], data[j] = data[j], data[i]
			swapValidData(fd.ValidData, i, j)
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			fd := singleData.(*StringFieldData)
			data := fd.Data
			data[i], data[j] = data[j], data[i]
			swapValidData(fd.ValidData, i, j)
		case schemapb.DataType_BinaryVector:
			fd := singleData.(*BinaryVectorFieldData)
			data := fd.Data
			dim := fd.Dim
			// dim for binary vector must be multiplier of 8, simple verion for swapping:
			steps := dim / 8
			if len(fd.ValidData) > 0 {
				ranks := ds.getNullableVectorDataRanks(field.FieldID, fd.ValidData)
				swapCompactNullableFixedRows(data, fd.ValidData, i, j, steps, &fd.L2PMapping, ranks)
			} else {
				swapFixedRows(data, i, j, steps)
			}
		case schemapb.DataType_FloatVector:
			fd := singleData.(*FloatVectorFieldData)
			data := fd.Data
			dim := fd.Dim
			if len(fd.ValidData) > 0 {
				ranks := ds.getNullableVectorDataRanks(field.FieldID, fd.ValidData)
				swapCompactNullableFixedRows(data, fd.ValidData, i, j, dim, &fd.L2PMapping, ranks)
			} else {
				swapFixedRows(data, i, j, dim)
			}
		case schemapb.DataType_Float16Vector:
			fd := singleData.(*Float16VectorFieldData)
			data := fd.Data
			dim := fd.Dim
			steps := dim * 2
			if len(fd.ValidData) > 0 {
				ranks := ds.getNullableVectorDataRanks(field.FieldID, fd.ValidData)
				swapCompactNullableFixedRows(data, fd.ValidData, i, j, steps, &fd.L2PMapping, ranks)
			} else {
				swapFixedRows(data, i, j, steps)
			}
		case schemapb.DataType_BFloat16Vector:
			fd := singleData.(*BFloat16VectorFieldData)
			data := fd.Data
			dim := fd.Dim
			steps := dim * 2
			if len(fd.ValidData) > 0 {
				ranks := ds.getNullableVectorDataRanks(field.FieldID, fd.ValidData)
				swapCompactNullableFixedRows(data, fd.ValidData, i, j, steps, &fd.L2PMapping, ranks)
			} else {
				swapFixedRows(data, i, j, steps)
			}
		case schemapb.DataType_Array:
			fd := singleData.(*ArrayFieldData)
			data := fd.Data
			data[i], data[j] = data[j], data[i]
			swapValidData(fd.ValidData, i, j)
		case schemapb.DataType_JSON:
			fd := singleData.(*JSONFieldData)
			data := fd.Data
			data[i], data[j] = data[j], data[i]
			swapValidData(fd.ValidData, i, j)
		case schemapb.DataType_Geometry:
			fd := singleData.(*GeometryFieldData)
			data := fd.Data
			data[i], data[j] = data[j], data[i]
			swapValidData(fd.ValidData, i, j)
		case schemapb.DataType_SparseFloatVector:
			fieldData := singleData.(*SparseFloatVectorFieldData)
			if len(fieldData.ValidData) > 0 {
				ranks := ds.getNullableVectorDataRanks(field.FieldID, fieldData.ValidData)
				swapCompactNullableSparseRows(fieldData.Contents, fieldData.ValidData, i, j, &fieldData.L2PMapping, ranks)
			} else {
				fieldData.Contents[i], fieldData.Contents[j] = fieldData.Contents[j], fieldData.Contents[i]
			}
		case schemapb.DataType_Int8Vector:
			fd := singleData.(*Int8VectorFieldData)
			data := fd.Data
			dim := fd.Dim
			if len(fd.ValidData) > 0 {
				ranks := ds.getNullableVectorDataRanks(field.FieldID, fd.ValidData)
				swapCompactNullableFixedRows(data, fd.ValidData, i, j, dim, &fd.L2PMapping, ranks)
			} else {
				swapFixedRows(data, i, j, dim)
			}
		case schemapb.DataType_ArrayOfVector:
			fieldData := singleData.(*VectorArrayFieldData)
			fieldData.Data[i], fieldData.Data[j] = fieldData.Data[j], fieldData.Data[i]
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
