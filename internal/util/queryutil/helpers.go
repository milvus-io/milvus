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

package queryutil

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// rowRef references a specific row in a specific result.
type rowRef struct {
	resultIdx int
	rowIdx    int64
}

// comparePK compares two primary keys.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func comparePK(a, b any) int {
	switch va := a.(type) {
	case int64:
		vb := b.(int64)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case string:
		vb := b.(string)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	}
	return 0
}

// buildMergedRetrieveResults builds merged result from selected rows.
func buildMergedRetrieveResults(results []*internalpb.RetrieveResults, selectedRows []rowRef) (*internalpb.RetrieveResults, error) {
	if len(selectedRows) == 0 || len(results) == 0 {
		return &internalpb.RetrieveResults{}, nil
	}

	// Use first result as template
	template := results[selectedRows[0].resultIdx]
	numFields := len(template.GetFieldsData())

	merged := &internalpb.RetrieveResults{
		FieldsData: make([]*schemapb.FieldData, numFields),
	}

	// Build merged IDs
	merged.Ids = buildMergedIDs(results, selectedRows)

	// Build merged field data
	for fieldIdx := 0; fieldIdx < numFields; fieldIdx++ {
		merged.FieldsData[fieldIdx] = buildMergedFieldData(results, selectedRows, fieldIdx)
	}

	return merged, nil
}

// buildMergedIDs builds merged IDs from selected rows.
func buildMergedIDs(results []*internalpb.RetrieveResults, selectedRows []rowRef) *schemapb.IDs {
	if len(selectedRows) == 0 {
		return nil
	}

	firstIDs := results[selectedRows[0].resultIdx].GetIds()
	if firstIDs == nil {
		return nil
	}

	switch firstIDs.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		ids := make([]int64, len(selectedRows))
		for i, ref := range selectedRows {
			ids[i] = results[ref.resultIdx].GetIds().GetIntId().GetData()[ref.rowIdx]
		}
		return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}}

	case *schemapb.IDs_StrId:
		ids := make([]string, len(selectedRows))
		for i, ref := range selectedRows {
			ids[i] = results[ref.resultIdx].GetIds().GetStrId().GetData()[ref.rowIdx]
		}
		return &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: ids}}}
	}

	return nil
}

// buildMergedFieldData builds merged field data from selected rows.
func buildMergedFieldData(results []*internalpb.RetrieveResults, selectedRows []rowRef, fieldIdx int) *schemapb.FieldData {
	template := results[selectedRows[0].resultIdx].GetFieldsData()[fieldIdx]

	newFd := &schemapb.FieldData{
		Type:      template.GetType(),
		FieldName: template.GetFieldName(),
		FieldId:   template.GetFieldId(),
		IsDynamic: template.GetIsDynamic(),
	}

	switch template.GetField().(type) {
	case *schemapb.FieldData_Scalars:
		newFd.Field = &schemapb.FieldData_Scalars{
			Scalars: buildMergedScalarField(results, selectedRows, fieldIdx),
		}
	case *schemapb.FieldData_Vectors:
		newFd.Field = &schemapb.FieldData_Vectors{
			Vectors: buildMergedVectorField(results, selectedRows, fieldIdx),
		}
	}

	return newFd
}

// buildMergedScalarField builds merged scalar field from selected rows.
func buildMergedScalarField(results []*internalpb.RetrieveResults, selectedRows []rowRef, fieldIdx int) *schemapb.ScalarField {
	template := results[selectedRows[0].resultIdx].GetFieldsData()[fieldIdx].GetScalars()
	newSf := &schemapb.ScalarField{}

	switch template.GetData().(type) {
	case *schemapb.ScalarField_BoolData:
		data := make([]bool, len(selectedRows))
		for i, ref := range selectedRows {
			data[i] = results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetBoolData().GetData()[ref.rowIdx]
		}
		newSf.Data = &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: data}}

	case *schemapb.ScalarField_IntData:
		data := make([]int32, len(selectedRows))
		for i, ref := range selectedRows {
			data[i] = results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetIntData().GetData()[ref.rowIdx]
		}
		newSf.Data = &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: data}}

	case *schemapb.ScalarField_LongData:
		data := make([]int64, len(selectedRows))
		for i, ref := range selectedRows {
			data[i] = results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetLongData().GetData()[ref.rowIdx]
		}
		newSf.Data = &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}}

	case *schemapb.ScalarField_FloatData:
		data := make([]float32, len(selectedRows))
		for i, ref := range selectedRows {
			data[i] = results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetFloatData().GetData()[ref.rowIdx]
		}
		newSf.Data = &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: data}}

	case *schemapb.ScalarField_DoubleData:
		data := make([]float64, len(selectedRows))
		for i, ref := range selectedRows {
			data[i] = results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetDoubleData().GetData()[ref.rowIdx]
		}
		newSf.Data = &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: data}}

	case *schemapb.ScalarField_StringData:
		data := make([]string, len(selectedRows))
		for i, ref := range selectedRows {
			data[i] = results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetStringData().GetData()[ref.rowIdx]
		}
		newSf.Data = &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: data}}

	case *schemapb.ScalarField_BytesData:
		data := make([][]byte, len(selectedRows))
		for i, ref := range selectedRows {
			data[i] = results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetBytesData().GetData()[ref.rowIdx]
		}
		newSf.Data = &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{Data: data}}

	case *schemapb.ScalarField_JsonData:
		data := make([][]byte, len(selectedRows))
		for i, ref := range selectedRows {
			data[i] = results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetJsonData().GetData()[ref.rowIdx]
		}
		newSf.Data = &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: data}}

	case *schemapb.ScalarField_ArrayData:
		data := make([]*schemapb.ScalarField, len(selectedRows))
		for i, ref := range selectedRows {
			data[i] = results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetArrayData().GetData()[ref.rowIdx]
		}
		newSf.Data = &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: data}}
	}

	return newSf
}

// buildMergedVectorField builds merged vector field from selected rows.
func buildMergedVectorField(results []*internalpb.RetrieveResults, selectedRows []rowRef, fieldIdx int) *schemapb.VectorField {
	template := results[selectedRows[0].resultIdx].GetFieldsData()[fieldIdx].GetVectors()
	dim := int(template.GetDim())

	newVf := &schemapb.VectorField{Dim: template.GetDim()}

	switch template.GetData().(type) {
	case *schemapb.VectorField_FloatVector:
		data := make([]float32, 0, len(selectedRows)*dim)
		for _, ref := range selectedRows {
			vec := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetVectors().GetFloatVector().GetData()
			start := int(ref.rowIdx) * dim
			data = append(data, vec[start:start+dim]...)
		}
		newVf.Data = &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: data}}

	case *schemapb.VectorField_BinaryVector:
		bytesPerRow := dim / 8
		data := make([]byte, 0, len(selectedRows)*bytesPerRow)
		for _, ref := range selectedRows {
			vec := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetVectors().GetBinaryVector()
			start := int(ref.rowIdx) * bytesPerRow
			data = append(data, vec[start:start+bytesPerRow]...)
		}
		newVf.Data = &schemapb.VectorField_BinaryVector{BinaryVector: data}

	case *schemapb.VectorField_Float16Vector:
		bytesPerRow := dim * 2
		data := make([]byte, 0, len(selectedRows)*bytesPerRow)
		for _, ref := range selectedRows {
			vec := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetVectors().GetFloat16Vector()
			start := int(ref.rowIdx) * bytesPerRow
			data = append(data, vec[start:start+bytesPerRow]...)
		}
		newVf.Data = &schemapb.VectorField_Float16Vector{Float16Vector: data}

	case *schemapb.VectorField_Bfloat16Vector:
		bytesPerRow := dim * 2
		data := make([]byte, 0, len(selectedRows)*bytesPerRow)
		for _, ref := range selectedRows {
			vec := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetVectors().GetBfloat16Vector()
			start := int(ref.rowIdx) * bytesPerRow
			data = append(data, vec[start:start+bytesPerRow]...)
		}
		newVf.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: data}

	case *schemapb.VectorField_SparseFloatVector:
		contents := make([][]byte, len(selectedRows))
		for i, ref := range selectedRows {
			contents[i] = results[ref.resultIdx].GetFieldsData()[fieldIdx].GetVectors().GetSparseFloatVector().GetContents()[ref.rowIdx]
		}
		newVf.Data = &schemapb.VectorField_SparseFloatVector{
			SparseFloatVector: &schemapb.SparseFloatArray{
				Contents: contents,
				Dim:      template.GetSparseFloatVector().GetDim(),
			},
		}
	}

	return newVf
}

// rangeSliceRetrieveResults extracts a contiguous range [start, end) from a RetrieveResult.
// This is more efficient than sliceRetrieveResults for contiguous ranges since it uses
// direct sub-slicing instead of element-by-element copying.
func rangeSliceRetrieveResults(result *internalpb.RetrieveResults, start, end int) *internalpb.RetrieveResults {
	if start >= end {
		return &internalpb.RetrieveResults{}
	}

	newResult := &internalpb.RetrieveResults{
		Ids:        rangeSliceIDs(result.GetIds(), start, end),
		FieldsData: make([]*schemapb.FieldData, len(result.GetFieldsData())),
	}

	for i, fd := range result.GetFieldsData() {
		newResult.FieldsData[i] = rangeSliceFieldData(fd, start, end)
	}

	return newResult
}

// rangeSliceIDs extracts a contiguous range [start, end) from IDs.
func rangeSliceIDs(ids *schemapb.IDs, start, end int) *schemapb.IDs {
	if ids == nil {
		return nil
	}

	switch ids.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		data := ids.GetIntId().GetData()
		return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: data[start:end]}}}
	case *schemapb.IDs_StrId:
		data := ids.GetStrId().GetData()
		return &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: data[start:end]}}}
	}

	return nil
}

// rangeSliceFieldData extracts a contiguous range [start, end) from field data.
func rangeSliceFieldData(fd *schemapb.FieldData, start, end int) *schemapb.FieldData {
	if fd == nil {
		return nil
	}

	newFd := &schemapb.FieldData{
		Type:      fd.GetType(),
		FieldName: fd.GetFieldName(),
		FieldId:   fd.GetFieldId(),
		IsDynamic: fd.GetIsDynamic(),
	}

	switch fd.GetField().(type) {
	case *schemapb.FieldData_Scalars:
		newFd.Field = &schemapb.FieldData_Scalars{
			Scalars: rangeSliceScalarField(fd.GetScalars(), start, end),
		}
	case *schemapb.FieldData_Vectors:
		newFd.Field = &schemapb.FieldData_Vectors{
			Vectors: rangeSliceVectorField(fd.GetVectors(), start, end),
		}
	}

	if len(fd.GetValidData()) > 0 {
		newFd.ValidData = fd.GetValidData()[start:end]
	}

	return newFd
}

// rangeSliceScalarField extracts a contiguous range [start, end) from scalar data.
func rangeSliceScalarField(sf *schemapb.ScalarField, start, end int) *schemapb.ScalarField {
	newSf := &schemapb.ScalarField{}

	switch sf.GetData().(type) {
	case *schemapb.ScalarField_BoolData:
		newSf.Data = &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: sf.GetBoolData().GetData()[start:end]}}
	case *schemapb.ScalarField_IntData:
		newSf.Data = &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: sf.GetIntData().GetData()[start:end]}}
	case *schemapb.ScalarField_LongData:
		newSf.Data = &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: sf.GetLongData().GetData()[start:end]}}
	case *schemapb.ScalarField_FloatData:
		newSf.Data = &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: sf.GetFloatData().GetData()[start:end]}}
	case *schemapb.ScalarField_DoubleData:
		newSf.Data = &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: sf.GetDoubleData().GetData()[start:end]}}
	case *schemapb.ScalarField_StringData:
		newSf.Data = &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: sf.GetStringData().GetData()[start:end]}}
	case *schemapb.ScalarField_BytesData:
		newSf.Data = &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{Data: sf.GetBytesData().GetData()[start:end]}}
	case *schemapb.ScalarField_JsonData:
		newSf.Data = &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: sf.GetJsonData().GetData()[start:end]}}
	case *schemapb.ScalarField_ArrayData:
		newSf.Data = &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: sf.GetArrayData().GetData()[start:end]}}
	}

	return newSf
}

// rangeSliceVectorField extracts a contiguous range [start, end) from vector data.
func rangeSliceVectorField(vf *schemapb.VectorField, start, end int) *schemapb.VectorField {
	dim := int(vf.GetDim())
	newVf := &schemapb.VectorField{Dim: vf.GetDim()}

	switch vf.GetData().(type) {
	case *schemapb.VectorField_FloatVector:
		data := vf.GetFloatVector().GetData()
		newVf.Data = &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: data[start*dim : end*dim]}}
	case *schemapb.VectorField_BinaryVector:
		bytesPerRow := dim / 8
		data := vf.GetBinaryVector()
		newVf.Data = &schemapb.VectorField_BinaryVector{BinaryVector: data[start*bytesPerRow : end*bytesPerRow]}
	case *schemapb.VectorField_Float16Vector:
		bytesPerRow := dim * 2
		data := vf.GetFloat16Vector()
		newVf.Data = &schemapb.VectorField_Float16Vector{Float16Vector: data[start*bytesPerRow : end*bytesPerRow]}
	case *schemapb.VectorField_Bfloat16Vector:
		bytesPerRow := dim * 2
		data := vf.GetBfloat16Vector()
		newVf.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: data[start*bytesPerRow : end*bytesPerRow]}
	case *schemapb.VectorField_SparseFloatVector:
		contents := vf.GetSparseFloatVector().GetContents()
		newVf.Data = &schemapb.VectorField_SparseFloatVector{
			SparseFloatVector: &schemapb.SparseFloatArray{
				Contents: contents[start:end],
				Dim:      vf.GetSparseFloatVector().GetDim(),
			},
		}
	}

	return newVf
}

// sliceIDs extracts IDs at the given indices.
func sliceIDs(ids *schemapb.IDs, indices []int) *schemapb.IDs {
	if ids == nil || len(indices) == 0 {
		return nil
	}

	switch ids.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		data := ids.GetIntId().GetData()
		newData := make([]int64, len(indices))
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: newData}}}

	case *schemapb.IDs_StrId:
		data := ids.GetStrId().GetData()
		newData := make([]string, len(indices))
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		return &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: newData}}}
	}

	return nil
}

// sliceFieldData extracts field data at the given indices.
func sliceFieldData(fd *schemapb.FieldData, indices []int) *schemapb.FieldData {
	if fd == nil || len(indices) == 0 {
		return nil
	}

	newFd := &schemapb.FieldData{
		Type:      fd.GetType(),
		FieldName: fd.GetFieldName(),
		FieldId:   fd.GetFieldId(),
		IsDynamic: fd.GetIsDynamic(),
	}

	switch fd.GetField().(type) {
	case *schemapb.FieldData_Scalars:
		newFd.Field = &schemapb.FieldData_Scalars{
			Scalars: sliceScalarField(fd.GetScalars(), indices),
		}
	case *schemapb.FieldData_Vectors:
		newFd.Field = &schemapb.FieldData_Vectors{
			Vectors: sliceVectorField(fd.GetVectors(), indices),
		}
	}

	return newFd
}

// sliceScalarField extracts scalar data at the given indices.
func sliceScalarField(sf *schemapb.ScalarField, indices []int) *schemapb.ScalarField {
	newSf := &schemapb.ScalarField{}

	switch sf.GetData().(type) {
	case *schemapb.ScalarField_BoolData:
		data := sf.GetBoolData().GetData()
		newData := make([]bool, len(indices))
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		newSf.Data = &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: newData}}

	case *schemapb.ScalarField_IntData:
		data := sf.GetIntData().GetData()
		newData := make([]int32, len(indices))
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		newSf.Data = &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: newData}}

	case *schemapb.ScalarField_LongData:
		data := sf.GetLongData().GetData()
		newData := make([]int64, len(indices))
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		newSf.Data = &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: newData}}

	case *schemapb.ScalarField_FloatData:
		data := sf.GetFloatData().GetData()
		newData := make([]float32, len(indices))
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		newSf.Data = &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: newData}}

	case *schemapb.ScalarField_DoubleData:
		data := sf.GetDoubleData().GetData()
		newData := make([]float64, len(indices))
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		newSf.Data = &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: newData}}

	case *schemapb.ScalarField_StringData:
		data := sf.GetStringData().GetData()
		newData := make([]string, len(indices))
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		newSf.Data = &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: newData}}

	case *schemapb.ScalarField_BytesData:
		data := sf.GetBytesData().GetData()
		newData := make([][]byte, len(indices))
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		newSf.Data = &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{Data: newData}}

	case *schemapb.ScalarField_JsonData:
		data := sf.GetJsonData().GetData()
		newData := make([][]byte, len(indices))
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		newSf.Data = &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: newData}}

	case *schemapb.ScalarField_ArrayData:
		data := sf.GetArrayData().GetData()
		newData := make([]*schemapb.ScalarField, len(indices))
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		newSf.Data = &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: newData}}
	}

	return newSf
}

// sliceVectorField extracts vector data at the given indices.
func sliceVectorField(vf *schemapb.VectorField, indices []int) *schemapb.VectorField {
	dim := int(vf.GetDim())
	newVf := &schemapb.VectorField{Dim: vf.GetDim()}

	switch vf.GetData().(type) {
	case *schemapb.VectorField_FloatVector:
		data := vf.GetFloatVector().GetData()
		newData := make([]float32, len(indices)*dim)
		for i, idx := range indices {
			copy(newData[i*dim:(i+1)*dim], data[idx*dim:(idx+1)*dim])
		}
		newVf.Data = &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: newData}}

	case *schemapb.VectorField_BinaryVector:
		bytesPerRow := dim / 8
		data := vf.GetBinaryVector()
		newData := make([]byte, len(indices)*bytesPerRow)
		for i, idx := range indices {
			copy(newData[i*bytesPerRow:(i+1)*bytesPerRow], data[idx*bytesPerRow:(idx+1)*bytesPerRow])
		}
		newVf.Data = &schemapb.VectorField_BinaryVector{BinaryVector: newData}

	case *schemapb.VectorField_Float16Vector:
		bytesPerRow := dim * 2
		data := vf.GetFloat16Vector()
		newData := make([]byte, len(indices)*bytesPerRow)
		for i, idx := range indices {
			copy(newData[i*bytesPerRow:(i+1)*bytesPerRow], data[idx*bytesPerRow:(idx+1)*bytesPerRow])
		}
		newVf.Data = &schemapb.VectorField_Float16Vector{Float16Vector: newData}

	case *schemapb.VectorField_Bfloat16Vector:
		bytesPerRow := dim * 2
		data := vf.GetBfloat16Vector()
		newData := make([]byte, len(indices)*bytesPerRow)
		for i, idx := range indices {
			copy(newData[i*bytesPerRow:(i+1)*bytesPerRow], data[idx*bytesPerRow:(idx+1)*bytesPerRow])
		}
		newVf.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: newData}

	case *schemapb.VectorField_SparseFloatVector:
		contents := vf.GetSparseFloatVector().GetContents()
		newContents := make([][]byte, len(indices))
		for i, idx := range indices {
			newContents[i] = contents[idx]
		}
		newVf.Data = &schemapb.VectorField_SparseFloatVector{
			SparseFloatVector: &schemapb.SparseFloatArray{
				Contents: newContents,
				Dim:      vf.GetSparseFloatVector().GetDim(),
			},
		}
	}

	return newVf
}

// getFieldValue extracts the value at rowIdx from field data
// Returns (value, isNull)
func getFieldValue(fd *schemapb.FieldData, rowIdx int) (any, bool) {
	// Check valid_data for nullable fields
	validData := fd.GetValidData()
	if len(validData) > rowIdx && !validData[rowIdx] {
		return nil, true
	}

	scalars := fd.GetScalars()
	if scalars == nil {
		return nil, true
	}

	switch scalars.GetData().(type) {
	case *schemapb.ScalarField_BoolData:
		data := scalars.GetBoolData().GetData()
		if rowIdx >= len(data) {
			return nil, true
		}
		return data[rowIdx], false

	case *schemapb.ScalarField_IntData:
		data := scalars.GetIntData().GetData()
		if rowIdx >= len(data) {
			return nil, true
		}
		return data[rowIdx], false

	case *schemapb.ScalarField_LongData:
		data := scalars.GetLongData().GetData()
		if rowIdx >= len(data) {
			return nil, true
		}
		return data[rowIdx], false

	case *schemapb.ScalarField_FloatData:
		data := scalars.GetFloatData().GetData()
		if rowIdx >= len(data) {
			return nil, true
		}
		return data[rowIdx], false

	case *schemapb.ScalarField_DoubleData:
		data := scalars.GetDoubleData().GetData()
		if rowIdx >= len(data) {
			return nil, true
		}
		return data[rowIdx], false

	case *schemapb.ScalarField_StringData:
		data := scalars.GetStringData().GetData()
		if rowIdx >= len(data) {
			return nil, true
		}
		return data[rowIdx], false
	}

	return nil, true
}

// getRowCount returns the number of rows in the result.
func getRowCount(result *internalpb.RetrieveResults) int {
	// Try to get count from IDs first (most reliable)
	if result.GetIds() != nil {
		return typeutil.GetSizeOfIDs(result.GetIds())
	}

	// Fall back to field data
	if len(result.GetFieldsData()) == 0 {
		return 0
	}

	fd := result.GetFieldsData()[0]
	if fd.GetScalars() != nil {
		switch data := fd.GetScalars().GetData().(type) {
		case *schemapb.ScalarField_BoolData:
			return len(data.BoolData.GetData())
		case *schemapb.ScalarField_IntData:
			return len(data.IntData.GetData())
		case *schemapb.ScalarField_LongData:
			return len(data.LongData.GetData())
		case *schemapb.ScalarField_FloatData:
			return len(data.FloatData.GetData())
		case *schemapb.ScalarField_DoubleData:
			return len(data.DoubleData.GetData())
		case *schemapb.ScalarField_StringData:
			return len(data.StringData.GetData())
		case *schemapb.ScalarField_JsonData:
			return len(data.JsonData.GetData())
		case *schemapb.ScalarField_ArrayData:
			return len(data.ArrayData.GetData())
		}
	}
	if fd.GetVectors() != nil {
		dim := int(fd.GetVectors().GetDim())
		if dim == 0 {
			return 0
		}
		switch data := fd.GetVectors().GetData().(type) {
		case *schemapb.VectorField_FloatVector:
			return len(data.FloatVector.GetData()) / dim
		case *schemapb.VectorField_BinaryVector:
			return len(data.BinaryVector) / (dim / 8)
		case *schemapb.VectorField_Float16Vector:
			return len(data.Float16Vector) / (dim * 2)
		case *schemapb.VectorField_Bfloat16Vector:
			return len(data.Bfloat16Vector) / (dim * 2)
		case *schemapb.VectorField_SparseFloatVector:
			return len(data.SparseFloatVector.GetContents())
		}
	}
	return 0
}

// compareValues compares two non-null values of the given data type
// Returns -1 if a < b, 0 if equal, 1 if a > b
func compareValues(a, b any, dataType schemapb.DataType) int {
	switch dataType {
	case schemapb.DataType_Bool:
		va := a.(bool)
		vb := b.(bool)
		if !va && vb {
			return -1
		}
		if va && !vb {
			return 1
		}
		return 0

	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		va := a.(int32)
		vb := b.(int32)
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0

	case schemapb.DataType_Int64:
		va := a.(int64)
		vb := b.(int64)
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0

	case schemapb.DataType_Float:
		va := a.(float32)
		vb := b.(float32)
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0

	case schemapb.DataType_Double:
		va := a.(float64)
		vb := b.(float64)
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0

	case schemapb.DataType_String, schemapb.DataType_VarChar:
		va := a.(string)
		vb := b.(string)
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0
	}

	return 0
}
