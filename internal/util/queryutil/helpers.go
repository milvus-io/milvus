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
	"fmt"

	"google.golang.org/protobuf/proto"

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
// schema provides field metadata (DataType, dim, nullable) used to drive
// the merge logic. Pass nil to fall back to template-based inference
// (backward compatible for tests and callers without schema).
func buildMergedRetrieveResults(results []*internalpb.RetrieveResults, selectedRows []rowRef, schema *schemapb.CollectionSchema) (*internalpb.RetrieveResults, error) {
	if len(selectedRows) == 0 || len(results) == 0 {
		return &internalpb.RetrieveResults{}, nil
	}

	// Use first result as template for field count / metadata
	template := results[selectedRows[0].resultIdx]
	numFields := len(template.GetFieldsData())

	// Validate all referenced results have the same number of fields.
	for _, ref := range selectedRows {
		refFields := len(results[ref.resultIdx].GetFieldsData())
		if refFields != numFields {
			return nil, fmt.Errorf(
				"FieldsData count mismatch: result[%d] has %d fields, expected %d",
				ref.resultIdx, refFields, numFields)
		}
	}

	// Validate element-level consistency across all results referenced by selectedRows.
	if err := validateElementLevelConsistency(results, selectedRows); err != nil {
		return nil, err
	}

	// Build field schema lookup map (fieldID → *FieldSchema).
	var fieldSchemaMap map[int64]*schemapb.FieldSchema
	if schema != nil {
		fieldSchemaMap = make(map[int64]*schemapb.FieldSchema, len(schema.GetFields()))
		for _, f := range schema.GetFields() {
			fieldSchemaMap[f.GetFieldID()] = f
		}
	}

	merged := &internalpb.RetrieveResults{
		FieldsData: make([]*schemapb.FieldData, numFields),
	}

	// Build merged IDs
	merged.Ids = buildMergedIDs(results, selectedRows)

	// Build merged field data
	for fieldIdx := 0; fieldIdx < numFields; fieldIdx++ {
		fieldID := template.GetFieldsData()[fieldIdx].GetFieldId()
		var fs *schemapb.FieldSchema
		if fieldSchemaMap != nil {
			fs = fieldSchemaMap[fieldID] // nil for system fields (RowID=0, Timestamp=1)
		}
		fd, err := buildMergedFieldData(results, selectedRows, fieldIdx, fs)
		if err != nil {
			return nil, err
		}
		merged.FieldsData[fieldIdx] = fd
	}

	// Propagate element-level metadata
	merged.ElementLevel = template.GetElementLevel()
	if merged.ElementLevel {
		merged.ElementIndices = buildMergedElementIndices(results, selectedRows)
	}

	return merged, nil
}

// buildMergedElementIndices extracts ElementIndices for selectedRows.
func buildMergedElementIndices(results []*internalpb.RetrieveResults, selectedRows []rowRef) []*internalpb.ElementIndices {
	indices := make([]*internalpb.ElementIndices, len(selectedRows))
	for i, ref := range selectedRows {
		elemIndices := results[ref.resultIdx].GetElementIndices()
		if int(ref.rowIdx) < len(elemIndices) {
			indices[i] = elemIndices[ref.rowIdx]
		}
	}
	return indices
}

// validateElementLevelConsistency checks that all results referenced by
// selectedRows have a consistent ElementLevel flag, and that element-level
// results have ElementIndices length matching their IDs length.
func validateElementLevelConsistency(results []*internalpb.RetrieveResults, _ []rowRef) error {
	if len(results) == 0 {
		return nil
	}
	isElementLevel := results[0].GetElementLevel()

	for i, r := range results {
		if r.GetElementLevel() != isElementLevel {
			return fmt.Errorf(
				"inconsistent element-level flag: result[%d] has ElementLevel=%v, expected %v",
				i, r.GetElementLevel(), isElementLevel)
		}
		if isElementLevel {
			idsLen := typeutil.GetSizeOfIDs(r.GetIds())
			indicesLen := len(r.GetElementIndices())
			if indicesLen != idsLen {
				return fmt.Errorf(
					"element_indices length (%d) does not match ids length (%d) in result[%d]",
					indicesLen, idsLen, i)
			}
		}
	}
	return nil
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
// fieldSchema provides DataType, dim, and nullable from the collection schema.
// It may be nil for system fields (RowID, Timestamp) or in tests; in that case
// the function falls back to template-based inference for backward compatibility.
func buildMergedFieldData(results []*internalpb.RetrieveResults, selectedRows []rowRef, fieldIdx int, fieldSchema *schemapb.FieldSchema) (*schemapb.FieldData, error) {
	template := results[selectedRows[0].resultIdx].GetFieldsData()[fieldIdx]
	isNullable := fieldSchema != nil && fieldSchema.GetNullable()

	newFd := &schemapb.FieldData{
		Type:      template.GetType(),
		FieldName: template.GetFieldName(),
		FieldId:   template.GetFieldId(),
		IsDynamic: template.GetIsDynamic(),
	}

	// Determine whether this field is a vector using schema (preferred) or template (fallback).
	isVector := false
	if fieldSchema != nil {
		isVector = typeutil.IsVectorType(fieldSchema.GetDataType())
	} else {
		_, isVector = template.GetField().(*schemapb.FieldData_Vectors)
	}

	if isVector {
		vecField, err := buildMergedVectorField(results, selectedRows, fieldIdx, fieldSchema)
		if err != nil {
			return nil, err
		}
		newFd.Field = &schemapb.FieldData_Vectors{
			Vectors: vecField,
		}
	} else {
		newFd.Field = &schemapb.FieldData_Scalars{
			Scalars: buildMergedScalarField(results, selectedRows, fieldIdx),
		}
	}

	// Preserve ValidData (nullable bitmap) for nullable fields.
	if isNullable {
		validData := make([]bool, len(selectedRows))
		for i, ref := range selectedRows {
			vd := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetValidData()
			if len(vd) > 0 && int(ref.rowIdx) < len(vd) {
				validData[i] = vd[ref.rowIdx]
			}
			// ValidData absent or rowIdx out of bounds: keep false (null semantics)
		}
		newFd.ValidData = validData
	}

	return newFd, nil
}

// buildMergedScalarField builds merged scalar field from selected rows.
// Bounds-checks each row access: nullable fields with all-null results may have an empty
// Data array (segcore omits the storage); out-of-bounds rows keep the Go zero value.
// The corresponding ValidData entry will be false, so users never see zero-filled nulls.
func buildMergedScalarField(results []*internalpb.RetrieveResults, selectedRows []rowRef, fieldIdx int) *schemapb.ScalarField {
	template := results[selectedRows[0].resultIdx].GetFieldsData()[fieldIdx].GetScalars()
	newSf := &schemapb.ScalarField{}

	switch template.GetData().(type) {
	case *schemapb.ScalarField_BoolData:
		data := make([]bool, len(selectedRows))
		for i, ref := range selectedRows {
			if src := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetBoolData().GetData(); int(ref.rowIdx) < len(src) {
				data[i] = src[ref.rowIdx]
			}
		}
		newSf.Data = &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: data}}

	case *schemapb.ScalarField_IntData:
		data := make([]int32, len(selectedRows))
		for i, ref := range selectedRows {
			if src := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetIntData().GetData(); int(ref.rowIdx) < len(src) {
				data[i] = src[ref.rowIdx]
			}
		}
		newSf.Data = &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: data}}

	case *schemapb.ScalarField_LongData:
		data := make([]int64, len(selectedRows))
		for i, ref := range selectedRows {
			if src := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetLongData().GetData(); int(ref.rowIdx) < len(src) {
				data[i] = src[ref.rowIdx]
			}
		}
		newSf.Data = &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}}

	case *schemapb.ScalarField_FloatData:
		data := make([]float32, len(selectedRows))
		for i, ref := range selectedRows {
			if src := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetFloatData().GetData(); int(ref.rowIdx) < len(src) {
				data[i] = src[ref.rowIdx]
			}
		}
		newSf.Data = &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: data}}

	case *schemapb.ScalarField_DoubleData:
		data := make([]float64, len(selectedRows))
		for i, ref := range selectedRows {
			if src := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetDoubleData().GetData(); int(ref.rowIdx) < len(src) {
				data[i] = src[ref.rowIdx]
			}
		}
		newSf.Data = &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: data}}

	case *schemapb.ScalarField_StringData:
		data := make([]string, len(selectedRows))
		for i, ref := range selectedRows {
			if src := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetStringData().GetData(); int(ref.rowIdx) < len(src) {
				data[i] = src[ref.rowIdx]
			}
		}
		newSf.Data = &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: data}}

	case *schemapb.ScalarField_BytesData:
		data := make([][]byte, len(selectedRows))
		for i, ref := range selectedRows {
			if src := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetBytesData().GetData(); int(ref.rowIdx) < len(src) {
				data[i] = src[ref.rowIdx]
			}
		}
		newSf.Data = &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{Data: data}}

	case *schemapb.ScalarField_JsonData:
		data := make([][]byte, len(selectedRows))
		for i, ref := range selectedRows {
			if src := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetJsonData().GetData(); int(ref.rowIdx) < len(src) {
				data[i] = src[ref.rowIdx]
			}
		}
		newSf.Data = &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: data}}

	case *schemapb.ScalarField_ArrayData:
		data := make([]*schemapb.ScalarField, len(selectedRows))
		for i, ref := range selectedRows {
			if src := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetArrayData().GetData(); int(ref.rowIdx) < len(src) {
				data[i] = src[ref.rowIdx]
			}
		}
		newSf.Data = &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
			Data:        data,
			ElementType: template.GetArrayData().GetElementType(),
		}}

	case *schemapb.ScalarField_GeometryData:
		data := make([][]byte, len(selectedRows))
		for i, ref := range selectedRows {
			if src := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetGeometryData().GetData(); int(ref.rowIdx) < len(src) {
				data[i] = src[ref.rowIdx]
			}
		}
		newSf.Data = &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: data}}

	case *schemapb.ScalarField_GeometryWktData:
		data := make([]string, len(selectedRows))
		for i, ref := range selectedRows {
			if src := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetGeometryWktData().GetData(); int(ref.rowIdx) < len(src) {
				data[i] = src[ref.rowIdx]
			}
		}
		newSf.Data = &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{Data: data}}

	case *schemapb.ScalarField_TimestamptzData:
		data := make([]int64, len(selectedRows))
		for i, ref := range selectedRows {
			if src := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetTimestamptzData().GetData(); int(ref.rowIdx) < len(src) {
				data[i] = src[ref.rowIdx]
			}
		}
		newSf.Data = &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{Data: data}}

	case *schemapb.ScalarField_MolData:
		data := make([][]byte, len(selectedRows))
		for i, ref := range selectedRows {
			if src := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetScalars().GetMolData().GetData(); int(ref.rowIdx) < len(src) {
				data[i] = src[ref.rowIdx]
			}
		}
		newSf.Data = &schemapb.ScalarField_MolData{MolData: &schemapb.MolArray{Data: data}}
	}

	return newSf
}

// buildCompactIndices pre-computes the compact data index for each result's vector field.
// In compact mode (nullable vectors), the data array only contains entries for valid rows.
// compactIdx[resultIdx][logicalRowIdx] = data array index, or -1 if null.
// Returns (nil, nil) for non-nullable fields (data index = row index).
//
// Returns error when the segcore contract is violated:
//   - nullable field with numRows > 0 but empty ValidData
//   - len(ValidData) != numRows
//
// These conditions indicate a segcore bug and must fail the request loudly
// rather than silently treating rows as null (which corrupts the downstream
// Contents/ValidData contract and causes index-out-of-range panics).
//
// Why hard-error instead of graceful fallback:
// All segcore paths that produce vector FieldData for nullable fields
// (SegmentGrowingImpl::bulk_subscript, ChunkedSegmentSealedImpl::get_raw_data,
// ChunkedSegmentSealedImpl::get_vector) call FilterVectorValidOffsets and write
// a full-length ValidData bitmap. The only path that omits ValidData is
// fill_with_empty(field_id, count) (2-arg overload, used when index is not ready),
// but that early-returns with numRows=count and no selected rows can reference it.
// Therefore, "empty ValidData + numRows > 0" in a merge input is never a legitimate
// state — it always indicates a segcore bug that must be surfaced, not masked.
func buildCompactIndices(results []*internalpb.RetrieveResults, fieldIdx int, isNullable bool) ([][]int, error) {
	if !isNullable {
		return nil, nil
	}

	indices := make([][]int, len(results))
	for ri, r := range results {
		numRows := typeutil.GetSizeOfIDs(r.GetIds())
		fd := r.GetFieldsData()[fieldIdx]
		vd := fd.GetValidData()

		if numRows == 0 {
			indices[ri] = nil
			continue
		}

		// Hard-error, not graceful fallback. All segcore vector output paths for
		// nullable fields populate ValidData:
		//   - get_raw_data:  FilterVectorValidOffsets → fill_with_empty(4-arg) → ValidData set
		//   - get_vector:    FilterVectorValidOffsets → CreateVectorDataArrayFrom → ValidData set
		//   - bulk_subscript (growing): FilterVectorValidOffsets → CreateEmptyVectorDataArray(4-arg) → ValidData set
		//   - bulk_subscript_not_exist_field: CreateEmptyVectorDataArray(0) + manual Add(false) × count → ValidData set
		// The only path that omits ValidData is fill_with_empty(2-arg) when index is not ready,
		// but that path is unreachable for merge inputs (HasRawData gate + empty IDs filtering).
		// Therefore empty ValidData here is always a segcore bug, not a legitimate state.
		if len(vd) == 0 {
			return nil, fmt.Errorf(
				"buildCompactIndices: nullable vector field fid=%d name=%q has empty ValidData but numRows=%d in result[%d]; "+
					"segcore must always provide ValidData for nullable fields with rows",
				fd.GetFieldId(), fd.GetFieldName(), numRows, ri)
		}

		if len(vd) != numRows {
			return nil, fmt.Errorf(
				"buildCompactIndices: nullable vector field fid=%d name=%q has len(ValidData)=%d but numRows=%d in result[%d]; "+
					"segcore violated the nullable contract (len(ValidData) must equal numRows)",
				fd.GetFieldId(), fd.GetFieldName(), len(vd), numRows, ri)
		}

		idx := make([]int, numRows)
		dataIdx := 0
		for i := 0; i < numRows; i++ {
			if vd[i] {
				idx[i] = dataIdx
				dataIdx++
			} else {
				idx[i] = -1
			}
		}
		indices[ri] = idx
	}
	return indices, nil
}

// getVecDataIdx returns the compact data index for a vector row.
// Returns -1 if the row is null. If compactIndices is nil, returns rowIdx directly.
// Callers must call buildCompactIndices first, which validates len(vd) == numRows;
// after that validation passes, out-of-range access here is impossible for correctly
// constructed selectedRows. If it somehow happens, the Go runtime panics with a
// clear stack trace — no silent fallback.
func getVecDataIdx(compactIndices [][]int, ref rowRef) int {
	if compactIndices == nil {
		return int(ref.rowIdx)
	}
	ci := compactIndices[ref.resultIdx]
	if ci == nil {
		return int(ref.rowIdx)
	}
	return ci[ref.rowIdx]
}

// buildMergedVectorField builds merged vector field from selected rows.
// For nullable vector fields, segcore uses compact mode: the data array only
// contains entries for valid (non-null) rows, and ValidData bitmap marks which
// logical rows are null. Null rows don't occupy space in the data array.
// buildCompactIndices/getVecDataIdx handle the logical→data index mapping.
//
// fieldSchema drives the type switch (DataType) and dim, avoiding the previous
// bug where template.GetData().(type) was used: when the template result had
// nil Data oneof (e.g., sparse field not yet in a segment via AlterCollection),
// the type switch missed all cases, producing nil Data in the merged output.
//
// fieldSchema may be nil for system fields or tests; in that case we fall back
// to scanning results for the first non-nil Data to infer the type.
func buildMergedVectorField(results []*internalpb.RetrieveResults, selectedRows []rowRef, fieldIdx int, fieldSchema *schemapb.FieldSchema) (*schemapb.VectorField, error) {
	isNullable := fieldSchema != nil && fieldSchema.GetNullable()

	// Determine DataType and dim from schema (preferred) or from result data (fallback).
	dataType := schemapb.DataType_None
	var dim int64
	if fieldSchema != nil {
		dataType = fieldSchema.GetDataType()
		if !typeutil.IsSparseFloatVectorType(dataType) {
			h := typeutil.CreateFieldSchemaHelper(fieldSchema)
			dim, _ = h.GetDim()
		}
	}

	// Fallback: if no schema, scan results for the first non-nil VectorField.Data
	// to infer type and dim. This keeps backward compatibility with tests that pass
	// nil schema.
	if dataType == schemapb.DataType_None {
		for _, ref := range selectedRows {
			vec := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetVectors()
			if vec != nil && vec.GetData() != nil {
				dim = vec.GetDim()
				switch vec.GetData().(type) {
				case *schemapb.VectorField_FloatVector:
					dataType = schemapb.DataType_FloatVector
				case *schemapb.VectorField_BinaryVector:
					dataType = schemapb.DataType_BinaryVector
				case *schemapb.VectorField_Float16Vector:
					dataType = schemapb.DataType_Float16Vector
				case *schemapb.VectorField_Bfloat16Vector:
					dataType = schemapb.DataType_BFloat16Vector
				case *schemapb.VectorField_Int8Vector:
					dataType = schemapb.DataType_Int8Vector
				case *schemapb.VectorField_SparseFloatVector:
					dataType = schemapb.DataType_SparseFloatVector
				case *schemapb.VectorField_VectorArray:
					dataType = schemapb.DataType_Array
				}
				break
			}
		}
	}

	newVf := &schemapb.VectorField{Dim: dim}
	compactIdx, err := buildCompactIndices(results, fieldIdx, isNullable)
	if err != nil {
		return nil, err
	}

	// vecDataOOB builds a descriptive error for vector data out-of-bounds access.
	// This indicates segcore returned truncated/malformed vector data.
	vecDataOOB := func(ref rowRef, di int, dataLen int) error {
		fd := results[ref.resultIdx].GetFieldsData()[fieldIdx]
		return fmt.Errorf(
			"buildMergedVectorField: vector data too short for %s field fid=%d name=%q in result[%d]: "+
				"dataIdx=%d requires offset beyond data length %d (dim=%d, numRows=%d); segcore returned truncated data",
			dataType, fd.GetFieldId(), fd.GetFieldName(), ref.resultIdx,
			di, dataLen, dim, typeutil.GetSizeOfIDs(results[ref.resultIdx].GetIds()))
	}

	switch dataType {
	case schemapb.DataType_FloatVector:
		data := make([]float32, 0, len(selectedRows)*int(dim))
		for _, ref := range selectedRows {
			di := getVecDataIdx(compactIdx, ref)
			if di < 0 {
				continue
			}
			vec := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetVectors().GetFloatVector().GetData()
			start := di * int(dim)
			if start+int(dim) > len(vec) {
				return nil, vecDataOOB(ref, di, len(vec))
			}
			data = append(data, vec[start:start+int(dim)]...)
		}
		newVf.Data = &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: data}}

	case schemapb.DataType_BinaryVector:
		bytesPerRow := int(dim) / 8
		data := make([]byte, 0, len(selectedRows)*bytesPerRow)
		for _, ref := range selectedRows {
			di := getVecDataIdx(compactIdx, ref)
			if di < 0 {
				continue
			}
			vec := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetVectors().GetBinaryVector()
			start := di * bytesPerRow
			if start+bytesPerRow > len(vec) {
				return nil, vecDataOOB(ref, di, len(vec))
			}
			data = append(data, vec[start:start+bytesPerRow]...)
		}
		newVf.Data = &schemapb.VectorField_BinaryVector{BinaryVector: data}

	case schemapb.DataType_Float16Vector:
		bytesPerRow := int(dim) * 2
		data := make([]byte, 0, len(selectedRows)*bytesPerRow)
		for _, ref := range selectedRows {
			di := getVecDataIdx(compactIdx, ref)
			if di < 0 {
				continue
			}
			vec := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetVectors().GetFloat16Vector()
			start := di * bytesPerRow
			if start+bytesPerRow > len(vec) {
				return nil, vecDataOOB(ref, di, len(vec))
			}
			data = append(data, vec[start:start+bytesPerRow]...)
		}
		newVf.Data = &schemapb.VectorField_Float16Vector{Float16Vector: data}

	case schemapb.DataType_BFloat16Vector:
		bytesPerRow := int(dim) * 2
		data := make([]byte, 0, len(selectedRows)*bytesPerRow)
		for _, ref := range selectedRows {
			di := getVecDataIdx(compactIdx, ref)
			if di < 0 {
				continue
			}
			vec := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetVectors().GetBfloat16Vector()
			start := di * bytesPerRow
			if start+bytesPerRow > len(vec) {
				return nil, vecDataOOB(ref, di, len(vec))
			}
			data = append(data, vec[start:start+bytesPerRow]...)
		}
		newVf.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: data}

	case schemapb.DataType_Int8Vector:
		bytesPerRow := int(dim)
		data := make([]byte, 0, len(selectedRows)*bytesPerRow)
		for _, ref := range selectedRows {
			di := getVecDataIdx(compactIdx, ref)
			if di < 0 {
				continue
			}
			vec := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetVectors().GetInt8Vector()
			start := di * bytesPerRow
			if start+bytesPerRow > len(vec) {
				return nil, vecDataOOB(ref, di, len(vec))
			}
			data = append(data, vec[start:start+bytesPerRow]...)
		}
		newVf.Data = &schemapb.VectorField_Int8Vector{Int8Vector: data}

	case schemapb.DataType_SparseFloatVector:
		contents := make([][]byte, 0, len(selectedRows))
		var maxDim int64
		for _, ref := range selectedRows {
			di := getVecDataIdx(compactIdx, ref)
			if di < 0 {
				continue
			}
			sparse := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetVectors().GetSparseFloatVector()
			if sparse == nil || di >= len(sparse.GetContents()) {
				fd := results[ref.resultIdx].GetFieldsData()[fieldIdx]
				return nil, fmt.Errorf(
					"buildMergedVectorField: sparse vector data missing for field fid=%d name=%q in result[%d]: "+
						"dataIdx=%d but SparseFloatArray is nil or has only %d contents (numRows=%d); segcore returned truncated data",
					fd.GetFieldId(), fd.GetFieldName(), ref.resultIdx,
					di, len(sparse.GetContents()), typeutil.GetSizeOfIDs(results[ref.resultIdx].GetIds()))
			}
			contents = append(contents, sparse.GetContents()[di])
			if sparse.GetDim() > maxDim {
				maxDim = sparse.GetDim()
			}
		}
		newVf.Data = &schemapb.VectorField_SparseFloatVector{
			SparseFloatVector: &schemapb.SparseFloatArray{
				Contents: contents,
				Dim:      maxDim,
			},
		}
		newVf.Dim = maxDim

	default:
		// VectorArray or unknown — scan for first non-nil VectorArray to get metadata.
		for _, ref := range selectedRows {
			va := results[ref.resultIdx].GetFieldsData()[fieldIdx].GetVectors().GetVectorArray()
			if va != nil {
				data := make([]*schemapb.VectorField, 0, len(selectedRows))
				for _, ref2 := range selectedRows {
					di := getVecDataIdx(compactIdx, ref2)
					if di < 0 {
						continue
					}
					va2 := results[ref2.resultIdx].GetFieldsData()[fieldIdx].GetVectors().GetVectorArray()
					if va2 == nil || di >= len(va2.GetData()) {
						fd := results[ref2.resultIdx].GetFieldsData()[fieldIdx]
						return nil, fmt.Errorf(
							"buildMergedVectorField: VectorArray data missing for field fid=%d name=%q in result[%d]: "+
								"dataIdx=%d but VectorArray is nil or has only %d entries (numRows=%d); segcore returned truncated data",
							fd.GetFieldId(), fd.GetFieldName(), ref2.resultIdx,
							di, len(va2.GetData()), typeutil.GetSizeOfIDs(results[ref2.resultIdx].GetIds()))
					}
					data = append(data, va2.GetData()[di])
				}
				newVf.Data = &schemapb.VectorField_VectorArray{
					VectorArray: &schemapb.VectorArray{
						Dim:         va.GetDim(),
						Data:        data,
						ElementType: va.GetElementType(),
					},
				}
				break
			}
		}
	}

	return newVf, nil
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

	// Propagate element-level metadata
	newResult.ElementLevel = result.GetElementLevel()
	if result.GetElementLevel() && len(result.GetElementIndices()) > 0 {
		newResult.ElementIndices = result.GetElementIndices()[start:end]
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
			Vectors: rangeSliceVectorField(fd.GetVectors(), start, end, fd.GetValidData()),
		}
	}

	if len(fd.GetValidData()) > 0 {
		newFd.ValidData = fd.GetValidData()[start:end]
	}

	return newFd
}

// rangeSliceStructArrayField extracts a contiguous range [start, end) from each sub-field.
func rangeSliceStructArrayField(sa *schemapb.StructArrayField, start, end int) *schemapb.StructArrayField {
	if sa == nil {
		return nil
	}
	newFields := make([]*schemapb.FieldData, len(sa.GetFields()))
	for i, subFd := range sa.GetFields() {
		newFields[i] = rangeSliceFieldData(subFd, start, end)
	}
	return &schemapb.StructArrayField{Fields: newFields}
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
		newSf.Data = &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
			Data:        sf.GetArrayData().GetData()[start:end],
			ElementType: sf.GetArrayData().GetElementType(),
		}}
	case *schemapb.ScalarField_GeometryData:
		newSf.Data = &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: sf.GetGeometryData().GetData()[start:end]}}
	case *schemapb.ScalarField_GeometryWktData:
		newSf.Data = &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{Data: sf.GetGeometryWktData().GetData()[start:end]}}
	case *schemapb.ScalarField_TimestamptzData:
		newSf.Data = &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{Data: sf.GetTimestamptzData().GetData()[start:end]}}
	case *schemapb.ScalarField_MolData:
		newSf.Data = &schemapb.ScalarField_MolData{MolData: &schemapb.MolArray{Data: sf.GetMolData().GetData()[start:end]}}
	}

	return newSf
}

// rangeSliceVectorField extracts a contiguous range [start, end) from vector data.
// validData is the field's ValidData bitmap. For nullable vectors in compact mode,
// data indices must be computed by counting valid rows, not using logical row indices.
func rangeSliceVectorField(vf *schemapb.VectorField, start, end int, validData []bool) *schemapb.VectorField {
	dim := int(vf.GetDim())
	newVf := &schemapb.VectorField{Dim: vf.GetDim()}

	// For compact mode: convert logical [start, end) to data [dataStart, dataEnd).
	dataStart, dataEnd := start, end
	if len(validData) > 0 {
		dataStart = 0
		for i := 0; i < start; i++ {
			if validData[i] {
				dataStart++
			}
		}
		dataEnd = dataStart
		for i := start; i < end; i++ {
			if validData[i] {
				dataEnd++
			}
		}
	}

	switch vf.GetData().(type) {
	case *schemapb.VectorField_FloatVector:
		data := vf.GetFloatVector().GetData()
		newVf.Data = &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: data[dataStart*dim : dataEnd*dim]}}
	case *schemapb.VectorField_BinaryVector:
		bytesPerRow := dim / 8
		data := vf.GetBinaryVector()
		newVf.Data = &schemapb.VectorField_BinaryVector{BinaryVector: data[dataStart*bytesPerRow : dataEnd*bytesPerRow]}
	case *schemapb.VectorField_Float16Vector:
		bytesPerRow := dim * 2
		data := vf.GetFloat16Vector()
		newVf.Data = &schemapb.VectorField_Float16Vector{Float16Vector: data[dataStart*bytesPerRow : dataEnd*bytesPerRow]}
	case *schemapb.VectorField_Bfloat16Vector:
		bytesPerRow := dim * 2
		data := vf.GetBfloat16Vector()
		newVf.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: data[dataStart*bytesPerRow : dataEnd*bytesPerRow]}
	case *schemapb.VectorField_Int8Vector:
		bytesPerRow := dim // 1 byte per element
		data := vf.GetInt8Vector()
		newVf.Data = &schemapb.VectorField_Int8Vector{Int8Vector: data[dataStart*bytesPerRow : dataEnd*bytesPerRow]}
	case *schemapb.VectorField_SparseFloatVector:
		contents := vf.GetSparseFloatVector().GetContents()
		newVf.Data = &schemapb.VectorField_SparseFloatVector{
			SparseFloatVector: &schemapb.SparseFloatArray{
				Contents: contents[dataStart:dataEnd],
				Dim:      vf.GetSparseFloatVector().GetDim(),
			},
		}
	case *schemapb.VectorField_VectorArray:
		data := vf.GetVectorArray().GetData()
		newVf.Data = &schemapb.VectorField_VectorArray{
			VectorArray: &schemapb.VectorArray{
				Dim:         vf.GetVectorArray().GetDim(),
				Data:        data[dataStart:dataEnd],
				ElementType: vf.GetVectorArray().GetElementType(),
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
			Vectors: sliceVectorField(fd.GetVectors(), indices, fd.GetValidData()),
		}
	}

	// Preserve ValidData (nullable bitmap) for nullable fields.
	if len(fd.GetValidData()) > 0 {
		validData := fd.GetValidData()
		newValidData := make([]bool, len(indices))
		for i, idx := range indices {
			newValidData[i] = validData[idx]
		}
		newFd.ValidData = newValidData
	}

	return newFd
}

// sliceScalarField extracts scalar data at the given indices.
// sliceStructArrayField extracts struct array sub-fields at the given indices.
func sliceStructArrayField(sa *schemapb.StructArrayField, indices []int) *schemapb.StructArrayField {
	if sa == nil {
		return nil
	}
	newFields := make([]*schemapb.FieldData, len(sa.GetFields()))
	for i, subFd := range sa.GetFields() {
		newFields[i] = sliceFieldData(subFd, indices)
	}
	return &schemapb.StructArrayField{Fields: newFields}
}

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
		newSf.Data = &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
			Data:        newData,
			ElementType: sf.GetArrayData().GetElementType(),
		}}

	case *schemapb.ScalarField_GeometryData:
		data := sf.GetGeometryData().GetData()
		newData := make([][]byte, len(indices))
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		newSf.Data = &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: newData}}

	case *schemapb.ScalarField_GeometryWktData:
		data := sf.GetGeometryWktData().GetData()
		newData := make([]string, len(indices))
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		newSf.Data = &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{Data: newData}}

	case *schemapb.ScalarField_TimestamptzData:
		data := sf.GetTimestamptzData().GetData()
		newData := make([]int64, len(indices))
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		newSf.Data = &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{Data: newData}}

	case *schemapb.ScalarField_MolData:
		data := sf.GetMolData().GetData()
		newData := make([][]byte, len(indices))
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		newSf.Data = &schemapb.ScalarField_MolData{MolData: &schemapb.MolArray{Data: newData}}
	}

	return newSf
}

// sliceVectorField extracts vector data at the given logical indices.
// validData is the field's ValidData bitmap for compact mode handling.
func sliceVectorField(vf *schemapb.VectorField, indices []int, validData []bool) *schemapb.VectorField {
	dim := int(vf.GetDim())
	newVf := &schemapb.VectorField{Dim: vf.GetDim()}

	// Pre-compute compact index mapping if nullable.
	// compactIdx[logicalRow] = data index, or -1 if null.
	var compactIdx []int
	if len(validData) > 0 {
		compactIdx = make([]int, len(validData))
		di := 0
		for i, v := range validData {
			if v {
				compactIdx[i] = di
				di++
			} else {
				compactIdx[i] = -1
			}
		}
	}
	// toDataIdx converts logical index to data index, skipping null rows.
	toDataIdx := func(logicalIdx int) int {
		if compactIdx == nil {
			return logicalIdx
		}
		return compactIdx[logicalIdx]
	}

	// Count valid rows in indices for output capacity.
	validCount := 0
	for _, idx := range indices {
		di := toDataIdx(idx)
		if di >= 0 {
			validCount++
		}
	}

	switch vf.GetData().(type) {
	case *schemapb.VectorField_FloatVector:
		data := vf.GetFloatVector().GetData()
		newData := make([]float32, 0, validCount*dim)
		for _, idx := range indices {
			di := toDataIdx(idx)
			if di < 0 {
				continue
			}
			newData = append(newData, data[di*dim:(di+1)*dim]...)
		}
		newVf.Data = &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: newData}}

	case *schemapb.VectorField_BinaryVector:
		bytesPerRow := dim / 8
		data := vf.GetBinaryVector()
		newData := make([]byte, 0, validCount*bytesPerRow)
		for _, idx := range indices {
			di := toDataIdx(idx)
			if di < 0 {
				continue
			}
			newData = append(newData, data[di*bytesPerRow:(di+1)*bytesPerRow]...)
		}
		newVf.Data = &schemapb.VectorField_BinaryVector{BinaryVector: newData}

	case *schemapb.VectorField_Float16Vector:
		bytesPerRow := dim * 2
		data := vf.GetFloat16Vector()
		newData := make([]byte, 0, validCount*bytesPerRow)
		for _, idx := range indices {
			di := toDataIdx(idx)
			if di < 0 {
				continue
			}
			newData = append(newData, data[di*bytesPerRow:(di+1)*bytesPerRow]...)
		}
		newVf.Data = &schemapb.VectorField_Float16Vector{Float16Vector: newData}

	case *schemapb.VectorField_Bfloat16Vector:
		bytesPerRow := dim * 2
		data := vf.GetBfloat16Vector()
		newData := make([]byte, 0, validCount*bytesPerRow)
		for _, idx := range indices {
			di := toDataIdx(idx)
			if di < 0 {
				continue
			}
			newData = append(newData, data[di*bytesPerRow:(di+1)*bytesPerRow]...)
		}
		newVf.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: newData}

	case *schemapb.VectorField_Int8Vector:
		bytesPerRow := dim // 1 byte per element
		data := vf.GetInt8Vector()
		newData := make([]byte, 0, validCount*bytesPerRow)
		for _, idx := range indices {
			di := toDataIdx(idx)
			if di < 0 {
				continue
			}
			newData = append(newData, data[di*bytesPerRow:(di+1)*bytesPerRow]...)
		}
		newVf.Data = &schemapb.VectorField_Int8Vector{Int8Vector: newData}

	case *schemapb.VectorField_SparseFloatVector:
		contents := vf.GetSparseFloatVector().GetContents()
		newContents := make([][]byte, 0, validCount)
		for _, idx := range indices {
			di := toDataIdx(idx)
			if di < 0 {
				continue
			}
			newContents = append(newContents, contents[di])
		}
		newVf.Data = &schemapb.VectorField_SparseFloatVector{
			SparseFloatVector: &schemapb.SparseFloatArray{
				Contents: newContents,
				Dim:      vf.GetSparseFloatVector().GetDim(),
			},
		}

	case *schemapb.VectorField_VectorArray:
		srcData := vf.GetVectorArray().GetData()
		newData := make([]*schemapb.VectorField, 0, validCount)
		for _, idx := range indices {
			di := toDataIdx(idx)
			if di < 0 {
				continue
			}
			newData = append(newData, srcData[di])
		}
		newVf.Data = &schemapb.VectorField_VectorArray{
			VectorArray: &schemapb.VectorArray{
				Dim:         vf.GetVectorArray().GetDim(),
				Data:        newData,
				ElementType: vf.GetVectorArray().GetElementType(),
			},
		}
	}

	return newVf
}

// calcRowSize computes the size in bytes of a single row in a RetrieveResult
// by summing the per-element size of each field. This is used during the
// merge selection phase (Phase 1) to track accumulated output size before
// the actual memory-copy phase (Phase 2), enabling early termination
// when maxOutputSize would be exceeded.
func calcRowSize(result *internalpb.RetrieveResults, rowIdx int64) int64 {
	var size int64
	for _, fd := range result.GetFieldsData() {
		size += calcFieldElementSize(fd, int(rowIdx))
	}
	return size
}

// calcFieldElementSize returns the byte size of a single
// element at rowIdx within a FieldData.
func calcFieldElementSize(fd *schemapb.FieldData, rowIdx int) int64 {
	if scalars := fd.GetScalars(); scalars != nil {
		switch data := scalars.GetData().(type) {
		case *schemapb.ScalarField_BoolData:
			return 1
		case *schemapb.ScalarField_IntData:
			return 4
		case *schemapb.ScalarField_LongData:
			return 8
		case *schemapb.ScalarField_FloatData:
			return 4
		case *schemapb.ScalarField_DoubleData:
			return 8
		case *schemapb.ScalarField_StringData:
			d := data.StringData.GetData()
			if rowIdx < len(d) {
				return int64(len(d[rowIdx]))
			}
			return 0
		case *schemapb.ScalarField_BytesData:
			d := data.BytesData.GetData()
			if rowIdx < len(d) {
				return int64(len(d[rowIdx]))
			}
			return 0
		case *schemapb.ScalarField_JsonData:
			d := data.JsonData.GetData()
			if rowIdx < len(d) {
				return int64(len(d[rowIdx]))
			}
			return 0
		case *schemapb.ScalarField_ArrayData:
			d := data.ArrayData.GetData()
			if rowIdx < len(d) {
				return int64(proto.Size(d[rowIdx]))
			}
			return 0
		case *schemapb.ScalarField_GeometryData:
			d := data.GeometryData.GetData()
			if rowIdx < len(d) {
				return int64(len(d[rowIdx]))
			}
			return 0
		case *schemapb.ScalarField_GeometryWktData:
			d := data.GeometryWktData.GetData()
			if rowIdx < len(d) {
				return int64(len(d[rowIdx]))
			}
			return 0
		case *schemapb.ScalarField_TimestamptzData:
			return 8
		case *schemapb.ScalarField_MolData:
			d := data.MolData.GetData()
			if rowIdx < len(d) {
				return int64(len(d[rowIdx]))
			}
			return 0
		}
	}
	if vectors := fd.GetVectors(); vectors != nil {
		dim := int(vectors.GetDim())
		switch vectors.GetData().(type) {
		case *schemapb.VectorField_FloatVector:
			return int64(dim * 4)
		case *schemapb.VectorField_BinaryVector:
			return int64(dim / 8)
		case *schemapb.VectorField_Float16Vector:
			return int64(dim * 2)
		case *schemapb.VectorField_Bfloat16Vector:
			return int64(dim * 2)
		case *schemapb.VectorField_Int8Vector:
			return int64(dim)
		case *schemapb.VectorField_SparseFloatVector:
			contents := vectors.GetSparseFloatVector().GetContents()
			if rowIdx < len(contents) {
				return int64(len(contents[rowIdx]))
			}
			return 0
		case *schemapb.VectorField_VectorArray:
			d := vectors.GetVectorArray().GetData()
			if rowIdx < len(d) {
				return int64(proto.Size(d[rowIdx]))
			}
			return 0
		}
	}
	return 0
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
		case *schemapb.ScalarField_GeometryData:
			return len(data.GeometryData.GetData())
		case *schemapb.ScalarField_GeometryWktData:
			return len(data.GeometryWktData.GetData())
		case *schemapb.ScalarField_TimestamptzData:
			return len(data.TimestamptzData.GetData())
		case *schemapb.ScalarField_MolData:
			return len(data.MolData.GetData())
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
		case *schemapb.VectorField_Int8Vector:
			return len(data.Int8Vector) / dim
		case *schemapb.VectorField_SparseFloatVector:
			return len(data.SparseFloatVector.GetContents())
		case *schemapb.VectorField_VectorArray:
			return len(data.VectorArray.GetData())
		}
	}
	if fd.GetStructArrays() != nil && len(fd.GetStructArrays().GetFields()) > 0 {
		// Use the first sub-field's row count
		return getRowCount(&internalpb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fd.GetStructArrays().GetFields()[0]},
		})
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
