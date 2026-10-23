// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package queryutil

import (
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// prepareComputedResults validates incoming worker/shard data once, before
// empty-result filtering or single-input shortcuts. Later materialization,
// ordering and paging use selectComputedResults without rescanning payloads.
func prepareComputedResults(results []*internalpb.RetrieveResults) ([]*schemapb.FieldData, error) {
	template, sources, err := indexComputedResults(results)
	if err != nil || template == nil {
		return nil, err
	}
	for i, source := range sources {
		if source == nil {
			continue
		}
		rows := typeutil.GetSizeOfIDs(results[i].GetIds())
		for _, column := range source {
			if err := validateComputedField(column, rows); err != nil {
				return nil, err
			}
		}
		for _, column := range template {
			if _, err := alignComputedField(column, source[column.FieldName]); err != nil {
				return nil, err
			}
		}
	}
	return typeutil.PrepareResultFieldData(template, 0), nil
}

// selectComputedResults gathers rows from already validated inputs using the
// ordinary FieldData helpers. Name mapping and row bounds do not inspect nested
// payloads; the reduce entry point owns payload validation.
func selectComputedResults(results []*internalpb.RetrieveResults, refs []rowRef) ([]*schemapb.FieldData, error) {
	template, sources, err := indexComputedResults(results)
	if err != nil || template == nil {
		return nil, err
	}
	for _, ref := range refs {
		if ref.resultIdx < 0 || ref.resultIdx >= len(results) || ref.rowIdx < 0 || ref.rowIdx >= int64(typeutil.GetSizeOfIDs(results[ref.resultIdx].GetIds())) {
			return nil, merr.WrapErrServiceInternalMsg("computed result source row out of range")
		}
	}
	output := typeutil.PrepareResultFieldData(template, int64(len(refs)))
	index := []int64{0}
	for i, column := range template {
		// Resolve names once per input, not once per selected row. Source IDs
		// and child ordering are local to that producer and are not identities.
		aligned := make([]*computedFieldSource, len(sources))
		for j, source := range sources {
			if source == nil {
				continue
			}
			var err error
			aligned[j], err = alignComputedField(column, source[column.FieldName])
			if err != nil {
				return nil, err
			}
		}
		for position, ref := range refs {
			index[0] = ref.rowIdx
			appendComputedField(output[i], aligned[ref.resultIdx], index, position)
		}
	}
	return output, nil
}

// indexComputedResults aligns column headers only; it never traverses rows.
func indexComputedResults(results []*internalpb.RetrieveResults) ([]*schemapb.FieldData, []map[string]*schemapb.FieldData, error) {
	var template []*schemapb.FieldData
	for _, result := range results {
		if len(result.GetComputedResults()) != 0 {
			template = result.GetComputedResults()
			break
		}
	}
	if template == nil {
		return nil, nil, nil
	}
	sources := make([]map[string]*schemapb.FieldData, len(results))
	for i, result := range results {
		columns := result.GetComputedResults()
		rows := typeutil.GetSizeOfIDs(result.GetIds())
		if len(columns) == 0 && rows == 0 {
			continue
		}
		if len(columns) != len(template) {
			return nil, nil, merr.WrapErrServiceInternalMsg("inconsistent computed column count")
		}
		sources[i] = make(map[string]*schemapb.FieldData, len(columns))
		for _, column := range columns {
			if column == nil || column.FieldName == "" {
				return nil, nil, merr.WrapErrServiceInternalMsg("missing computed column name")
			}
			if _, ok := sources[i][column.FieldName]; ok {
				return nil, nil, merr.WrapErrServiceInternalMsg("duplicate computed column %s", column.FieldName)
			}
			sources[i][column.FieldName] = column
		}
	}
	return template, sources, nil
}

// computedFieldSource borrows payloads and arranges child references in the
// destination's order. Only names and types identify computed fields.
type computedFieldSource struct {
	data     *schemapb.FieldData
	children []*computedFieldSource
}

func alignComputedField(template, source *schemapb.FieldData) (*computedFieldSource, error) {
	if template == nil || source == nil || template.FieldName != source.FieldName || template.Type != source.Type {
		return nil, merr.WrapErrServiceInternalMsg("inconsistent computed field layout for %s", template.GetFieldName())
	}
	result := &computedFieldSource{data: source}
	if template.Type == schemapb.DataType_ArrayOfStruct {
		want, have := template.GetStructArrays().GetFields(), source.GetStructArrays().GetFields()
		if len(want) != len(have) {
			return nil, merr.WrapErrServiceInternalMsg("inconsistent computed field layout for %s", template.FieldName)
		}
		byName := make(map[string]*schemapb.FieldData, len(have))
		for _, child := range have {
			if child == nil || child.FieldName == "" || byName[child.FieldName] != nil {
				return nil, merr.WrapErrServiceInternalMsg("invalid computed struct child")
			}
			byName[child.FieldName] = child
		}
		result.children = make([]*computedFieldSource, len(want))
		for i, child := range want {
			aligned, err := alignComputedField(child, byName[child.GetFieldName()])
			if err != nil {
				return nil, err
			}
			result.children[i] = aligned
			delete(byName, child.GetFieldName())
		}
	} else if template.Type == schemapb.DataType_Array &&
		template.GetScalars().GetArrayData().GetElementType() != source.GetScalars().GetArrayData().GetElementType() {
		return nil, merr.WrapErrServiceInternalMsg("inconsistent computed field layout for %s", template.FieldName)
	}
	return result, nil
}

func validateComputedField(fd *schemapb.FieldData, rows int) error {
	if fd == nil {
		return merr.WrapErrServiceInternalMsg("missing computed field data")
	}
	if len(fd.ValidData) != 0 && len(fd.ValidData) != rows {
		return merr.WrapErrServiceInternalMsg("computed field validity does not align with rows")
	}
	if fd.Type == schemapb.DataType_ArrayOfStruct {
		fields := fd.GetStructArrays().GetFields()
		if len(fields) == 0 {
			return merr.WrapErrServiceInternalMsg("missing computed struct fields")
		}
		seen := make(map[string]bool, len(fields))
		for i, field := range fields {
			if field == nil || field.Type != schemapb.DataType_Array || field.FieldName == "" || seen[field.FieldName] {
				return merr.WrapErrServiceInternalMsg("invalid computed struct child")
			}
			seen[field.FieldName] = true
			if err := validateComputedField(field, rows); err != nil {
				return err
			}
			if i > 0 {
				for row, values := range field.GetScalars().GetArrayData().Data {
					n, _ := computedScalarLength(values)
					first, _ := computedScalarLength(fields[0].GetScalars().GetArrayData().Data[row])
					if n != first {
						return merr.WrapErrServiceInternalMsg("computed struct child lengths differ")
					}
				}
			}
		}
		return nil
	}
	if len(fd.ValidData) > 0 && len(fd.GetScalars().GetValidData()) > 0 && !slices.Equal(fd.ValidData, fd.GetScalars().ValidData) {
		return merr.WrapErrServiceInternalMsg("conflicting computed field validity masks")
	}
	n, err := validateComputedScalar(fd.GetScalars(), fd.Type)
	if err != nil {
		return err
	}
	if n != rows {
		return merr.WrapErrServiceInternalMsg("computed field rows (%d) do not align with IDs (%d)", n, rows)
	}
	return nil
}

// The payload carries each array level's element type. Empty arrays need no
// deeper schema to be selected, concatenated or returned to the client.
func validateComputedScalar(sf *schemapb.ScalarField, expected schemapb.DataType) (int, error) {
	if sf == nil {
		return 0, merr.WrapErrServiceInternalMsg("missing computed scalar data")
	}
	n, actual := computedScalarLength(sf)
	if expected == schemapb.DataType_Text || expected == schemapb.DataType_String {
		expected = schemapb.DataType_VarChar
	}
	if expected == schemapb.DataType_Int8 || expected == schemapb.DataType_Int16 {
		expected = schemapb.DataType_Int32
	}
	if n < 0 || actual != expected {
		return 0, merr.WrapErrServiceInternalMsg("computed scalar payload type mismatch")
	}
	if len(sf.ValidData) != 0 && len(sf.ValidData) != n {
		return 0, merr.WrapErrServiceInternalMsg("computed scalar validity length mismatch")
	}
	if actual == schemapb.DataType_Array {
		values := sf.GetArrayData()
		for _, child := range values.Data {
			if _, err := validateComputedScalar(child, values.ElementType); err != nil {
				return 0, err
			}
		}
	}
	return n, nil
}

func computedScalarLength(sf *schemapb.ScalarField) (int, schemapb.DataType) {
	switch sf.GetData().(type) {
	case *schemapb.ScalarField_BoolData:
		return len(sf.GetBoolData().GetData()), schemapb.DataType_Bool
	case *schemapb.ScalarField_IntData:
		return len(sf.GetIntData().GetData()), schemapb.DataType_Int32
	case *schemapb.ScalarField_LongData:
		return len(sf.GetLongData().GetData()), schemapb.DataType_Int64
	case *schemapb.ScalarField_FloatData:
		return len(sf.GetFloatData().GetData()), schemapb.DataType_Float
	case *schemapb.ScalarField_DoubleData:
		return len(sf.GetDoubleData().GetData()), schemapb.DataType_Double
	case *schemapb.ScalarField_StringData:
		return len(sf.GetStringData().GetData()), schemapb.DataType_VarChar
	case *schemapb.ScalarField_JsonData:
		return len(sf.GetJsonData().GetData()), schemapb.DataType_JSON
	case *schemapb.ScalarField_ArrayData:
		return len(sf.GetArrayData().GetData()), schemapb.DataType_Array
	default:
		return -1, schemapb.DataType_None
	}
}

// Preserve full-length validity even when some shards omit an all-valid mask.
// Struct validity is carried on the parent FieldData; the ordinary helper only
// handles scalar/vector validity, so recurse here for computed structs.
func appendComputedField(dst *schemapb.FieldData, source *computedFieldSource, index []int64, previous int) {
	src := source.data
	setValidity := func(values []bool) {
		if dst.GetStructArrays() != nil {
			dst.ValidData = values
		} else {
			typeutil.SetFieldDataValidData(dst, values)
		}
	}
	srcValid, dstValid := typeutil.GetFieldDataValidData(src), typeutil.GetFieldDataValidData(dst)
	if len(srcValid) > 0 && len(dstValid) == 0 && previous > 0 {
		dstValid = make([]bool, previous)
		for i := range dstValid {
			dstValid[i] = true
		}
		setValidity(dstValid)
	}
	if src.GetStructArrays() != nil {
		if len(srcValid) > 0 {
			setValidity(append(dstValid, srcValid[index[0]]))
		} else if len(dstValid) > 0 {
			setValidity(append(dstValid, true))
		}
		for i, child := range source.children {
			appendComputedField(dst.GetStructArrays().Fields[i], child, index, previous)
		}
	} else {
		typeutil.AppendFieldDataByColumn(dst, src, index)
		if len(srcValid) == 0 && len(dstValid) > 0 {
			setValidity(append(dstValid, true))
		}
	}
}
