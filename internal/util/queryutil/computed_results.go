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
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// prepareComputedResults validates incoming worker/shard data once, before
// empty-result filtering or single-input shortcuts. Later materialization,
// ordering and paging use selectComputedResults without rescanning payloads.
func prepareComputedResults(results []*internalpb.RetrieveResults) ([]*schemapb.FieldData, error) {
	template, _, err := indexComputedResults(results)
	if err != nil || template == nil {
		return nil, err
	}
	for _, result := range results {
		rows := typeutil.GetSizeOfIDs(result.GetIds())
		for _, column := range result.GetComputedResults() {
			if err := validateComputedField(column, rows); err != nil {
				return nil, err
			}
		}
	}
	return typeutil.PrepareResultFieldData(template, 0), nil
}

// selectComputedResults gathers rows from already validated inputs using the
// ordinary scalar FieldData helpers. Name mapping and row bounds do not inspect
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
		for position, ref := range refs {
			index[0] = ref.rowIdx
			appendComputedField(output[i], sources[ref.resultIdx][column.FieldName], index, position)
		}
	}
	return output, nil
}

// indexComputedResults aligns column headers by name and checks their types.
// It never traverses payload rows.
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
		for _, column := range template {
			source := sources[i][column.FieldName]
			if source == nil || source.Type != column.Type {
				return nil, nil, merr.WrapErrServiceInternalMsg("inconsistent computed field layout for %s", column.FieldName)
			}
		}
	}
	return template, sources, nil
}

// indexComputedResults has already checked that fd is non-nil.
// Computed scalar/JSON columns carry one value per result row. JSON content
// remains opaque through reduction; native Grep validates it before export.
func validateComputedField(fd *schemapb.FieldData, rows int) error {
	if !typeutil.ValidateAndNormalizeFieldDataValidData(fd) {
		return merr.WrapErrServiceInternalMsg("conflicting computed field validity masks")
	}
	validData := typeutil.GetFieldDataValidData(fd)
	if len(validData) != 0 && len(validData) != rows {
		return merr.WrapErrServiceInternalMsg("computed field validity does not align with rows")
	}
	sf := fd.GetScalars()
	if sf == nil {
		return merr.WrapErrServiceInternalMsg("missing computed scalar data")
	}
	n, actual := computedScalarLength(sf)
	expected := fd.Type
	if expected == schemapb.DataType_Text || expected == schemapb.DataType_String {
		expected = schemapb.DataType_VarChar
	}
	if expected == schemapb.DataType_Int8 || expected == schemapb.DataType_Int16 {
		expected = schemapb.DataType_Int32
	}
	if n < 0 || actual != expected {
		return merr.WrapErrServiceInternalMsg("computed scalar payload type mismatch")
	}
	if len(validData) != 0 && len(validData) != n {
		return merr.WrapErrServiceInternalMsg("computed scalar validity length mismatch")
	}
	if n != rows {
		return merr.WrapErrServiceInternalMsg("computed field rows (%d) do not align with IDs (%d)", n, rows)
	}
	return nil
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
	default:
		return -1, schemapb.DataType_None
	}
}

// Preserve full-length validity even when some shards omit an all-valid mask.
// Column identity has already been resolved by name; the scalar append helper
// never matches computed results by collection field ID.
func appendComputedField(dst, src *schemapb.FieldData, index []int64, previous int) {
	srcValid, dstValid := typeutil.GetFieldDataValidData(src), typeutil.GetFieldDataValidData(dst)
	if len(srcValid) > 0 && len(dstValid) == 0 && previous > 0 {
		dstValid = make([]bool, previous)
		for i := range dstValid {
			dstValid[i] = true
		}
		typeutil.SetFieldDataValidData(dst, dstValid)
	}
	typeutil.AppendFieldDataByColumn(dst, src, index)
	if len(srcValid) == 0 && len(dstValid) > 0 {
		typeutil.SetFieldDataValidData(dst, append(dstValid, true))
	}
}
