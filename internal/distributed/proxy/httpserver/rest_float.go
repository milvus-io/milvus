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

package httpserver

import (
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// JSON cannot represent non-finite numbers. Use the protobuf JSON spellings
// for scalar values; finite values retain their original numeric type. These
// spellings also round-trip through the REST scalar input parser.
func restFloatValue[T ~float32 | ~float64](value T) any {
	switch {
	case math.IsNaN(float64(value)):
		return "NaN"
	case math.IsInf(float64(value), 1):
		return "Infinity"
	case math.IsInf(float64(value), -1):
		return "-Infinity"
	default:
		return value
	}
}

// Keep the typed slice, and its backing storage, for the usual finite case.
// Only arrays with a non-finite member need an interface slice for strings.
func restFloatSlice[T ~float32 | ~float64](values []T) (any, bool) {
	for _, value := range values {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			out := make([]any, len(values))
			for i, element := range values {
				out[i] = restFloatValue(element)
			}
			return out, true
		}
	}
	return values, false
}

// The low-level REST API and legacy Array responses encode Go protobuf structs,
// rather than protobuf JSON. Override only the fields containing non-finite
// scalars, retaining their existing field names, numeric enums, int64 encoding,
// validity, and oneof wrapper shape. The original protobufs are never mutated.
type restScalarField struct {
	*schemapb.ScalarField
	Data any
}

func restLegacyScalarField(field *schemapb.ScalarField) (any, bool) {
	switch data := field.GetData().(type) {
	case *schemapb.ScalarField_FloatData:
		values, changed := restFloatSlice(data.FloatData.GetData())
		if changed {
			return restScalarField{field, map[string]any{"FloatData": map[string]any{"data": values}}}, true
		}
	case *schemapb.ScalarField_DoubleData:
		values, changed := restFloatSlice(data.DoubleData.GetData())
		if changed {
			return restScalarField{field, map[string]any{"DoubleData": map[string]any{"data": values}}}, true
		}
	case *schemapb.ScalarField_ArrayData:
		rows, changed := restScalarFieldSlice(data.ArrayData.GetData())
		if changed {
			array := struct {
				*schemapb.ArrayArray
				Data []any `json:"data,omitempty"`
			}{data.ArrayData, rows}
			return restScalarField{field, map[string]any{"ArrayData": array}}, true
		}
	}
	return field, false
}

func restScalarFieldSlice(fields []*schemapb.ScalarField) ([]any, bool) {
	var out []any
	for i, field := range fields {
		value, changed := restLegacyScalarField(field)
		if changed && out == nil {
			out = make([]any, len(fields))
			for j := 0; j < i; j++ {
				out[j] = fields[j]
			}
		}
		if out != nil {
			out[i] = value
		}
	}
	return out, out != nil
}

func restLegacyFieldData(field *schemapb.FieldData) (any, bool) {
	var replacement any
	var changed bool
	switch data := field.GetField().(type) {
	case *schemapb.FieldData_Scalars:
		var scalars any
		scalars, changed = restLegacyScalarField(data.Scalars)
		if changed {
			replacement = map[string]any{"Scalars": scalars}
		}
	case *schemapb.FieldData_StructArrays:
		var fields []any
		fields, changed = restFieldDataSlice(data.StructArrays.GetFields())
		if changed {
			replacement = map[string]any{"StructArrays": map[string]any{"fields": fields}}
		}
	}
	if !changed {
		return field, false
	}
	return struct {
		*schemapb.FieldData
		Field any
	}{field, replacement}, true
}

func restFieldDataSlice(fields []*schemapb.FieldData) ([]any, bool) {
	var out []any
	for i, field := range fields {
		value, changed := restLegacyFieldData(field)
		if changed && out == nil {
			out = make([]any, len(fields))
			for j := 0; j < i; j++ {
				out[j] = fields[j]
			}
		}
		if out != nil {
			out[i] = value
		}
	}
	return out, out != nil
}

func restDefaultValue(value *schemapb.ValueField) (any, bool) {
	var replacement any
	switch typed := value.GetData().(type) {
	case *schemapb.ValueField_FloatData:
		if math.IsNaN(float64(typed.FloatData)) || math.IsInf(float64(typed.FloatData), 0) {
			replacement = map[string]any{"FloatData": restFloatValue(typed.FloatData)}
		}
	case *schemapb.ValueField_DoubleData:
		if math.IsNaN(typed.DoubleData) || math.IsInf(typed.DoubleData, 0) {
			replacement = map[string]any{"DoubleData": restFloatValue(typed.DoubleData)}
		}
	}
	if replacement == nil {
		return value, false
	}
	return struct {
		*schemapb.ValueField
		Data any
	}{value, replacement}, true
}

func restFieldSchemaSlice(fields []*schemapb.FieldSchema) ([]any, bool) {
	var out []any
	for i, field := range fields {
		value, changed := restDefaultValue(field.GetDefaultValue())
		if changed && out == nil {
			out = make([]any, len(fields))
			for j := 0; j < i; j++ {
				out[j] = fields[j]
			}
		}
		if out != nil {
			if changed {
				out[i] = struct {
					*schemapb.FieldSchema
					DefaultValue any `json:"default_value,omitempty"`
				}{field, value}
			} else {
				out[i] = field
			}
		}
	}
	return out, out != nil
}

func restLegacyCollectionSchema(schema *schemapb.CollectionSchema) (any, bool) {
	fields, fieldsChanged := restFieldSchemaSlice(schema.GetFields())
	var structs []any
	for i, field := range schema.GetStructArrayFields() {
		subs, changed := restFieldSchemaSlice(field.GetFields())
		if changed && structs == nil {
			structs = make([]any, len(schema.GetStructArrayFields()))
			for j := 0; j < i; j++ {
				structs[j] = schema.GetStructArrayFields()[j]
			}
		}
		if structs != nil {
			if changed {
				structs[i] = struct {
					*schemapb.StructArrayFieldSchema
					Fields []any `json:"fields,omitempty"`
				}{field, subs}
			} else {
				structs[i] = field
			}
		}
	}
	if !fieldsChanged && structs == nil {
		return schema, false
	}
	if !fieldsChanged {
		fields = make([]any, len(schema.GetFields()))
		for i, field := range schema.GetFields() {
			fields[i] = field
		}
	}
	if structs == nil {
		structs = make([]any, len(schema.GetStructArrayFields()))
		for i, field := range schema.GetStructArrayFields() {
			structs[i] = field
		}
	}
	return struct {
		*schemapb.CollectionSchema
		Fields            []any `json:"fields,omitempty"`
		StructArrayFields []any `json:"struct_array_fields,omitempty"`
	}{schema, fields, structs}, true
}

func restLegacySearchResultData(result *schemapb.SearchResultData) (any, bool) {
	fields, fieldsChanged := restFieldDataSlice(result.GetFieldsData())
	group, groupChanged := restLegacyFieldData(result.GetGroupByFieldValue())
	groups, groupsChanged := restFieldDataSlice(result.GetGroupByFieldValues())
	if !fieldsChanged && !groupChanged && !groupsChanged {
		return result, false
	}
	// Nil replacement slices would hide original finite fields when another
	// scalar changes, so keep their original slices in that case.
	var outputFields, outputGroups any
	if len(result.GetFieldsData()) > 0 {
		outputFields = result.GetFieldsData()
	}
	if len(result.GetGroupByFieldValues()) > 0 {
		outputGroups = result.GetGroupByFieldValues()
	}
	if result.GetGroupByFieldValue() == nil {
		group = nil
	}
	if fieldsChanged {
		outputFields = fields
	}
	if groupsChanged {
		outputGroups = groups
	}
	return struct {
		*schemapb.SearchResultData
		FieldsData         any `json:"fields_data,omitempty"`
		GroupByFieldValue  any `json:"group_by_field_value,omitempty"`
		GroupByFieldValues any `json:"group_by_field_values,omitempty"`
	}{result, outputFields, group, outputGroups}, true
}

func restLegacyResponse(response any) any {
	switch result := response.(type) {
	case *milvuspb.QueryResults:
		fields, changed := restFieldDataSlice(result.GetFieldsData())
		if changed {
			return struct {
				*milvuspb.QueryResults
				FieldsData []any `json:"fields_data,omitempty"`
			}{result, fields}
		}
	case *milvuspb.SearchResults:
		data, changed := restLegacySearchResultData(result.GetResults())
		if changed {
			return struct {
				*milvuspb.SearchResults
				Results any `json:"results,omitempty"`
			}{result, data}
		}
	case *milvuspb.DescribeCollectionResponse:
		schema, changed := restLegacyCollectionSchema(result.GetSchema())
		if changed {
			return struct {
				*milvuspb.DescribeCollectionResponse
				Schema any `json:"schema,omitempty"`
			}{result, schema}
		}
	}
	return response
}
