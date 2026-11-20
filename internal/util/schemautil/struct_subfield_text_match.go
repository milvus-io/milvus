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

package schemautil

import (
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ValidateVarCharArrayTextMatch checks text options on newly supplied VarChar arrays.
func ValidateVarCharArrayTextMatch(field *schemapb.FieldSchema) error {
	if field.GetDataType() != schemapb.DataType_Array || field.GetElementType() != schemapb.DataType_VarChar {
		return nil
	}
	var match, analyzer bool
	for _, kv := range field.GetTypeParams() {
		if kv.GetKey() != common.EnableMatchKey && kv.GetKey() != common.EnableAnalyzerKey {
			continue
		}
		value, err := strconv.ParseBool(kv.GetValue())
		if err != nil {
			return merr.WrapErrParameterInvalidMsg("invalid %s on field %s", kv.GetKey(), field.GetName())
		}
		if kv.GetKey() == common.EnableMatchKey {
			match = value
		} else {
			analyzer = value
		}
	}
	if match && !analyzer {
		return merr.WrapErrParameterInvalidMsg("field %s with enable_match requires enable_analyzer", field.GetName())
	}
	return nil
}

// ValidateStructSubFieldTextMatch accepts text parameters on scalar VarChar
// leaves. A nested Array has its own type schema and cannot be a text document.
func ValidateStructSubFieldTextMatch(field *schemapb.FieldSchema) error {
	if field.GetDataType() == schemapb.DataType_Array && field.GetElementType() == schemapb.DataType_VarChar {
		return ValidateVarCharArrayTextMatch(field)
	}
	params := [][]*commonpb.KeyValuePair{field.GetTypeParams()}
	for node := field.GetTypeSchema(); node != nil; node = node.GetArrayElement() {
		params = append(params, node.GetTypeParams())
	}
	for _, kvs := range params {
		for _, kv := range kvs {
			if kv.GetKey() != common.EnableMatchKey && kv.GetKey() != common.EnableAnalyzerKey {
				continue
			}
			if enabled, err := strconv.ParseBool(kv.GetValue()); err == nil && !enabled {
				continue
			}
			return merr.WrapErrParameterInvalidMsg("%s is not supported on struct sub-field %s", kv.GetKey(), field.GetName())
		}
	}
	return nil
}

// ValidateStructSubFieldsTextMatch applies ValidateStructSubFieldTextMatch to every
// sub-field of the given structs.
func ValidateStructSubFieldsTextMatch(schemas []*schemapb.StructArrayFieldSchema) error {
	for _, structField := range schemas {
		for _, field := range structField.GetFields() {
			if err := ValidateStructSubFieldTextMatch(field); err != nil {
				return err
			}
		}
	}
	return nil
}

// BuildTextStats reports whether a worker can build the index from the stored
// column alone. An evolved sub-field can require a sibling column to obtain
// the lengths of missing rows; QueryNode indexes its reconstructed column.
func BuildTextStats(field *schemapb.FieldSchema) bool {
	return typeutil.CreateFieldSchemaHelper(field).EnableMatch() && !IsAddedStructSubField(field)
}
