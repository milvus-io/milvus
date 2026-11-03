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
)

// RejectStructSubFieldTextMatch rejects enable_match and enable_analyzer on a
// struct sub-field, including the levels of a nested Array type_schema. Text
// indexes are only built for top-level fields, so on a sub-field these params
// would be stored and silently ignored.
//
// Call it only on sub-fields a user supplies: earlier versions accepted these
// params, so a stored struct may carry them, and a snapshot restore or a
// struct that gets a new sub-field must keep working.
func RejectStructSubFieldTextMatch(field *schemapb.FieldSchema) error {
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

// RejectStructSubFieldsTextMatch applies RejectStructSubFieldTextMatch to every
// sub-field of the given structs.
func RejectStructSubFieldsTextMatch(schemas []*schemapb.StructArrayFieldSchema) error {
	for _, structField := range schemas {
		for _, field := range structField.GetFields() {
			if err := RejectStructSubFieldTextMatch(field); err != nil {
				return err
			}
		}
	}
	return nil
}
