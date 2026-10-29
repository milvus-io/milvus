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

package ddl

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// The checks in this file reject element-nullable and nested Array fields at
// the proxy entries that accept a field schema, until the feature is complete.
// proxy.enableElementNullAndNestedArray turns them off for tests. The file and
// its call sites are removed once the feature is complete.

func arrayFeatureGateEnabled() bool {
	return !paramtable.Get().ProxyCfg.EnableElementNullAndNestedArray.GetAsBool()
}

// checkArrayFeatureGate checks every field and struct sub-field of schema.
func checkArrayFeatureGate(schema *schemapb.CollectionSchema) error {
	if !arrayFeatureGateEnabled() {
		return nil
	}
	for _, field := range schema.GetFields() {
		if err := checkArrayFeatureGateField(field); err != nil {
			return err
		}
	}
	for _, structField := range schema.GetStructArrayFields() {
		if err := checkArrayFeatureGateStruct(structField); err != nil {
			return err
		}
	}
	return nil
}

// checkArrayFeatureGateStruct checks every sub-field of a struct array field.
func checkArrayFeatureGateStruct(structField *schemapb.StructArrayFieldSchema) error {
	if !arrayFeatureGateEnabled() {
		return nil
	}
	for _, field := range structField.GetFields() {
		if err := checkArrayFeatureGateField(field); err != nil {
			return err
		}
	}
	return nil
}

// checkArrayFeatureGateField rejects an element-nullable or nested Array field.
// A type_schema is only valid on a nested Array, so it counts as one.
func checkArrayFeatureGateField(field *schemapb.FieldSchema) error {
	if !arrayFeatureGateEnabled() || field == nil {
		return nil
	}
	if field.GetElementNullable() ||
		field.GetElementType() == schemapb.DataType_Array ||
		field.GetTypeSchema() != nil {
		return merr.WrapErrParameterInvalidMsg(
			"element_nullable and nested Array fields are not supported yet, field name = %s", field.GetName())
	}
	return nil
}
