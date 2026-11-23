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
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// isPreserveFieldIDs reports whether a CreateCollection request asks RootCoord
// to keep the field IDs of its schema (the preserve_field_ids property).
func isPreserveFieldIDs(properties []*commonpb.KeyValuePair) bool {
	value, ok := funcutil.TryGetAttrByKeyFromRepeatedKV(util.PreserveFieldIdsKey, properties)
	return ok && value == "true"
}

// takePreservedSystemFields removes the dynamic field ($meta) and the
// namespace field ($namespace_id) from a schema with preserved field IDs and
// returns canonical forwarding stubs carrying only their source field IDs and
// system roles. RootCoord creates both fields itself and keeps these IDs, so
// they skip user-field validation while preserving partition-key semantics when
// forwarded.
func takePreservedSystemFields(schema *schemapb.CollectionSchema, properties []*commonpb.KeyValuePair) ([]*schemapb.FieldSchema, error) {
	var taken []*schemapb.FieldSchema
	fields := make([]*schemapb.FieldSchema, 0, len(schema.GetFields()))
	metaCount, namespaceCount := 0, 0
	for _, field := range schema.GetFields() {
		switch field.GetName() {
		case common.MetaFieldName:
			metaCount++
			if metaCount > 1 {
				return nil, merr.WrapErrParameterInvalidMsg("the schema contains more than one %s field", common.MetaFieldName)
			}
			if !schema.GetEnableDynamicField() {
				return nil, merr.WrapErrParameterInvalidMsg(
					"the schema contains the dynamic field %s but collection %s does not enable the dynamic field",
					common.MetaFieldName, schema.GetName())
			}
			if !field.GetIsDynamic() || field.GetDataType() != schemapb.DataType_JSON {
				return nil, merr.WrapErrParameterInvalidMsg(
					"with %s=true the dynamic field %s must have is_dynamic=true and data type JSON; only its field ID is used",
					util.PreserveFieldIdsKey, common.MetaFieldName)
			}
			taken = append(taken, &schemapb.FieldSchema{
				FieldID:   field.GetFieldID(),
				Name:      common.MetaFieldName,
				DataType:  schemapb.DataType_JSON,
				IsDynamic: true,
			})
		case common.NamespaceFieldName:
			namespaceCount++
			if namespaceCount > 1 {
				return nil, merr.WrapErrParameterInvalidMsg("the schema contains more than one %s field", common.NamespaceFieldName)
			}
			if err := common.ValidateNamespaceMode(properties...); err != nil {
				return nil, err
			}
			if !schema.GetEnableNamespace() || typeutil.IsExternalCollection(schema) || !common.IsNamespaceModePartitionKey(properties...) {
				return nil, merr.WrapErrParameterInvalidMsg(
					"the schema contains the namespace field %s but the collection does not get a namespace field",
					common.NamespaceFieldName)
			}
			if field.GetDataType() != schemapb.DataType_VarChar {
				return nil, merr.WrapErrParameterInvalidMsg(
					"with %s=true the namespace field %s must have data type VarChar; only its field ID is used",
					util.PreserveFieldIdsKey, common.NamespaceFieldName)
			}
			taken = append(taken, &schemapb.FieldSchema{
				FieldID:        field.GetFieldID(),
				Name:           common.NamespaceFieldName,
				DataType:       schemapb.DataType_VarChar,
				IsPartitionKey: true,
			})
		default:
			fields = append(fields, field)
		}
	}
	if schema.GetEnableDynamicField() && metaCount == 0 {
		return nil, merr.WrapErrParameterInvalidMsg(
			"collection %s enables the dynamic field, so with %s=true the schema fields must include the dynamic field: "+
				"name %s, is_dynamic=true, data type JSON, and the source field ID. Only its field ID is used; "+
				"Milvus creates the field with its standard attributes",
			schema.GetName(), util.PreserveFieldIdsKey, common.MetaFieldName)
	}
	if schema.GetEnableNamespace() && !typeutil.IsExternalCollection(schema) {
		if err := common.ValidateNamespaceMode(properties...); err != nil {
			return nil, err
		}
		if common.IsNamespaceModePartitionKey(properties...) && namespaceCount == 0 {
			return nil, merr.WrapErrParameterInvalidMsg(
				"the collection gets a namespace field, so with %s=true the schema fields must include it: "+
					"name %s, data type VarChar, and the source field ID. Only its field ID is used; "+
					"Milvus creates the field with its standard attributes",
				util.PreserveFieldIdsKey, common.NamespaceFieldName)
		}
	}
	schema.Fields = fields
	return taken, nil
}
