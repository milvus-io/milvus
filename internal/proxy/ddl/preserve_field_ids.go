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
)

// isPreserveFieldIDs reports whether a CreateCollection request asks RootCoord
// to keep the field IDs of its schema (the preserve_field_ids property).
func isPreserveFieldIDs(properties []*commonpb.KeyValuePair) bool {
	value, ok := funcutil.TryGetAttrByKeyFromRepeatedKV(util.PreserveFieldIdsKey, properties)
	return ok && value == "true"
}

// takePreservedSystemFields removes the dynamic field ($meta) and the
// namespace field ($namespace_id) from a schema with preserved field IDs and
// returns them. They carry only the source field IDs: RootCoord creates both
// fields itself and keeps these IDs, so they skip the user-field validation
// and are put back before the request is forwarded.
func takePreservedSystemFields(schema *schemapb.CollectionSchema) ([]*schemapb.FieldSchema, error) {
	var taken []*schemapb.FieldSchema
	fields := make([]*schemapb.FieldSchema, 0, len(schema.GetFields()))
	for _, field := range schema.GetFields() {
		switch field.GetName() {
		case common.MetaFieldName:
			if !field.GetIsDynamic() || field.GetDataType() != schemapb.DataType_JSON {
				return nil, merr.WrapErrParameterInvalidMsg(
					"with %s=true the dynamic field %s must have is_dynamic=true and data type JSON; only its field ID is used",
					util.PreserveFieldIdsKey, common.MetaFieldName)
			}
			taken = append(taken, field)
		case common.NamespaceFieldName:
			if field.GetDataType() != schemapb.DataType_VarChar {
				return nil, merr.WrapErrParameterInvalidMsg(
					"with %s=true the namespace field %s must have data type VarChar; only its field ID is used",
					util.PreserveFieldIdsKey, common.NamespaceFieldName)
			}
			taken = append(taken, field)
		default:
			fields = append(fields, field)
		}
	}
	schema.Fields = fields
	return taken, nil
}
