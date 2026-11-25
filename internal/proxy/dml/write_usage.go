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

package dml

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

// writeRequestBytes measures client fields before Proxy fills defaults or
// generates fields. Resolve types from the same schema used for the write.
func writeRequestBytes(fields []*schemapb.FieldData, schema *schemaInfo) (int64, error) {
	var total int64
	for _, field := range fields {
		name := field.GetFieldName()
		if data := field.GetStructArrays(); data != nil {
			parent := schema.SchemaHelper.GetStructArrayFieldFromName(name)
			for _, child := range data.GetFields() {
				n, err := storage.LogicalWriteFieldBytes(child, findStructChildSchema(parent, child.GetFieldName()))
				if err != nil {
					return 0, err
				}
				total += int64(n)
			}
			continue
		}
		if field.GetIsDynamic() {
			name = common.MetaFieldName
		}
		fieldSchema, err := schema.SchemaHelper.GetUserFieldFromNameDefaultJSON(name)
		if err != nil {
			return 0, err
		}
		n, err := storage.LogicalWriteFieldBytes(field, fieldSchema)
		if err != nil {
			return 0, err
		}
		total += int64(n)
	}
	return total, nil
}

func primaryKeyBytes(ids *schemapb.IDs) int64 {
	total := int64(len(ids.GetIntId().GetData())) * 8
	for _, id := range ids.GetStrId().GetData() {
		total += int64(len(id))
	}
	return total
}
