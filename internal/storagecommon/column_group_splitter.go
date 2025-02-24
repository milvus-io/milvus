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

package storagecommon

import "github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

type ColumnGroupSplitter struct {
	schema *schemapb.CollectionSchema
}

type ColumnGroup struct {
	Columns []int
}

func NewColumnGroupSplitter(schema *schemapb.CollectionSchema) *ColumnGroupSplitter {
	return &ColumnGroupSplitter{schema: schema}
}

func (s *ColumnGroupSplitter) SplitByFieldType() []ColumnGroup {
	groups := make([]ColumnGroup, 0)
	shortColumnGroups := make([]int, 0)
	for i, field := range s.schema.Fields {
		if isShortField(field) {
			shortColumnGroups = append(shortColumnGroups, i)
		} else {
			groups = append(groups, ColumnGroup{Columns: []int{i}})
		}
	}
	groups = append(groups, ColumnGroup{Columns: shortColumnGroups})
	return groups
}

func isShortField(field *schemapb.FieldSchema) bool {
	switch field.DataType {
	case schemapb.DataType_Bool, schemapb.DataType_Int8, schemapb.DataType_Int16,
		schemapb.DataType_Int32, schemapb.DataType_Int64,
		schemapb.DataType_Float, schemapb.DataType_Double:
		return true
	default:
		return false
	}
}
