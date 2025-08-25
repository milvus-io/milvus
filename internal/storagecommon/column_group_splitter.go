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

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	// column group id for short columns
	DefaultShortColumnGroupID = 0
)

type ColumnGroup struct {
	GroupID typeutil.UniqueID
	Columns []int // column indices
}

// SplitBySchema is a generic function to split columns by schema based on data type
func SplitBySchema(fields []*schemapb.FieldSchema) []ColumnGroup {
	groups := make([]ColumnGroup, 0)
	shortColumnGroup := ColumnGroup{Columns: make([]int, 0), GroupID: DefaultShortColumnGroupID}
	for i, field := range fields {
		if IsVectorDataType(field.DataType) || field.DataType == schemapb.DataType_Text {
			groups = append(groups, ColumnGroup{Columns: []int{i}, GroupID: field.GetFieldID()})
		} else {
			shortColumnGroup.Columns = append(shortColumnGroup.Columns, i)
		}
	}
	if len(shortColumnGroup.Columns) > 0 {
		groups = append([]ColumnGroup{shortColumnGroup}, groups...)
	}
	return groups
}

func IsVectorDataType(dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_BinaryVector,
		schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector,
		schemapb.DataType_Int8Vector,
		schemapb.DataType_FloatVector,
		schemapb.DataType_SparseFloatVector,
		schemapb.DataType_ArrayOfVector:
		return true
	}
	return false
}
