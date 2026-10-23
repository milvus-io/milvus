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

package arrowconv

import (
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// AlignComputedStructArray matches children by name and retains the original
// value buffers. It is used before concatenating computed columns from different
// segments; collection-field identity is handled separately by schema field IDs.
// The caller owns the result, and the input is borrowed, including sliced arrays.
func AlignComputedStructArray(column arrow.Array, target *arrow.StructType) (*array.Struct, error) {
	if column == nil || column.DataType().ID() != arrow.STRUCT || target == nil {
		return nil, merr.WrapErrServiceInternalMsg("missing computed StructArray layout")
	}
	source := column.DataType().(*arrow.StructType)
	if source.NumFields() == 0 || source.NumFields() != target.NumFields() {
		return nil, merr.WrapErrServiceInternalMsg("computed StructArray child counts differ")
	}
	indices := make(map[string]int, source.NumFields())
	for i, field := range source.Fields() {
		if _, exists := indices[field.Name]; exists || field.Name == "" || field.Type.ID() != arrow.LIST {
			return nil, merr.WrapErrServiceInternalMsg("invalid computed StructArray child %q", field.Name)
		}
		indices[field.Name] = i
	}
	children := make([]arrow.ArrayData, target.NumFields())
	for i, field := range target.Fields() {
		index, found := indices[field.Name]
		if !found || !field.Equal(source.Field(index)) {
			return nil, merr.WrapErrServiceInternalMsg("inconsistent computed StructArray child %q", field.Name)
		}
		// Keep raw child data together with the original parent offset. Using
		// Struct.Field here would apply a sliced parent's offset twice.
		children[i] = column.Data().Children()[index]
		delete(indices, field.Name)
	}
	data := array.NewData(target, column.Len(), column.Data().Buffers(), children, column.NullN(), column.Data().Offset())
	defer data.Release()
	return array.NewStructData(data), nil
}
