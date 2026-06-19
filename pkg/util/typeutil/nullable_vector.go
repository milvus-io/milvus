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

package typeutil

import "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

// IsSupportedNullableVectorType reports whether this vector type currently
// supports nullable rows. Supported nullable vector payload data is compact:
// ValidData has one entry per logical row, while vector data only contains
// valid rows.
func IsSupportedNullableVectorType(dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_FloatVector,
		schemapb.DataType_BinaryVector,
		schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector,
		schemapb.DataType_Int8Vector,
		schemapb.DataType_SparseFloatVector:
		return true
	default:
		return false
	}
}

// BuildNullableVectorDataIndices maps logical row indexes to compact vector
// payload indexes. Null rows are mapped to -1. The returned validCount is the
// number of physical vector entries expected in compact payload data.
func BuildNullableVectorDataIndices(validData []bool) ([]int, int) {
	indices := make([]int, len(validData))
	validCount := 0
	for i, valid := range validData {
		if valid {
			indices[i] = validCount
			validCount++
		} else {
			indices[i] = -1
		}
	}
	return indices, validCount
}
