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

package proxy

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func Test_CheckVecIndexWithDataTypeExist(t *testing.T) {
	cases := []struct {
		indexType string
		dataType  schemapb.DataType
		want      bool
	}{
		{"HNSW", schemapb.DataType_FloatVector, true},
		{"HNSW", schemapb.DataType_BinaryVector, false},
		{"HNSW", schemapb.DataType_Float16Vector, true},

		{"SPARSE_WAND", schemapb.DataType_SparseFloatVector, true},
		{"SPARSE_WAND", schemapb.DataType_FloatVector, false},
		{"SPARSE_WAND", schemapb.DataType_Float16Vector, false},

		{"GPU_BRUTE_FORCE", schemapb.DataType_FloatVector, true},
		{"GPU_BRUTE_FORCE", schemapb.DataType_Float16Vector, false},
		{"GPU_BRUTE_FORCE", schemapb.DataType_BinaryVector, false},

		{"BIN_IVF_FLAT", schemapb.DataType_BinaryVector, true},
		{"BIN_IVF_FLAT", schemapb.DataType_FloatVector, false},

		{"DISKANN", schemapb.DataType_FloatVector, true},
		{"DISKANN", schemapb.DataType_Float16Vector, true},
		{"DISKANN", schemapb.DataType_BFloat16Vector, true},
		{"DISKANN", schemapb.DataType_BinaryVector, false},
	}

	for _, test := range cases {
		if got := CheckVecIndexWithDataTypeExist(test.indexType, test.dataType); got != test.want {
			t.Errorf("CheckVecIndexWithDataTypeExist(%v, %v) = %v", test.indexType, test.dataType, test.want)
		}
	}
}
