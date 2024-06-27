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
	"github.com/milvus-io/milvus/pkg/util/indexparamcheck"
)

func Test_CheckVecIndexWithDataTypeExist(t *testing.T) {
	cases := []struct {
		indexType string
		dataType  schemapb.DataType
		want      bool
	}{
		{indexparamcheck.IndexHNSW, schemapb.DataType_FloatVector, true},
		{indexparamcheck.IndexHNSW, schemapb.DataType_BinaryVector, false},
		{indexparamcheck.IndexHNSW, schemapb.DataType_Float16Vector, true},

		{indexparamcheck.IndexSparseWand, schemapb.DataType_SparseFloatVector, true},
		{indexparamcheck.IndexSparseWand, schemapb.DataType_FloatVector, false},
		{indexparamcheck.IndexSparseWand, schemapb.DataType_Float16Vector, false},

		{indexparamcheck.IndexGpuBF, schemapb.DataType_FloatVector, true},
		{indexparamcheck.IndexGpuBF, schemapb.DataType_Float16Vector, false},
		{indexparamcheck.IndexGpuBF, schemapb.DataType_BinaryVector, false},

		{indexparamcheck.IndexFaissBinIvfFlat, schemapb.DataType_BinaryVector, true},
		{indexparamcheck.IndexFaissBinIvfFlat, schemapb.DataType_FloatVector, false},

		{indexparamcheck.IndexDISKANN, schemapb.DataType_FloatVector, true},
		{indexparamcheck.IndexDISKANN, schemapb.DataType_Float16Vector, true},
		{indexparamcheck.IndexDISKANN, schemapb.DataType_BFloat16Vector, true},
		{indexparamcheck.IndexDISKANN, schemapb.DataType_BinaryVector, false},
	}

	for _, test := range cases {
		if got := CheckVecIndexWithDataTypeExist(test.indexType, test.dataType); got != test.want {
			t.Errorf("CheckVecIndexWithDataTypeExist(%v, %v) = %v", test.indexType, test.dataType, test.want)
		}
	}
}
