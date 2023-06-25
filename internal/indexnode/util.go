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

package indexnode

import (
	"unsafe"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func estimateFieldDataSize(dim int64, numRows int64, dataType schemapb.DataType) (uint64, error) {
	if dataType == schemapb.DataType_FloatVector {
		var value float32
		/* #nosec G103 */
		return uint64(dim) * uint64(numRows) * uint64(unsafe.Sizeof(value)), nil
	}
	if dataType == schemapb.DataType_BinaryVector {
		return uint64(dim) / 8 * uint64(numRows), nil
	}

	return 0, nil
}
