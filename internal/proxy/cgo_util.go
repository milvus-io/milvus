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

/*
#cgo pkg-config: milvus_segcore
#include "segcore/check_vec_index_c.h"
#include <stdlib.h>
*/
import "C"

import (
	"unsafe"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func CheckVecIndexWithDataTypeExist(name string, dType schemapb.DataType) bool {
	cIndexName := C.CString(name)
	cType := uint32(dType)
	defer C.free(unsafe.Pointer(cIndexName))
	check := bool(C.CheckVecIndexWithDataType(cIndexName, cType))
	return check
}
