// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package metricsinfo

/*
#cgo pkg-config: milvus_common milvus_segcore

#include <stdlib.h>
#include "common/vector_index_c.h"
#include "common/memory_c.h"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

func PurgeMemory(maxBinsSize uint64) (bool, error) {
	var res C.int32_t
	cMaxBinsSize := C.uint64_t(maxBinsSize)
	status := C.PurgeMemory(cMaxBinsSize, &res)
	if status.error_code == 0 {
		return int32(res) > 0, nil
	}
	defer C.free(unsafe.Pointer(status.error_msg))

	errorMsg := string(C.GoString(status.error_msg))
	errorCode := int32(status.error_code)

	return false, fmt.Errorf("PurgeMemory failed, errorCode = %d, errorMsg = %s", errorCode, errorMsg)
}
