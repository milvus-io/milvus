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
#cgo pkg-config: milvus_core
#include "segcore/check_vec_index_c.h"
#include <stdlib.h>
*/
import "C"

import (
	"runtime"
	"sync"
	"unsafe"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
)

var (
	dp      atomic.Pointer[conc.Pool[any]]
	dynOnce sync.Once
)

func initDynamicPool() {
	dynOnce.Do(func() {
		pool := conc.NewPool[any](
			hardware.GetCPUNum(),
			conc.WithPreAlloc(false),
			conc.WithDisablePurge(false),
			conc.WithPreHandler(runtime.LockOSThread), // lock os thread for cgo thread disposal
		)

		dp.Store(pool)
		log.Info("init dynamicPool done", zap.Int("size", hardware.GetCPUNum()))
	})
}

// GetDynamicPool returns the singleton pool for dynamic cgo operations.
func GetDynamicPool() *conc.Pool[any] {
	initDynamicPool()
	return dp.Load()
}

func CheckVecIndexWithDataTypeExist(name string, dType schemapb.DataType) bool {
	var result bool
	GetDynamicPool().Submit(func() (any, error) {
		cIndexName := C.CString(name)
		cType := uint32(dType)
		defer C.free(unsafe.Pointer(cIndexName))
		result = bool(C.CheckVecIndexWithDataType(cIndexName, cType))
		return nil, nil
	}).Await()

	return result
}
