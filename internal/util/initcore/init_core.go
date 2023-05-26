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

package initcore

/*
#cgo pkg-config: milvus_common

#include <stdlib.h>
#include <stdint.h>
#include "common/init_c.h"
*/
import "C"

import (
	"unsafe"

	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func InitLocalStorageConfig(path string) {
	CLocalRootPath := C.CString(path)
	C.InitLocalRootPath(CLocalRootPath)
	C.free(unsafe.Pointer(CLocalRootPath))
}

func InitTraceConfig(params *paramtable.ComponentParam) {
	config := C.CTraceConfig{
		exporter:       C.CString(params.TraceCfg.Exporter.GetValue()),
		sampleFraction: C.int(params.TraceCfg.SampleFraction.GetAsInt()),
		jaegerURL:      C.CString(params.TraceCfg.JaegerURL.GetValue()),
		otlpEndpoint:   C.CString(params.TraceCfg.OtlpEndpoint.GetValue()),
		nodeID:         C.int(paramtable.GetNodeID()),
	}
	C.InitTrace(&config)
}
