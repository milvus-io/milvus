// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

//go:build linux || darwin

package hardware

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include "common/jemalloc_stats_c.h"
*/
import "C"

// JemallocStats represents jemalloc memory statistics
type JemallocStats struct {
	Allocated uint64 // Actually used memory by the application
	Resident  uint64 // Physical memory held by the process (RSS)
	Cached    uint64 // Cached/unreturned memory (resident - allocated)
	Available bool   // Whether jemalloc stats are available
}

// GetJemallocStats retrieves jemalloc memory statistics
// Returns JemallocStats with memory information
func GetJemallocStats() JemallocStats {
	cStats := C.GetJemallocStats()

	return JemallocStats{
		Allocated: uint64(cStats.allocated),
		Resident:  uint64(cStats.resident),
		Cached:    uint64(cStats.cached),
		Available: cStats.available != 0,
	}
}
