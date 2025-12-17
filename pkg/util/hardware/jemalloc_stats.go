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
#include "monitor/jemalloc_stats_c.h"
*/
import "C"

// JemallocStats represents comprehensive jemalloc memory statistics
// All sizes are in bytes
type JemallocStats struct {
	// Core memory metrics (from jemalloc)
	Allocated uint64 // Total bytes allocated by the application
	Active    uint64 // Total bytes in active pages (includes fragmentation)
	Metadata  uint64 // Total bytes dedicated to jemalloc metadata
	Resident  uint64 // Total bytes in physically resident data pages (RSS)
	Mapped    uint64 // Total bytes in virtual memory mappings
	Retained  uint64 // Total bytes in retained virtual memory (could be returned to OS)

	// Derived metrics (calculated by C code)
	Fragmentation uint64 // Internal fragmentation (active - allocated)
	Overhead      uint64 // Memory overhead (resident - active)

	// Status
	Success bool // Whether stats were successfully retrieved
}

// GetJemallocStats retrieves comprehensive jemalloc memory statistics
// Returns JemallocStats with detailed memory information
// On platforms without jemalloc support, all metrics will be 0 and Success will be false
func GetJemallocStats() JemallocStats {
	cStats := C.GetJemallocStats()

	return JemallocStats{
		Allocated:     uint64(cStats.allocated),
		Active:        uint64(cStats.active),
		Metadata:      uint64(cStats.metadata),
		Resident:      uint64(cStats.resident),
		Mapped:        uint64(cStats.mapped),
		Retained:      uint64(cStats.retained),
		Fragmentation: uint64(cStats.fragmentation),
		Overhead:      uint64(cStats.overhead),
		Success:       bool(cStats.success),
	}
}
