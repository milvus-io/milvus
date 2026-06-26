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

package metrics

/*
#cgo pkg-config: milvus_core

#include "monitor/monitor_c.h"
*/
import "C"

import "unsafe"

import _ "github.com/milvus-io/milvus/internal/util/cgo"

type CacheShardDiskUsageStats struct {
	DataType  string
	Shard     string
	DiskBytes float64
}

func goString(value *C.char) string {
	if value == nil {
		return ""
	}
	return C.GoString(value)
}

func GetCacheShardDiskUsageStats() []CacheShardDiskUsageStats {
	cStats := C.GetCacheShardDiskUsageStats()
	defer C.DeleteCacheShardDiskUsageStats(cStats)

	if cStats.len <= 0 || cStats.stats == nil {
		return nil
	}

	stats := unsafe.Slice(cStats.stats, int(cStats.len))
	result := make([]CacheShardDiskUsageStats, 0, len(stats))
	for _, stat := range stats {
		result = append(result, CacheShardDiskUsageStats{
			DataType:  goString(stat.data_type),
			Shard:     goString(stat.shard),
			DiskBytes: float64(stat.disk_bytes),
		})
	}
	return result
}
