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
#cgo pkg-config: milvus_core

#include "futures/future_c.h"
*/
import "C"

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const defaultJSONStatsBuildMaxWorkers = 8

// ResolveJSONStatsBuildExecutorPoolSize returns the process-wide hard worker
// limit after applying the safe automatic default and the cgroup-aware CPU cap.
func ResolveJSONStatsBuildExecutorPoolSize(pt *paramtable.ComponentParam) int {
	cpuNum := hardware.GetCPUNum()
	if cpuNum < 1 {
		cpuNum = 1
	}

	maxWorkers := pt.DataNodeCfg.JSONStatsBuildMaxWorkers.GetAsInt()
	if maxWorkers <= 0 {
		maxWorkers = defaultJSONStatsBuildMaxWorkers
	}
	if maxWorkers > cpuNum {
		maxWorkers = cpuNum
	}
	return maxWorkers
}

// UpdateJSONStatsBuildExecutorPoolSize resizes the C++ Folly executor.
func UpdateJSONStatsBuildExecutorPoolSize(threads int) {
	if threads < 1 {
		threads = 1
	}
	C.executor_set_json_stats_build_thread_num(C.int(threads))
}

// ResolveJSONStatsBuildMaxInflightBytes returns the process-wide materialized
// chunk budget. Zero disables the byte limit.
func ResolveJSONStatsBuildMaxInflightBytes(pt *paramtable.ComponentParam) int64 {
	maxInflightBytes := pt.DataNodeCfg.JSONStatsBuildMaxInflightBytes.GetAsInt64()
	if maxInflightBytes < 0 {
		return paramtable.DefaultJSONStatsBuildMaxInflightBytes
	}
	return maxInflightBytes
}

// UpdateJSONStatsBuildMaxInflightBytes updates the C++ shared byte budget.
func UpdateJSONStatsBuildMaxInflightBytes(maxInflightBytes int64) {
	if maxInflightBytes < 0 {
		maxInflightBytes = paramtable.DefaultJSONStatsBuildMaxInflightBytes
	}
	C.executor_set_json_stats_build_max_inflight_bytes(C.int64_t(maxInflightBytes))
}

// RegisterJSONStatsBuildExecutorWatcher applies maxWorkers updates without
// interrupting tasks already running in the executor.
func RegisterJSONStatsBuildExecutorWatcher(pt *paramtable.ComponentParam, source string) {
	key := pt.DataNodeCfg.JSONStatsBuildMaxWorkers.Key
	handler := func(evt *config.Event) {
		if !evt.HasUpdated {
			return
		}
		threads := ResolveJSONStatsBuildExecutorPoolSize(pt)
		UpdateJSONStatsBuildExecutorPoolSize(threads)
		mlog.Info(context.TODO(), "json stats build executor resized",
			mlog.String("source", source),
			mlog.String("trigger", key),
			mlog.Int("threads", threads))
	}
	pt.Watch(key, config.NewHandler("json-stats-build-executor."+source+"."+key, handler))
}

// RegisterJSONStatsBuildMemoryBudgetWatcher applies maxInflightBytes updates.
// Shrinking the budget does not interrupt active chunks; it only blocks later
// dispatch until current reservations fall below the new capacity.
func RegisterJSONStatsBuildMemoryBudgetWatcher(pt *paramtable.ComponentParam, source string) {
	key := pt.DataNodeCfg.JSONStatsBuildMaxInflightBytes.Key
	handler := func(evt *config.Event) {
		if !evt.HasUpdated {
			return
		}
		maxInflightBytes := ResolveJSONStatsBuildMaxInflightBytes(pt)
		UpdateJSONStatsBuildMaxInflightBytes(maxInflightBytes)
		mlog.Info(context.TODO(), "json stats build memory budget resized",
			mlog.String("source", source),
			mlog.String("trigger", key),
			mlog.Int64("maxInflightBytes", maxInflightBytes))
	}
	pt.Watch(key, config.NewHandler("json-stats-build-memory-budget."+source+"."+key, handler))
}
