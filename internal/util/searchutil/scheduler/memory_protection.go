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

package scheduler

import (
	"time"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Package-level indirections so unit tests can stub hardware readings.
var (
	getUsedMemory  = hardware.GetUsedMemoryCount
	getTotalMemory = hardware.GetMemoryCount
)

// memProtectionRefreshInterval bounds how often admission re-reads process memory.
const memProtectionRefreshInterval = 100 * time.Millisecond

// memProtector caches memory readings so per-task admission stays syscall-free.
type memProtector struct {
	lastRefresh atomic.Int64
	rejecting   atomic.Bool
}

// shouldReject reports whether new read tasks must be rejected due to memory pressure.
func (p *memProtector) shouldReject(now time.Time) bool {
	cfg := &paramtable.Get().QueryNodeCfg
	if !cfg.SchedulerMemProtectionEnabled.GetAsBool() {
		return false
	}
	waterLevel := cfg.SchedulerMemProtectionWaterLevel.GetAsFloat()
	if waterLevel <= 0 || waterLevel > 1 {
		return false
	}
	last := p.lastRefresh.Load()
	if now.UnixNano()-last >= int64(memProtectionRefreshInterval) && p.lastRefresh.CompareAndSwap(last, now.UnixNano()) {
		total := getTotalMemory()
		p.rejecting.Store(total > 0 && float64(getUsedMemory()) >= waterLevel*float64(total))
	}
	return p.rejecting.Load()
}
