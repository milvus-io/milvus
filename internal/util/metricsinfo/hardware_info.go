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

import (
	"github.com/milvus-io/milvus/internal/log"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	"go.uber.org/zap"
)

func GetCPUCoreCount(logical bool) int {
	c, err := cpu.Counts(logical)
	if err != nil {
		log.Warn("failed to get cpu counts",
			zap.Error(err))
		return 0
	}

	return c
}

func GetCPUUsage() float64 {
	percents, err := cpu.Percent(0, false)
	if err != nil {
		log.Warn("failed to get cpu usage",
			zap.Error(err))
		return 0
	}

	if len(percents) != 1 {
		log.Warn("something wrong in cpu.Percent, len(percents) must be equal to 1",
			zap.Int("len(percents)", len(percents)))
		return 0
	}

	return percents[0]
}

func GetMemoryCount() uint64 {
	stats, err := mem.VirtualMemory()
	if err != nil {
		log.Warn("failed to get memory count",
			zap.Error(err))
		return 0
	}

	return stats.Total
}

func GetUsedMemoryCount() uint64 {
	stats, err := mem.VirtualMemory()
	if err != nil {
		log.Warn("failed to get memory usage count",
			zap.Error(err))
		return 0
	}

	return stats.Used
}

// TODO(dragondriver): not accurate to calculate disk usage when we use distributed storage
func GetDiskCount() uint64 {
	return 100 * 1024 * 1024
}

// TODO(dragondriver): not accurate to calculate disk usage when we use distributed storage
func GetDiskUsage() uint64 {
	return 2 * 1024 * 1024
}
