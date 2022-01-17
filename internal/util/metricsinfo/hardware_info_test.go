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
	"testing"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

func Test_GetCPUCoreCount(t *testing.T) {
	log.Info("TestGetCPUCoreCount",
		zap.Int("physical CPUCoreCount", GetCPUCoreCount(false)))

	log.Info("TestGetCPUCoreCount",
		zap.Int("logical CPUCoreCount", GetCPUCoreCount(true)))
}

func Test_GetCPUUsage(t *testing.T) {
	log.Info("TestGetCPUUsage",
		zap.Float64("CPUUsage", GetCPUUsage()))
}

func Test_GetMemoryCount(t *testing.T) {
	log.Info("TestGetMemoryCount",
		zap.Uint64("MemoryCount", GetMemoryCount()))
}

func Test_GetUsedMemoryCount(t *testing.T) {
	log.Info("TestGetUsedMemoryCount",
		zap.Uint64("UsedMemoryCount", GetUsedMemoryCount()))
}

func Test_GetDiskCount(t *testing.T) {
	log.Info("TestGetDiskCount",
		zap.Uint64("DiskCount", GetDiskCount()))
}

func Test_GetDiskUsage(t *testing.T) {
	log.Info("TestGetDiskUsage",
		zap.Uint64("DiskUsage", GetDiskUsage()))
}
