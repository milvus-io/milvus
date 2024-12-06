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

package hardware

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
)

func Test_GetCPUCoreCount(t *testing.T) {
	log.Info("TestGetCPUCoreCount",
		zap.Int("physical CPUCoreCount", GetCPUNum()))
}

func Test_GetCPUUsage(t *testing.T) {
	log.Info("TestGetCPUUsage",
		zap.Float64("CPUUsage", GetCPUUsage()))
}

func Test_GetMemoryCount(t *testing.T) {
	log.Info("TestGetMemoryCount",
		zap.Uint64("MemoryCount", GetMemoryCount()))

	assert.NotZero(t, GetMemoryCount())
}

func Test_GetUsedMemoryCount(t *testing.T) {
	log.Info("TestGetUsedMemoryCount",
		zap.Uint64("UsedMemoryCount", GetUsedMemoryCount()))
}

func TestGetDiskUsage(t *testing.T) {
	used, total, err := GetDiskUsage("/")
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, used, 0.0)
	assert.GreaterOrEqual(t, total, 0.0)
}

func TestGetIOWait(t *testing.T) {
	iowait, err := GetIOWait()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, iowait, 0.0)
}

func Test_GetMemoryUsageRatio(t *testing.T) {
	log.Info("TestGetMemoryUsageRatio",
		zap.Float64("Memory usage ratio", GetMemoryUseRatio()))
	assert.True(t, GetMemoryUseRatio() > 0)
}
