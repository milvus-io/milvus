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
	"context"
	"math"
	"os"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

func Test_GetCPUCoreCount(t *testing.T) {
	mlog.Info(context.TODO(), "TestGetCPUCoreCount",
		mlog.Int("physical CPUCoreCount", GetCPUNum()))
}

func Test_GetCPUUsage(t *testing.T) {
	mlog.Info(context.TODO(), "TestGetCPUUsage",
		mlog.Float64("CPUUsage", GetCPUUsage()))
}

func Test_GetMemoryCount(t *testing.T) {
	mlog.Info(context.TODO(), "TestGetMemoryCount",
		mlog.Uint64("MemoryCount", GetMemoryCount()))

	assert.NotZero(t, GetMemoryCount())
}

func TestGetMemoryCountWarnings(t *testing.T) {
	const hostMemory = uint64(16 << 30)
	cases := []struct {
		name  string
		limit uint64
		err   error
	}{
		{name: "unlimited", limit: math.MaxUint64},
		{name: "above_host", limit: hostMemory * 2},
		{name: "container_read_error", err: os.ErrPermission},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Keep the package's background memory watcher outside these mocks.
			hostMock := mockey.Mock(mem.VirtualMemory).IncludeCurrentGoRoutine().Return(&mem.VirtualMemoryStat{Total: hostMemory}, nil).Build()
			t.Cleanup(func() { hostMock.UnPatch() })
			containerMock := mockey.Mock(getContainerMemLimit).IncludeCurrentGoRoutine().Return(tc.limit, tc.err).Build()
			t.Cleanup(func() { containerMock.UnPatch() })
			var warnings []string
			// Capture the call before rate limiting can suppress it.
			warnMock := mockey.Mock(mlog.RatedWarn).IncludeCurrentGoRoutine().To(func(_ context.Context, _ rate.Limit, msg string, fields ...mlog.Field) {
				warnings = append(warnings, msg)
				if tc.err != nil {
					assert.Contains(t, fields, mlog.Err(tc.err))
				}
			}).Build()
			t.Cleanup(func() { warnMock.UnPatch() })

			assert.Equal(t, hostMemory, GetMemoryCount())
			if tc.err == nil {
				assert.Empty(t, warnings)
			} else {
				assert.Equal(t, []string{"failed to get container memory limit"}, warnings)
			}
		})
	}
}

func Test_GetUsedMemoryCount(t *testing.T) {
	mlog.Info(context.TODO(), "TestGetUsedMemoryCount",
		mlog.Uint64("UsedMemoryCount", GetUsedMemoryCount()))
}

func TestGetDiskUsage(t *testing.T) {
	used, total, err := GetDiskUsage("/")
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, used, 0.0)
	assert.GreaterOrEqual(t, total, 0.0)

	used, total, err = GetDiskUsage("/dir_not_exist")
	assert.NoError(t, err)
	assert.Equal(t, 0.0, used)
	assert.Equal(t, 0.0, total)
}

func TestGetIOWait(t *testing.T) {
	iowait, err := GetIOWait()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, iowait, 0.0)
}

func Test_GetMemoryUsageRatio(t *testing.T) {
	mlog.Info(context.TODO(), "TestGetMemoryUsageRatio",
		mlog.Float64("Memory usage ratio", GetMemoryUseRatio()))
	assert.True(t, GetMemoryUseRatio() > 0)
}
