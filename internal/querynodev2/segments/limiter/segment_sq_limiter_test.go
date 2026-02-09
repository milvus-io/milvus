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

package limiter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestConcurrentLimiter(t *testing.T) {
	l := newConcurrentLimiter()
	assert.Equal(t, int64(-1), l.GetCurrentConcurrent())

	paramtable.Get().QueryNodeCfg.SchedulePolicyName.SwapTempValue("user-task-polling")
	cpuNum := []int{1, 2, 4, 8, 16, 32, 64, 128, 256}

	for _, cpu := range cpuNum {
		threshold, err := getThreshold(hardware.SystemMetrics{
			CPUNum: cpu,
		})
		require.NoError(t, err)

		l.UpdateLimit(hardware.SystemMetrics{
			CPUNum:          cpu,
			AverageCPUUsage: threshold.hwmThreshold,
		})
		assert.Equal(t, threshold.hwmConcurrencyLimit, l.GetCurrentConcurrent())

		l.UpdateLimit(hardware.SystemMetrics{
			CPUNum:          cpu,
			AverageCPUUsage: threshold.lwmThreshold,
		})
		assert.Equal(t, threshold.lwmConcurrencyLimit, l.GetCurrentConcurrent())

		l.UpdateLimit(hardware.SystemMetrics{
			CPUNum:          cpu,
			AverageCPUUsage: (threshold.lwmThreshold + threshold.hwmThreshold) / 2,
		})
		assert.Equal(t, (threshold.lwmConcurrencyLimit+threshold.hwmConcurrencyLimit)/2, l.GetCurrentConcurrent())
	}
}
