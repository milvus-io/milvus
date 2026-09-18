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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func stubMemory(t *testing.T, used, total uint64) {
	oldUsed, oldTotal := getUsedMemory, getTotalMemory
	getUsedMemory = func() uint64 { return used }
	getTotalMemory = func() uint64 { return total }
	t.Cleanup(func() {
		getUsedMemory, getTotalMemory = oldUsed, oldTotal
	})
}

func setMemProtection(t *testing.T, enabled, waterLevel string) {
	pt := paramtable.Get()
	pt.Save(pt.QueryNodeCfg.SchedulerMemProtectionEnabled.Key, enabled)
	pt.Save(pt.QueryNodeCfg.SchedulerMemProtectionWaterLevel.Key, waterLevel)
	t.Cleanup(func() {
		pt.Reset(pt.QueryNodeCfg.SchedulerMemProtectionEnabled.Key)
		pt.Reset(pt.QueryNodeCfg.SchedulerMemProtectionWaterLevel.Key)
	})
}

func TestMemProtectorDisabledByDefault(t *testing.T) {
	paramtable.Init()
	stubMemory(t, 100, 100)
	p := &memProtector{}
	assert.False(t, p.shouldReject(time.Now()))
}

func TestMemProtectorRejectsAboveWaterLevel(t *testing.T) {
	paramtable.Init()
	setMemProtection(t, "true", "0.9")
	stubMemory(t, 95, 100)
	p := &memProtector{}
	assert.True(t, p.shouldReject(time.Now()))
}

func TestMemProtectorAllowsBelowWaterLevel(t *testing.T) {
	paramtable.Init()
	setMemProtection(t, "true", "0.9")
	stubMemory(t, 50, 100)
	p := &memProtector{}
	assert.False(t, p.shouldReject(time.Now()))
}

func TestMemProtectorInvalidWaterLevelDisables(t *testing.T) {
	paramtable.Init()
	stubMemory(t, 100, 100)
	for _, level := range []string{"0", "-0.5", "1.5"} {
		setMemProtection(t, "true", level)
		p := &memProtector{}
		assert.False(t, p.shouldReject(time.Now()), "waterLevel=%s", level)
	}
}

func TestMemProtectorCachesBetweenRefreshes(t *testing.T) {
	paramtable.Init()
	setMemProtection(t, "true", "0.9")
	stubMemory(t, 95, 100)
	p := &memProtector{}
	t0 := time.Now()
	assert.True(t, p.shouldReject(t0))

	// A reading change within the refresh interval must not flip the cached verdict.
	getUsedMemory = func() uint64 { return 10 }
	assert.True(t, p.shouldReject(t0))

	// After the interval elapses the verdict refreshes from the new reading.
	assert.False(t, p.shouldReject(t0.Add(2*memProtectionRefreshInterval)))
}

func TestSchedulerRejectsTaskUnderMemoryPressure(t *testing.T) {
	paramtable.Init()
	setMemProtection(t, "true", "0.9")
	stubMemory(t, 95, 100)

	scheduler := newScheduler(newFIFOPolicy())
	scheduler.Start()
	defer scheduler.Stop()

	task := newMockTask(mockTaskConfig{
		nq:          1,
		executeCost: time.Millisecond,
		execution: func(ctx context.Context) error {
			return nil
		},
	})
	err := scheduler.Add(task)
	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
}
