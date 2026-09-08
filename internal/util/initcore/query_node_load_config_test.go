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

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Read the native controller's effective capacities through the real C/Go
// metrics export, rather than checking only the values passed to its setters.
func nativeLoadAdmissionLimits(t *testing.T) (budgetBytes, slots int64) {
	t.Helper()
	families, err := metrics.NewCRegistry().Gather()
	require.NoError(t, err)
	foundBytes, foundSlots := false, false
	for _, family := range families {
		switch family.GetName() {
		case "internal_load_admission_capacity_bytes":
			require.Len(t, family.Metric, 1)
			budgetBytes = int64(family.Metric[0].GetGauge().GetValue())
			foundBytes = true
		case "internal_load_admission_capacity_slots":
			require.Len(t, family.Metric, 1)
			slots = int64(family.Metric[0].GetGauge().GetValue())
			foundSlots = true
		}
	}
	require.True(t, foundBytes)
	require.True(t, foundSlots)
	return
}

func TestQueryNodeLoadConfigPublishesNativeLimits(t *testing.T) {
	beforeBytes, beforeSlots := nativeLoadAdmissionLimits(t)
	t.Cleanup(func() { applyQueryNodeLoadConfig(false, beforeBytes, beforeSlots) })
	pt := &paramtable.ComponentParam{}
	pt.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true), paramtable.Files(nil)))
	enabledKey := pt.QueryNodeCfg.StorageV2EnableAsyncLoad.Key
	bytesKey := pt.CommonCfg.LoadTransientBudgetBytes.Key
	slotsKey := pt.CommonCfg.LoadAdmissionSlots.Key
	cpuSlots := int64(2 * hardware.GetCPUNum())
	expect := func(wantBytes, wantSlots int64) {
		t.Helper()
		gotBytes, gotSlots := nativeLoadAdmissionLimits(t)
		assert.Equal(t, wantBytes, gotBytes)
		assert.Equal(t, wantSlots, gotSlots)
	}

	registerQueryNodeLoadConfig(t.Context(), pt, applyQueryNodeLoadConfig)
	expect(0, 0)
	// A value equal to ParamItem.DefaultValue is still explicit. It must take
	// effect even though the legacy-mode effective default was unlimited.
	require.NoError(t, pt.Save(bytesKey, "2147483648"))
	expect(2*1024*1024*1024, 0)
	require.NoError(t, pt.Save(slotsKey, strconv.FormatInt(cpuSlots, 10)))
	expect(2*1024*1024*1024, cpuSlots)
	require.NoError(t, pt.Remove(bytesKey))
	require.NoError(t, pt.Remove(slotsKey))
	expect(0, 0)
	require.NoError(t, pt.Save(enabledKey, "true"))
	expect(2*1024*1024*1024, cpuSlots)
	require.NoError(t, pt.Save(bytesKey, "0"))
	expect(0, cpuSlots)
	require.NoError(t, pt.Save(slotsKey, "3"))
	expect(0, 3)
	require.NoError(t, pt.Save(enabledKey, "false"))
	expect(0, 3)
	require.NoError(t, pt.Save(bytesKey, "1234"))
	expect(1234, 3)
	require.NoError(t, pt.Remove(slotsKey))
	expect(1234, 0)
	require.NoError(t, pt.Save(enabledKey, "true"))
	expect(1234, cpuSlots)
	require.NoError(t, pt.Remove(bytesKey))
	expect(2*1024*1024*1024, cpuSlots)
	require.NoError(t, pt.Reset(enabledKey))
	expect(0, 0)
}

func TestCommonCoreCallbacksDoNotOwnLoadAdmission(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	beforeBytes, beforeSlots := nativeLoadAdmissionLimits(t)
	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.LoadTransientBudgetBytes.Key)
		pt.Reset(pt.CommonCfg.LoadAdmissionSlots.Key)
		applyQueryNodeLoadConfig(false, beforeBytes, beforeSlots)
	})
	SetupCoreConfigChangelCallback()
	applyQueryNodeLoadConfig(false, 1234, 7)
	// This is also the setup used by DataNode. Runtime updates must not write
	// QueryNode's process-wide capacities through these shared callbacks.
	require.NoError(t, pt.Save(pt.CommonCfg.LoadTransientBudgetBytes.Key, "4096"))
	require.NoError(t, pt.Save(pt.CommonCfg.LoadAdmissionSlots.Key, "32"))
	gotBytes, gotSlots := nativeLoadAdmissionLimits(t)
	assert.EqualValues(t, 1234, gotBytes)
	assert.EqualValues(t, 7, gotSlots)
}

func TestQueryNodeLoadConfigSerializesUpdatesAcrossKeys(t *testing.T) {
	pt := &paramtable.ComponentParam{}
	pt.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true), paramtable.Files(nil)))
	firstApply := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFirst) }) }
	defer release()
	var calls, active atomic.Int32
	var overlap atomic.Bool
	var lastBytes, lastSlots atomic.Int64
	var tasks sync.WaitGroup
	tasks.Add(1)
	go func() {
		defer tasks.Done()
		registerQueryNodeLoadConfig(t.Context(), pt, func(enabled bool, budgetBytes, slots int64) {
			if active.Add(1) != 1 {
				overlap.Store(true)
			}
			defer active.Add(-1)
			if calls.Add(1) == 1 {
				close(firstApply)
				<-releaseFirst
			}
			lastBytes.Store(budgetBytes)
			lastSlots.Store(slots)
		})
	}()
	<-firstApply
	for key, value := range map[string]string{
		pt.CommonCfg.LoadTransientBudgetBytes.Key: "1234",
		pt.CommonCfg.LoadAdmissionSlots.Key:       "7",
	} {
		tasks.Add(1)
		go func() {
			defer tasks.Done()
			assert.NoError(t, pt.Save(key, value))
		}()
	}
	// Both source updates land before either callback can complete. The
	// callback must read the current configuration after acquiring its lock.
	updated := assert.Eventually(t, func() bool {
		return pt.CommonCfg.LoadTransientBudgetBytes.GetValue() == "1234" &&
			pt.CommonCfg.LoadAdmissionSlots.GetValue() == "7"
	}, 2*time.Second, time.Millisecond)
	release()
	tasks.Wait()
	require.True(t, updated)
	assert.False(t, overlap.Load())
	assert.EqualValues(t, 3, calls.Load())
	assert.EqualValues(t, 1234, lastBytes.Load())
	assert.EqualValues(t, 7, lastSlots.Load())
}

func TestApplyQueryNodeLoadConfigOrdersRolloutAndLimits(t *testing.T) {
	var calls []string
	enabled := mockey.Mock(updateStorageV2AsyncLoadEnabled).To(func(value bool) {
		calls = append(calls, "enabled="+strconv.FormatBool(value))
	}).Build()
	defer enabled.UnPatch()
	memory := mockey.Mock(UpdateLoadTransientBudgetBytes).To(func(value int64) {
		calls = append(calls, "bytes="+strconv.FormatInt(value, 10))
	}).Build()
	defer memory.UnPatch()
	slots := mockey.Mock(UpdateLoadAdmissionSlots).To(func(value int64) {
		calls = append(calls, "slots="+strconv.FormatInt(value, 10))
	}).Build()
	defer slots.UnPatch()

	applyQueryNodeLoadConfig(true, 4096, 3)
	assert.Equal(t, []string{"bytes=4096", "slots=3", "enabled=true"}, calls)
	calls = nil
	applyQueryNodeLoadConfig(false, 0, 0)
	assert.Equal(t, []string{"enabled=false", "bytes=0", "slots=0"}, calls)
}
