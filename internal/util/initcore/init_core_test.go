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
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/metrics"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestInitSparseInterimIndexAlgorithms(t *testing.T) {
	params := &paramtable.ComponentParam{}
	params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	cfg := &params.QueryNodeCfg
	t.Cleanup(func() {
		params.Reset(cfg.InterimIndexSparseIPAlgo.Key)
		params.Reset(cfg.InterimIndexSparseBM25Algo.Key)
		params.Reset(cfg.InterimIndexTargetIndexVersion.Key)
		require.NoError(t, InitInterminIndexConfig(params))
	})
	for _, tc := range []struct {
		name, ip, bm25, version string
		wantErr                 bool
	}{
		{name: "inherit", version: "8"},
		{name: "independent", ip: "sindi", bm25: "daat_wand", version: "10"},
		{name: "bm25_sindi", ip: "TAAT_NAIVE", bm25: "SINDI", version: "10"},
		{name: "invalid_ip", ip: "INVALID", version: "10", wantErr: true},
		{name: "invalid_bm25", bm25: "INVALID", version: "10", wantErr: true},
		{name: "ip_old_version", ip: "SINDI", version: "9", wantErr: true},
		{name: "bm25_old_version", bm25: "SINDI", version: "9", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, params.Save(cfg.InterimIndexSparseIPAlgo.Key, tc.ip))
			require.NoError(t, params.Save(cfg.InterimIndexSparseBM25Algo.Key, tc.bm25))
			require.NoError(t, params.Save(cfg.InterimIndexTargetIndexVersion.Key, tc.version))
			err := InitInterminIndexConfig(params)
			if tc.wantErr {
				require.Error(t, err)
				assert.Equal(t, int32(merr.CodeConfigInvalid), merr.Code(err))
				assert.False(t, merr.IsRetryableErr(err))
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestResolveArrowIOThreadPoolCapacity(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	defer pt.Reset(pt.CommonCfg.ArrowIOThreadPoolCoefficient.Key)
	defer pt.Reset(pt.CommonCfg.ArrowIOThreadPoolMaxCapacity.Key)

	for _, tc := range []struct {
		name, coefficient, maxCapacity string
		want                           int
		wantErr                        bool
	}{
		{name: "default", coefficient: "", maxCapacity: "0", want: hardware.GetCPUNum()},
		{name: "negative", coefficient: "-1", maxCapacity: "1", wantErr: true},
		{name: "negative_fraction", coefficient: "-0.5", maxCapacity: "1", wantErr: true},
		{name: "nan", coefficient: "NaN", maxCapacity: "0", wantErr: true},
		{name: "infinite", coefficient: "+Inf", maxCapacity: "0", wantErr: true},
		{name: "overflow", coefficient: "1e100", maxCapacity: "0", wantErr: true},
		{name: "overflow_capped", coefficient: "1e100", maxCapacity: "3", want: 3},
		{name: "zero_fixed_default", coefficient: "0", maxCapacity: "0", want: defaultArrowIOThreadPoolCapacity},
		{name: "zero_ignores_cap", coefficient: "0", maxCapacity: "1", want: defaultArrowIOThreadPoolCapacity},
		{name: "positive", coefficient: "2", maxCapacity: "0", want: 2 * hardware.GetCPUNum()},
		{name: "positive_fraction", coefficient: "0.5", maxCapacity: "0", want: max(1, hardware.GetCPUNum()/2)},
		{name: "positive_capped", coefficient: "2", maxCapacity: "1", want: 1},
		{name: "positive_minimum", coefficient: "0.000001", maxCapacity: "0", want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.coefficient == "" {
				pt.Reset(pt.CommonCfg.ArrowIOThreadPoolCoefficient.Key)
			} else {
				assert.NoError(t, pt.Save(pt.CommonCfg.ArrowIOThreadPoolCoefficient.Key, tc.coefficient))
			}
			assert.NoError(t, pt.Save(pt.CommonCfg.ArrowIOThreadPoolMaxCapacity.Key, tc.maxCapacity))
			got, err := ResolveArrowIOThreadPoolCapacity()
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestResolveArrowCPUThreadPoolCapacity(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	defer pt.Reset(pt.CommonCfg.ArrowCPUThreadPoolCoefficient.Key)
	defer pt.Reset(pt.CommonCfg.ArrowCPUThreadPoolMaxCapacity.Key)
	for _, tc := range []struct {
		name, coefficient, maxCapacity string
		want                           int
		wantErr                        bool
	}{
		{name: "default", coefficient: "1", maxCapacity: "0", want: hardware.GetCPUNum()},
		{name: "zero", coefficient: "0", maxCapacity: "1", want: hardware.GetCPUNum()},
		{name: "scaled", coefficient: "2", maxCapacity: "0", want: 2 * hardware.GetCPUNum()},
		{name: "fraction", coefficient: "0.5", maxCapacity: "0", want: max(1, hardware.GetCPUNum()/2)},
		{name: "capped", coefficient: "2", maxCapacity: "1", want: 1},
		{name: "minimum", coefficient: "0.000001", maxCapacity: "0", want: 1},
		{name: "negative", coefficient: "-1", maxCapacity: "0", wantErr: true},
		{name: "nan", coefficient: "NaN", maxCapacity: "0", wantErr: true},
		{name: "infinite", coefficient: "+Inf", maxCapacity: "0", wantErr: true},
		{name: "malformed", coefficient: "bad", maxCapacity: "0", wantErr: true},
		{name: "overflow", coefficient: "1e100", maxCapacity: "0", wantErr: true},
		{name: "overflow_capped", coefficient: "1e100", maxCapacity: "3", want: 3},
		{name: "invalid_cap", coefficient: "1", maxCapacity: "bad", wantErr: true},
		{name: "negative_cap", coefficient: "1", maxCapacity: "-1", wantErr: true},
		{name: "cap_overflow", coefficient: "1", maxCapacity: "4294967296", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, pt.Save(pt.CommonCfg.ArrowCPUThreadPoolCoefficient.Key, tc.coefficient))
			require.NoError(t, pt.Save(pt.CommonCfg.ArrowCPUThreadPoolMaxCapacity.Key, tc.maxCapacity))
			got, err := ResolveArrowCPUThreadPoolCapacity()
			if tc.wantErr {
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestInitStorageRuntime(t *testing.T) {
	// Tokio cannot be reset, so each scenario needs its own process.
	if os.Getenv("MILVUS_TEST_STORAGE_RUNTIME") == "" {
		executable, err := os.Executable()
		require.NoError(t, err)
		for _, scenario := range []string{"success", "invalid_cpu", "invalid_io"} {
			t.Run(scenario, func(t *testing.T) {
				cmd := exec.Command(executable, "-test.run=^TestInitStorageRuntime$") // #nosec G204 -- Re-exec the current test binary with a fixed test filter.
				cmd.Env = append(os.Environ(), "MILVUS_TEST_STORAGE_RUNTIME="+scenario)
				output, err := cmd.CombinedOutput()
				require.NoError(t, err, "%s", output)
			})
		}
		return
	}
	paramtable.Init()
	pt := paramtable.Get()
	require.NoError(t, pt.Save(pt.CommonCfg.ArrowCPUThreadPoolCoefficient.Key, "2"))
	require.NoError(t, pt.Save(pt.CommonCfg.ArrowCPUThreadPoolMaxCapacity.Key, "1"))
	require.NoError(t, pt.Save(pt.CommonCfg.ArrowIOThreadPoolCoefficient.Key, "4"))
	require.NoError(t, pt.Save(pt.CommonCfg.ArrowIOThreadPoolMaxCapacity.Key, "3"))
	if scenario := os.Getenv("MILVUS_TEST_STORAGE_RUNTIME"); scenario != "success" {
		key := pt.CommonCfg.ArrowCPUThreadPoolCoefficient.Key
		if scenario == "invalid_io" {
			key = pt.CommonCfg.ArrowIOThreadPoolCoefficient.Key
		}
		require.NoError(t, pt.Save(key, "-1"))
		err := InitStorageRuntime()
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		// A subsequent component must see the original failure, not success.
		require.NoError(t, pt.Save(key, "1"))
		assert.Same(t, err, InitStorageRuntime())
		return
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { assert.NoError(t, InitStorageRuntime()) })
	}
	wg.Wait()
	require.NoError(t, pt.Save(pt.CommonCfg.ArrowIOThreadPoolCoefficient.Key, "0"))
	require.NoError(t, InitStorageRuntime())
	families, err := metrics.NewCRegistry().Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() == "internal_arrow_io_pool_capacity" {
			require.Len(t, mf.GetMetric(), 1)
			assert.Equal(t, float64(3), mf.GetMetric()[0].GetGauge().GetValue())
			return
		}
	}
	t.Fatal("internal_arrow_io_pool_capacity gauge not found")
}

func TestTracer(t *testing.T) {
	paramtable.Init()
	InitTraceConfig(paramtable.Get())

	paramtable.Get().Save(paramtable.Get().TraceCfg.Exporter.Key, "stdout")
	ResetTraceConfig(paramtable.Get())
}

func TestOtlpHang(t *testing.T) {
	paramtable.Init()
	InitTraceConfig(paramtable.Get())

	paramtable.Get().Save(paramtable.Get().TraceCfg.Exporter.Key, "otlp")
	paramtable.Get().Save(paramtable.Get().TraceCfg.InitTimeoutSeconds.Key, "1")
	defer paramtable.Get().Reset(paramtable.Get().TraceCfg.Exporter.Key)
	defer paramtable.Get().Reset(paramtable.Get().TraceCfg.InitTimeoutSeconds.Key)

	assert.NotPanics(t, func() {
		ResetTraceConfig(paramtable.Get())
	})
}

func TestSetupCoreConfigChangeCallback(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()

	assert.NotPanics(t, func() { SetupCoreConfigChangelCallback() })

	// verify thread pool callbacks are triggered by saving valid values
	assert.NoError(t, pt.Save(pt.CommonCfg.HighPriorityThreadCoreCoefficient.Key, "8"))
	assert.Equal(t, "8", pt.CommonCfg.HighPriorityThreadCoreCoefficient.GetValue())

	assert.NoError(t, pt.Save(pt.CommonCfg.MiddlePriorityThreadCoreCoefficient.Key, "4"))
	assert.Equal(t, "4", pt.CommonCfg.MiddlePriorityThreadCoreCoefficient.GetValue())

	assert.NoError(t, pt.Save(pt.CommonCfg.LowPriorityThreadCoreCoefficient.Key, "2"))
	assert.Equal(t, "2", pt.CommonCfg.LowPriorityThreadCoreCoefficient.GetValue())

	assert.NoError(t, pt.Save(pt.CommonCfg.ThreadPoolMaxThreadsSize.Key, "32"))
	assert.Equal(t, "32", pt.CommonCfg.ThreadPoolMaxThreadsSize.GetValue())

	previousReadWindow := getStorageV2AsyncLoadReadWindowSizeBytes()
	t.Cleanup(func() {
		pt.Reset(pt.QueryNodeCfg.StorageV2AsyncLoadReadWindowSizeBytes.Key)
		updateStorageV2AsyncLoadReadWindowSizeBytes(previousReadWindow)
	})
	assert.NoError(t, pt.Save(pt.QueryNodeCfg.StorageV2AsyncLoadReadWindowSizeBytes.Key, "0"))
	assert.EqualValues(t, paramtable.DefaultStorageV2AsyncLoadReadWindowSizeBytes, getStorageV2AsyncLoadReadWindowSizeBytes())
	assert.NoError(t, pt.Save(pt.QueryNodeCfg.StorageV2AsyncLoadReadWindowSizeBytes.Key, "1048576"))
	assert.EqualValues(t, 1048576, getStorageV2AsyncLoadReadWindowSizeBytes())
}

func TestRegisterConfigWatcherWithCatchUpSerializesInitialSyncAndUpdates(t *testing.T) {
	var current atomic.Bool
	var applied atomic.Bool
	var syncCalls atomic.Int32
	callbackRegistered := make(chan func(), 1)
	firstRead := make(chan struct{})
	secondSyncStarted := make(chan struct{})
	secondSyncApplied := make(chan struct{})
	releaseFirstSync := make(chan struct{})

	done := make(chan struct{})
	go func() {
		registerConfigWatcherWithCatchUp(func(syncConfig func()) {
			callbackRegistered <- syncConfig
		}, func() {
			value := current.Load()
			call := syncCalls.Add(1)
			switch call {
			case 1:
				close(firstRead)
				<-releaseFirstSync
			case 2:
				close(secondSyncStarted)
			}
			applied.Store(value)
			if call == 2 {
				close(secondSyncApplied)
			}
		})
		close(done)
	}()

	callback := <-callbackRegistered
	<-firstRead
	current.Store(true)
	callbackInvoked := make(chan struct{})
	callbackDone := make(chan struct{})
	go func() {
		close(callbackInvoked)
		callback()
		close(callbackDone)
	}()
	<-callbackInvoked

	overlapped := false
	select {
	case <-secondSyncStarted:
		overlapped = true
		<-secondSyncApplied
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirstSync)
	<-done
	<-callbackDone
	assert.False(t, overlapped, "config update ran concurrently with the initial catch-up")
	assert.True(t, applied.Load(), "the concurrent update must be applied after the stale catch-up")
}

func TestRegisterStorageV2AsyncLoadReadWindowConfigCatchesUp(t *testing.T) {
	pt := &paramtable.ComponentParam{}
	pt.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true), paramtable.Files(nil)))
	item := &pt.QueryNodeCfg.StorageV2AsyncLoadReadWindowSizeBytes
	previous := getStorageV2AsyncLoadReadWindowSizeBytes()
	t.Cleanup(func() {
		updateStorageV2AsyncLoadReadWindowSizeBytes(previous)
	})

	assert.NoError(t, pt.Save(item.Key, "1048576"))
	updateStorageV2AsyncLoadReadWindowSizeBytes(32 * 1024 * 1024)
	registerStorageV2AsyncLoadReadWindowConfig(pt)

	assert.EqualValues(t, 1048576, getStorageV2AsyncLoadReadWindowSizeBytes())
}

func TestRegisterStorageV2AsyncLoadReadWindowConfigHandlesDelete(t *testing.T) {
	pt := &paramtable.ComponentParam{}
	pt.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true), paramtable.Files(nil)))
	item := &pt.QueryNodeCfg.StorageV2AsyncLoadReadWindowSizeBytes
	previous := getStorageV2AsyncLoadReadWindowSizeBytes()
	t.Cleanup(func() {
		updateStorageV2AsyncLoadReadWindowSizeBytes(previous)
	})

	assert.NoError(t, pt.Save(item.Key, "1048576"))
	registerStorageV2AsyncLoadReadWindowConfig(pt)
	assert.EqualValues(t, 1048576, getStorageV2AsyncLoadReadWindowSizeBytes())

	assert.NoError(t, pt.Remove(item.Key))
	assert.EqualValues(t, paramtable.DefaultStorageV2AsyncLoadReadWindowSizeBytes, getStorageV2AsyncLoadReadWindowSizeBytes())
}

func TestRegisterQueryNodeLoadConfigCatchesUp(t *testing.T) {
	pt := &paramtable.ComponentParam{}
	pt.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true), paramtable.Files(nil)))
	item := &pt.QueryNodeCfg.StorageV2EnableAsyncLoad
	assert.NoError(t, pt.Save(item.Key, "true"))

	var applied atomic.Bool
	registerQueryNodeLoadConfig(t.Context(), pt, func(enabled bool, budgetBytes, slots int64) error {
		applied.Store(enabled)
		if enabled {
			assert.EqualValues(t, 2*1024*1024*1024, budgetBytes)
			assert.Positive(t, slots)
		} else {
			assert.Zero(t, budgetBytes)
			assert.Zero(t, slots)
		}

		return nil
	})
	assert.True(t, applied.Load())

	assert.NoError(t, pt.Save(item.Key, "false"))
	assert.False(t, applied.Load())
}

func TestLazyColumnGroupHotUpdate(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	SetupCoreConfigChangelCallback()
	key := pt.QueryNodeCfg.TieredLazyColumnGroupEnabled.Key
	previous := pt.QueryNodeCfg.TieredLazyColumnGroupEnabled.GetValue()
	t.Cleanup(func() { assert.NoError(t, pt.Save(key, previous)) })
	for _, value := range []string{"false", "true", "false"} {
		assert.NoError(t, pt.Save(key, value))
		assert.Equal(t, value == "true", getLazyColumnGroupEnabled())
	}
}

func TestTieredLazyIndexSlotUpdate(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	item := &params.QueryNodeCfg.TieredLazyIndexSlotEnabled
	previous := item.GetValue()
	t.Cleanup(func() {
		assert.NoError(t, params.Save(item.Key, previous))
		assert.NoError(t, UpdateTieredStorageConfig(params))
	})
	for _, value := range []string{"true", "false"} {
		assert.NoError(t, params.Save(item.Key, value))
		assert.NoError(t, UpdateTieredStorageConfig(params))
	}
}

// TestRegisterArrowReaderConfigWatchers verifies the lifted helper registers
// a handler under each of the two arrow-reader range/hole keys, using the
// same sentinel approach as above.
func TestRegisterArrowReaderConfigWatchers(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	defer pt.Reset(pt.CommonCfg.ArrowReaderHoleSizeLimitBytes.Key)
	defer pt.Reset(pt.CommonCfg.ArrowReaderRangeSizeLimitBytes.Key)

	assert.NotPanics(t, func() { RegisterArrowReaderConfigWatchers(pt, "test") })

	var holeFires, rangeFires atomic.Int32
	holeSentinel := config.NewHandler("sentinel-hole", func(*config.Event) { holeFires.Add(1) })
	rangeSentinel := config.NewHandler("sentinel-range", func(*config.Event) { rangeFires.Add(1) })
	pt.Watch(pt.CommonCfg.ArrowReaderHoleSizeLimitBytes.Key, holeSentinel)
	pt.Watch(pt.CommonCfg.ArrowReaderRangeSizeLimitBytes.Key, rangeSentinel)
	defer pt.Unwatch(pt.CommonCfg.ArrowReaderHoleSizeLimitBytes.Key, holeSentinel)
	defer pt.Unwatch(pt.CommonCfg.ArrowReaderRangeSizeLimitBytes.Key, rangeSentinel)

	assert.NoError(t, pt.Save(pt.CommonCfg.ArrowReaderHoleSizeLimitBytes.Key, "32768"))
	assert.NoError(t, pt.Save(pt.CommonCfg.ArrowReaderRangeSizeLimitBytes.Key, "1048576"))

	assert.Positive(t, holeFires.Load(),
		"helper must have registered a handler on ArrowReaderHoleSizeLimitBytes")
	assert.Positive(t, rangeFires.Load(),
		"helper must have registered a handler on ArrowReaderRangeSizeLimitBytes")

	// Drive the handler's error branch: a negative value makes the C-side
	// InitArrowReaderConfig return ConfigInvalid; the handler must log and
	// return without panicking. This exercises the `if err != nil` branch.
	assert.NotPanics(t, func() {
		_ = pt.Save(pt.CommonCfg.ArrowReaderRangeSizeLimitBytes.Key, "-1")
	})
}

func TestInitArrowReaderConfig(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()

	assert.NoError(t, InitArrowReaderConfig(pt))

	assert.NoError(t, pt.Save(pt.CommonCfg.ArrowReaderHoleSizeLimitBytes.Key, "32768"))
	assert.NoError(t, pt.Save(pt.CommonCfg.ArrowReaderRangeSizeLimitBytes.Key, "1048576"))
	assert.NoError(t, InitArrowReaderConfig(pt))
}

func TestInitExternalVectorNullPolicy(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	defer pt.Reset(pt.CommonCfg.ExternalVectorPartialNullPolicy.Key)
	defer func() {
		_ = pt.Save(pt.CommonCfg.ExternalVectorPartialNullPolicy.Key, "error")
		assert.NoError(t, InitExternalVectorNullPolicy(pt))
	}()

	assert.NoError(t, pt.Save(pt.CommonCfg.ExternalVectorPartialNullPolicy.Key, "error"))
	assert.NoError(t, InitExternalVectorNullPolicy(pt))
	assert.False(t, externalVectorPartialNullAsRowNullEnabled())

	assert.NoError(t, pt.Save(pt.CommonCfg.ExternalVectorPartialNullPolicy.Key, " NULL "))
	assert.NoError(t, InitExternalVectorNullPolicy(pt))
	assert.True(t, externalVectorPartialNullAsRowNullEnabled())

	assert.NoError(t, pt.Save(pt.CommonCfg.ExternalVectorPartialNullPolicy.Key, "zero"))
	err := InitExternalVectorNullPolicy(pt)
	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
}

func TestInitExternalSparseConversionConfig(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	key := pt.CommonCfg.ExternalSparseConversionMemoryLimit.Key
	original := pt.CommonCfg.ExternalSparseConversionMemoryLimit.GetValue()
	t.Cleanup(func() {
		assert.NoError(t, pt.Save(key, original))
		assert.NoError(t, InitExternalSparseConversionConfig(pt))
	})
	for _, value := range []string{"1", " 268435456 ", "9223372036854775807"} {
		assert.NoError(t, pt.Save(key, value))
		assert.NoError(t, InitExternalSparseConversionConfig(pt))
		expected, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		assert.NoError(t, err)
		assert.Equal(t, expected, externalSparseConversionMemoryLimit())
	}
	previous := externalSparseConversionMemoryLimit()
	for _, value := range []string{"0", "-1", "", "invalid", "9223372036854775808"} {
		assert.NoError(t, pt.Save(key, value))
		assert.ErrorIs(t, InitExternalSparseConversionConfig(pt), merr.ErrParameterInvalid)
		assert.Equal(t, previous, externalSparseConversionMemoryLimit())
	}
}

func TestInitLoonReaderConfig(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	defer pt.Reset(pt.CommonCfg.StorageReaderThreadPoolSize.Key)
	defer pt.Reset(pt.CommonCfg.IndexBuildReadWindowBytes.Key)

	// Defaults (0/0) keep the pre-existing sequential behavior and must not
	// create the pool: GetParallelism() reports 1 when no pool exists.
	assert.NoError(t, InitLoonReaderConfig(pt))
	assert.LessOrEqual(t, EffectiveLoonReaderThreadPoolSize(), int32(1))

	assert.NoError(t, pt.Save(pt.CommonCfg.IndexBuildReadWindowBytes.Key, "536870912"))
	assert.NoError(t, InitLoonReaderConfig(pt))

	// Out-of-range pool sizes are rejected instead of being narrowed to
	// int32: 4294967296 would wrap to 0 and silently disable the pool.
	for _, invalid := range []string{"-1", "4294967296", "1025"} {
		assert.NoError(t, pt.Save(pt.CommonCfg.StorageReaderThreadPoolSize.Key, invalid))
		err := InitLoonReaderConfig(pt)
		assert.Error(t, err, "pool size %s must be rejected", invalid)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	}
	assert.NoError(t, pt.Reset(pt.CommonCfg.StorageReaderThreadPoolSize.Key))

	// An out-of-range window is rejected before the (non-destroyable)
	// reader pool is resized, so a rejected update leaves nothing applied.
	before := EffectiveLoonReaderThreadPoolSize()
	assert.NoError(t, pt.Save(pt.CommonCfg.StorageReaderThreadPoolSize.Key, "8"))
	assert.NoError(t, pt.Save(pt.CommonCfg.IndexBuildReadWindowBytes.Key, "8589934592"))
	err := InitLoonReaderConfig(pt)
	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Equal(t, before, EffectiveLoonReaderThreadPoolSize(),
		"the reader pool must not be resized when the window is rejected")
}

// TestInitLoonReaderConfigSerialized pins the read-then-apply critical
// section. Config-event handlers run inline on the updating goroutine with no
// cross-handler ordering guarantee, and resizing the reader pool is not
// idempotent, so a call that read a stale paramtable value must not be able to
// apply after a newer one. Holding the lock must therefore block the whole
// call, not just the C-side apply.
func TestInitLoonReaderConfigSerialized(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	defer pt.Reset(pt.CommonCfg.StorageReaderThreadPoolSize.Key)
	defer pt.Reset(pt.CommonCfg.IndexBuildReadWindowBytes.Key)

	loonReaderConfigMu.Lock()
	locked := true
	defer func() {
		if locked {
			loonReaderConfigMu.Unlock()
		}
	}()

	done := make(chan error, 1)
	go func() { done <- InitLoonReaderConfig(pt) }()

	select {
	case <-done:
		t.Fatal("InitLoonReaderConfig completed while the config lock was held; read-then-apply is not serialized")
	case <-time.After(200 * time.Millisecond):
	}

	loonReaderConfigMu.Unlock()
	locked = false

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("InitLoonReaderConfig did not finish after the lock was released")
	}
}

// TestInitLoonReaderConfigConcurrent exercises the same path from several
// goroutines so `go test -race` covers the lock itself. Values stay at the
// defaults: a non-zero pool size cannot be undone at runtime and would leak
// into every later test in this binary.
func TestInitLoonReaderConfigConcurrent(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	defer pt.Reset(pt.CommonCfg.StorageReaderThreadPoolSize.Key)
	defer pt.Reset(pt.CommonCfg.IndexBuildReadWindowBytes.Key)

	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = InitLoonReaderConfig(pt)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		assert.NoError(t, err, "concurrent call %d", i)
	}
	assert.LessOrEqual(t, EffectiveLoonReaderThreadPoolSize(), int32(1))
}

// TestRegisterLoonReaderConfigWatchers verifies the helper registers a handler
// under both loon reader keys and that the handler survives a rejected update.
func TestRegisterLoonReaderConfigWatchers(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	defer pt.Reset(pt.CommonCfg.StorageReaderThreadPoolSize.Key)
	defer pt.Reset(pt.CommonCfg.IndexBuildReadWindowBytes.Key)

	assert.NotPanics(t, func() { RegisterLoonReaderConfigWatchers(pt, "test") })

	var poolFires, windowFires atomic.Int32
	poolSentinel := config.NewHandler("sentinel-loon-pool", func(*config.Event) { poolFires.Add(1) })
	windowSentinel := config.NewHandler("sentinel-loon-window", func(*config.Event) { windowFires.Add(1) })
	pt.Watch(pt.CommonCfg.StorageReaderThreadPoolSize.Key, poolSentinel)
	pt.Watch(pt.CommonCfg.IndexBuildReadWindowBytes.Key, windowSentinel)
	defer pt.Unwatch(pt.CommonCfg.StorageReaderThreadPoolSize.Key, poolSentinel)
	defer pt.Unwatch(pt.CommonCfg.IndexBuildReadWindowBytes.Key, windowSentinel)

	// Updating only the window must not warn about the pool: with the pool
	// disabled (default 0) the effective size reads back as 1 because the
	// singleton does not exist, which is not a failure to destroy it.
	assert.NoError(t, pt.Save(pt.CommonCfg.IndexBuildReadWindowBytes.Key, "536870912"))
	assert.Positive(t, windowFires.Load(),
		"helper must have registered a handler on IndexBuildReadWindowBytes")

	// Drive the handler's error branch: an out-of-range window is rejected
	// by InitLoonReaderConfig, so the handler logs and returns.
	assert.NotPanics(t, func() {
		_ = pt.Save(pt.CommonCfg.IndexBuildReadWindowBytes.Key, "8589934592")
	})
	assert.NoError(t, pt.Save(pt.CommonCfg.IndexBuildReadWindowBytes.Key, "0"))

	assert.NoError(t, pt.Save(pt.CommonCfg.StorageReaderThreadPoolSize.Key, "0"))
	assert.Positive(t, poolFires.Load(),
		"helper must have registered a handler on StorageReaderThreadPoolSize")
}

func TestUpdateLoadTransientBudgetBytes(t *testing.T) {
	assert.NotPanics(t, func() {
		UpdateLoadTransientBudgetBytes(0)
		UpdateLoadTransientBudgetBytes(128 * 1024 * 1024)
	})
}

func TestUpdateLoadAdmissionSlots(t *testing.T) {
	defer UpdateLoadAdmissionSlots(0)
	assert.NotPanics(t, func() {
		UpdateLoadAdmissionSlots(0)
		UpdateLoadAdmissionSlots(16)
		UpdateLoadAdmissionSlots(-1)
	})
}

func TestUpdateStorageV2AsyncLoadReadWindowSizeBytes(t *testing.T) {
	previous := getStorageV2AsyncLoadReadWindowSizeBytes()
	t.Cleanup(func() {
		updateStorageV2AsyncLoadReadWindowSizeBytes(previous)
	})

	updateStorageV2AsyncLoadReadWindowSizeBytes(0)
	assert.EqualValues(t, paramtable.DefaultStorageV2AsyncLoadReadWindowSizeBytes, getStorageV2AsyncLoadReadWindowSizeBytes())
	updateStorageV2AsyncLoadReadWindowSizeBytes(16 * 1024 * 1024)
	assert.EqualValues(t, 16*1024*1024, getStorageV2AsyncLoadReadWindowSizeBytes())
}

func TestInitStorageV2FileSystemUsesProvidedLocalConfig(t *testing.T) {
	params := &paramtable.ComponentParam{}
	params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true), paramtable.Files(nil)))
	assert.NoError(t, params.Save(params.CommonCfg.StorageType.Key, "local"))
	assert.NoError(t, params.Save(params.CommonCfg.StorageTalonMode.Key, "3"))
	// Invalid values supplied by the caller must be rejected, even when the
	// global configuration has a valid default. This fails before entering C++.
	assert.PanicsWithValue(t, `invalid common.storage.talon.mode: "3"`, func() {
		_ = InitStorageV2FileSystem(params)
	})
}

func TestInitStorageV2FileSystem(t *testing.T) {
	// init local storage
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "local")
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, "/tmp")
	err := InitStorageV2FileSystem(paramtable.Get())
	assert.NoError(t, err)

	// init remote storage
	paramtable.Get().Save(paramtable.Get().MinioCfg.Address.Key, "oss-cn-hangzhou.aliyuncs.com")
	paramtable.Get().Save(paramtable.Get().MinioCfg.BucketName.Key, "test-oss-0815")
	paramtable.Get().Save(paramtable.Get().MinioCfg.AccessKeyID.Key, "test")
	paramtable.Get().Save(paramtable.Get().MinioCfg.SecretAccessKey.Key, "test")
	paramtable.Get().Save(paramtable.Get().MinioCfg.RootPath.Key, "test")
	paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "remote")
	paramtable.Get().Save(paramtable.Get().MinioCfg.CloudProvider.Key, "aliyun")
	paramtable.Get().Save(paramtable.Get().MinioCfg.IAMEndpoint.Key, "")
	paramtable.Get().Save(paramtable.Get().MinioCfg.LogLevel.Key, "warn")
	paramtable.Get().Save(paramtable.Get().MinioCfg.Region.Key, "oss-cn-hangzhou")
	paramtable.Get().Save(paramtable.Get().MinioCfg.UseSSL.Key, "false")
	paramtable.Get().Save(paramtable.Get().MinioCfg.SslCACert.Key, "")
	paramtable.Get().Save(paramtable.Get().MinioCfg.UseIAM.Key, "false")
	paramtable.Get().Save(paramtable.Get().MinioCfg.UseVirtualHost.Key, "false")
	paramtable.Get().Save(paramtable.Get().MinioCfg.RequestTimeoutMs.Key, "10000")

	err = InitStorageV2FileSystem(paramtable.Get())
	assert.NoError(t, err)
}
