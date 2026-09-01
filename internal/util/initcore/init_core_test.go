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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

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
}

// TestRegisterArrowIOThreadPoolWatchers verifies the lifted helper registers
// a handler under each of the two watched keys. The sentinel handler we
// register after the helper fires whenever the dispatcher receives an event
// for the key, which proves that the dispatcher has an active handler list
// for the key (and therefore that the helper's Watch calls landed correctly).
//
// Note on `HasUpdated`: paramtable's Save() unconditionally sets
// HasUpdated=true on its synthetic runtime event, so the short-circuit branch
// inside the helper is exercised only by file/etcd refresh events in
// production, not by Save() in a unit test. We don't try to assert it here.
func TestRegisterArrowIOThreadPoolWatchers(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	defer pt.Reset(pt.CommonCfg.ArrowIOThreadPoolCoefficient.Key)
	defer pt.Reset(pt.CommonCfg.ArrowIOThreadPoolMaxCapacity.Key)

	assert.NotPanics(t, func() { RegisterArrowIOThreadPoolWatchers(pt, "test") })

	var coefFires, maxFires atomic.Int32
	coefSentinel := config.NewHandler("sentinel-coef", func(*config.Event) { coefFires.Add(1) })
	maxSentinel := config.NewHandler("sentinel-max", func(*config.Event) { maxFires.Add(1) })
	pt.Watch(pt.CommonCfg.ArrowIOThreadPoolCoefficient.Key, coefSentinel)
	pt.Watch(pt.CommonCfg.ArrowIOThreadPoolMaxCapacity.Key, maxSentinel)
	defer pt.Unwatch(pt.CommonCfg.ArrowIOThreadPoolCoefficient.Key, coefSentinel)
	defer pt.Unwatch(pt.CommonCfg.ArrowIOThreadPoolMaxCapacity.Key, maxSentinel)

	assert.NoError(t, pt.Save(pt.CommonCfg.ArrowIOThreadPoolCoefficient.Key, "2"))
	assert.NoError(t, pt.Save(pt.CommonCfg.ArrowIOThreadPoolMaxCapacity.Key, "64"))

	assert.Positive(t, coefFires.Load(),
		"helper must have registered a handler on ArrowIOThreadPoolCoefficient")
	assert.Positive(t, maxFires.Load(),
		"helper must have registered a handler on ArrowIOThreadPoolMaxCapacity")
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
