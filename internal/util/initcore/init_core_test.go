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
	"sync/atomic"
	"testing"

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

func TestInitLoonReaderConfigRejectsOutOfRangePoolSize(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	defer pt.Reset(pt.CommonCfg.StorageReaderThreadPoolSize.Key)

	// The default (0) is accepted and leaves the pool uninitialized, which
	// milvus-storage reports as parallelism 1.
	assert.NoError(t, InitLoonReaderConfig(pt))
	assert.EqualValues(t, 1, EffectiveLoonReaderThreadPoolSize())

	// 4294967296 is the case the bound exists for: it truncates to 0 when
	// narrowed to int32, which the C side accepts as "disabled" and reports
	// as success, silently dropping the operator's configuration.
	for _, v := range []string{"-1", "1025", "4294967296", "9223372036854775807"} {
		assert.NoError(t, pt.Save(pt.CommonCfg.StorageReaderThreadPoolSize.Key, v))
		err := InitLoonReaderConfig(pt)
		assert.Error(t, err, "pool size %s must be rejected", v)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		// A rejected value must not have reached the C side.
		assert.EqualValues(t, 1, EffectiveLoonReaderThreadPoolSize())
	}

	// The upper bound itself is a valid, accepted value. Only validation is
	// asserted here — actually creating a 1024-thread pool would leak into
	// every later test in this package, since the pool is a process-wide
	// singleton that cannot be destroyed.
	assert.NoError(t, pt.Save(pt.CommonCfg.StorageReaderThreadPoolSize.Key,
		strconv.Itoa(maxStorageReaderThreadPoolSize)))
	assert.LessOrEqual(t, pt.CommonCfg.StorageReaderThreadPoolSize.GetAsInt64(),
		int64(maxStorageReaderThreadPoolSize))
}

func TestRegisterLoonReaderConfigWatchers(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	defer pt.Reset(pt.CommonCfg.StorageReaderThreadPoolSize.Key)
	defer pt.Reset(pt.CommonCfg.IndexBuildReadWindowBytes.Key)

	assert.NotPanics(t, func() { RegisterLoonReaderConfigWatchers(pt, "test") })

	var poolFires, windowFires atomic.Int32
	poolSentinel := config.NewHandler("sentinel-pool", func(*config.Event) { poolFires.Add(1) })
	windowSentinel := config.NewHandler("sentinel-window", func(*config.Event) { windowFires.Add(1) })
	pt.Watch(pt.CommonCfg.StorageReaderThreadPoolSize.Key, poolSentinel)
	pt.Watch(pt.CommonCfg.IndexBuildReadWindowBytes.Key, windowSentinel)
	defer pt.Unwatch(pt.CommonCfg.StorageReaderThreadPoolSize.Key, poolSentinel)
	defer pt.Unwatch(pt.CommonCfg.IndexBuildReadWindowBytes.Key, windowSentinel)

	// Hot-reloading only the window keeps the pool disabled. The handler must
	// not report the pool as "not fully applied": with no pool the C side
	// reports parallelism 1, which is the absent-pool reading of 0, not a
	// failed rollback.
	assert.NoError(t, pt.Save(pt.CommonCfg.IndexBuildReadWindowBytes.Key, "1048576"))
	assert.Positive(t, windowFires.Load(),
		"helper must have registered a handler on IndexBuildReadWindowBytes")
	assert.EqualValues(t, 0, pt.CommonCfg.StorageReaderThreadPoolSize.GetAsInt64())
	assert.EqualValues(t, 1, EffectiveLoonReaderThreadPoolSize())

	// An out-of-range pool size drives the handler's error branch: it must log
	// and return rather than panic.
	assert.NotPanics(t, func() {
		_ = pt.Save(pt.CommonCfg.StorageReaderThreadPoolSize.Key, "1025")
	})
	assert.Positive(t, poolFires.Load(),
		"helper must have registered a handler on StorageReaderThreadPoolSize")
	assert.EqualValues(t, 1, EffectiveLoonReaderThreadPoolSize())
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
