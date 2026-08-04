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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/config"
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

	previousReadWindow := GetStorageV2AsyncLoadReadWindowSizeBytes()
	t.Cleanup(func() {
		pt.Reset(pt.QueryNodeCfg.StorageV2AsyncLoadReadWindowSizeBytes.Key)
		UpdateStorageV2AsyncLoadReadWindowSizeBytes(previousReadWindow)
	})
	assert.NoError(t, pt.Save(pt.QueryNodeCfg.StorageV2AsyncLoadReadWindowSizeBytes.Key, "0"))
	assert.EqualValues(t, 0, GetStorageV2AsyncLoadReadWindowSizeBytes())
	assert.NoError(t, pt.Save(pt.QueryNodeCfg.StorageV2AsyncLoadReadWindowSizeBytes.Key, "1048576"))
	assert.EqualValues(t, 1048576, GetStorageV2AsyncLoadReadWindowSizeBytes())
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
	previous := GetStorageV2AsyncLoadReadWindowSizeBytes()
	t.Cleanup(func() {
		UpdateStorageV2AsyncLoadReadWindowSizeBytes(previous)
	})

	assert.NoError(t, pt.Save(item.Key, "1048576"))
	UpdateStorageV2AsyncLoadReadWindowSizeBytes(0)
	registerStorageV2AsyncLoadReadWindowConfig(pt)

	assert.EqualValues(t, 1048576, GetStorageV2AsyncLoadReadWindowSizeBytes())
}

func TestRegisterStorageV2AsyncLoadEnabledWatcherCatchesUp(t *testing.T) {
	pt := &paramtable.ComponentParam{}
	pt.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true), paramtable.Files(nil)))
	item := &pt.QueryNodeCfg.StorageV2EnableAsyncLoad
	assert.NoError(t, pt.Save(item.Key, "true"))

	var applied atomic.Bool
	registerStorageV2AsyncLoadEnabledWatcher(t.Context(), pt, "test", applied.Store)
	assert.True(t, applied.Load())

	assert.NoError(t, pt.Save(item.Key, "false"))
	assert.False(t, applied.Load())
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

func TestUpdateLoadTransientBudgetBytes(t *testing.T) {
	assert.NotPanics(t, func() {
		UpdateLoadTransientBudgetBytes(0)
		UpdateLoadTransientBudgetBytes(128 * 1024 * 1024)
	})
}

func TestUpdateStorageV2AsyncLoadReadWindowSizeBytes(t *testing.T) {
	previous := GetStorageV2AsyncLoadReadWindowSizeBytes()
	t.Cleanup(func() {
		UpdateStorageV2AsyncLoadReadWindowSizeBytes(previous)
	})

	UpdateStorageV2AsyncLoadReadWindowSizeBytes(0)
	assert.EqualValues(t, 0, GetStorageV2AsyncLoadReadWindowSizeBytes())
	UpdateStorageV2AsyncLoadReadWindowSizeBytes(16 * 1024 * 1024)
	assert.EqualValues(t, 16*1024*1024, GetStorageV2AsyncLoadReadWindowSizeBytes())
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
