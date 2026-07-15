// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pyudf

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestProductionRuntimeInvalidState(t *testing.T) {
	var runtime *ProductionRuntime
	_, err := runtime.Acquire(context.Background(), "rank_udf", "L2_rerank")
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.ErrorContains(t, err, "production runtime is nil")

	runtime = &ProductionRuntime{}
	_, err = runtime.Acquire(context.Background(), "rank_udf", "L2_rerank")
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.ErrorContains(t, err, "production runtime cache is nil")
}

func TestProductionRuntimeDisabled(t *testing.T) {
	var capabilityCalls atomic.Int64
	var initializeCalls atomic.Int64
	runtime, err := newProductionRuntime(
		context.Background(),
		Config{Enabled: false},
		nil,
		func() BuildCapability {
			capabilityCalls.Add(1)
			return BuildCapability{Available: true}
		},
		func() error {
			initializeCalls.Add(1)
			return nil
		},
		func() ResourceLoader { return nil },
	)
	require.NoError(t, err)
	require.NotNil(t, runtime)
	_, err = runtime.Acquire(context.Background(), "rank_udf", "L2_rerank")
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.ErrorContains(t, err, "enabled is false")
	assert.Zero(t, capabilityCalls.Load())
	assert.Zero(t, initializeCalls.Load())
	runtime.Close()
	runtime.Close()
}

func TestProductionRuntimeInitialization(t *testing.T) {
	config := Config{Enabled: true}

	t.Run("capability before initialization", func(t *testing.T) {
		var initializeCalls atomic.Int64
		runtime, err := newProductionRuntime(
			context.Background(),
			config,
			newResourceInfo(),
			func() BuildCapability { return BuildCapability{Reason: "not compiled"} },
			func() error {
				initializeCalls.Add(1)
				return nil
			},
			func() ResourceLoader { return nil },
		)
		require.Error(t, err)
		assert.Nil(t, runtime)
		assert.Zero(t, initializeCalls.Load())
	})

	t.Run("initialization is deferred until acquire", func(t *testing.T) {
		var initializeCalls atomic.Int64
		source := newResourceInfo()
		runtime, err := newProductionRuntime(
			context.Background(),
			config,
			source,
			func() BuildCapability { return BuildCapability{Available: true} },
			func() error {
				initializeCalls.Add(1)
				return nil
			},
			func() ResourceLoader { return nil },
		)
		require.NoError(t, err)
		assert.Zero(t, initializeCalls.Load())
		require.NoError(t, source.OnFileResourceSync(fileresource.SyncEvent{Version: 1}))
		assert.Zero(t, initializeCalls.Load())

		_, err = runtime.Acquire(context.Background(), "rank_udf", "L2_rerank")
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Equal(t, int64(1), initializeCalls.Load())
	})

	t.Run("initialization failure is cached", func(t *testing.T) {
		initErr := merr.WrapErrServiceInternalMsg("init failed")
		var initializeCalls atomic.Int64
		runtime, err := newProductionRuntime(
			context.Background(),
			config,
			newResourceInfo(),
			func() BuildCapability { return BuildCapability{Available: true} },
			func() error {
				initializeCalls.Add(1)
				return initErr
			},
			func() ResourceLoader { return nil },
		)
		require.NoError(t, err)

		_, firstErr := runtime.Acquire(context.Background(), "rank_udf", "L2_rerank")
		_, secondErr := runtime.Acquire(context.Background(), "rank_udf", "L2_rerank")
		assert.ErrorIs(t, firstErr, initErr)
		assert.ErrorIs(t, secondErr, initErr)
		assert.ErrorContains(t, firstErr, "initialize embedded runtime")
		assert.Equal(t, int64(1), initializeCalls.Load())
	})

	t.Run("concurrent acquisitions initialize once", func(t *testing.T) {
		var initializeCalls atomic.Int64
		runtime, err := newProductionRuntime(
			context.Background(),
			config,
			newResourceInfo(),
			func() BuildCapability { return BuildCapability{Available: true} },
			func() error {
				initializeCalls.Add(1)
				return nil
			},
			func() ResourceLoader { return nil },
		)
		require.NoError(t, err)

		const callers = 8
		done := make(chan struct{}, callers)
		for i := 0; i < callers; i++ {
			go func() {
				defer func() { done <- struct{}{} }()
				_, _ = runtime.Acquire(context.Background(), "rank_udf", "L2_rerank")
			}()
		}
		for i := 0; i < callers; i++ {
			<-done
		}
		assert.Equal(t, int64(1), initializeCalls.Load())
	})

	t.Run("owns cache and resource source", func(t *testing.T) {
		loaded := &fakeLoadedResource{}
		var initializeCalls atomic.Int64
		var loaderCalls atomic.Int64
		source := newResourceInfo()
		runtime, err := newProductionRuntime(
			context.Background(),
			config,
			source,
			func() BuildCapability { return BuildCapability{Available: true} },
			func() error {
				initializeCalls.Add(1)
				return nil
			},
			func() ResourceLoader {
				loaderCalls.Add(1)
				return &fakeResourceLoader{load: func(context.Context, fileresource.ResolvedFileResource, string) (LoadedResource, error) {
					return loaded, nil
				}}
			},
		)
		require.NoError(t, err)
		assert.Equal(t, int64(1), loaderCalls.Load())
		assert.Zero(t, initializeCalls.Load())
		_, err = runtime.Acquire(context.Background(), "rank_udf", "L2_rerank")
		assert.ErrorIs(t, err, merr.ErrServiceUnavailable)

		require.NoError(t, source.OnFileResourceSync(fileresource.SyncEvent{
			Version:   1,
			Resources: []*fileresource.ResolvedFileResource{testWheelResource(1, "rank_udf")},
		}))
		lease, err := runtime.Acquire(context.Background(), "rank_udf", "L2_rerank")
		require.NoError(t, err)
		lease.Release()
		assert.Equal(t, int64(1), initializeCalls.Load())
		runtime.Close()
		require.Eventually(t, func() bool {
			return loaded.closeCount.Load() == 1
		}, time.Second, time.Millisecond)
		_, err = runtime.Acquire(context.Background(), "rank_udf", "L2_rerank")
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
	})
}
