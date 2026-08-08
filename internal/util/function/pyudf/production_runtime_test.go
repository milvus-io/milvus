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
	"math"
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
		func() BuildCapability {
			capabilityCalls.Add(1)
			return BuildCapability{Available: true}
		},
		func() error {
			initializeCalls.Add(1)
			return nil
		},
		func(int32) ResourceLoader { return nil },
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
	config := Config{
		Enabled:              true,
		LoadTimeout:          time.Second,
		InstancesPerResource: 2,
	}

	t.Run("capability before initialization", func(t *testing.T) {
		var initializeCalls atomic.Int64
		runtime, err := newProductionRuntime(
			context.Background(),
			config,
			func() BuildCapability { return BuildCapability{Reason: "not compiled"} },
			func() error {
				initializeCalls.Add(1)
				return nil
			},
			func(int32) ResourceLoader { return nil },
		)
		require.Error(t, err)
		assert.Nil(t, runtime)
		assert.Zero(t, initializeCalls.Load())
	})

	t.Run("initialization failure", func(t *testing.T) {
		initErr := merr.WrapErrServiceInternalMsg("init failed")
		runtime, err := newProductionRuntime(
			context.Background(),
			config,
			func() BuildCapability { return BuildCapability{Available: true} },
			func() error { return initErr },
			func(int32) ResourceLoader { return nil },
		)
		require.Error(t, err)
		assert.ErrorIs(t, err, initErr)
		assert.Nil(t, runtime)
	})

	t.Run("instance overflow", func(t *testing.T) {
		config := config
		config.InstancesPerResource = math.MaxInt32 + 1
		runtime, err := newProductionRuntime(
			context.Background(),
			config,
			func() BuildCapability { return BuildCapability{Available: true} },
			func() error { return nil },
			func(int32) ResourceLoader { return nil },
		)
		require.Error(t, err)
		assert.Nil(t, runtime)
	})

	t.Run("owns cache and listener", func(t *testing.T) {
		loaded := &fakeLoadedResource{}
		var configuredInstances int32
		runtime, err := newProductionRuntime(
			context.Background(),
			config,
			func() BuildCapability { return BuildCapability{Available: true} },
			func() error { return nil },
			func(instances int32) ResourceLoader {
				configuredInstances = instances
				return &fakeResourceLoader{load: func(context.Context, fileresource.ResolvedFileResource, string) (LoadedResource, error) {
					return loaded, nil
				}}
			},
		)
		require.NoError(t, err)
		assert.Equal(t, int32(2), configuredInstances)
		_, err = runtime.Acquire(context.Background(), "rank_udf", "L2_rerank")
		assert.ErrorIs(t, err, merr.ErrServiceUnavailable)

		require.NoError(t, runtime.OnFileResourceSync(fileresource.SyncEvent{
			Version:   1,
			Resources: []*fileresource.ResolvedFileResource{testWheelResource(1, "rank_udf")},
		}))
		lease, err := runtime.Acquire(context.Background(), "rank_udf", "L2_rerank")
		require.NoError(t, err)
		lease.Release()
		runtime.Close()
		assert.Equal(t, int64(1), loaded.closeCount.Load())
		_, err = runtime.Acquire(context.Background(), "rank_udf", "L2_rerank")
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
	})
}
