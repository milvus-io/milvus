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

package expr

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/internal/util/function/pyudf"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type fakeManagedPyUDFRuntime struct {
	acquire func(context.Context, string, string) (pyudf.Lease, error)
	onSync  func(fileresource.SyncEvent) error
}

func (runtime *fakeManagedPyUDFRuntime) Acquire(ctx context.Context, resourceName, stage string) (pyudf.Lease, error) {
	if runtime.acquire == nil {
		return nil, merr.WrapErrServiceUnavailableMsg("not ready")
	}
	return runtime.acquire(ctx, resourceName, stage)
}

func (runtime *fakeManagedPyUDFRuntime) OnFileResourceSync(event fileresource.SyncEvent) error {
	if runtime.onSync == nil {
		return nil
	}
	return runtime.onSync(event)
}

func testPyUDFConfig() pyudf.Config {
	return pyudf.Config{
		Enabled:              true,
		LoadTimeout:          time.Second,
		ExecutorThreads:      1,
		MaxQueueSize:         1,
		InstancesPerResource: 1,
	}
}

func TestMustNewGlobalPyUDFRuntime(t *testing.T) {
	t.Run("constructs final runtime", func(t *testing.T) {
		type contextKey struct{}
		var creationContext context.Context
		var configCalls atomic.Int64
		var runtimeCalls atomic.Int64
		expected := &fakeManagedPyUDFRuntime{}

		runtime := mustNewGlobalPyUDFRuntime(
			func() (pyudf.Config, error) {
				configCalls.Add(1)
				return testPyUDFConfig(), nil
			},
			func(ctx context.Context, config pyudf.Config) (managedPyUDFRuntime, error) {
				creationContext = ctx
				runtimeCalls.Add(1)
				assert.Equal(t, testPyUDFConfig(), config)
				return expected, nil
			},
		)

		assert.Same(t, expected, runtime)
		assert.Equal(t, int64(1), configCalls.Load())
		assert.Equal(t, int64(1), runtimeCalls.Load())
		assert.NotNil(t, creationContext)
		assert.Nil(t, creationContext.Value(contextKey{}))
		assert.NoError(t, creationContext.Err())
	})

	t.Run("nil dependencies", func(t *testing.T) {
		assertPanicsWithError(t, merr.ErrServiceInternal, func() {
			mustNewGlobalPyUDFRuntime(nil, nil)
		})
	})

	t.Run("configuration error", func(t *testing.T) {
		configErr := merr.WrapErrServiceUnavailableMsg("configuration unavailable")
		assertPanicsWithError(t, configErr, func() {
			mustNewGlobalPyUDFRuntime(
				func() (pyudf.Config, error) { return pyudf.Config{}, configErr },
				func(context.Context, pyudf.Config) (managedPyUDFRuntime, error) {
					t.Fatal("runtime factory should not be called")
					return nil, nil
				},
			)
		})
	})

	t.Run("runtime error", func(t *testing.T) {
		initErr := merr.WrapErrSegcoreUnsupported(2003, "not compiled")
		assertPanicsWithError(t, initErr, func() {
			mustNewGlobalPyUDFRuntime(
				func() (pyudf.Config, error) { return testPyUDFConfig(), nil },
				func(context.Context, pyudf.Config) (managedPyUDFRuntime, error) {
					return nil, initErr
				},
			)
		})
	})

	t.Run("nil runtime", func(t *testing.T) {
		assertPanicsWithError(t, merr.ErrServiceInternal, func() {
			mustNewGlobalPyUDFRuntime(
				func() (pyudf.Config, error) { return testPyUDFConfig(), nil },
				func(context.Context, pyudf.Config) (managedPyUDFRuntime, error) { return nil, nil },
			)
		})
	})
}

func TestGlobalPyUDFRuntimeDirectForwarding(t *testing.T) {
	requestContext := context.WithValue(context.Background(), struct{}{}, "request")
	event := fileresource.SyncEvent{
		Version: 1,
		Resources: []*fileresource.ResolvedFileResource{{
			ID:        1,
			Name:      "rank_udf",
			Path:      "/remote/rank.whl",
			LocalPath: "/local/rank.whl",
		}},
	}
	var received fileresource.SyncEvent
	runtime := mustNewGlobalPyUDFRuntime(
		func() (pyudf.Config, error) { return testPyUDFConfig(), nil },
		func(context.Context, pyudf.Config) (managedPyUDFRuntime, error) {
			return &fakeManagedPyUDFRuntime{
				onSync: func(actual fileresource.SyncEvent) error {
					received = actual
					return nil
				},
				acquire: func(ctx context.Context, resourceName, stage string) (pyudf.Lease, error) {
					assert.Same(t, requestContext, ctx)
					assert.Equal(t, "rank_udf", resourceName)
					assert.Equal(t, "L2_rerank", stage)
					return nil, merr.WrapErrServiceUnavailableMsg("not ready")
				},
			}, nil
		},
	)

	require.NoError(t, runtime.OnFileResourceSync(event))
	assert.Equal(t, event, received)
	_, err := runtime.Acquire(requestContext, "rank_udf", "L2_rerank")
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
}

func assertPanicsWithError(t *testing.T, target error, fn func()) {
	t.Helper()
	defer func() {
		value := recover()
		require.NotNil(t, value)
		err, ok := value.(error)
		require.True(t, ok, "panic value is %T, not error", value)
		assert.ErrorIs(t, err, target)
	}()
	fn()
}

var _ managedPyUDFRuntime = (*fakeManagedPyUDFRuntime)(nil)
