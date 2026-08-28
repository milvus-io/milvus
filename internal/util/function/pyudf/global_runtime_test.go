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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type providerTestRuntime struct{}

func (*providerTestRuntime) Acquire(context.Context, string, string) (Lease, error) {
	return nil, merr.WrapErrServiceUnavailableMsg("not ready")
}

func TestGlobalRuntimeProviderDoesNotInitializeAtPackageLoad(t *testing.T) {
	assert.Nil(t, globalRuntimeProvider.runtime)
	assert.NoError(t, globalRuntimeProvider.initErr)
}

func TestRuntimeProviderInitializesOnce(t *testing.T) {
	delegate := &providerTestRuntime{}
	var factoryCalls atomic.Int64
	provider := newRuntimeProvider(func() (Runtime, error) {
		factoryCalls.Add(1)
		return delegate, nil
	})
	assert.Zero(t, factoryCalls.Load())

	const callers = 16
	results := make(chan Runtime, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			runtime, err := provider.Get()
			results <- runtime
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	for runtime := range results {
		assert.Same(t, delegate, runtime)
	}
	assert.Equal(t, int64(1), factoryCalls.Load())
}

func TestRuntimeProviderInitializationErrors(t *testing.T) {
	t.Run("factory error is cached", func(t *testing.T) {
		initErr := merr.WrapErrServiceInternalMsg("bad config")
		var factoryCalls atomic.Int64
		provider := newRuntimeProvider(func() (Runtime, error) {
			factoryCalls.Add(1)
			return nil, initErr
		})

		_, firstErr := provider.Get()
		_, secondErr := provider.Get()
		assert.ErrorIs(t, firstErr, initErr)
		assert.ErrorIs(t, secondErr, initErr)
		assert.ErrorContains(t, firstErr, "initialize global runtime")
		assert.Equal(t, int64(1), factoryCalls.Load())
	})

	t.Run("nil initializer", func(t *testing.T) {
		_, err := newRuntimeProvider(nil).Get()
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
	})

	t.Run("nil runtime", func(t *testing.T) {
		provider := newRuntimeProvider(func() (Runtime, error) { return nil, nil })
		_, err := provider.Get()
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
	})
}
