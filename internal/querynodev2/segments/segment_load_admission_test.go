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

package segments

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type admissionResult struct {
	release func()
	err     error
}

func acquireAdmissionAsync(
	ctx context.Context,
	admission *segmentLoadAdmission,
	weight uint64,
	priority commonpb.LoadPriority,
) <-chan admissionResult {
	result := make(chan admissionResult, 1)
	go func() {
		release, err := admission.acquire(ctx, weight, priority)
		result <- admissionResult{release: release, err: err}
	}()
	return result
}

func requireAdmissionWaiters(t *testing.T, admission *segmentLoadAdmission, count int) {
	t.Helper()
	require.Eventually(t, func() bool {
		admission.mu.Lock()
		defer admission.mu.Unlock()
		return len(admission.waiters) == count
	}, time.Second, time.Millisecond)
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestSegmentLoadAdmissionBlocksUntilRelease(t *testing.T) {
	admission := newSegmentLoadAdmission(10)
	releaseFirst, err := admission.acquire(context.Background(), 6, commonpb.LoadPriority_LOW)
	require.NoError(t, err)

	second := acquireAdmissionAsync(context.Background(), admission, 5, commonpb.LoadPriority_LOW)
	requireAdmissionWaiters(t, admission, 1)
	assert.Never(t, func() bool { return len(second) > 0 }, 20*time.Millisecond, time.Millisecond)

	releaseFirst()
	result := <-second
	require.NoError(t, result.err)
	result.release()
}

func TestSegmentLoadAdmissionOversizedIsExclusive(t *testing.T) {
	admission := newSegmentLoadAdmission(10)
	releaseFirst, err := admission.acquire(context.Background(), 4, commonpb.LoadPriority_LOW)
	require.NoError(t, err)

	oversized := acquireAdmissionAsync(context.Background(), admission, 11, commonpb.LoadPriority_LOW)
	requireAdmissionWaiters(t, admission, 1)
	releaseFirst()
	overResult := <-oversized
	require.NoError(t, overResult.err)

	small := acquireAdmissionAsync(context.Background(), admission, 1, commonpb.LoadPriority_LOW)
	requireAdmissionWaiters(t, admission, 1)
	overResult.release()
	smallResult := <-small
	require.NoError(t, smallResult.err)
	smallResult.release()
}

func TestSegmentLoadAdmissionRemovesCanceledWaiter(t *testing.T) {
	admission := newSegmentLoadAdmission(10)
	releaseFirst, err := admission.acquire(context.Background(), 10, commonpb.LoadPriority_LOW)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	waiter := acquireAdmissionAsync(ctx, admission, 1, commonpb.LoadPriority_LOW)
	requireAdmissionWaiters(t, admission, 1)
	cancel()
	result := <-waiter
	assert.ErrorIs(t, result.err, context.Canceled)
	requireAdmissionWaiters(t, admission, 0)

	releaseFirst()
	releaseNext, err := admission.acquire(context.Background(), 10, commonpb.LoadPriority_LOW)
	require.NoError(t, err)
	releaseNext()
}

func TestSegmentLoadAdmissionPrioritizesHighWaiter(t *testing.T) {
	admission := newSegmentLoadAdmission(10)
	releaseFirst, err := admission.acquire(context.Background(), 10, commonpb.LoadPriority_LOW)
	require.NoError(t, err)

	low := acquireAdmissionAsync(context.Background(), admission, 10, commonpb.LoadPriority_LOW)
	requireAdmissionWaiters(t, admission, 1)
	high := acquireAdmissionAsync(context.Background(), admission, 10, commonpb.LoadPriority_HIGH)
	requireAdmissionWaiters(t, admission, 2)

	releaseFirst()
	highResult := <-high
	require.NoError(t, highResult.err)
	assert.Never(t, func() bool { return len(low) > 0 }, 20*time.Millisecond, time.Millisecond)
	highResult.release()

	lowResult := <-low
	require.NoError(t, lowResult.err)
	lowResult.release()
}

func TestSegmentLoadAdmissionZeroWeightBypassesQueue(t *testing.T) {
	admission := newSegmentLoadAdmission(10)
	releaseFirst, err := admission.acquire(context.Background(), 10, commonpb.LoadPriority_LOW)
	require.NoError(t, err)

	releaseZero, err := admission.acquire(context.Background(), 0, commonpb.LoadPriority_LOW)
	require.NoError(t, err)
	releaseZero()
	releaseFirst()
}

func TestSegmentLoadAdmissionRunReleasesAfterError(t *testing.T) {
	admission := newSegmentLoadAdmission(10)
	releaseFirst, err := admission.acquire(context.Background(), 10, commonpb.LoadPriority_LOW)
	require.NoError(t, err)

	loadErr := errors.New("load failed")
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- admission.run(context.Background(), 10, commonpb.LoadPriority_LOW, func() error {
			close(started)
			return loadErr
		})
	}()

	requireAdmissionWaiters(t, admission, 1)
	assert.Never(t, func() bool { return isClosed(started) }, 20*time.Millisecond, time.Millisecond)
	releaseFirst()
	require.ErrorIs(t, <-result, loadErr)

	releaseNext, err := admission.acquire(context.Background(), 10, commonpb.LoadPriority_LOW)
	require.NoError(t, err)
	releaseNext()
}

func TestSegmentLoadAdmissionDynamicCapacityDecrease(t *testing.T) {
	var capacity atomic.Uint64
	capacity.Store(20)
	admission := newSegmentLoadAdmissionWithCapacity(capacity.Load)

	releaseFirst, err := admission.acquire(context.Background(), 10, commonpb.LoadPriority_LOW)
	require.NoError(t, err)
	capacity.Store(10)

	second := acquireAdmissionAsync(context.Background(), admission, 10, commonpb.LoadPriority_LOW)
	requireAdmissionWaiters(t, admission, 1)
	assert.Never(t, func() bool { return len(second) > 0 }, 20*time.Millisecond, time.Millisecond)

	releaseFirst()
	result := <-second
	require.NoError(t, result.err)
	result.release()
}

func TestSegmentLoadAdmissionDynamicCapacityIncrease(t *testing.T) {
	var capacity atomic.Uint64
	capacity.Store(10)
	admission := newSegmentLoadAdmissionWithCapacity(capacity.Load)

	releaseFirst, err := admission.acquire(context.Background(), 10, commonpb.LoadPriority_LOW)
	require.NoError(t, err)
	second := acquireAdmissionAsync(context.Background(), admission, 10, commonpb.LoadPriority_LOW)
	requireAdmissionWaiters(t, admission, 1)

	capacity.Store(20)
	third := acquireAdmissionAsync(context.Background(), admission, 1, commonpb.LoadPriority_LOW)
	secondResult := <-second
	require.NoError(t, secondResult.err)
	requireAdmissionWaiters(t, admission, 1)

	secondResult.release()
	thirdResult := <-third
	require.NoError(t, thirdResult.err)
	thirdResult.release()
	releaseFirst()
}

func TestSegmentLoadAdmissionFromConfig(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()

	params.Save(params.QueryNodeCfg.TieredEvictionEnabled.Key, "false")
	assert.Nil(t, newSegmentLoadAdmissionFromConfig())

	params.Save(params.QueryNodeCfg.TieredEvictionEnabled.Key, "true")
	defer params.Reset(params.QueryNodeCfg.TieredEvictionEnabled.Key)
	params.Save(params.QueryNodeCfg.TieredMaxLoadingMemoryRatio.Key, "0.25")
	defer params.Reset(params.QueryNodeCfg.TieredMaxLoadingMemoryRatio.Key)

	admission := newSegmentLoadAdmissionFromConfig()
	require.NotNil(t, admission)
	assert.Equal(t, uint64(float64(hardware.GetMemoryCount())*0.25), admission.getCapacity())

	params.Save(params.QueryNodeCfg.TieredMaxLoadingMemoryRatio.Key, "0.5")
	assert.Equal(t, uint64(float64(hardware.GetMemoryCount())*0.5), admission.getCapacity())
}
