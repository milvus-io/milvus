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

package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestDQLBackpressureControllerObserve(t *testing.T) {
	now := time.Unix(100, 0)
	controller := newDQLBackpressureController(dqlBackpressureControllerConfig{
		enabled:                true,
		maxConcurrency:         8,
		slowdownMinConcurrency: 2,
		slowdownRatio:          0.5,
		slowdownInterval:       time.Second,
		recoverInterval:        time.Second,
		recoverStep:            4,
		recoverQuiet:           2 * time.Second,
	})
	controller.now = func() time.Time {
		return now
	}

	controller.Observe(errors.Wrap(merr.WrapErrTooManyRequests(1024), "query node queue full"))
	assert.Equal(t, int64(4), controller.CurrentConcurrency())

	controller.Observe(errors.Wrap(merr.WrapErrTooManyRequests(1024), "query node queue still full"))
	assert.Equal(t, int64(4), controller.CurrentConcurrency())

	now = now.Add(time.Second)
	controller.Observe(errors.Wrap(merr.WrapErrTooManyRequests(1024), "query node queue still full after interval"))
	assert.Equal(t, int64(2), controller.CurrentConcurrency())

	controller.Observe(errors.New("normal execution error"))
	assert.Equal(t, int64(2), controller.CurrentConcurrency())

	controller.Observe(nil)
	assert.Equal(t, int64(2), controller.CurrentConcurrency())

	now = now.Add(time.Second)
	controller.Observe(nil)
	assert.Equal(t, int64(2), controller.CurrentConcurrency())

	now = now.Add(time.Second)
	controller.Observe(nil)
	assert.Equal(t, int64(6), controller.CurrentConcurrency())

	now = now.Add(time.Second)
	controller.Observe(nil)
	assert.Equal(t, int64(8), controller.CurrentConcurrency())
}

func TestDQLBackpressureControllerMetrics(t *testing.T) {
	metrics.ProxyDQLBackpressureConcurrency.Reset()
	metrics.ProxyDQLBackpressureEventsTotal.Reset()
	metrics.ProxyDQLBackpressureWaitDuration.Reset()
	defer metrics.ProxyDQLBackpressureConcurrency.Reset()
	defer metrics.ProxyDQLBackpressureEventsTotal.Reset()
	defer metrics.ProxyDQLBackpressureWaitDuration.Reset()

	now := time.Unix(100, 0)
	controller := newDQLBackpressureController(dqlBackpressureControllerConfig{
		enabled:                true,
		maxConcurrency:         8,
		slowdownMinConcurrency: 2,
		slowdownRatio:          0.5,
		slowdownInterval:       time.Second,
		recoverInterval:        time.Second,
		recoverStep:            4,
		recoverQuiet:           time.Second,
	})
	controller.now = func() time.Time {
		return now
	}
	nodeID := paramtable.GetStringNodeID()

	assert.Equal(t, float64(8), testutil.ToFloat64(metrics.ProxyDQLBackpressureConcurrency.WithLabelValues(nodeID, "current")))
	assert.Equal(t, float64(8), testutil.ToFloat64(metrics.ProxyDQLBackpressureConcurrency.WithLabelValues(nodeID, "max")))
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.ProxyDQLBackpressureConcurrency.WithLabelValues(nodeID, "slowdown_min")))

	controller.Observe(errors.Wrap(merr.WrapErrTooManyRequests(1024), "query node queue full"))
	assert.Equal(t, float64(4), testutil.ToFloat64(metrics.ProxyDQLBackpressureConcurrency.WithLabelValues(nodeID, "current")))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ProxyDQLBackpressureEventsTotal.WithLabelValues(nodeID, "slowdown", "too_many_requests")))

	now = now.Add(time.Second)
	controller.Observe(nil)
	assert.Equal(t, float64(8), testutil.ToFloat64(metrics.ProxyDQLBackpressureConcurrency.WithLabelValues(nodeID, "current")))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ProxyDQLBackpressureEventsTotal.WithLabelValues(nodeID, "recover", "success")))

	require.True(t, controller.Acquire(context.Background()))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ProxyDQLBackpressureConcurrency.WithLabelValues(nodeID, "inflight")))
	controller.Release()
	assert.Equal(t, float64(0), testutil.ToFloat64(metrics.ProxyDQLBackpressureConcurrency.WithLabelValues(nodeID, "inflight")))
}

func TestDQLBackpressureControllerLazyRecoverAfterQuietPeriod(t *testing.T) {
	metrics.ProxyDQLBackpressureConcurrency.Reset()
	metrics.ProxyDQLBackpressureEventsTotal.Reset()
	defer metrics.ProxyDQLBackpressureConcurrency.Reset()
	defer metrics.ProxyDQLBackpressureEventsTotal.Reset()

	now := time.Unix(100, 0)
	controller := newDQLBackpressureController(dqlBackpressureControllerConfig{
		enabled:                true,
		maxConcurrency:         20,
		slowdownMinConcurrency: 2,
		slowdownRatio:          0.5,
		slowdownInterval:       time.Second,
		recoverInterval:        time.Second,
		recoverStep:            4,
		recoverQuiet:           3 * time.Second,
	})
	controller.now = func() time.Time {
		return now
	}
	nodeID := paramtable.GetStringNodeID()

	controller.Observe(merr.WrapErrTooManyRequests(1024))
	assert.Equal(t, int64(10), controller.CurrentConcurrency())

	now = now.Add(2 * time.Second)
	require.True(t, controller.Acquire(context.Background()))
	assert.Equal(t, int64(10), controller.CurrentConcurrency())
	controller.Release()

	now = now.Add(2 * time.Second)
	require.True(t, controller.Acquire(context.Background()))
	assert.Equal(t, int64(14), controller.CurrentConcurrency())
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ProxyDQLBackpressureEventsTotal.WithLabelValues(nodeID, "recover", "quiet_period")))
	controller.Release()

	now = now.Add(3 * time.Second)
	require.True(t, controller.Acquire(context.Background()))
	assert.Equal(t, int64(20), controller.CurrentConcurrency())
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.ProxyDQLBackpressureEventsTotal.WithLabelValues(nodeID, "recover", "quiet_period")))
	controller.Release()

	assert.Equal(t, float64(20), testutil.ToFloat64(metrics.ProxyDQLBackpressureConcurrency.WithLabelValues(nodeID, "current")))
}

func TestDQLBackpressureControllerWaitDurationMetric(t *testing.T) {
	metrics.ProxyDQLBackpressureConcurrency.Reset()
	metrics.ProxyDQLBackpressureEventsTotal.Reset()
	metrics.ProxyDQLBackpressureWaitDuration.Reset()
	defer metrics.ProxyDQLBackpressureConcurrency.Reset()
	defer metrics.ProxyDQLBackpressureEventsTotal.Reset()
	defer metrics.ProxyDQLBackpressureWaitDuration.Reset()

	now := time.Unix(100, 0)
	controller := newDQLBackpressureController(dqlBackpressureControllerConfig{
		enabled:                true,
		maxConcurrency:         1,
		slowdownMinConcurrency: 1,
		slowdownRatio:          0.5,
	})
	controller.now = func() time.Time {
		return now
	}

	require.True(t, controller.Acquire(context.Background()))

	acquired := make(chan bool, 1)
	go func() {
		acquired <- controller.Acquire(context.Background())
	}()

	select {
	case <-acquired:
		t.Fatalf("acquire should block when inflight reaches current concurrency")
	case <-time.After(50 * time.Millisecond):
	}

	now = now.Add(25 * time.Millisecond)
	controller.Release()

	select {
	case ok := <-acquired:
		assert.True(t, ok)
	case <-time.After(time.Second):
		t.Fatalf("acquire did not resume after release")
	}

	assert.Equal(t, 1, testutil.CollectAndCount(metrics.ProxyDQLBackpressureWaitDuration))
}

func TestDQLBackpressureControllerAcquireBlocksAtCurrentLimit(t *testing.T) {
	controller := newDQLBackpressureController(dqlBackpressureControllerConfig{
		enabled:                true,
		maxConcurrency:         2,
		slowdownMinConcurrency: 1,
		slowdownRatio:          0.5,
		slowdownInterval:       time.Second,
		recoverInterval:        time.Second,
	})

	require.True(t, controller.Acquire(context.Background()))
	require.True(t, controller.Acquire(context.Background()))

	acquired := make(chan bool, 1)
	go func() {
		acquired <- controller.Acquire(context.Background())
	}()

	select {
	case <-acquired:
		t.Fatalf("acquire should block when inflight reaches current concurrency")
	case <-time.After(50 * time.Millisecond):
	}

	controller.Release()

	select {
	case ok := <-acquired:
		assert.True(t, ok)
	case <-time.After(time.Second):
		t.Fatalf("acquire did not resume after release")
	}
}

func TestDQLBackpressureControllerUpdateConfig(t *testing.T) {
	now := time.Unix(100, 0)
	cfg := dqlBackpressureControllerConfig{
		enabled:                true,
		maxConcurrency:         8,
		slowdownMinConcurrency: 1,
		slowdownRatio:          0.5,
		slowdownInterval:       time.Second,
		recoverInterval:        time.Second,
	}
	controller := newDQLBackpressureController(cfg)
	controller.now = func() time.Time {
		return now
	}

	cfg.slowdownRatio = 0.25
	controller.updateConfig(cfg)
	controller.Observe(merr.WrapErrTooManyRequests(1024))
	assert.Equal(t, int64(2), controller.CurrentConcurrency())

	now = now.Add(time.Second)
	cfg.slowdownMinConcurrency = 4
	controller.updateConfig(cfg)
	controller.Observe(merr.WrapErrTooManyRequests(1024))
	assert.Equal(t, int64(4), controller.CurrentConcurrency())

	cfg.enabled = false
	controller.updateConfig(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.True(t, controller.Acquire(ctx))
	assert.Equal(t, int64(0), controller.Inflight())

	cfg.enabled = true
	controller.updateConfig(cfg)
	assert.True(t, controller.Acquire(context.Background()))
	assert.Equal(t, int64(1), controller.Inflight())
	cfg.enabled = false
	controller.updateConfig(cfg)
	controller.Release()
	assert.Equal(t, int64(0), controller.Inflight())
}

func TestDQLBackpressureControllerDisableWakesBlockedAcquire(t *testing.T) {
	controller := newDQLBackpressureController(dqlBackpressureControllerConfig{
		enabled:                true,
		maxConcurrency:         1,
		slowdownMinConcurrency: 1,
		slowdownRatio:          0.5,
	})

	require.True(t, controller.Acquire(context.Background()))

	acquired := make(chan bool, 1)
	go func() {
		acquired <- controller.Acquire(context.Background())
	}()

	select {
	case <-acquired:
		t.Fatalf("acquire should block while backpressure is enabled and full")
	case <-time.After(50 * time.Millisecond):
	}

	controller.updateConfig(dqlBackpressureControllerConfig{
		enabled:                false,
		maxConcurrency:         1,
		slowdownMinConcurrency: 1,
		slowdownRatio:          0.5,
	})

	select {
	case ok := <-acquired:
		assert.True(t, ok)
	case <-time.After(time.Second):
		t.Fatalf("acquire did not resume after backpressure was disabled")
	}

	controller.Release()
	assert.Equal(t, int64(0), controller.Inflight())
}
