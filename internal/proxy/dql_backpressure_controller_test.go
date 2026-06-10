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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestDQLBackpressureControllerObserve(t *testing.T) {
	now := time.Unix(100, 0)
	controller := newDQLBackpressureController(dqlBackpressureControllerConfig{
		enabled:          true,
		maxConcurrency:   8,
		minConcurrency:   2,
		reduceRatio:      0.5,
		decreaseInterval: time.Second,
		recoverInterval:  time.Second,
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
	assert.Equal(t, int64(3), controller.CurrentConcurrency())

	now = now.Add(time.Second)
	controller.Observe(nil)
	assert.Equal(t, int64(4), controller.CurrentConcurrency())
}

func TestDQLBackpressureControllerAcquireBlocksAtCurrentLimit(t *testing.T) {
	controller := newDQLBackpressureController(dqlBackpressureControllerConfig{
		enabled:          true,
		maxConcurrency:   2,
		minConcurrency:   1,
		reduceRatio:      0.5,
		decreaseInterval: time.Second,
		recoverInterval:  time.Second,
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
		enabled:          true,
		maxConcurrency:   8,
		minConcurrency:   1,
		reduceRatio:      0.5,
		decreaseInterval: time.Second,
		recoverInterval:  time.Second,
	}
	controller := newDQLBackpressureController(cfg)
	controller.now = func() time.Time {
		return now
	}

	cfg.reduceRatio = 0.25
	controller.updateConfig(cfg)
	controller.Observe(merr.WrapErrTooManyRequests(1024))
	assert.Equal(t, int64(2), controller.CurrentConcurrency())

	now = now.Add(time.Second)
	cfg.minConcurrency = 4
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
		enabled:        true,
		maxConcurrency: 1,
		minConcurrency: 1,
		reduceRatio:    0.5,
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
		enabled:        false,
		maxConcurrency: 1,
		minConcurrency: 1,
		reduceRatio:    0.5,
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
