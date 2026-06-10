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
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type dqlBackpressureControllerConfig struct {
	enabled          bool
	maxConcurrency   int64
	minConcurrency   int64
	reduceRatio      float64
	decreaseInterval time.Duration
	recoverInterval  time.Duration
}

type dqlBackpressureController struct {
	mu sync.Mutex

	enabled          bool
	maxConcurrency   int64
	minConcurrency   int64
	reduceRatio      float64
	decreaseInterval time.Duration
	recoverInterval  time.Duration

	currentConcurrency int64
	inflight           int64
	lastDecrease       time.Time
	lastRecover        time.Time
	notifyCh           chan struct{}
	now                func() time.Time
}

func newDQLBackpressureController(cfg dqlBackpressureControllerConfig) *dqlBackpressureController {
	c := &dqlBackpressureController{
		notifyCh: make(chan struct{}, 1),
		now:      time.Now,
	}
	c.updateConfig(cfg)
	return c
}

func normalizeDQLBackpressureConfig(cfg dqlBackpressureControllerConfig) dqlBackpressureControllerConfig {
	maxConcurrency := max(cfg.maxConcurrency, int64(1))
	minConcurrency := cfg.minConcurrency
	if minConcurrency <= 0 {
		minConcurrency = 1
	}
	minConcurrency = min(minConcurrency, maxConcurrency)

	reduceRatio := cfg.reduceRatio
	if reduceRatio <= 0 || reduceRatio >= 1 {
		reduceRatio = 0.5
	}

	cfg.maxConcurrency = maxConcurrency
	cfg.minConcurrency = minConcurrency
	cfg.reduceRatio = reduceRatio
	return cfg
}

func (c *dqlBackpressureController) updateConfig(cfg dqlBackpressureControllerConfig) {
	cfg = normalizeDQLBackpressureConfig(cfg)

	c.mu.Lock()
	defer c.mu.Unlock()

	oldEnabled := c.enabled
	oldMaxConcurrency := c.maxConcurrency

	c.enabled = cfg.enabled
	c.maxConcurrency = cfg.maxConcurrency
	c.minConcurrency = cfg.minConcurrency
	c.reduceRatio = cfg.reduceRatio
	c.decreaseInterval = cfg.decreaseInterval
	c.recoverInterval = cfg.recoverInterval

	if c.currentConcurrency == 0 || !oldEnabled && c.enabled || c.currentConcurrency == oldMaxConcurrency {
		c.currentConcurrency = c.maxConcurrency
	} else {
		c.currentConcurrency = min(max(c.currentConcurrency, c.minConcurrency), c.maxConcurrency)
	}
	if !c.enabled {
		c.notify()
	}
}

func (c *dqlBackpressureController) Acquire(ctx context.Context) bool {
	for {
		c.mu.Lock()
		if !c.enabled {
			c.mu.Unlock()
			return true
		}
		if c.inflight < c.currentConcurrency {
			c.inflight++
			c.mu.Unlock()
			return true
		}
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			return false
		case <-c.notifyCh:
		}
	}
}

func (c *dqlBackpressureController) Release() {
	c.mu.Lock()
	if c.inflight > 0 {
		c.inflight--
	}
	c.mu.Unlock()
	c.notify()
}

func (c *dqlBackpressureController) Observe(err error) {
	if err != nil {
		if errors.Is(err, merr.ErrServiceTooManyRequests) ||
			errors.Is(err, merr.ErrServiceResourceInsufficient) {
			c.decrease()
		}
		return
	}
	c.recover()
}

func (c *dqlBackpressureController) CurrentConcurrency() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.currentConcurrency
}

func (c *dqlBackpressureController) Inflight() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inflight
}

func (c *dqlBackpressureController) decrease() {
	now := c.now()

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.enabled {
		return
	}
	if c.decreaseInterval > 0 && !c.lastDecrease.IsZero() && now.Sub(c.lastDecrease) < c.decreaseInterval {
		return
	}
	next := int64(float64(c.currentConcurrency) * c.reduceRatio)
	if next >= c.currentConcurrency {
		next = c.currentConcurrency - 1
	}
	c.currentConcurrency = max(c.minConcurrency, next)
	c.lastDecrease = now
	c.lastRecover = now
}

func (c *dqlBackpressureController) recover() {
	now := c.now()

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.enabled {
		return
	}
	if c.currentConcurrency >= c.maxConcurrency {
		return
	}
	if c.recoverInterval > 0 && !c.lastRecover.IsZero() && now.Sub(c.lastRecover) < c.recoverInterval {
		return
	}
	c.currentConcurrency++
	c.lastRecover = now
	c.notify()
}

func (c *dqlBackpressureController) notify() {
	select {
	case c.notifyCh <- struct{}{}:
	default:
	}
}
