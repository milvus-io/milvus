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

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	dqlBackpressureStateCurrent     = "current"
	dqlBackpressureStateInflight    = "inflight"
	dqlBackpressureStateMax         = "max"
	dqlBackpressureStateSlowdownMin = "slowdown_min"

	dqlBackpressureEventSlowdown      = "slowdown"
	dqlBackpressureEventRecover       = "recover"
	dqlBackpressureEventConfigUpdate  = "config_update"
	dqlBackpressureEventAcquireCancel = "acquire_cancel"

	dqlBackpressureReasonTooManyRequests      = "too_many_requests"
	dqlBackpressureReasonResourceInsufficient = "resource_insufficient"
	dqlBackpressureReasonSuccess              = "success"
	dqlBackpressureReasonQuietPeriod          = "quiet_period"
	dqlBackpressureReasonEnabled              = "enabled"
	dqlBackpressureReasonDisabled             = "disabled"
	dqlBackpressureReasonContextCancel        = "context_cancel"
)

type dqlBackpressureControllerConfig struct {
	enabled                bool
	maxConcurrency         int64
	slowdownMinConcurrency int64
	slowdownRatio          float64
	slowdownInterval       time.Duration
	recoverInterval        time.Duration
	recoverStep            int64
	recoverQuiet           time.Duration
}

type dqlBackpressureController struct {
	mu sync.Mutex

	enabled                bool
	maxConcurrency         int64
	slowdownMinConcurrency int64
	slowdownRatio          float64
	slowdownInterval       time.Duration
	recoverInterval        time.Duration
	recoverStep            int64
	recoverQuiet           time.Duration

	currentConcurrency int64
	inflight           int64
	lastSlowdown       time.Time
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
	slowdownMinConcurrency := cfg.slowdownMinConcurrency
	if slowdownMinConcurrency <= 0 {
		slowdownMinConcurrency = 1
	}
	slowdownMinConcurrency = min(slowdownMinConcurrency, maxConcurrency)

	slowdownRatio := cfg.slowdownRatio
	if slowdownRatio <= 0 || slowdownRatio >= 1 {
		slowdownRatio = 0.5
	}
	recoverStep := cfg.recoverStep
	if recoverStep <= 0 {
		recoverStep = 4
	}
	recoverQuiet := cfg.recoverQuiet
	if recoverQuiet < 0 {
		recoverQuiet = 0
	}

	cfg.maxConcurrency = maxConcurrency
	cfg.slowdownMinConcurrency = slowdownMinConcurrency
	cfg.slowdownRatio = slowdownRatio
	cfg.recoverStep = recoverStep
	cfg.recoverQuiet = recoverQuiet
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
	c.slowdownMinConcurrency = cfg.slowdownMinConcurrency
	c.slowdownRatio = cfg.slowdownRatio
	c.slowdownInterval = cfg.slowdownInterval
	c.recoverInterval = cfg.recoverInterval
	c.recoverStep = cfg.recoverStep
	c.recoverQuiet = cfg.recoverQuiet

	if c.currentConcurrency == 0 || !oldEnabled && c.enabled || c.currentConcurrency == oldMaxConcurrency {
		c.currentConcurrency = c.maxConcurrency
	} else {
		c.currentConcurrency = min(max(c.currentConcurrency, c.slowdownMinConcurrency), c.maxConcurrency)
	}
	c.updateConcurrencyMetricsLocked()
	if c.enabled {
		c.recordEventLocked(dqlBackpressureEventConfigUpdate, dqlBackpressureReasonEnabled)
	} else {
		c.recordEventLocked(dqlBackpressureEventConfigUpdate, dqlBackpressureReasonDisabled)
	}
	if !c.enabled {
		c.notify()
	}
}

func (c *dqlBackpressureController) Acquire(ctx context.Context) bool {
	var waitStart time.Time
	for {
		c.mu.Lock()
		if !c.enabled {
			c.mu.Unlock()
			c.recordWaitDuration(waitStart)
			return true
		}
		c.recoverByElapsedQuietPeriodLocked(c.now())
		if c.inflight < c.currentConcurrency {
			c.inflight++
			c.updateConcurrencyMetricsLocked()
			c.mu.Unlock()
			c.recordWaitDuration(waitStart)
			return true
		}
		if waitStart.IsZero() {
			waitStart = c.now()
		}
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			c.recordWaitDuration(waitStart)
			c.recordEvent(dqlBackpressureEventAcquireCancel, dqlBackpressureReasonContextCancel)
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
	c.updateConcurrencyMetricsLocked()
	c.mu.Unlock()
	c.notify()
}

func (c *dqlBackpressureController) Observe(err error) {
	if err != nil {
		if reason, ok := dqlBackpressureSlowdownReason(err); ok {
			c.slowdown(reason)
		}
		return
	}
	c.recover()
}

func dqlBackpressureSlowdownReason(err error) (string, bool) {
	if errors.Is(err, merr.ErrServiceTooManyRequests) {
		return dqlBackpressureReasonTooManyRequests, true
	}
	if errors.Is(err, merr.ErrServiceResourceInsufficient) {
		return dqlBackpressureReasonResourceInsufficient, true
	}
	return "", false
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

func (c *dqlBackpressureController) slowdown(reason string) {
	now := c.now()

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.enabled {
		return
	}
	if c.slowdownInterval > 0 && !c.lastSlowdown.IsZero() && now.Sub(c.lastSlowdown) < c.slowdownInterval {
		return
	}
	next := int64(float64(c.currentConcurrency) * c.slowdownRatio)
	if next >= c.currentConcurrency {
		next = c.currentConcurrency - 1
	}
	c.currentConcurrency = max(c.slowdownMinConcurrency, next)
	c.lastSlowdown = now
	c.lastRecover = now
	c.updateConcurrencyMetricsLocked()
	c.recordEventLocked(dqlBackpressureEventSlowdown, reason)
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
	if c.recoverQuiet > 0 && !c.lastSlowdown.IsZero() && now.Sub(c.lastSlowdown) < c.recoverQuiet {
		return
	}
	if c.recoverInterval > 0 && !c.lastRecover.IsZero() && now.Sub(c.lastRecover) < c.recoverInterval {
		return
	}
	c.currentConcurrency = min(c.maxConcurrency, c.currentConcurrency+c.recoverStep)
	c.lastRecover = now
	c.updateConcurrencyMetricsLocked()
	c.recordEventLocked(dqlBackpressureEventRecover, dqlBackpressureReasonSuccess)
	c.notify()
}

func (c *dqlBackpressureController) recoverByElapsedQuietPeriodLocked(now time.Time) {
	if c.currentConcurrency >= c.maxConcurrency {
		return
	}
	if c.lastSlowdown.IsZero() {
		return
	}
	recoverStart := c.lastSlowdown.Add(c.recoverQuiet)
	if now.Before(recoverStart) {
		return
	}

	if c.recoverInterval <= 0 {
		c.currentConcurrency = min(c.maxConcurrency, c.currentConcurrency+c.recoverStep)
		c.lastRecover = now
		c.updateConcurrencyMetricsLocked()
		c.recordEventLocked(dqlBackpressureEventRecover, dqlBackpressureReasonQuietPeriod)
		c.notify()
		return
	}

	lastRecover := c.lastRecover
	if lastRecover.Before(recoverStart) {
		lastRecover = recoverStart
	}
	if now.Sub(lastRecover) < c.recoverInterval {
		return
	}

	steps := int64(now.Sub(lastRecover) / c.recoverInterval)
	if steps <= 0 {
		return
	}
	c.currentConcurrency = min(c.maxConcurrency, c.currentConcurrency+steps*c.recoverStep)
	c.lastRecover = lastRecover.Add(time.Duration(steps) * c.recoverInterval)
	c.updateConcurrencyMetricsLocked()
	c.recordEventLocked(dqlBackpressureEventRecover, dqlBackpressureReasonQuietPeriod)
	c.notify()
}

func (c *dqlBackpressureController) notify() {
	select {
	case c.notifyCh <- struct{}{}:
	default:
	}
}

func (c *dqlBackpressureController) updateConcurrencyMetricsLocked() {
	nodeID := paramtable.GetStringNodeID()
	metrics.ProxyDQLBackpressureConcurrency.WithLabelValues(nodeID, dqlBackpressureStateCurrent).Set(float64(c.currentConcurrency))
	metrics.ProxyDQLBackpressureConcurrency.WithLabelValues(nodeID, dqlBackpressureStateInflight).Set(float64(c.inflight))
	metrics.ProxyDQLBackpressureConcurrency.WithLabelValues(nodeID, dqlBackpressureStateMax).Set(float64(c.maxConcurrency))
	metrics.ProxyDQLBackpressureConcurrency.WithLabelValues(nodeID, dqlBackpressureStateSlowdownMin).Set(float64(c.slowdownMinConcurrency))
}

func (c *dqlBackpressureController) recordEvent(event string, reason string) {
	metrics.ProxyDQLBackpressureEventsTotal.WithLabelValues(paramtable.GetStringNodeID(), event, reason).Inc()
}

func (c *dqlBackpressureController) recordEventLocked(event string, reason string) {
	metrics.ProxyDQLBackpressureEventsTotal.WithLabelValues(paramtable.GetStringNodeID(), event, reason).Inc()
}

func (c *dqlBackpressureController) recordWaitDuration(waitStart time.Time) {
	if waitStart.IsZero() {
		return
	}
	metrics.ProxyDQLBackpressureWaitDuration.WithLabelValues(paramtable.GetStringNodeID()).Observe(float64(c.now().Sub(waitStart).Microseconds()) / 1000.0)
}
