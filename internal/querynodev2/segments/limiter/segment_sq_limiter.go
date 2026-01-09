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

package limiter

import (
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/searchutil/scheduler"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var limiter *concurrentLimiter

func init() {
	limiter = newConcurrentLimiter()
	hardware.RegisterSystemMetricsListener(&hardware.SystemMetricsListener{
		Cooldown:  5 * time.Second, // update the limit every 5 seconds.
		Condition: func(metrics hardware.SystemMetrics, listener *hardware.SystemMetricsListener) bool { return true },
		Callback: func(metrics hardware.SystemMetrics, listener *hardware.SystemMetricsListener) {
			limiter.UpdateLimit(metrics)
		},
	})
}

// GetCurrentSegmentSQConcurrentLimit returns the current segment SQ concurrent limit.
// Return -1 if there's no limit.
func GetCurrentSegmentSQConcurrentLimit() int {
	return int(limiter.GetCurrentConcurrent())
}

func newConcurrentLimiter() *concurrentLimiter {
	return &concurrentLimiter{
		Limit: atomic.NewInt64(-1),
	}
}

type concurrentLimiter struct {
	Limit *atomic.Int64
}

func (l *concurrentLimiter) GetCurrentConcurrent() int64 {
	return l.Limit.Load()
}

func (l *concurrentLimiter) SetLimit(limit int64) {
	l.Limit.Store(limit)
}

func (l *concurrentLimiter) UpdateLimit(m hardware.SystemMetrics) {
	previous := l.GetCurrentConcurrent()
	defer func() {
		current := l.GetCurrentConcurrent()
		if current != previous {
			log.Info(
				"segment level search/query concurrency limit updates",
				log.FieldComponent("limiter"),
				log.FieldModule(typeutil.QueryNodeRole),
				zap.Int64("previous", previous),
				zap.Int64("current", current),
			)
		}
		if nodeID := paramtable.GetStringNodeID(); nodeID != "" {
			metrics.QueryNodeSegmentSQPerTaskConcurrencyLimit.WithLabelValues(nodeID).Set(float64(current))
		}
	}()

	if !isSegmentSearchConcurrentLimitEnabled() {
		// if the segment search concurrent limit is disabled, set the limit to -1 to disable the limit.
		l.SetLimit(-1)
		return
	}

	threshold, err := getThreshold(m)
	if err != nil {
		log.Warn("failed to get threshold for segment search concurrent limit",
			log.FieldComponent("limiter"),
			log.FieldModule(typeutil.QueryNodeRole),
			zap.Error(err),
		)
		return
	}

	newLimit := threshold.GetLimit(m.AverageCPUUsage)
	threshold.RegisterMetrics()
	l.SetLimit(newLimit)
}

type threshold struct {
	lwmThreshold        float64
	hwmThreshold        float64
	lwmConcurrencyLimit int64
	hwmConcurrencyLimit int64
}

func (t *threshold) RegisterMetrics() {
	nodeID := paramtable.GetStringNodeID()

	metrics.QueryNodeSegmentSQPerTaskConcurrencyLimitLwmThreshold.WithLabelValues(nodeID).Set(t.lwmThreshold)
	metrics.QueryNodeSegmentSQPerTaskConcurrencyLimitHwmThreshold.WithLabelValues(nodeID).Set(t.hwmThreshold)
	metrics.QueryNodeSegmentSQPerTaskConcurrencyLimitOfCPULwm.WithLabelValues(nodeID).Set(float64(t.lwmConcurrencyLimit))
	metrics.QueryNodeSegmentSQPerTaskConcurrencyLimitOfCPUHwm.WithLabelValues(nodeID).Set(float64(t.hwmConcurrencyLimit))
}

func (t *threshold) GetLimit(cpuUsage float64) int64 {
	if cpuUsage <= t.lwmThreshold {
		return t.lwmConcurrencyLimit
	} else if cpuUsage >= t.hwmThreshold {
		return t.hwmConcurrencyLimit
	} else {
		diff := t.hwmThreshold - t.lwmThreshold
		k := float64(t.hwmConcurrencyLimit-t.lwmConcurrencyLimit) / diff

		offset := cpuUsage - t.lwmThreshold
		return int64(offset*k + float64(t.lwmConcurrencyLimit))
	}
}

// getThreshold gets the threshold for the segment search concurrent limit.
func getThreshold(m hardware.SystemMetrics) (*threshold, error) {
	lwmThreshold := paramtable.Get().QueryNodeCfg.SchedulePolicySegmentSQConcurrencyLimitCPUUsageLwmThreshold.GetAsFloat()
	hwmThreshold := paramtable.Get().QueryNodeCfg.SchedulePolicySegmentSQConcurrencyLimitCPUUsageHwmThreshold.GetAsFloat()
	maxConcurrencyRatio := paramtable.Get().QueryNodeCfg.SchedulePolicySegmentSQConcurrencyLimitOfLwm.GetAsFloat()

	if maxConcurrencyRatio < 0 || maxConcurrencyRatio > 1 {
		return nil, merr.WrapErrServiceInternalInvalidMsg("max concurrency ratio is invalid, should be in [0, 1], got: %f", maxConcurrencyRatio)
	}

	if lwmThreshold < 0 || lwmThreshold > 1 {
		return nil, merr.WrapErrServiceInternalInvalidMsg("lwm threshold is invalid, should be in [0, 1], got: %f", lwmThreshold)
	}

	if hwmThreshold < 0 || hwmThreshold > 1 {
		return nil, merr.WrapErrServiceInternalInvalidMsg("hwm threshold is invalid, should be in [0, 1], got: %f", hwmThreshold)
	}
	if lwmThreshold >= hwmThreshold {
		return nil, merr.WrapErrServiceInternalInvalidMsg("lwm threshold should be less than hwm threshold, got: lwm=%f, hwm=%f", lwmThreshold, hwmThreshold)
	}
	if hwmThreshold-lwmThreshold < 1e-2 {
		return nil, merr.WrapErrServiceInternalInvalidMsg("hwm threshold and lwm threshold are too close, got: lwm=%f, hwm=%f", lwmThreshold, hwmThreshold)
	}

	lwmConcurrencyLimit := int64(float64(m.CPUNum) * maxConcurrencyRatio)
	hwmConcurrencyLimit := int64(1)
	if lwmConcurrencyLimit < hwmConcurrencyLimit {
		lwmConcurrencyLimit = hwmConcurrencyLimit
	}
	return &threshold{
		lwmThreshold:        lwmThreshold,
		hwmThreshold:        hwmThreshold,
		lwmConcurrencyLimit: lwmConcurrencyLimit,
		hwmConcurrencyLimit: hwmConcurrencyLimit,
	}, nil
}

// isSegmentSearchConcurrentLimitEnabled checks if the segment search concurrent limit is enabled.
func isSegmentSearchConcurrentLimitEnabled() bool {
	enabledVal := paramtable.Get().QueryNodeCfg.SchedulePolicySegmentSQLimitEnabled.GetValue()
	if len(enabledVal) != 0 {
		// if the enabled is set, return the value of enabled.
		return paramtable.Get().QueryNodeCfg.SchedulePolicySegmentSQLimitEnabled.GetAsBool()
	}
	// otherwise, open it automatically when the schedule policy is user-task-polling.
	return paramtable.Get().QueryNodeCfg.SchedulePolicyName.GetValue() == scheduler.SchedulePolicyNameUserTaskPolling
}
