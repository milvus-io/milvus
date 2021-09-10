// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package metricsinfo

import (
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

// TODO(dragondriver): load from config file
const DefaultMetricsRetention = time.Second * 5

// TODO(dragondriver): we can use a map to manage the metrics if there are too many kind metrics
type MetricsCacheManager struct {
	systemInfoMetrics                *milvuspb.GetMetricsResponse
	systemInfoMetricsInvalid         bool
	systemInfoMetricsLastUpdatedTime time.Time
	systemInfoMetricsMtx             sync.RWMutex

	retention    time.Duration
	retentionMtx sync.RWMutex // necessary?
}

func NewMetricsCacheManager() *MetricsCacheManager {
	manager := &MetricsCacheManager{
		systemInfoMetrics:                nil,
		systemInfoMetricsInvalid:         false,
		systemInfoMetricsLastUpdatedTime: time.Now(),
		systemInfoMetricsMtx:             sync.RWMutex{},
		retention:                        DefaultMetricsRetention,
	}

	return manager
}

func (manager *MetricsCacheManager) GetRetention() time.Duration {
	manager.retentionMtx.RLock()
	defer manager.retentionMtx.RUnlock()

	return manager.retention
}

func (manager *MetricsCacheManager) SetRetention(retention time.Duration) {
	manager.retentionMtx.Lock()
	defer manager.retentionMtx.Unlock()

	manager.retention = retention
}

func (manager *MetricsCacheManager) ResetRetention() {
	manager.retentionMtx.Lock()
	defer manager.retentionMtx.Unlock()

	manager.retention = DefaultMetricsRetention
}

func (manager *MetricsCacheManager) InvalidateSystemInfoMetrics() {
	manager.systemInfoMetricsMtx.Lock()
	defer manager.systemInfoMetricsMtx.Unlock()

	manager.systemInfoMetricsInvalid = true
}

func (manager *MetricsCacheManager) IsSystemInfoMetricsValid() bool {
	retention := manager.GetRetention()

	manager.systemInfoMetricsMtx.RLock()
	defer manager.systemInfoMetricsMtx.RUnlock()

	return (!manager.systemInfoMetricsInvalid) &&
		(manager.systemInfoMetrics != nil) &&
		(time.Since(manager.systemInfoMetricsLastUpdatedTime) < retention)
}

func (manager *MetricsCacheManager) GetSystemInfoMetrics() (*milvuspb.GetMetricsResponse, error) {
	retention := manager.GetRetention()

	manager.systemInfoMetricsMtx.RLock()
	defer manager.systemInfoMetricsMtx.RUnlock()

	if manager.systemInfoMetricsInvalid ||
		manager.systemInfoMetrics == nil ||
		time.Since(manager.systemInfoMetricsLastUpdatedTime) >= retention {

		return nil, ErrInvalidSystemInfosMetricCache
	}

	return manager.systemInfoMetrics, nil
}

func (manager *MetricsCacheManager) UpdateSystemInfoMetrics(infos *milvuspb.GetMetricsResponse) {
	manager.systemInfoMetricsMtx.Lock()
	defer manager.systemInfoMetricsMtx.Unlock()

	manager.systemInfoMetrics = infos
	manager.systemInfoMetricsInvalid = false
	manager.systemInfoMetricsLastUpdatedTime = time.Now()
}
