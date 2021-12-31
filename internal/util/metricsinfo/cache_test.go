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
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/milvuspb"

	"github.com/stretchr/testify/assert"
)

func Test_NewMetricsCacheManager(t *testing.T) {
	manager := NewMetricsCacheManager()
	assert.NotNil(t, manager)
}

func TestMetricsCacheManager_GetRetention(t *testing.T) {
	manager := NewMetricsCacheManager()
	assert.NotNil(t, manager)

	assert.Equal(t, DefaultMetricsRetention, manager.GetRetention())

	retention := time.Second * 3
	manager.SetRetention(retention)
	assert.Equal(t, retention, manager.GetRetention())

	manager.ResetRetention()
	assert.Equal(t, DefaultMetricsRetention, manager.GetRetention())
}

func TestMetricsCacheManager_SetRetention(t *testing.T) {
	manager := NewMetricsCacheManager()
	assert.NotNil(t, manager)

	retention := time.Second * 3
	manager.SetRetention(retention)
	assert.Equal(t, retention, manager.GetRetention())
}

func TestMetricsCacheManager_ResetRetention(t *testing.T) {
	manager := NewMetricsCacheManager()
	assert.NotNil(t, manager)

	assert.Equal(t, DefaultMetricsRetention, manager.GetRetention())

	retention := time.Second * 3
	manager.SetRetention(retention)
	assert.Equal(t, retention, manager.GetRetention())

	manager.ResetRetention()
	assert.Equal(t, DefaultMetricsRetention, manager.GetRetention())
}

func TestMetricsCacheManager_InvalidateSystemInfoMetrics(t *testing.T) {
	manager := NewMetricsCacheManager()
	assert.NotNil(t, manager)

	manager.InvalidateSystemInfoMetrics()
	assert.Equal(t, true, manager.systemInfoMetricsInvalid)
	assert.Equal(t, false, manager.IsSystemInfoMetricsValid())
}

func TestMetricsCacheManager_IsSystemInfoMetricsValid(t *testing.T) {
	manager := NewMetricsCacheManager()
	assert.NotNil(t, manager)

	manager.InvalidateSystemInfoMetrics()
	assert.Equal(t, false, manager.IsSystemInfoMetricsValid())

	bigRetention := time.Hour * 24
	smallRetention := time.Millisecond

	manager.SetRetention(bigRetention)
	manager.UpdateSystemInfoMetrics(&milvuspb.GetMetricsResponse{})
	assert.Equal(t, true, manager.IsSystemInfoMetricsValid())

	manager.UpdateSystemInfoMetrics(nil)
	assert.Equal(t, false, manager.IsSystemInfoMetricsValid())

	manager.SetRetention(smallRetention)
	manager.UpdateSystemInfoMetrics(&milvuspb.GetMetricsResponse{})
	time.Sleep(smallRetention)
	assert.Equal(t, false, manager.IsSystemInfoMetricsValid())
}

func TestMetricsCacheManager_UpdateSystemInfoMetrics(t *testing.T) {
	manager := NewMetricsCacheManager()
	assert.NotNil(t, manager)

	manager.InvalidateSystemInfoMetrics()
	assert.Equal(t, false, manager.IsSystemInfoMetricsValid())
	resp, err := manager.GetSystemInfoMetrics()
	assert.NotNil(t, err)
	assert.Nil(t, resp)

	bigRetention := time.Hour * 24
	smallRetention := time.Millisecond

	manager.SetRetention(bigRetention)
	manager.UpdateSystemInfoMetrics(&milvuspb.GetMetricsResponse{})
	assert.Equal(t, true, manager.IsSystemInfoMetricsValid())
	resp, err = manager.GetSystemInfoMetrics()
	assert.Nil(t, err)
	assert.NotNil(t, resp)

	manager.UpdateSystemInfoMetrics(nil)
	assert.Equal(t, false, manager.IsSystemInfoMetricsValid())
	resp, err = manager.GetSystemInfoMetrics()
	assert.NotNil(t, err)
	assert.Nil(t, resp)

	manager.SetRetention(smallRetention)
	manager.UpdateSystemInfoMetrics(&milvuspb.GetMetricsResponse{})
	time.Sleep(smallRetention)
	assert.Equal(t, false, manager.IsSystemInfoMetricsValid())
	resp, err = manager.GetSystemInfoMetrics()
	assert.NotNil(t, err)
	assert.Nil(t, resp)
}

func TestMetricsCacheManager_GetSystemInfoMetrics(t *testing.T) {
	manager := NewMetricsCacheManager()
	assert.NotNil(t, manager)

	manager.InvalidateSystemInfoMetrics()
	assert.Equal(t, false, manager.IsSystemInfoMetricsValid())
	resp, err := manager.GetSystemInfoMetrics()
	assert.NotNil(t, err)
	assert.Nil(t, resp)

	bigRetention := time.Hour * 24
	smallRetention := time.Millisecond

	manager.SetRetention(bigRetention)
	manager.UpdateSystemInfoMetrics(&milvuspb.GetMetricsResponse{})
	assert.Equal(t, true, manager.IsSystemInfoMetricsValid())
	resp, err = manager.GetSystemInfoMetrics()
	assert.Nil(t, err)
	assert.NotNil(t, resp)

	manager.UpdateSystemInfoMetrics(nil)
	assert.Equal(t, false, manager.IsSystemInfoMetricsValid())
	resp, err = manager.GetSystemInfoMetrics()
	assert.NotNil(t, err)
	assert.Nil(t, resp)

	manager.SetRetention(smallRetention)
	manager.UpdateSystemInfoMetrics(&milvuspb.GetMetricsResponse{})
	time.Sleep(smallRetention)
	assert.Equal(t, false, manager.IsSystemInfoMetricsValid())
	resp, err = manager.GetSystemInfoMetrics()
	assert.NotNil(t, err)
	assert.Nil(t, resp)
}
