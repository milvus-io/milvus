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
	"encoding/json"
	"fmt"

	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

const (
	// MetricTypeKey are the key of metric type in GetMetrics request.
	MetricTypeKey = "metric_type"

	// SystemInfoMetrics means users request for system information metrics.
	SystemInfoMetrics = "system_info"
)

// ParseMetricType returns the metric type of req
func ParseMetricType(req string) (string, error) {
	m := make(map[string]interface{})
	err := json.Unmarshal([]byte(req), &m)
	if err != nil {
		return "", fmt.Errorf("failed to decode the request: %s", err.Error())
	}
	metricType, exist := m[MetricTypeKey]
	if !exist {
		return "", fmt.Errorf("%s not found in request", MetricTypeKey)
	}
	return metricType.(string), nil
}

// ConstructRequestByMetricType constructs a request according to the metric type
func ConstructRequestByMetricType(metricType string) (*milvuspb.GetMetricsRequest, error) {
	m := make(map[string]interface{})
	m[MetricTypeKey] = metricType
	binary, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("failed to construct request by metric type %s: %s", metricType, err.Error())
	}
	return &milvuspb.GetMetricsRequest{
		Base:    nil,
		Request: string(binary),
	}, nil
}
