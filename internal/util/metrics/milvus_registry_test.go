/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pkgmetrics "github.com/milvus-io/milvus/pkg/v3/metrics"
)

func TestMilvusRegistryGather_NilGoRegistry(t *testing.T) {
	r := &MilvusRegistry{
		GoRegistry: nil,
		CRegistry:  nil,
	}
	res, err := r.Gather()
	assert.NoError(t, err)
	assert.Nil(t, res)
}

func TestMilvusRegistryGather_NilCRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector())
	r := &MilvusRegistry{
		GoRegistry: reg,
		CRegistry:  nil,
	}
	res, err := r.Gather()
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Greater(t, len(res), 0)
}

func TestCRegistryGatherCoreMetricsProcessor(t *testing.T) {
	const name = "test_core_metrics_processor"
	calls := 0
	pkgmetrics.SetCoreMetricsProcessor(func(families map[string]*dto.MetricFamily) {
		calls++
		families[name] = &dto.MetricFamily{
			Name: proto.String(name),
			Type: dto.MetricType_GAUGE.Enum(),
			Metric: []*dto.Metric{
				{Gauge: &dto.Gauge{Value: proto.Float64(42)}},
			},
		}
	})
	t.Cleanup(func() { pkgmetrics.SetCoreMetricsProcessor(nil) })

	registry := NewCRegistry()
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	found := false
	for _, family := range families {
		if family.GetName() == name {
			require.Len(t, family.GetMetric(), 1)
			assert.Equal(t, float64(42), family.Metric[0].GetGauge().GetValue())
			found = true
		}
	}
	require.True(t, found)

	pkgmetrics.SetCoreMetricsProcessor(nil)
	families, err = registry.Gather()
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	for _, family := range families {
		require.NotEqual(t, name, family.GetName())
	}
}
