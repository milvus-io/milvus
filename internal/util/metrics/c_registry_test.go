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

package metrics

import (
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pkgmetrics "github.com/milvus-io/milvus/pkg/v3/metrics"
)

func TestApplyCollectionLevelMetricsModeAggregatesCacheShardMetric(t *testing.T) {
	previous := pkgmetrics.CollectionLevelMetricsMode()
	pkgmetrics.SetCollectionLevelMetricsMode(pkgmetrics.CollectionLevelMetricsModeAggregate)
	t.Cleanup(func() {
		pkgmetrics.SetCollectionLevelMetricsMode(previous)
	})

	metricFamilies := map[string]*dto.MetricFamily{
		cacheShardDiskUsageMetricName: cacheShardMetricFamily(
			cacheShardSample{"vector_index", "pchannel_1_1v0", 10},
			cacheShardSample{"vector_index", "pchannel_1_2v0", 20},
			cacheShardSample{"scalar_index", "pchannel_1_3v0", 5},
		),
	}

	applyCollectionLevelMetricsMode(metricFamilies)

	metricFamily := metricFamilies[cacheShardDiskUsageMetricName]
	require.Len(t, metricFamily.Metric, 2)
	values := make(map[string]float64, 2)
	for _, metric := range metricFamily.Metric {
		labels := make(map[string]string, len(metric.Label))
		for _, label := range metric.Label {
			labels[label.GetName()] = label.GetValue()
		}
		require.Equal(t, pkgmetrics.AllLabel, labels["shard"])
		values[labels["data_type"]] = metric.GetGauge().GetValue()
	}
	require.Equal(t, float64(30), values["vector_index"])
	require.Equal(t, float64(5), values["scalar_index"])
}

func TestApplyCollectionLevelMetricsModePreservesFullCacheShardMetric(t *testing.T) {
	previous := pkgmetrics.CollectionLevelMetricsMode()
	pkgmetrics.SetCollectionLevelMetricsMode(pkgmetrics.CollectionLevelMetricsModeFull)
	t.Cleanup(func() {
		pkgmetrics.SetCollectionLevelMetricsMode(previous)
	})

	metricFamilies := map[string]*dto.MetricFamily{
		cacheShardDiskUsageMetricName: cacheShardMetricFamily(
			cacheShardSample{"vector_index", "pchannel_1_1v0", 10},
			cacheShardSample{"vector_index", "pchannel_1_2v0", 20},
		),
	}

	applyCollectionLevelMetricsMode(metricFamilies)

	require.Len(t, metricFamilies[cacheShardDiskUsageMetricName].Metric, 2)
}

type cacheShardSample struct {
	dataType string
	shard    string
	value    float64
}

func cacheShardMetricFamily(samples ...cacheShardSample) *dto.MetricFamily {
	metrics := make([]*dto.Metric, 0, len(samples))
	for _, sample := range samples {
		metrics = append(metrics, &dto.Metric{
			Label: []*dto.LabelPair{
				{Name: proto.String("data_type"), Value: proto.String(sample.dataType)},
				{Name: proto.String("shard"), Value: proto.String(sample.shard)},
			},
			Gauge: &dto.Gauge{Value: proto.Float64(sample.value)},
		})
	}
	gaugeType := dto.MetricType_GAUGE
	return &dto.MetricFamily{
		Name:   proto.String(cacheShardDiskUsageMetricName),
		Type:   &gaugeType,
		Metric: metrics,
	}
}
