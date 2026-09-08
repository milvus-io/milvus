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
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestStatsDiscoveryMetricsRegistration(t *testing.T) {
	registry := prometheus.NewRegistry()
	RegisterDataCoord(registry)
	t.Cleanup(func() { StatsDiscoveryPending.Reset(); StatsDiscoveryChecks.Reset() })
	StatsDiscoveryPending.WithLabelValues("key").Set(3)
	StatsDiscoveryChecks.WithLabelValues("deferred", "event", "TextIndexJob").Inc()
	require.Equal(t, float64(3), testutil.ToFloat64(StatsDiscoveryPending.WithLabelValues("key")))
	families, err := registry.Gather()
	require.NoError(t, err)
	count := 0
	for _, family := range families {
		if !strings.HasPrefix(family.GetName(), "milvus_datacoord_stats_discovery_") {
			continue
		}
		count++
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				require.Contains(t, []string{"scope", "result", "mode", "subjob"}, label.GetName())
			}
		}
	}
	require.Equal(t, 7, count)
}
