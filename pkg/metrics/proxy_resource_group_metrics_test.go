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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func resourceGroupSQLatencySeries() int {
	ch := make(chan prometheus.Metric, 64)
	ProxyResourceGroupSQLatency.Collect(ch)
	return len(ch)
}

// The per-resource-group latency family carries db_name and collection_name,
// so dropping a collection or a database must take its series down with the
// rest of the proxy's collection-scoped metrics, or every scoped query leaves a
// series behind for the life of the proxy.
func TestCleanupProxyMetricsDropsResourceGroupSQLatency(t *testing.T) {
	ProxyResourceGroupSQLatency.Reset()
	const node = "7"
	for _, queryType := range []string{SearchLabel, HybridSearchLabel, QueryLabel} {
		ProxyResourceGroupSQLatency.WithLabelValues(node, queryType, "db_a", "coll_1", "rg-a").Observe(1)
		ProxyResourceGroupSQLatency.WithLabelValues(node, queryType, "db_a", "coll_2", "rg-b").Observe(1)
		ProxyResourceGroupSQLatency.WithLabelValues(node, queryType, "db_b", "coll_1", "rg-a").Observe(1)
	}
	assert.Equal(t, 9, resourceGroupSQLatencySeries())

	CleanupProxyCollectionMetrics(7, "db_a", "coll_1")
	assert.Equal(t, 6, resourceGroupSQLatencySeries(),
		"dropping a collection must remove its series for every query type and resource group")

	CleanupProxyDBMetrics(7, "db_a")
	assert.Equal(t, 3, resourceGroupSQLatencySeries(),
		"dropping a database must remove every collection's series in it")

	CleanupProxyDBMetrics(7, "db_b")
	assert.Equal(t, 0, resourceGroupSQLatencySeries())
}
