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
	"github.com/stretchr/testify/require"
)

func TestProxyMetricsResourceGroupLabelsAndCleanup(t *testing.T) {
	ProxyFunctionCall.Reset()
	ProxySearchVectors.Reset()
	ProxySQLatency.Reset()
	t.Cleanup(func() { ProxyFunctionCall.Reset(); ProxySearchVectors.Reset(); ProxySQLatency.Reset() })
	r := prometheus.NewRegistry()
	RegisterProxy(r)
	for _, coll := range []string{"coll", "other"} {
		for _, rg := range []string{"", "rg-a", "rg-b"} {
			ProxyFunctionCall.WithLabelValues("7", "Search", SuccessLabel, CauseNA, "db", coll, rg).Inc()
			ProxySearchVectors.WithLabelValues("7", "db", coll, rg).Add(3)
			ProxySQLatency.WithLabelValues("7", SearchLabel, "db", coll, rg).Observe(17)
		}
	}
	check := func(want int) {
		t.Helper()
		families, err := r.Gather()
		require.NoError(t, err)
		found := 0
		for _, f := range families {
			assert.NotContains(t, f.GetName(), "milvus_proxy_resource_group_")
			switch f.GetName() {
			case "milvus_proxy_req_count", "milvus_proxy_search_vectors_count", "milvus_proxy_sq_latency":
				found++
				require.Len(t, f.Metric, want)
				groups := map[string]int{}
				for _, m := range f.Metric {
					hasRG := false
					for _, l := range m.Label {
						if l.GetName() == "rg" {
							hasRG = true
							groups[l.GetValue()]++
						}
					}
					require.True(t, hasRG)
				}
				assert.Equal(t, map[string]int{"": want / 3, "rg-a": want / 3, "rg-b": want / 3}, groups)
			}
		}
		if want > 0 {
			require.Equal(t, 3, found)
		} else {
			require.Zero(t, found)
		}
	}
	check(6)
	CleanupProxyCollectionMetrics(7, "db", "coll")
	check(3)
	CleanupProxyDBMetrics(7, "db")
	check(0)
}
