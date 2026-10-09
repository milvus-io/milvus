// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseTextMetrics(t *testing.T) {
	const metricsText = `# HELP milvus_c_registry_test_total Test metric.
# TYPE milvus_c_registry_test_total counter
milvus_c_registry_test_total{source="core"} 1
`

	metricFamilies, err := parseTextMetrics(metricsText)
	require.NoError(t, err)
	require.Contains(t, metricFamilies, "milvus_c_registry_test_total")
}

func TestParseTextMetricsUsesLegacyValidation(t *testing.T) {
	_, err := parseTextMetrics(`{"metric.name",source="core"} 1`)
	require.ErrorContains(t, err, "invalid metric name")
}
