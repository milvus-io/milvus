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
	"sort"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	clientmodel "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryCoordBalanceEpochMetricsRegistration(t *testing.T) {
	resetQueryCoordBalanceEpochMetricsForTest()
	t.Cleanup(resetQueryCoordBalanceEpochMetricsForTest)

	registry := prometheus.NewRegistry()
	require.NotPanics(t, func() { RegisterQueryCoord(registry) })

	QueryCoordBalanceEpochActive.WithLabelValues("rg-a", "planning").Set(1)
	QueryCoordBalanceEpochTotal.WithLabelValues("rg-a", "completed").Inc()
	QueryCoordBalanceEpochPlansTotal.WithLabelValues("rg-a", "segment", "planned").Inc()
	QueryCoordBalanceEpochAdmissionTotal.WithLabelValues("rg-a", "accepted").Inc()
	QueryCoordBalanceEpochSnapshotRetriesTotal.WithLabelValues("rg-a").Inc()
	QueryCoordBalanceEpochObjective.WithLabelValues("rg-a", "observed").Set(10)
	QueryCoordBalanceEpochCarryOver.WithLabelValues("rg-a", "channel").Set(2)
	QueryCoordBalanceEpochDurationSeconds.WithLabelValues("rg-a", "completed").Observe(1)

	families, err := registry.Gather()
	require.NoError(t, err)

	expected := map[string]struct {
		metricType clientmodel.MetricType
		labels     []string
	}{
		"milvus_querycoord_balance_epoch_active":                 {clientmodel.MetricType_GAUGE, []string{"resource_group", "state"}},
		"milvus_querycoord_balance_epoch_total":                  {clientmodel.MetricType_COUNTER, []string{"resource_group", "result"}},
		"milvus_querycoord_balance_epoch_plans_total":            {clientmodel.MetricType_COUNTER, []string{"kind", "resource_group", "result"}},
		"milvus_querycoord_balance_epoch_admission_total":        {clientmodel.MetricType_COUNTER, []string{"reason", "resource_group"}},
		"milvus_querycoord_balance_epoch_snapshot_retries_total": {clientmodel.MetricType_COUNTER, []string{"resource_group"}},
		"milvus_querycoord_balance_epoch_objective":              {clientmodel.MetricType_GAUGE, []string{"phase", "resource_group"}},
		"milvus_querycoord_balance_epoch_carry_over":             {clientmodel.MetricType_GAUGE, []string{"kind", "resource_group"}},
		"milvus_querycoord_balance_epoch_duration_seconds":       {clientmodel.MetricType_HISTOGRAM, []string{"resource_group", "result"}},
	}

	gatheredNames := make([]string, 0, len(expected))
	for _, family := range families {
		want, ok := expected[family.GetName()]
		if !ok {
			continue
		}
		gatheredNames = append(gatheredNames, family.GetName())
		assert.Equal(t, want.metricType, family.GetType(), family.GetName())
		require.Len(t, family.Metric, 1, family.GetName())
		labels := make([]string, 0, len(family.Metric[0].Label))
		for _, label := range family.Metric[0].Label {
			labels = append(labels, label.GetName())
		}
		sort.Strings(labels)
		assert.Equal(t, want.labels, labels, family.GetName())
	}
	sort.Strings(gatheredNames)
	expectedNames := make([]string, 0, len(expected))
	for name := range expected {
		expectedNames = append(expectedNames, name)
	}
	sort.Strings(expectedNames)
	assert.Equal(t, expectedNames, gatheredNames)
}

func resetQueryCoordBalanceEpochMetricsForTest() {
	QueryCoordBalanceEpochActive.Reset()
	QueryCoordBalanceEpochTotal.Reset()
	QueryCoordBalanceEpochPlansTotal.Reset()
	QueryCoordBalanceEpochAdmissionTotal.Reset()
	QueryCoordBalanceEpochSnapshotRetriesTotal.Reset()
	QueryCoordBalanceEpochObjective.Reset()
	QueryCoordBalanceEpochCarryOver.Reset()
	QueryCoordBalanceEpochDurationSeconds.Reset()
}
