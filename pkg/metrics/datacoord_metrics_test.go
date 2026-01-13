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
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestDataCoordNumSegmentsWithStorageVersion(t *testing.T) {
	// Create a new registry to avoid conflicts with global metrics
	registry := prometheus.NewRegistry()

	// Create a new GaugeVec with the same labels as DataCoordNumSegments
	testGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "milvus",
			Subsystem: "datacoord",
			Name:      "test_segment_num",
			Help:      "test number of segments",
		}, []string{
			segmentStateLabelName,
			segmentLevelLabelName,
			segmentIsSortedLabelName,
			segmentStorageVersionLabelName,
		})

	registry.MustRegister(testGauge)

	// Test with different storage versions
	testCases := []struct {
		state          string
		level          string
		isSorted       string
		storageVersion string
		value          float64
	}{
		{"Flushed", "L1", "sorted", "0", 10},  // StorageV1
		{"Flushed", "L1", "sorted", "2", 20},  // StorageV2
		{"Flushed", "L2", "unsorted", "0", 5}, // StorageV1
		{"Growing", "L0", "unsorted", "2", 3}, // StorageV2
	}

	for _, tc := range testCases {
		testGauge.WithLabelValues(tc.state, tc.level, tc.isSorted, tc.storageVersion).Set(tc.value)
	}

	// Verify each metric value
	for _, tc := range testCases {
		value := testutil.ToFloat64(testGauge.WithLabelValues(tc.state, tc.level, tc.isSorted, tc.storageVersion))
		assert.Equal(t, tc.value, value, "metric value should match for state=%s, level=%s, sorted=%s, version=%s",
			tc.state, tc.level, tc.isSorted, tc.storageVersion)
	}

	// Test DeletePartialMatch by storage version
	deleted := testGauge.DeletePartialMatch(prometheus.Labels{segmentStorageVersionLabelName: "0"})
	assert.Equal(t, 2, deleted, "should delete 2 metrics with StorageV1")

	// Verify remaining metrics
	value := testutil.ToFloat64(testGauge.WithLabelValues("Flushed", "L1", "sorted", "2"))
	assert.Equal(t, float64(20), value, "StorageV2 metric should still exist")
}

func TestDataCoordNumSegmentsRegistration(t *testing.T) {
	// Test that DataCoordNumSegments can be used with 4 labels including storage version
	registry := prometheus.NewRegistry()
	RegisterDataCoord(registry)

	// This should not panic - using all 4 labels
	DataCoordNumSegments.WithLabelValues("Flushed", "L1", "sorted", "0").Set(1)
	DataCoordNumSegments.WithLabelValues("Growing", "L0", "unsorted", "2").Set(2)

	// Verify values
	value := testutil.ToFloat64(DataCoordNumSegments.WithLabelValues("Flushed", "L1", "sorted", "0"))
	assert.Equal(t, float64(1), value)

	value = testutil.ToFloat64(DataCoordNumSegments.WithLabelValues("Growing", "L0", "unsorted", "2"))
	assert.Equal(t, float64(2), value)

	// Clean up
	DataCoordNumSegments.Reset()
}

func TestDataCoordNumSegmentsLabelNames(t *testing.T) {
	// Verify the metric has the expected number of labels
	desc := DataCoordNumSegments.WithLabelValues("state", "level", "sorted", "version").Desc()
	assert.NotNil(t, desc)

	// The metric should work with exactly 4 label values
	assert.NotPanics(t, func() {
		DataCoordNumSegments.WithLabelValues("Flushed", "L1", "sorted", "0").Inc()
	})

	// Clean up
	DataCoordNumSegments.Reset()
}
