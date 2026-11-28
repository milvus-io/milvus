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

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestDataNodeCompactionStageLatency(t *testing.T) {
	// Observe values for different stages, should not panic
	stages := []string{"init_writer", "load_delta", "init_reader", "sort_read", "sort_sort", "sort_write", "flush", "compress"}
	assert.NotPanics(t, func() {
		for _, stage := range stages {
			DataNodeCompactionStageLatency.WithLabelValues("1", "SortCompaction", stage).Observe(100)
		}
	})

	DataNodeCompactionStageLatency.Reset()
}

func TestCleanupDataNodeCompactionMetrics(t *testing.T) {
	// Set up metrics for node 1
	DataNodeCompactionLatency.WithLabelValues("1", "SortCompaction").Observe(100)
	DataNodeCompactionLatencyInQueue.WithLabelValues("1").Observe(50)
	DataNodeCompactionStageLatency.WithLabelValues("1", "SortCompaction", "init_writer").Observe(10)
	DataNodeCompactionStageLatency.WithLabelValues("1", "SortCompaction", "flush").Observe(20)

	// Set up metrics for node 2 (should not be cleaned up)
	DataNodeCompactionLatency.WithLabelValues("2", "SortCompaction").Observe(200)
	DataNodeCompactionLatencyInQueue.WithLabelValues("2").Observe(100)
	DataNodeCompactionStageLatency.WithLabelValues("2", "SortCompaction", "init_writer").Observe(30)

	// Cleanup metrics for node 1 - should not panic
	assert.NotPanics(t, func() {
		CleanupDataNodeCompactionMetrics(1)
	})

	// Cleanup for non-existent node - should not panic
	assert.NotPanics(t, func() {
		CleanupDataNodeCompactionMetrics(999)
	})

	// Clean up
	DataNodeCompactionLatency.Reset()
	DataNodeCompactionLatencyInQueue.Reset()
	DataNodeCompactionStageLatency.Reset()
}

func TestCleanupDataNodeWriteMetrics(t *testing.T) {
	DataNodeWriteDataCount.Reset()
	t.Cleanup(DataNodeWriteDataCount.Reset)
	for _, source := range []string{StreamingDataSourceLabel, CompactionDataSourceLabel} {
		for _, kind := range []string{InsertLabel, DeleteLabel} {
			DataNodeWriteDataCount.WithLabelValues("1", source, kind, "10").Add(3)
		}
	}
	otherNode := DataNodeWriteDataCount.WithLabelValues("2", StreamingDataSourceLabel, InsertLabel, "10")
	otherCollection := DataNodeWriteDataCount.WithLabelValues("1", StreamingDataSourceLabel, InsertLabel, "11")
	otherNode.Add(4)
	otherCollection.Add(5)
	CleanupDataNodeWriteMetrics(1, 10)
	CleanupDataNodeWriteMetrics(1, 10)
	assert.Equal(t, 2, testutil.CollectAndCount(DataNodeWriteDataCount))
	assert.Equal(t, 4.0, testutil.ToFloat64(otherNode))
	assert.Equal(t, 5.0, testutil.ToFloat64(otherCollection))
}

func TestCleanupDataNodeWriteMetricsAggregate(t *testing.T) {
	mode := CollectionLevelMetricsMode()
	SetCollectionLevelMetricsMode(CollectionLevelMetricsModeAggregate)
	t.Cleanup(func() { SetCollectionLevelMetricsMode(mode) })
	t.Cleanup(DataNodeWriteDataCount.Reset)
	for _, id := range []string{"10", "11"} {
		DataNodeWriteDataCount.WithLabelValues("1", StreamingDataSourceLabel, InsertLabel, id).Inc()
	}
	CleanupDataNodeWriteMetrics(1, 10)
	assert.Equal(t, 1, testutil.CollectAndCount(DataNodeWriteDataCount))
	assert.Equal(t, 2.0, testutil.ToFloat64(DataNodeWriteDataCount.WithLabelValues("1", StreamingDataSourceLabel, InsertLabel, AllLabel)))
}
