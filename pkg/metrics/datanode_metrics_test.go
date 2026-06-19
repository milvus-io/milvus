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
