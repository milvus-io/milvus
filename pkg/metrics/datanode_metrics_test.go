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
	"github.com/stretchr/testify/require"
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

func TestCleanupDataNodeCollectionMetrics(t *testing.T) {
	DataNodeConsumeMsgCount.Reset()
	DataNodeGrowingSourceSyncFailureCount.Reset()
	DataNodeWriteDataCount.Reset()
	DataNodeCompactionDeleteCount.Reset()
	DataNodeCompactionMissingDeleteCount.Reset()
	t.Cleanup(func() {
		dataNodeCollectionMetricRefs.Lock()
		delete(dataNodeCollectionMetricRefs.refs, dataNodeCollectionMetricKey{nodeID: 1, collectionID: 10})
		dataNodeCollectionMetricRefs.Unlock()
		DataNodeConsumeMsgCount.Reset()
		DataNodeGrowingSourceSyncFailureCount.Reset()
		DataNodeWriteDataCount.Reset()
		DataNodeCompactionDeleteCount.Reset()
		DataNodeCompactionMissingDeleteCount.Reset()
	})

	DataNodeConsumeMsgCount.WithLabelValues("1", InsertLabel, "10").Inc()
	DataNodeConsumeMsgCount.WithLabelValues("1", DeleteLabel, "10").Inc()
	DataNodeConsumeMsgCount.WithLabelValues("1", InsertLabel, "11").Inc()
	DataNodeGrowingSourceSyncFailureCount.WithLabelValues("1", "10", "vchan-1").Set(3)
	DataNodeWriteDataCount.WithLabelValues("1", StreamingDataSourceLabel, InsertLabel, "10").Inc()
	DataNodeCompactionDeleteCount.WithLabelValues("10").Inc()
	DataNodeCompactionMissingDeleteCount.WithLabelValues("10").Inc()

	CleanupDataNodeCollectionMetrics(1, 10, "vchan-1")

	// Flowgraph-owned insert/delete counters are cleaned up.
	require.Equal(t, 1, testutil.CollectAndCount(DataNodeConsumeMsgCount))
	assert.Equal(t, float64(1), testutil.ToFloat64(DataNodeConsumeMsgCount.WithLabelValues("1", InsertLabel, "11")))
	// Cross-owner collectors survive: import, compaction, or an in-flight flush
	// may still update them after the last flowgraph has closed.
	assert.Equal(t, float64(3), testutil.ToFloat64(DataNodeGrowingSourceSyncFailureCount.WithLabelValues("1", "10", "vchan-1")))
	assert.Equal(t, float64(1), testutil.ToFloat64(DataNodeWriteDataCount.WithLabelValues("1", StreamingDataSourceLabel, InsertLabel, "10")))
	assert.Equal(t, float64(1), testutil.ToFloat64(DataNodeCompactionDeleteCount.WithLabelValues("10")))
	assert.Equal(t, float64(1), testutil.ToFloat64(DataNodeCompactionMissingDeleteCount.WithLabelValues("10")))
}

func TestCleanupDataNodeCollectionMetricsWaitsForLastChannel(t *testing.T) {
	DataNodeConsumeMsgCount.Reset()
	t.Cleanup(func() {
		dataNodeCollectionMetricRefs.Lock()
		delete(dataNodeCollectionMetricRefs.refs, dataNodeCollectionMetricKey{nodeID: 1, collectionID: 10})
		dataNodeCollectionMetricRefs.Unlock()
		DataNodeConsumeMsgCount.Reset()
	})

	AcquireDataNodeCollectionMetrics(1, 10)
	AcquireDataNodeCollectionMetrics(1, 10)
	DataNodeConsumeMsgCount.WithLabelValues("1", InsertLabel, "10").Inc()
	DataNodeConsumeMsgCount.WithLabelValues("1", DeleteLabel, "10").Inc()
	DataNodeConsumeMsgCount.WithLabelValues("1", InsertLabel, "11").Inc()

	CleanupDataNodeCollectionMetrics(1, 10, "vchan-1")
	require.Equal(t, 3, testutil.CollectAndCount(DataNodeConsumeMsgCount))
	assert.Equal(t, float64(1), testutil.ToFloat64(DataNodeConsumeMsgCount.WithLabelValues("1", InsertLabel, "10")))
	assert.Equal(t, float64(1), testutil.ToFloat64(DataNodeConsumeMsgCount.WithLabelValues("1", DeleteLabel, "10")))

	CleanupDataNodeCollectionMetrics(1, 10, "vchan-2")
	require.Equal(t, 1, testutil.CollectAndCount(DataNodeConsumeMsgCount))
	assert.Equal(t, float64(1), testutil.ToFloat64(DataNodeConsumeMsgCount.WithLabelValues("1", InsertLabel, "11")))
}

// TestCleanupDataNodeCollectionMetricsUnmatchedCleanupIsScoped: a close path
// that never called Acquire (e.g. a channel of a DIFFERENT collection) must not
// disturb another collection's live refcount or series. Only that collection's
// own last release may delete them.
//
// NOTE(refcount hazard, deliberately NOT asserted here): an unmatched Cleanup
// for the SAME collection while another channel holds one ref currently DOES
// delete the live series — with a shared (nodeID, collectionID) counter the
// unmatched call is indistinguishable from the matched final release. That
// scenario is unreachable today (DataSyncService.close() only runs Cleanup for
// services whose init succeeded, and successful init always Acquires), but
// guarding it would require keying refs per vchannel, i.e. an Acquire API
// change.
func TestCleanupDataNodeCollectionMetricsUnmatchedCleanupIsScoped(t *testing.T) {
	DataNodeConsumeMsgCount.Reset()
	t.Cleanup(func() {
		dataNodeCollectionMetricRefs.Lock()
		delete(dataNodeCollectionMetricRefs.refs, dataNodeCollectionMetricKey{nodeID: 1, collectionID: 20})
		delete(dataNodeCollectionMetricRefs.refs, dataNodeCollectionMetricKey{nodeID: 1, collectionID: 21})
		dataNodeCollectionMetricRefs.Unlock()
		DataNodeConsumeMsgCount.Reset()
	})

	// Channel A of collection 20 is live and holds a ref.
	AcquireDataNodeCollectionMetrics(1, 20)
	DataNodeConsumeMsgCount.WithLabelValues("1", InsertLabel, "20").Inc()
	DataNodeConsumeMsgCount.WithLabelValues("1", DeleteLabel, "20").Inc()

	// Close path of a channel of collection 21 that never Acquired.
	CleanupDataNodeCollectionMetrics(1, 21, "vchan-unmatched")

	// Collection 20's series and its refcount are untouched.
	require.Equal(t, 2, testutil.CollectAndCount(DataNodeConsumeMsgCount))
	assert.Equal(t, float64(1), testutil.ToFloat64(DataNodeConsumeMsgCount.WithLabelValues("1", InsertLabel, "20")))
	dataNodeCollectionMetricRefs.Lock()
	refs := dataNodeCollectionMetricRefs.refs[dataNodeCollectionMetricKey{nodeID: 1, collectionID: 20}]
	dataNodeCollectionMetricRefs.Unlock()
	assert.Equal(t, 1, refs)

	// The proper release of channel A deletes them.
	CleanupDataNodeCollectionMetrics(1, 20, "vchan-a")
	require.Equal(t, 0, testutil.CollectAndCount(DataNodeConsumeMsgCount))
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
