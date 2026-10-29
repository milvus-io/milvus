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
	"github.com/stretchr/testify/require"
)

// TestReplicateGatedAppendsIsAGaugeWithoutACounterName guards the append gate's
// gauge: it goes up and down, so its name must not carry the _total suffix that
// marks a counter.
func TestReplicateGatedAppendsIsAGaugeWithoutACounterName(t *testing.T) {
	r := prometheus.NewRegistry()
	r.MustRegister(StreamingServiceClientReplicateGatedAppends)

	gauge := StreamingServiceClientReplicateGatedAppends.WithLabelValues("1", "test-gated-pchannel")
	gauge.Inc()
	gauge.Inc()
	gauge.Dec()
	assert.Equal(t, float64(1), testutil.ToFloat64(gauge))
	gauge.Dec()
	assert.Zero(t, testutil.ToFloat64(gauge))

	families, err := r.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	assert.Equal(t, "milvus_streaming_replicate_gated_appends", families[0].GetName())
	assert.Equal(t, "GAUGE", families[0].GetType().String())
	StreamingServiceClientReplicateGatedAppends.DeleteLabelValues("1", "test-gated-pchannel")
}

// TestBroadcasterAppendUnrecoverableIsACounter guards the broadcaster's
// unrecoverable-append metric: it only ever climbs, so it is a counter and its
// name carries the _total suffix a counter must have.
func TestBroadcasterAppendUnrecoverableIsACounter(t *testing.T) {
	r := prometheus.NewRegistry()
	r.MustRegister(StreamingCoordBroadcasterAppendUnrecoverableTotal)

	counter := StreamingCoordBroadcasterAppendUnrecoverableTotal.WithLabelValues("1", "SplitShard", "STREAMING_CODE_SHARD_FENCED")
	counter.Inc()
	counter.Inc()
	assert.Equal(t, float64(2), testutil.ToFloat64(counter))

	families, err := r.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	assert.Equal(t, "milvus_streamingcoord_broadcaster_append_unrecoverable_total", families[0].GetName())
	assert.Equal(t, "COUNTER", families[0].GetType().String())
	StreamingCoordBroadcasterAppendUnrecoverableTotal.DeleteLabelValues("1", "SplitShard", "STREAMING_CODE_SHARD_FENCED")
}

// TestReplicateViolationsIsACounter guards the replicate violation counter's
// exported name and type.
func TestReplicateViolationsIsACounter(t *testing.T) {
	r := prometheus.NewRegistry()
	r.MustRegister(StreamingServiceClientReplicateViolations)

	counter := StreamingServiceClientReplicateViolations.WithLabelValues("1", "test-violation-pchannel")
	counter.Inc()
	assert.Equal(t, float64(1), testutil.ToFloat64(counter))

	families, err := r.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	assert.Equal(t, "milvus_streaming_replicate_violation_total", families[0].GetName())
	assert.Equal(t, "COUNTER", families[0].GetType().String())
	StreamingServiceClientReplicateViolations.DeleteLabelValues("1", "test-violation-pchannel")
}

// TestSplitSourcesAwaitingCheckpointIsAGauge guards the retired-source gauge:
// it rises when a source starts waiting on DataCoord and falls when the wait
// ends, so it is a gauge, named without a counter's _total suffix.
func TestSplitSourcesAwaitingCheckpointIsAGauge(t *testing.T) {
	r := prometheus.NewRegistry()
	r.MustRegister(StreamingNodeSplitSourcesAwaitingCheckpoint)

	gauge := StreamingNodeSplitSourcesAwaitingCheckpoint.WithLabelValues("1", "test-awaiting-pchannel")
	gauge.Set(2)
	assert.Equal(t, float64(2), testutil.ToFloat64(gauge))
	gauge.Set(0)
	assert.Zero(t, testutil.ToFloat64(gauge))

	families, err := r.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	assert.Equal(t, "milvus_streamingnode_split_sources_awaiting_checkpoint", families[0].GetName())
	assert.Equal(t, "GAUGE", families[0].GetType().String())
	StreamingNodeSplitSourcesAwaitingCheckpoint.DeleteLabelValues("1", "test-awaiting-pchannel")
}
