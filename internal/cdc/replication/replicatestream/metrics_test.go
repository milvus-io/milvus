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

package replicatestream

import (
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func newTestReplicateInfo(source, target string) *streamingpb.ReplicatePChannelMeta {
	return &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: source,
		TargetChannelName: target,
		TargetCluster: &commonpb.MilvusCluster{
			ClusterId: "test-cluster",
		},
	}
}

// TestInitLastReplicatedTimeTick verifies that seeding from the resume
// checkpoint reports the real backlog (checkpoint physical time), so a
// restarted CDC pod does not read as 0.
func TestInitLastReplicatedTimeTick(t *testing.T) {
	source, target := "TestInit-source", "TestInit-target"
	defer metrics.CDCLastReplicatedTimeTick.DeleteLabelValues(source, target)

	// A checkpoint 300s in the past -> the gauge reports that physical time,
	// which against wall-clock now is a ~300s backlog, not 0.
	checkpoint := tsoutil.ComposeTSByTime(time.Now().Add(-300 * time.Second))
	InitLastReplicatedTimeTick(newTestReplicateInfo(source, target), checkpoint)

	got := testutil.ToFloat64(metrics.CDCLastReplicatedTimeTick.WithLabelValues(source, target))
	assert.InDelta(t, tsoutil.PhysicalTimeSeconds(checkpoint), got, 1)
	assert.InDelta(t, 300, float64(time.Now().Unix())-got, 5)
}

func TestInitLastReplicatedTimeTickIgnoresZero(t *testing.T) {
	source, target := "TestInitZero-source", "TestInitZero-target"

	InitLastReplicatedTimeTick(newTestReplicateInfo(source, target), 0)

	assert.False(t, metrics.CDCLastReplicatedTimeTick.DeleteLabelValues(source, target))
}

func TestInitLastReplicatedTimeTickIgnoresNilInfo(t *testing.T) {
	assert.NotPanics(t, func() {
		InitLastReplicatedTimeTick(nil, tsoutil.ComposeTSByTime(time.Now()))
	})
}

func TestReplicateMetricsOnCloseKeepsLastReplicatedSeries(t *testing.T) {
	source, target := "TestOnClose-source", "TestOnClose-target"

	m := NewReplicateMetrics(newTestReplicateInfo(source, target))
	m.UpdateLastReplicatedTimeTick(tsoutil.ComposeTSByTime(time.Now()))

	m.OnClose()

	assert.True(t, metrics.CDCLastReplicatedTimeTick.DeleteLabelValues(source, target))
}

func TestDeleteLastReplicatedTimeTickDeletesExactSeries(t *testing.T) {
	source, target := "TestDelete-source", "TestDelete-target"
	info := newTestReplicateInfo(source, target)
	InitLastReplicatedTimeTick(info, tsoutil.ComposeTSByTime(time.Now()))

	DeleteLastReplicatedTimeTick(info)

	assert.False(t, metrics.CDCLastReplicatedTimeTick.DeleteLabelValues(source, target))
}

func TestReplicateMetricsSharedTargetConnections(t *testing.T) {
	target := t.Name()
	newStream := func(channel string) ReplicateMetrics {
		info := newTestReplicateInfo(channel, channel)
		info.TargetCluster.ClusterId = target
		stream := NewReplicateMetrics(info)
		stream.OnInitiate()
		t.Cleanup(stream.OnClose)
		return stream
	}
	connections := metrics.CDCStreamRPCConnections
	t.Cleanup(func() { metrics.CDCStreamRPCReconnectTimes.DeleteLabelValues(target) })
	checkCounts := func(connected, disconnected float64) {
		t.Helper()
		require.Equal(t, connected, testutil.ToFloat64(connections.WithLabelValues(target, metrics.CDCStatusConnected)))
		require.Equal(t, disconnected, testutil.ToFloat64(connections.WithLabelValues(target, metrics.CDCStatusDisconnected)))
	}

	first, second := newStream("ch1"), newStream("ch2")
	checkCounts(0, 2)
	first.OnConnect()
	second.OnConnect()
	checkCounts(2, 0)
	first.OnClose()
	checkCounts(1, 0)

	second.OnDisconnect()
	checkCounts(0, 1)
	second.OnConnect()
	checkCounts(1, 0)
	replacement := newStream("ch1")
	checkCounts(1, 1)
	first.OnClose() // A repeated close must not release the replacement.
	checkCounts(1, 1)
	var transitions sync.WaitGroup
	transitions.Go(replacement.OnClose) // Closed before it could connect.
	transitions.Go(func() {
		second.OnDisconnect()
		second.OnConnect()
	})
	transitions.Wait()
	checkCounts(1, 0)
	require.Equal(t, 4.0, testutil.ToFloat64(metrics.CDCStreamRPCReconnectTimes.WithLabelValues(target)))

	second.OnDisconnect()
	second.OnClose()
	require.False(t, connections.DeleteLabelValues(target, metrics.CDCStatusConnected))
	require.False(t, connections.DeleteLabelValues(target, metrics.CDCStatusDisconnected))
}
