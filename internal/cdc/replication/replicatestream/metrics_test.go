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
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/cdc/cluster"
	"github.com/milvus-io/milvus/internal/cdc/meta"
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

func TestReplicateStreamClientSharedConnectionMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics.RegisterCDC(registry)
	targetCluster := t.Name()
	t.Cleanup(func() { metrics.CDCStreamRPCReconnectTimes.DeleteLabelValues(targetCluster) })

	connectionCounts := func() map[string]float64 {
		families, err := registry.Gather()
		require.NoError(t, err)
		counts := make(map[string]float64)
		for _, family := range families {
			if family.GetName() != "milvus_cdc_stream_rpc_connections" {
				continue
			}
			for _, sample := range family.GetMetric() {
				labels := make(map[string]string)
				for _, label := range sample.GetLabel() {
					labels[label.GetName()] = label.GetValue()
				}
				if labels[metrics.CDCLabelTargetCluster] == targetCluster {
					counts[labels[metrics.CDCLabelConnectionStatus]] = sample.GetGauge().GetValue()
				}
			}
		}
		return counts
	}
	wait := func(ready <-chan struct{}) {
		t.Helper()
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			t.Fatal("replication stream did not reach the expected stage")
		}
	}
	start := func(channel string, target *cluster.MockMilvusClient) ReplicateStreamClient {
		info := newTestReplicateInfo(channel, channel+"-target")
		info.TargetCluster.ClusterId = targetCluster
		client := NewReplicateStreamClient(context.Background(), target, &meta.ReplicateChannel{Key: channel, Value: info})
		t.Cleanup(client.Close)
		return client
	}

	firstTarget := cluster.NewMockMilvusClient(t)
	firstConnected := make(chan struct{})
	firstTarget.EXPECT().CreateReplicateStream(mock.Anything).RunAndReturn(
		func(ctx context.Context, _ ...grpc.CallOption) (milvuspb.MilvusService_CreateReplicateStreamClient, error) {
			return &connectionMetricsTestStream{ctx: ctx, connected: firstConnected}, nil
		}).Once()
	first := start("first", firstTarget)
	wait(firstConnected)

	secondTarget := cluster.NewMockMilvusClient(t)
	secondConnected, disconnect := make(chan struct{}), make(chan struct{})
	secondTarget.EXPECT().CreateReplicateStream(mock.Anything).RunAndReturn(
		func(ctx context.Context, _ ...grpc.CallOption) (milvuspb.MilvusService_CreateReplicateStreamClient, error) {
			return &connectionMetricsTestStream{ctx: ctx, connected: secondConnected, disconnect: disconnect}, nil
		}).Once()
	dialing, reconnect, reconnected := make(chan struct{}), make(chan struct{}), make(chan struct{})
	secondTarget.EXPECT().CreateReplicateStream(mock.Anything).RunAndReturn(
		func(ctx context.Context, _ ...grpc.CallOption) (milvuspb.MilvusService_CreateReplicateStreamClient, error) {
			close(dialing)
			select {
			case <-reconnect:
				return &connectionMetricsTestStream{ctx: ctx, connected: reconnected}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).Once()
	second := start("second", secondTarget)
	wait(secondConnected)
	assert.Equal(t, map[string]float64{"connected": 2, "disconnected": 0}, connectionCounts())

	first.Close()
	assert.Equal(t, map[string]float64{"connected": 1, "disconnected": 0}, connectionCounts(),
		"closing the first stream must preserve the second stream's connection series")

	close(disconnect)
	wait(dialing)
	assert.Equal(t, map[string]float64{"connected": 0, "disconnected": 1}, connectionCounts())
	close(reconnect)
	wait(reconnected)
	assert.Equal(t, map[string]float64{"connected": 1, "disconnected": 0}, connectionCounts())

	second.Close()
	require.Empty(t, connectionCounts())
}

type connectionMetricsTestStream struct {
	milvuspb.MilvusService_CreateReplicateStreamClient
	ctx        context.Context
	connected  chan struct{}
	disconnect <-chan struct{}
}

func (s *connectionMetricsTestStream) CloseSend() error { return nil }

func (s *connectionMetricsTestStream) Recv() (*milvuspb.ReplicateResponse, error) {
	close(s.connected)
	select {
	case <-s.disconnect:
		return nil, assert.AnError
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func TestDeleteLastReplicatedTimeTickDeletesExactSeries(t *testing.T) {
	source, target := "TestDelete-source", "TestDelete-target"
	info := newTestReplicateInfo(source, target)
	InitLastReplicatedTimeTick(info, tsoutil.ComposeTSByTime(time.Now()))

	DeleteLastReplicatedTimeTick(info)

	assert.False(t, metrics.CDCLastReplicatedTimeTick.DeleteLabelValues(source, target))
}
