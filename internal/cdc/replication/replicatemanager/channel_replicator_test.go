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

package replicatemanager

import (
	"context"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/cdc/cluster"
	"github.com/milvus-io/milvus/internal/cdc/meta"
	"github.com/milvus-io/milvus/internal/cdc/replication/replicatestream"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	pulsar2 "github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func newMockPulsarMessageID() *commonpb.MessageID {
	pulsarID := pulsar.EarliestMessageID()
	msgID := pulsar2.NewPulsarID(pulsarID).Marshal()
	return &commonpb.MessageID{
		Id:      msgID,
		WALName: commonpb.WALName_Pulsar,
	}
}

func TestChannelReplicator_StartReplicateChannel(t *testing.T) {
	mockMilvusClient := cluster.NewMockMilvusClient(t)
	mockMilvusClient.EXPECT().GetReplicateInfo(mock.Anything, mock.Anything).
		Return(&milvuspb.GetReplicateInfoResponse{
			Checkpoint: &commonpb.ReplicateCheckpoint{
				Pchannel:  "test-source-channel",
				MessageId: newMockPulsarMessageID(),
			},
		}, nil)
	mockMilvusClient.EXPECT().Close(mock.Anything).Return(nil)

	scanner := mock_streaming.NewMockScanner(t)
	scanner.EXPECT().Close().Return()
	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().Read(mock.Anything, mock.Anything).Return(scanner)
	streaming.SetWALForTest(wal)

	rs := replicatestream.NewMockReplicateStreamClient(t)
	rs.EXPECT().Close().Return()

	mc := &commonpb.MilvusCluster{ClusterId: "test-cluster"}
	replicateInfo := &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "test-source-channel",
		TargetChannelName: "test-target-channel",
		TargetCluster:     mc,
	}
	replicator := NewChannelReplicator(&meta.ReplicateChannel{
		Value:       replicateInfo,
		ModRevision: 0,
	})
	assert.NotNil(t, replicator)

	replicator.(*channelReplicator).createRscFunc = func(ctx context.Context,
		c cluster.MilvusClient,
		rm *meta.ReplicateChannel,
	) replicatestream.ReplicateStreamClient {
		return rs
	}
	replicator.(*channelReplicator).createMcFunc = func(ctx context.Context,
		cluster *commonpb.MilvusCluster,
	) (cluster.MilvusClient, error) {
		return mockMilvusClient, nil
	}

	replicator.StartReplication()
	time.Sleep(200 * time.Millisecond)
	replicator.StopReplication()
}

// TestChannelReplicatorConsumeLoopDeletesLagSeriesOnRemoval verifies the
// in-band removal path: when the consume loop replicates an
// AlterReplicateConfig message that removes this replication, the lag series
// is deleted.
func TestChannelReplicatorConsumeLoopDeletesLagSeriesOnRemoval(t *testing.T) {
	paramtable.Get().Save(paramtable.Get().CommonCfg.ClusterPrefix.Key, "current-cluster")
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.ClusterPrefix.Key)

	source, target := "TestRemoval-source", "TestRemoval-target"
	replicateInfo := &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: source,
		TargetChannelName: target,
		TargetCluster:     &commonpb.MilvusCluster{ClusterId: "removed-target-cluster"},
	}

	// An AlterReplicateConfig whose topology no longer contains the
	// current->target edge, i.e. this replication is removed.
	msg := message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ReplicateConfiguration: &commonpb.ReplicateConfiguration{
				Clusters: []*commonpb.MilvusCluster{
					{
						ClusterId:       "current-cluster",
						ConnectionParam: &commonpb.ConnectionParam{Uri: "localhost:19530"},
						Pchannels:       []string{source},
					},
				},
			},
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithAllVChannel().
		MustBuildMutable().
		WithLastConfirmedUseMessageID().
		WithTimeTick(1).
		IntoImmutableMessage(pulsar2.NewPulsarID(pulsar.EarliestMessageID()))

	rs := replicatestream.NewMockReplicateStreamClient(t)
	rs.EXPECT().Replicate(mock.Anything).Return(nil)
	rs.EXPECT().BlockUntilFinish().Return()

	replicator := &channelReplicator{
		channel: &meta.ReplicateChannel{
			Key:         "removal-key",
			ModRevision: 1,
			Value:       replicateInfo,
		},
		streamClient:  rs,
		msgChan:       make(adaptor.ChanMessageHandler),
		asyncNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
	}
	replicatestream.InitLastReplicatedTimeTick(replicateInfo, tsoutil.ComposeTSByTime(time.Now()))

	go func() { replicator.msgChan <- msg }()
	replicator.startConsumeLoop()

	assert.False(t, metrics.CDCLastReplicatedTimeTick.DeleteLabelValues(source, target))
}

func TestChannelReplicatorInitMetricFromInitializedCheckpoint(t *testing.T) {
	source, target := "TestInitMetric-source", "TestInitMetric-target"
	checkpoint := tsoutil.ComposeTSByTime(time.Now().Add(-5 * time.Minute))
	defer metrics.CDCLastReplicatedTimeTick.DeleteLabelValues(source, target)

	replicator := NewChannelReplicator(&meta.ReplicateChannel{
		Value: &streamingpb.ReplicatePChannelMeta{
			SourceChannelName: source,
			TargetChannelName: target,
			InitializedCheckpoint: &commonpb.ReplicateCheckpoint{
				TimeTick: checkpoint,
			},
		},
	})
	channelReplicator, ok := replicator.(*channelReplicator)
	assert.True(t, ok)

	channelReplicator.initLastReplicatedTimeTickMetric()

	got := testutil.ToFloat64(metrics.CDCLastReplicatedTimeTick.WithLabelValues(source, target))
	assert.InDelta(t, tsoutil.PhysicalTimeSeconds(checkpoint), got, 1)
}

// TestChannelReplicatorConsumeLoopSkipsStaleTopologyChange covers the case where
// the replicator resumes from a checkpoint that predates its own creation — the
// position a `restore secondary` leaves on the target. The replay then walks
// over a topology change that removed this edge before it was re-created. That
// message must neither be forwarded to the secondary, where a configuration
// without the secondary turns it back into a standalone primary, nor end the
// consume loop.
func TestChannelReplicatorConsumeLoopSkipsStaleTopologyChange(t *testing.T) {
	paramtable.Get().Save(paramtable.Get().CommonCfg.ClusterPrefix.Key, "current-cluster")
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.ClusterPrefix.Key)

	source, target := "TestStale-source", "TestStale-target"
	// The task was created by the configuration appended at time tick 100.
	replicateInfo := &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: source,
		TargetChannelName: target,
		TargetCluster:     &commonpb.MilvusCluster{ClusterId: "removed-target-cluster"},
		InitializedCheckpoint: &commonpb.ReplicateCheckpoint{
			ClusterId: "current-cluster",
			Pchannel:  source,
			TimeTick:  100,
		},
	}

	// A detach broadcast from before the task existed.
	staleMsg := message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ReplicateConfiguration: &commonpb.ReplicateConfiguration{
				Clusters: []*commonpb.MilvusCluster{
					{
						ClusterId:       "current-cluster",
						ConnectionParam: &commonpb.ConnectionParam{Uri: "localhost:19530"},
						Pchannels:       []string{source},
					},
				},
			},
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithAllVChannel().
		MustBuildMutable().
		WithLastConfirmedUseMessageID().
		WithTimeTick(50).
		IntoImmutableMessage(pulsar2.NewPulsarID(pulsar.EarliestMessageID()))

	rs := replicatestream.NewMockReplicateStreamClient(t)
	// No Replicate and no BlockUntilFinish expectation: the message is dropped
	// before either is reached, and the mock fails the test on any other call.

	replicator := &channelReplicator{
		channel: &meta.ReplicateChannel{
			Key:         "stale-key",
			ModRevision: 1,
			Value:       replicateInfo,
		},
		streamClient:  rs,
		msgChan:       make(adaptor.ChanMessageHandler),
		asyncNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
	}

	done := make(chan struct{})
	go func() {
		replicator.startConsumeLoop()
		close(done)
	}()
	replicator.msgChan <- staleMsg

	// The loop must still be running: give it a moment, then stop it ourselves.
	select {
	case <-done:
		t.Fatal("consume loop stopped on a topology change that predates the task")
	case <-time.After(200 * time.Millisecond):
	}
	replicator.asyncNotifier.Cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consume loop did not stop after cancel")
	}
}
