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

package util

import (
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	pulsar2 "github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	testCurrentCluster = "current-cluster"
	testTargetCluster  = "target-cluster"
	testSourceChannel  = "current-cluster-rootcoord-dml_0"
	testTargetChannel  = "target-cluster-rootcoord-dml_0"
)

// task builds the replication task metadata under test. initTimeTick is the
// time tick of the AlterReplicateConfig that created it; zero means the field
// is absent, as it is for tasks written by an older version.
func task(initTimeTick uint64) *streamingpb.ReplicatePChannelMeta {
	meta := &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: testSourceChannel,
		TargetChannelName: testTargetChannel,
		TargetCluster:     &commonpb.MilvusCluster{ClusterId: testTargetCluster},
	}
	if initTimeTick != 0 {
		meta.InitializedCheckpoint = &commonpb.ReplicateCheckpoint{
			ClusterId: testCurrentCluster,
			Pchannel:  testSourceChannel,
			TimeTick:  initTimeTick,
		}
	}
	return meta
}

// alterMsg builds an AlterReplicateConfig message at the given time tick. When
// withEdge is true the topology still carries current -> target; otherwise the
// current cluster stands alone, which is what a detach broadcasts.
func alterMsg(timeTick uint64, withEdge bool, ignore bool) message.ImmutableMessage {
	clusters := []*commonpb.MilvusCluster{
		{
			ClusterId:       testCurrentCluster,
			ConnectionParam: &commonpb.ConnectionParam{Uri: "localhost:19530"},
			Pchannels:       []string{testSourceChannel},
		},
	}
	var topology []*commonpb.CrossClusterTopology
	if withEdge {
		clusters = append(clusters, &commonpb.MilvusCluster{
			ClusterId:       testTargetCluster,
			ConnectionParam: &commonpb.ConnectionParam{Uri: "localhost:19531"},
			Pchannels:       []string{testTargetChannel},
		})
		topology = []*commonpb.CrossClusterTopology{
			{SourceClusterId: testCurrentCluster, TargetClusterId: testTargetCluster},
		}
	}

	return message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ReplicateConfiguration: &commonpb.ReplicateConfiguration{
				Clusters:             clusters,
				CrossClusterTopology: topology,
			},
			Ignore: ignore,
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithAllVChannel().
		MustBuildMutable().
		WithLastConfirmedUseMessageID().
		WithTimeTick(timeTick).
		IntoImmutableMessage(pulsar2.NewPulsarID(pulsar.EarliestMessageID()))
}

func withCurrentCluster(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().CommonCfg.ClusterPrefix.Key, testCurrentCluster)
	t.Cleanup(func() { paramtable.Get().Reset(paramtable.Get().CommonCfg.ClusterPrefix.Key) })
}

func TestIsStaleTopologyChange(t *testing.T) {
	// The task was created by the configuration appended at time tick 100.
	const created = uint64(100)

	t.Run("older than the task is stale", func(t *testing.T) {
		assert.True(t, IsStaleTopologyChange(alterMsg(created-1, false, false), task(created)))
	})

	t.Run("the message that created the task is stale", func(t *testing.T) {
		// Its own creating message carries no instruction for it either.
		assert.True(t, IsStaleTopologyChange(alterMsg(created, true, false), task(created)))
	})

	t.Run("newer than the task is current", func(t *testing.T) {
		assert.False(t, IsStaleTopologyChange(alterMsg(created+1, false, false), task(created)))
	})

	t.Run("no initialized time tick enforces no ordering", func(t *testing.T) {
		// Tasks written by an older version keep the previous behaviour.
		assert.False(t, IsStaleTopologyChange(alterMsg(1, false, false), task(0)))
	})
}

func TestIsReplicationRemovedByAlterReplicateConfigMessage(t *testing.T) {
	withCurrentCluster(t)
	const created = uint64(100)

	t.Run("a current detach removes the replication", func(t *testing.T) {
		assert.True(t, IsReplicationRemovedByAlterReplicateConfigMessage(
			alterMsg(created+10, false, false), task(created)))
	})

	t.Run("a current configuration that keeps the edge does not", func(t *testing.T) {
		assert.False(t, IsReplicationRemovedByAlterReplicateConfigMessage(
			alterMsg(created+10, true, false), task(created)))
	})

	// The regression: replaying the WAL from a checkpoint that predates the task
	// walks over topology changes that removed this edge before it was
	// re-created. Acting on them makes the replicator delete itself moments
	// after starting.
	t.Run("a detach that predates the task does not remove it", func(t *testing.T) {
		assert.False(t, IsReplicationRemovedByAlterReplicateConfigMessage(
			alterMsg(created-10, false, false), task(created)))
	})

	t.Run("a detach predating a task with no initialized time tick still removes it", func(t *testing.T) {
		assert.True(t, IsReplicationRemovedByAlterReplicateConfigMessage(
			alterMsg(1, false, false), task(0)))
	})

	t.Run("an ignored message never removes the replication", func(t *testing.T) {
		assert.False(t, IsReplicationRemovedByAlterReplicateConfigMessage(
			alterMsg(created+10, false, true), task(created)))
	})
}
