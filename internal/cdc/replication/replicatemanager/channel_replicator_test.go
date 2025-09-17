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

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/cdc/cluster"
	"github.com/milvus-io/milvus/internal/cdc/replication/replicatestream"
	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	pulsar2 "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
			Checkpoints: []*commonpb.ReplicateCheckpoint{
				{
					Pchannel:  "test-source-channel",
					MessageId: newMockPulsarMessageID(),
				},
			},
		}, nil)
	mockMilvusClient.EXPECT().Close(mock.Anything).Return(nil)

	mockClusterClient := cluster.NewMockClusterClient(t)
	mockClusterClient.EXPECT().CreateMilvusClient(mock.Anything, mock.Anything).
		Return(mockMilvusClient, nil)
	resource.InitForTest(t,
		resource.OptClusterClient(mockClusterClient),
	)

	scanner := mock_streaming.NewMockScanner(t)
	scanner.EXPECT().Close().Return()
	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().Read(mock.Anything, mock.Anything).Return(scanner)
	streaming.SetWALForTest(wal)

	rs := replicatestream.NewMockReplicateStreamClient(t)
	rs.EXPECT().Close().Return()

	cluster := &commonpb.MilvusCluster{ClusterId: "test-cluster"}
	replicateInfo := &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "test-source-channel",
		TargetChannelName: "test-target-channel",
		TargetCluster:     cluster,
	}
	replicator := NewChannelReplicator(replicateInfo)
	replicator.(*channelReplicator).createRscFunc = func(ctx context.Context,
		replicateInfo *streamingpb.ReplicatePChannelMeta,
	) replicatestream.ReplicateStreamClient {
		return rs
	}
	assert.NotNil(t, replicator)

	replicator.StartReplicate()
	replicator.StopReplicate()

	state := replicator.GetState()
	assert.Equal(t, typeutil.LifetimeStateStopped, state)
}
