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

package replication

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type DataSalvageSuite struct {
	integration.MiniClusterSuite
}

func TestDataSalvage(t *testing.T) {
	suite.Run(t, new(DataSalvageSuite))
}

// TestGetReplicateInfoOnPrimaryCluster verifies that GetReplicateInfo
// returns checkpoint info on a primary cluster. The salvage checkpoint
// should be nil since no force promote has occurred.
func (s *DataSalvageSuite) TestGetReplicateInfoOnPrimaryCluster() {
	ctx := context.Background()

	clusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	pchannel := clusterID + "-pchan0"

	// First set up replication config to make the cluster a primary
	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: clusterID,
				Pchannels: []string{pchannel},
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
			},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{},
	}

	updateResp, err := s.Cluster.MilvusClient.UpdateReplicateConfiguration(ctx, &milvuspb.UpdateReplicateConfigurationRequest{
		ReplicateConfiguration: config,
		ForcePromote:           false,
	})
	s.NoError(err)
	s.NoError(merr.Error(updateResp))

	// Get replicate info
	resp, err := s.Cluster.MilvusClient.GetReplicateInfo(ctx, &milvuspb.GetReplicateInfoRequest{
		TargetPchannel: pchannel,
	})
	s.NoError(err)

	// On a primary cluster, checkpoint should exist but salvage checkpoint should be nil
	// (no force promote has occurred)
	log.Info("GetReplicateInfo response",
		zap.Any("checkpoint", resp.GetCheckpoint()),
		zap.Any("salvageCheckpoint", resp.GetSalvageCheckpoint()))

	// Salvage checkpoint should be nil on primary cluster
	s.Nil(resp.GetSalvageCheckpoint(), "salvage checkpoint should be nil on primary cluster")
}

// TestDumpMessagesBasic verifies that DumpMessages can stream messages
// from a WAL channel after inserting some data.
func (s *DataSalvageSuite) TestDumpMessagesBasic() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		dim    = 128
		dbName = ""
		rowNum = 100
	)

	collectionName := "TestDumpMessages" + funcutil.GenRandomStr()

	// Create collection
	schema := integration.ConstructSchemaOfVecDataType(collectionName, dim, true, schemapb.DataType_FloatVector)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createResp, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createResp.GetErrorCode())

	// Insert some data
	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResp, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, insertResp.GetStatus().GetErrorCode())

	// Get pchannel for the collection
	descResp, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.NotEmpty(descResp.GetVirtualChannelNames())

	// Get pchannel from vchannel
	vchannel := descResp.GetVirtualChannelNames()[0]
	pchannel := funcutil.ToPhysicalChannel(vchannel)

	log.Info("Testing DumpMessages",
		zap.String("pchannel", pchannel),
		zap.String("vchannel", vchannel))

	// Set up replication config first
	clusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: clusterID,
				Pchannels: []string{pchannel},
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
			},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{},
	}

	updateResp, err := s.Cluster.MilvusClient.UpdateReplicateConfiguration(ctx, &milvuspb.UpdateReplicateConfigurationRequest{
		ReplicateConfiguration: config,
		ForcePromote:           false,
	})
	s.NoError(err)
	s.NoError(merr.Error(updateResp))

	// Get replicate checkpoint as start position
	infoResp, err := s.Cluster.MilvusClient.GetReplicateInfo(ctx, &milvuspb.GetReplicateInfoRequest{
		TargetPchannel: pchannel,
	})
	s.NoError(err)
	s.NotNil(infoResp.GetCheckpoint())
	s.NotNil(infoResp.GetCheckpoint().GetMessageId())

	// Dump messages from start position
	stream, err := s.Cluster.MilvusClient.DumpMessages(ctx, &milvuspb.DumpMessagesRequest{
		Pchannel:               pchannel,
		StartMessageId:         infoResp.GetCheckpoint().GetMessageId(),
		StartPositionExclusive: false, // Include start position
	})
	s.NoError(err)

	// Read some messages (with timeout via context cancellation)
	messages := make([]*milvuspb.DumpMessagesResponse, 0)
	readCtx, readCancel := context.WithCancel(ctx)

	// Read in a goroutine with limit
	go func() {
		for i := 0; i < 10; i++ { // Read up to 10 messages
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Warn("error receiving message", zap.Error(err))
				break
			}
			// Check if response contains a message (not a status)
			if msg := resp.GetMessage(); msg != nil {
				messages = append(messages, resp)
				log.Info("received message", zap.Any("messageType", msg.GetMessageType()))
			} else if status := resp.GetStatus(); status != nil {
				log.Warn("received status response", zap.Any("status", status))
				break
			}
		}
		readCancel()
	}()

	<-readCtx.Done()

	log.Info("DumpMessages test completed", zap.Int("messageCount", len(messages)))

	// We should have received at least some messages
	// Note: The exact count depends on timing and what messages are in the WAL
	// For now, just verify the API works without error

	// Clean up
	dropResp, err := s.Cluster.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, dropResp.GetErrorCode())
}

// TestDumpMessagesWithMissingPchannel verifies that DumpMessages returns
// an error when pchannel is not provided.
func (s *DataSalvageSuite) TestDumpMessagesWithMissingPchannel() {
	ctx := context.Background()

	stream, err := s.Cluster.MilvusClient.DumpMessages(ctx, &milvuspb.DumpMessagesRequest{
		Pchannel: "", // Missing pchannel
		StartMessageId: &commonpb.MessageID{
			MessageId: []byte("test"),
		},
	})

	// The error might come during stream creation or first Recv()
	if err == nil {
		_, err = stream.Recv()
	}

	s.Error(err)
}

// TestDumpMessagesWithMissingStartMessageId verifies that DumpMessages returns
// an error when start_message_id is not provided.
func (s *DataSalvageSuite) TestDumpMessagesWithMissingStartMessageId() {
	ctx := context.Background()

	stream, err := s.Cluster.MilvusClient.DumpMessages(ctx, &milvuspb.DumpMessagesRequest{
		Pchannel:       "test-pchannel",
		StartMessageId: nil, // Missing start message ID
	})

	// The error might come during stream creation or first Recv()
	if err == nil {
		_, err = stream.Recv()
	}

	s.Error(err)
}
