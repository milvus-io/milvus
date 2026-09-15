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
	"encoding/base64"
	"testing"
	"time"

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
	"github.com/milvus-io/milvus/tests/integration"
)

type DataSalvageSuite struct {
	integration.MiniClusterSuite
}

func (s *DataSalvageSuite) SetupSuite() {
	// Persisted start positions below are decoded as Pulsar message IDs.
	s.WithMilvusConfig("mq.type", "pulsar")
	s.MiniClusterSuite.SetupSuite()
}

func TestDataSalvage(t *testing.T) {
	suite.Run(t, new(DataSalvageSuite))
}

// TestGetReplicateInfoOnPrimaryCluster verifies that GetReplicateInfo
// returns no secondary checkpoint on a primary cluster. The salvage checkpoint
// should also be nil since no force promote has occurred.
func (s *DataSalvageSuite) TestGetReplicateInfoOnPrimaryCluster() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clusterID := s.Cluster.RootPath()
	pchannel := replicationPChannels(clusterID)[0]

	// First set up replication config to make the cluster a primary
	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: clusterID,
				Pchannels: replicationPChannels(clusterID),
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
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(updateResp))

	// Get replicate info
	resp, err := s.Cluster.MilvusClient.GetReplicateInfo(ctx, &milvuspb.GetReplicateInfoRequest{
		TargetPchannel: pchannel,
	})
	s.Require().NoError(err)

	// A primary has no live secondary checkpoint or saved salvage checkpoint.
	s.Nil(resp.GetCheckpoint())
	log.Info("GetReplicateInfo response",
		zap.Any("checkpoint", resp.GetCheckpoint()),
		zap.Any("salvageCheckpoint", resp.GetSalvageCheckpoint()))

	// Salvage checkpoint should be nil on primary cluster
	s.Nil(resp.GetSalvageCheckpoint(), "salvage checkpoint should be nil on primary cluster")
}

// TestDumpMessagesBasic verifies that DumpMessages can stream messages
// from a WAL channel after inserting some data.
func (s *DataSalvageSuite) TestDumpMessagesBasic() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	s.Require().NoError(err)

	createResp, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, createResp.GetErrorCode())

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
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, insertResp.GetStatus().GetErrorCode())

	// Get pchannel for the collection
	descResp, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		CollectionName: collectionName,
	})
	s.Require().NoError(err)
	s.Require().NotEmpty(descResp.GetVirtualChannelNames())

	// Get pchannel from vchannel
	vchannel := descResp.GetVirtualChannelNames()[0]
	pchannel := funcutil.ToPhysicalChannel(vchannel)

	log.Info("Testing DumpMessages",
		zap.String("pchannel", pchannel),
		zap.String("vchannel", vchannel))

	// Set up replication config first
	clusterID := s.Cluster.RootPath()
	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: clusterID,
				Pchannels: replicationPChannels(clusterID),
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
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(updateResp))

	// The primary has no live replication checkpoint. Read the collection's
	// persisted WAL start position from MixCoord instead.
	internalDesc, err := s.Cluster.MixCoordClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DescribeCollection},
		DbName:         "default",
		CollectionName: collectionName,
	})
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(internalDesc.GetStatus()))
	var startPosition []byte
	for _, position := range internalDesc.GetStartPositions() {
		if position.GetKey() == pchannel {
			startPosition = position.GetData()
			break
		}
	}
	s.Require().NotEmpty(startPosition)
	stream, err := s.Cluster.MilvusClient.DumpMessages(ctx, &milvuspb.DumpMessagesRequest{
		Pchannel: pchannel,
		StartMessageId: &commonpb.MessageID{
			Id:      base64.StdEncoding.EncodeToString(startPosition),
			WALName: commonpb.WALName_Pulsar,
		},
	})
	s.Require().NoError(err)
	resp, err := stream.Recv()
	s.Require().NoError(err)
	s.Require().NotNil(resp.GetMessage())

	// Clean up
	dropResp, err := s.Cluster.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		CollectionName: collectionName,
	})
	s.Require().NoError(err)
	s.Equal(commonpb.ErrorCode_Success, dropResp.GetErrorCode())
}

// TestDumpMessagesWithMissingPchannel verifies that DumpMessages returns
// an error when pchannel is not provided.
func (s *DataSalvageSuite) TestDumpMessagesWithMissingPchannel() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := s.Cluster.MilvusClient.DumpMessages(ctx, &milvuspb.DumpMessagesRequest{
		Pchannel: "", // Missing pchannel
		StartMessageId: &commonpb.MessageID{
			Id: "test",
		},
	})

	// The error might come during stream creation or first Recv()
	if err == nil {
		_, err = stream.Recv()
	}

	s.Error(err)
	s.NoError(ctx.Err(), "invalid requests must fail before the deadline")
}

// TestDumpMessagesWithMissingStartMessageId verifies that DumpMessages returns
// an error when start_message_id is not provided.
func (s *DataSalvageSuite) TestDumpMessagesWithMissingStartMessageId() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := s.Cluster.MilvusClient.DumpMessages(ctx, &milvuspb.DumpMessagesRequest{
		Pchannel:       "test-pchannel",
		StartMessageId: nil, // Missing start message ID
	})

	// The error might come during stream creation or first Recv()
	if err == nil {
		_, err = stream.Recv()
	}

	s.Error(err)
	s.NoError(ctx.Err(), "invalid requests must fail before the deadline")
}

// TestDumpMessagesWithMalformedStartMessageId verifies that DumpMessages
// returns an error (instead of panicking and crashing the process) when
// start_message_id is non-empty but cannot be unmarshaled. See issue #50341.
func (s *DataSalvageSuite) TestDumpMessagesWithMalformedStartMessageId() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := s.Cluster.MilvusClient.DumpMessages(ctx, &milvuspb.DumpMessagesRequest{
		Pchannel: "test-pchannel",
		StartMessageId: &commonpb.MessageID{
			Id:      "not-a-valid-base64-msgid!!!", // non-empty but undecodable
			WALName: commonpb.WALName_Pulsar,
		},
	})

	// The error might come during stream creation or first Recv()
	if err == nil {
		_, err = stream.Recv()
	}

	s.Error(err)
	s.NoError(ctx.Err(), "invalid requests must fail before the deadline")

	// The server must stay up: a follow-up RPC should still succeed.
	resp, err := s.Cluster.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.Require().NoError(err)
	s.NoError(merr.Error(resp.GetStatus()))
}
