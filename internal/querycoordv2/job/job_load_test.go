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

package job

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type LoadCollectionJobSuite struct {
	suite.Suite
}

func (suite *LoadCollectionJobSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *LoadCollectionJobSuite) SetupTest() {
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()
}

func (suite *LoadCollectionJobSuite) buildBroadcastResult(collectionID int64, partitionIDs []int64) message.BroadcastResultAlterLoadConfigMessageV2 {
	controlChannel := "_ctrl_channel"
	replicas := []*messagespb.LoadReplicaConfig{
		{ReplicaId: 1, ResourceGroupName: "__default_resource_group"},
	}
	broadcastMsg := message.NewAlterLoadConfigMessageBuilderV2().
		WithHeader(&messagespb.AlterLoadConfigMessageHeader{
			CollectionId: collectionID,
			PartitionIds: partitionIDs,
			Replicas:     replicas,
		}).
		WithBody(&messagespb.AlterLoadConfigMessageBody{}).
		WithBroadcast([]string{controlChannel}).
		MustBuildBroadcast()

	specializedMsg := message.MustAsBroadcastAlterLoadConfigMessageV2(broadcastMsg)
	return message.BroadcastResultAlterLoadConfigMessageV2{
		Message: specializedMsg,
		Results: map[string]*message.AppendResult{
			controlChannel: {},
		},
	}
}

// TestDescribeCollectionNotFound tests that Execute returns nil when the collection is not found.
func (suite *LoadCollectionJobSuite) TestDescribeCollectionNotFound() {
	ctx := context.Background()
	collectionID := int64(1000)

	broker := meta.NewMockBroker(suite.T())
	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).
		Return(nil, merr.WrapErrCollectionNotFound(collectionID))

	result := suite.buildBroadcastResult(collectionID, []int64{100, 101})
	job := NewLoadCollectionJob(ctx, result, nil, nil, broker, nil, nil, nil, nil, nil)

	err := job.Execute()
	suite.NoError(err)
}

// TestDescribeCollectionOtherError tests that Execute returns the error when DescribeCollection fails.
func (suite *LoadCollectionJobSuite) TestDescribeCollectionOtherError() {
	ctx := context.Background()
	collectionID := int64(1001)

	expectedErr := errors.New("broker unavailable")
	broker := meta.NewMockBroker(suite.T())
	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).
		Return(nil, expectedErr)

	result := suite.buildBroadcastResult(collectionID, []int64{200, 201})
	job := NewLoadCollectionJob(ctx, result, nil, nil, broker, nil, nil, nil, nil, nil)

	err := job.Execute()
	suite.Error(err)
	suite.True(errors.Is(err, expectedErr))
}

// TestDescribeCollectionSuccess tests that Execute proceeds with VirtualChannelNames from DescribeCollection.
func (suite *LoadCollectionJobSuite) TestDescribeCollectionSuccess() {
	ctx := context.Background()
	collectionID := int64(1002)
	channels := []string{"ch1", "ch2"}

	broker := meta.NewMockBroker(suite.T())
	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).
		Return(&milvuspb.DescribeCollectionResponse{
			CollectionID:        collectionID,
			VirtualChannelNames: channels,
		}, nil)

	result := suite.buildBroadcastResult(collectionID, []int64{300, 301})
	// We pass nil for meta to test that DescribeCollection is called before SpawnReplicasWithReplicaConfig.
	// SpawnReplicasWithReplicaConfig will panic on nil meta, proving that DescribeCollection was called first.
	job := NewLoadCollectionJob(ctx, result, nil, nil, broker, nil, nil, nil, nil, nil)

	// This should panic at SpawnReplicasWithReplicaConfig because meta is nil,
	// but this proves DescribeCollection was called and returned successfully first.
	suite.Panics(func() {
		job.Execute()
	})
}

func TestLoadCollectionJob(t *testing.T) {
	suite.Run(t, new(LoadCollectionJobSuite))
}
