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

package milvusclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type ReplicateSuite struct {
	MockSuiteBase
}

func (s *ReplicateSuite) TestGetReplicateConfiguration() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		expectedConfig := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{
					ClusterId: "source-cluster",
					Pchannels: []string{"source-channel-1"},
					ConnectionParam: &commonpb.ConnectionParam{
						Uri: "localhost:19530",
					},
				},
				{
					ClusterId: "target-cluster",
					Pchannels: []string{"target-channel-1"},
					ConnectionParam: &commonpb.ConnectionParam{
						Uri: "localhost:19531",
					},
				},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "source-cluster", TargetClusterId: "target-cluster"},
			},
		}

		s.mock.EXPECT().GetReplicateConfiguration(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, req *milvuspb.GetReplicateConfigurationRequest) (*milvuspb.GetReplicateConfigurationResponse, error) {
				s.NotNil(req)
				return &milvuspb.GetReplicateConfigurationResponse{
					Status:        merr.Success(),
					Configuration: expectedConfig,
				}, nil
			}).Once()

		config, err := s.client.GetReplicateConfiguration(ctx)
		s.NoError(err)
		s.True(proto.Equal(expectedConfig, config))
	})

	s.Run("failure", func() {
		s.mock.EXPECT().GetReplicateConfiguration(mock.Anything, mock.Anything).
			Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.GetReplicateConfiguration(ctx)
		s.Error(err)
	})
}

func TestReplicate(t *testing.T) {
	suite.Run(t, new(ReplicateSuite))
}
