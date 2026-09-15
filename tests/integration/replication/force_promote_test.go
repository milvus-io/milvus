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
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/tests/integration"
)

type ForcePromoteSuite struct {
	integration.MiniClusterSuite
}

func TestForcePromote(t *testing.T) {
	suite.Run(t, new(ForcePromoteSuite))
}

// MiniClusterV3 configures two DML channels in the child processes.
func replicationPChannels(clusterID string) []string {
	return []string{clusterID + "-rootcoord-dml_0", clusterID + "-rootcoord-dml_1"}
}

// TestNormalUpdateReplicateConfiguration verifies that normal (non-force) updates
// work correctly on a primary cluster.
func (s *ForcePromoteSuite) TestNormalUpdateReplicateConfiguration() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clusterID := s.Cluster.RootPath()
	pchannels := replicationPChannels(clusterID)

	// Create a valid single-cluster config (making current cluster primary)
	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: clusterID,
				Pchannels: pchannels,
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "http://localhost:19530",
					Token: "test-token",
				},
			},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{},
	}

	// Call without force_promote
	req := &milvuspb.UpdateReplicateConfigurationRequest{
		ReplicateConfiguration: config,
		ForcePromote:           false,
	}

	resp, err := s.Cluster.MilvusClient.UpdateReplicateConfiguration(ctx, req)

	// Normal update on primary should succeed
	s.NoError(err)
	s.NotNil(resp)
	err = merr.Error(resp)
	s.NoError(err)
}

// TestUpdateReplicateConfigurationIdempotent verifies that calling
// UpdateReplicateConfiguration with the same configuration is idempotent.
func (s *ForcePromoteSuite) TestUpdateReplicateConfigurationIdempotent() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clusterID := s.Cluster.RootPath()
	pchannels := replicationPChannels(clusterID)

	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: clusterID,
				Pchannels: pchannels,
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "http://localhost:19530",
					Token: "test-token",
				},
			},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{},
	}

	req := &milvuspb.UpdateReplicateConfigurationRequest{
		ReplicateConfiguration: config,
		ForcePromote:           false,
	}

	// First call
	resp1, err := s.Cluster.MilvusClient.UpdateReplicateConfiguration(ctx, req)
	s.NoError(err)
	s.NotNil(resp1)
	s.NoError(merr.Error(resp1))

	// Second call with same config should also succeed (idempotent)
	resp2, err := s.Cluster.MilvusClient.UpdateReplicateConfiguration(ctx, req)
	s.NoError(err)
	s.NotNil(resp2)
	s.NoError(merr.Error(resp2))
}
