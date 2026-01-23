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
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type ForcePromoteSuite struct {
	integration.MiniClusterSuite
}

func TestForcePromote(t *testing.T) {
	suite.Run(t, new(ForcePromoteSuite))
}

// TestForcePromoteOnPrimaryClusterShouldFail verifies that force promote
// returns an error when called on a primary cluster (no replication configured).
// Force promote is only intended for secondary clusters during failover.
func (s *ForcePromoteSuite) TestForcePromoteOnPrimaryClusterShouldFail() {
	ctx := context.Background()

	// Get current cluster ID
	clusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()

	// Create a valid force promote config (single cluster, no topology)
	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: clusterID,
				Pchannels: []string{clusterID + "-pchan0"},
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
			},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{},
	}

	// Call UpdateReplicateConfiguration with force_promote=true on primary cluster
	req := &milvuspb.UpdateReplicateConfigurationRequest{
		ReplicateConfiguration: config,
		ForcePromote:           true,
	}

	resp, err := s.Cluster.MilvusClient.UpdateReplicateConfiguration(ctx, req)

	// Should return an error because we're on a primary cluster
	s.NoError(err) // RPC should succeed
	s.NotNil(resp)
	err = merr.Error(resp)
	s.Error(err)
	s.Contains(err.Error(), "force promote can only be used on secondary clusters")
}

// TestForcePromoteWithMultipleClustersShouldFail verifies that force promote
// rejects configurations with more than one cluster.
func (s *ForcePromoteSuite) TestForcePromoteWithMultipleClustersShouldFail() {
	ctx := context.Background()

	clusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()

	// Create config with multiple clusters (invalid for force promote)
	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: clusterID,
				Pchannels: []string{clusterID + "-pchan0"},
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
			},
			{
				ClusterId: "other-cluster",
				Pchannels: []string{"other-cluster-pchan0"},
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "other-host:19530",
					Token: "other-token",
				},
			},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{},
	}

	req := &milvuspb.UpdateReplicateConfigurationRequest{
		ReplicateConfiguration: config,
		ForcePromote:           true,
	}

	resp, err := s.Cluster.MilvusClient.UpdateReplicateConfiguration(ctx, req)

	// Should return an error about multiple clusters
	s.NoError(err)
	s.NotNil(resp)
	err = merr.Error(resp)
	s.Error(err)
	// Either fails at validation or at the primary check first
	// The exact error depends on which validation runs first
	errMsg := err.Error()
	s.True(
		strings.Contains(errMsg, "primary count is not 1") ||
			strings.Contains(errMsg, "force promote can only be used on secondary clusters"),
		"unexpected error: %s", errMsg,
	)
}

// TestForcePromoteWithTopologyShouldFail verifies that force promote
// rejects configurations that contain cross-cluster topology.
func (s *ForcePromoteSuite) TestForcePromoteWithTopologyShouldFail() {
	ctx := context.Background()

	clusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()

	// Create config with topology (invalid for force promote)
	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: clusterID,
				Pchannels: []string{clusterID + "-pchan0"},
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
			},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{
				SourceClusterId: clusterID,
				TargetClusterId: "other-cluster",
			},
		},
	}

	req := &milvuspb.UpdateReplicateConfigurationRequest{
		ReplicateConfiguration: config,
		ForcePromote:           true,
	}

	resp, err := s.Cluster.MilvusClient.UpdateReplicateConfiguration(ctx, req)

	// Should return an error about topology
	s.NoError(err)
	s.NotNil(resp)
	err = merr.Error(resp)
	s.Error(err)
	// The error might be about wrong configuration or the primary check
	errMsg := err.Error()
	s.True(
		strings.Contains(errMsg, "wrong replicate configuration") ||
			strings.Contains(errMsg, "force promote can only be used on secondary clusters"),
		"unexpected error: %s", errMsg,
	)
}

// TestForcePromoteWithWrongClusterShouldFail verifies that force promote
// rejects configurations where the cluster ID doesn't match the current cluster.
func (s *ForcePromoteSuite) TestForcePromoteWithWrongClusterShouldFail() {
	ctx := context.Background()

	// Create config with wrong cluster ID
	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: "wrong-cluster-id",
				Pchannels: []string{"wrong-cluster-id-pchan0"},
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
			},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{},
	}

	req := &milvuspb.UpdateReplicateConfigurationRequest{
		ReplicateConfiguration: config,
		ForcePromote:           true,
	}

	resp, err := s.Cluster.MilvusClient.UpdateReplicateConfiguration(ctx, req)

	// Should return an error about wrong cluster
	s.NoError(err)
	s.NotNil(resp)
	err = merr.Error(resp)
	s.Error(err)
	// Should fail either with current cluster not found or primary check
	errMsg := err.Error()
	s.True(
		strings.Contains(errMsg, "current cluster not found") ||
			strings.Contains(errMsg, "force promote can only be used on secondary clusters"),
		"unexpected error: %s", errMsg,
	)
}

// TestNormalUpdateReplicateConfiguration verifies that normal (non-force) updates
// work correctly on a primary cluster.
func (s *ForcePromoteSuite) TestNormalUpdateReplicateConfiguration() {
	ctx := context.Background()

	clusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()

	// Create a valid single-cluster config (making current cluster primary)
	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: clusterID,
				Pchannels: []string{clusterID + "-pchan0"},
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
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
	ctx := context.Background()

	clusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()

	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: clusterID,
				Pchannels: []string{clusterID + "-pchan0"},
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
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
