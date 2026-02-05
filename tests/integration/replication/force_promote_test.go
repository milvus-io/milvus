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
	"fmt"
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

// getPChannelNames generates the correct pchannel names for the current cluster.
// Pchannels are named <RootCoordDml>_<n> where n goes from 0 to DmlChannelNum-1.
func (s *ForcePromoteSuite) getPChannelNames() []string {
	rootCoordDml := paramtable.Get().CommonCfg.RootCoordDml.GetValue()
	dmlChannelNum := paramtable.Get().RootCoordCfg.DmlChannelNum.GetAsInt()
	pchannels := make([]string, dmlChannelNum)
	for i := 0; i < dmlChannelNum; i++ {
		pchannels[i] = fmt.Sprintf("%s_%d", rootCoordDml, i)
	}
	return pchannels
}

// TestForcePromoteOnPrimaryClusterShouldFail verifies that force promote
// returns an error when called on a primary cluster (no replication configured).
// Force promote is only intended for secondary clusters during failover.
func (s *ForcePromoteSuite) TestForcePromoteOnPrimaryClusterShouldFail() {
	ctx := context.Background()

	// Force promote requires empty cluster and topology fields
	// The configuration is auto-constructed from the cluster's existing meta
	config := &commonpb.ReplicateConfiguration{}

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

// TestForcePromoteWithNonEmptyClustersShouldFail verifies that force promote
// rejects configurations with non-empty clusters field.
// Force promote auto-constructs the configuration from existing meta.
func (s *ForcePromoteSuite) TestForcePromoteWithNonEmptyClustersShouldFail() {
	ctx := context.Background()

	clusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	pchannels := s.getPChannelNames()

	// Create config with clusters (invalid for force promote - must be empty)
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
	}

	req := &milvuspb.UpdateReplicateConfigurationRequest{
		ReplicateConfiguration: config,
		ForcePromote:           true,
	}

	resp, err := s.Cluster.MilvusClient.UpdateReplicateConfiguration(ctx, req)

	// Should return an error about non-empty cluster/topology fields
	s.NoError(err)
	s.NotNil(resp)
	err = merr.Error(resp)
	s.Error(err)
	s.Contains(err.Error(), "force promote requires empty cluster and topology fields")
}

// TestForcePromoteWithTopologyShouldFail verifies that force promote
// rejects configurations that contain cross-cluster topology.
func (s *ForcePromoteSuite) TestForcePromoteWithTopologyShouldFail() {
	ctx := context.Background()

	clusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()

	// Create config with only topology (no clusters) - still invalid for force promote
	config := &commonpb.ReplicateConfiguration{
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

	// Should return an error about non-empty topology field
	s.NoError(err)
	s.NotNil(resp)
	err = merr.Error(resp)
	s.Error(err)
	s.Contains(err.Error(), "force promote requires empty cluster and topology fields")
}

// TestNormalUpdateReplicateConfiguration verifies that normal (non-force) updates
// work correctly on a primary cluster.
func (s *ForcePromoteSuite) TestNormalUpdateReplicateConfiguration() {
	ctx := context.Background()

	clusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	pchannels := s.getPChannelNames()

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
	ctx := context.Background()

	clusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	pchannels := s.getPChannelNames()

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
