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

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/tests/integration"
)

type GetReplicateConfigurationSuite struct {
	integration.MiniClusterSuite
}

func (s *GetReplicateConfigurationSuite) TestGetReplicateConfiguration() {
	ctx := s.Cluster.GetContext()

	// Call GetReplicateConfiguration
	resp, err := s.Cluster.MilvusClient.GetReplicateConfiguration(ctx, &milvuspb.GetReplicateConfigurationRequest{})
	s.NoError(err)
	s.True(merr.Ok(resp.GetStatus()), "expected success status, got: %v", resp.GetStatus())

	// Configuration may be nil or empty if no replication is configured
	// This test just verifies the API works and returns a valid response
}

func (s *GetReplicateConfigurationSuite) TestGetReplicateConfigurationWithCancelledContext() {
	ctx, cancel := context.WithCancel(s.Cluster.GetContext())
	cancel() // Cancel the context immediately

	// Call GetReplicateConfiguration with cancelled context
	_, err := s.Cluster.MilvusClient.GetReplicateConfiguration(ctx, &milvuspb.GetReplicateConfigurationRequest{})
	// Expect an error due to cancelled context
	s.Error(err)
}

func TestGetReplicateConfiguration(t *testing.T) {
	suite.Run(t, new(GetReplicateConfigurationSuite))
}
