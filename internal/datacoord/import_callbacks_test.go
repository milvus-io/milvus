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

package datacoord

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
)

type ImportCallbacksTestSuite struct {
	suite.Suite
	server *Server
	meta   *meta
}

func TestImportCallbacksTestSuite(t *testing.T) {
	suite.Run(t, new(ImportCallbacksTestSuite))
}

func (s *ImportCallbacksTestSuite) SetupTest() {
	s.server = &Server{}
	s.meta = &meta{}
	s.server.meta = s.meta
}

func (s *ImportCallbacksTestSuite) TestValidateImportRequest_BalanceError() {
	// Use a context with timeout to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	mockey.PatchConvey("TestValidateImportRequest_BalanceError", s.T(), func() {
		// Mock importMeta.CountJobBy to pass max jobs check
		mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
			return 1
		}).Build()

		s.server.importMeta = &importMeta{}

		// Create test files and options
		files := []*msgpb.ImportFile{
			{Id: 1, Paths: []string{"/test/file1.json"}},
		}
		options := []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		}

		// Test without proper balance setup - should timeout or return error
		err := s.server.validateImportRequest(ctx, files, options)
		s.Assert().Error(err)
	})
}

func TestValidateImportRequest_SuccessWithMock(t *testing.T) {
	mockey.PatchConvey("TestValidateImportRequest with mock balancer", t, func() {
		ctx := context.Background()

		// Create server
		server := &Server{}

		// Mock importMeta.CountJobBy
		mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
			return 1
		}).Build()

		server.importMeta = &importMeta{}

		// Mock balance.GetWithContext to return a balancer with nil replication config
		mockey.Mock(balance.GetWithContext).To(func(ctx context.Context) (balancer.Balancer, error) {
			// Create a mock balancer that returns nil replication config
			mockBalancer := &struct{ balancer.Balancer }{}

			// Mock GetLatestChannelAssignment method
			mockey.Mock((*struct{ balancer.Balancer }).GetLatestChannelAssignment).To(
				func(_ *struct{ balancer.Balancer }) (*channel.WatchChannelAssignmentsCallbackParam, error) {
					return &channel.WatchChannelAssignmentsCallbackParam{
						ReplicateConfiguration: nil, // No replication configuration
					}, nil
				}).Build()

			return mockBalancer, nil
		}).Build()

		files := []*msgpb.ImportFile{
			{Id: 1, Paths: []string{"/test/file1.json"}},
		}
		options := []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		}

		err := server.validateImportRequest(ctx, files, options)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
	})
}

func (s *ImportCallbacksTestSuite) TestValidateImportRequest_InvalidTimeout() {
	ctx := context.Background()

	files := []*msgpb.ImportFile{
		{Id: 1, Paths: []string{"/test/file1.json"}},
	}
	options := []*commonpb.KeyValuePair{
		{Key: "timeout", Value: "invalid"},
	}

	err := s.server.validateImportRequest(ctx, files, options)
	s.Assert().Error(err)
	s.Assert().Contains(err.Error(), "timeout")
}

func (s *ImportCallbacksTestSuite) TestValidateImportRequest_MaxJobsExceeded() {
	ctx := context.Background()

	mockey.PatchConvey("TestValidateImportRequest_MaxJobsExceeded", s.T(), func() {
		// Mock importMeta.CountJobBy to return max jobs exceeded
		mockey.Mock((*importMeta).CountJobBy).To(func(_ *importMeta, _ context.Context, _ ...ImportJobFilter) int {
			return 2000 // Exceeds default MaxImportJobNum (1024)
		}).Build()

		s.server.importMeta = &importMeta{}

		files := []*msgpb.ImportFile{
			{Id: 1, Paths: []string{"/test/file1.json"}},
		}
		options := []*commonpb.KeyValuePair{
			{Key: "timeout", Value: "300s"},
		}

		err := s.server.validateImportRequest(ctx, files, options)
		s.Assert().Error(err)
	})
}

// Note: importV1AckCallback tests require complex mocking of broadcast results
// These are better tested in integration tests rather than unit tests

// Additional integration-style tests can be added here
