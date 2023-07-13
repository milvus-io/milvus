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

// delegator package contains the logic of shard delegator.

package cluster

import (
	context "context"
	"testing"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	internalpb "github.com/milvus-io/milvus/internal/proto/internalpb"
	querypb "github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type RemoteWorkerSuite struct {
	suite.Suite

	mockClient *mocks.MockQueryNode
	worker     *remoteWorker
}

func (s *RemoteWorkerSuite) SetupTest() {
	s.mockClient = &mocks.MockQueryNode{}
	s.worker = &remoteWorker{client: s.mockClient}
}

func (s *RemoteWorkerSuite) TearDownTest() {
	s.mockClient = nil
	s.worker = nil
}

func (s *RemoteWorkerSuite) TestLoadSegments() {
	s.Run("normal_run", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		s.mockClient.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).
			Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := s.worker.LoadSegments(ctx, &querypb.LoadSegmentsRequest{})

		s.NoError(err)
	})

	s.Run("client_return_error", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		s.mockClient.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).
			Return(nil, errors.New("mocked error"))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := s.worker.LoadSegments(ctx, &querypb.LoadSegmentsRequest{})

		s.Error(err)
	})

	s.Run("client_return_fail_status", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		s.mockClient.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).
			Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "mocked failure"}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := s.worker.LoadSegments(ctx, &querypb.LoadSegmentsRequest{})

		s.Error(err)
	})
}

func (s *RemoteWorkerSuite) TestReleaseSegments() {
	s.Run("normal_run", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		s.mockClient.EXPECT().ReleaseSegments(mock.Anything, mock.AnythingOfType("*querypb.ReleaseSegmentsRequest")).
			Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := s.worker.ReleaseSegments(ctx, &querypb.ReleaseSegmentsRequest{})

		s.NoError(err)
	})

	s.Run("client_return_error", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		s.mockClient.EXPECT().ReleaseSegments(mock.Anything, mock.AnythingOfType("*querypb.ReleaseSegmentsRequest")).
			Return(nil, errors.New("mocked error"))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := s.worker.ReleaseSegments(ctx, &querypb.ReleaseSegmentsRequest{})

		s.Error(err)
	})

	s.Run("client_return_fail_status", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		s.mockClient.EXPECT().ReleaseSegments(mock.Anything, mock.AnythingOfType("*querypb.ReleaseSegmentsRequest")).
			Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "mocked failure"}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := s.worker.ReleaseSegments(ctx, &querypb.ReleaseSegmentsRequest{})

		s.Error(err)
	})
}

func (s *RemoteWorkerSuite) TestDelete() {
	s.Run("normal_run", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		s.mockClient.EXPECT().Delete(mock.Anything, mock.AnythingOfType("*querypb.DeleteRequest")).
			Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := s.worker.Delete(ctx, &querypb.DeleteRequest{})

		s.NoError(err)
	})

	s.Run("client_return_error", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		s.mockClient.EXPECT().Delete(mock.Anything, mock.AnythingOfType("*querypb.DeleteRequest")).
			Return(nil, errors.New("mocked error"))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := s.worker.Delete(ctx, &querypb.DeleteRequest{})

		s.Error(err)
	})

	s.Run("client_return_fail_status", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		s.mockClient.EXPECT().Delete(mock.Anything, mock.AnythingOfType("*querypb.DeleteRequest")).
			Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "mocked failure"}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := s.worker.Delete(ctx, &querypb.DeleteRequest{})

		s.Error(err)
	})
}

func (s *RemoteWorkerSuite) TestSearch() {
	s.Run("normal_run", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		var result *internalpb.SearchResults
		var err error

		result = &internalpb.SearchResults{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}
		s.mockClient.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Return(result, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sr, serr := s.worker.SearchSegments(ctx, &querypb.SearchRequest{})

		s.Equal(err, serr)
		s.Equal(result, sr)
	})

	s.Run("client_return_error", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		var result *internalpb.SearchResults
		err := errors.New("mocked error")
		s.mockClient.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Return(result, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sr, serr := s.worker.SearchSegments(ctx, &querypb.SearchRequest{})

		s.Equal(err, serr)
		s.Equal(result, sr)
	})

	s.Run("client_return_fail_status", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		var result *internalpb.SearchResults
		var err error

		result = &internalpb.SearchResults{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError},
		}
		s.mockClient.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Return(result, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sr, serr := s.worker.SearchSegments(ctx, &querypb.SearchRequest{})

		s.Equal(err, serr)
		s.Equal(result, sr)
	})

	s.Run("client_search_compatible", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		var result *internalpb.SearchResults
		var err error

		grpcErr := status.Error(codes.Unimplemented, "method not implemented")
		s.mockClient.EXPECT().SearchSegments(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Return(result, grpcErr)
		s.mockClient.EXPECT().Search(mock.Anything, mock.AnythingOfType("*querypb.SearchRequest")).
			Return(result, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sr, serr := s.worker.SearchSegments(ctx, &querypb.SearchRequest{})

		s.Equal(err, serr)
		s.Equal(result, sr)
	})
}

func (s *RemoteWorkerSuite) TestQuery() {
	s.Run("normal_run", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		var result *internalpb.RetrieveResults
		var err error

		result = &internalpb.RetrieveResults{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}
		s.mockClient.EXPECT().QuerySegments(mock.Anything, mock.AnythingOfType("*querypb.QueryRequest")).
			Return(result, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sr, serr := s.worker.QuerySegments(ctx, &querypb.QueryRequest{})

		s.Equal(err, serr)
		s.Equal(result, sr)
	})

	s.Run("client_return_error", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		var result *internalpb.RetrieveResults

		err := errors.New("mocked error")
		s.mockClient.EXPECT().QuerySegments(mock.Anything, mock.AnythingOfType("*querypb.QueryRequest")).
			Return(result, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sr, serr := s.worker.QuerySegments(ctx, &querypb.QueryRequest{})

		s.Equal(err, serr)
		s.Equal(result, sr)
	})

	s.Run("client_return_fail_status", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		var result *internalpb.RetrieveResults
		var err error

		result = &internalpb.RetrieveResults{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError},
		}
		s.mockClient.EXPECT().QuerySegments(mock.Anything, mock.AnythingOfType("*querypb.QueryRequest")).
			Return(result, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sr, serr := s.worker.QuerySegments(ctx, &querypb.QueryRequest{})

		s.Equal(err, serr)
		s.Equal(result, sr)
	})

	s.Run("client_query_compatible", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		var result *internalpb.RetrieveResults
		var err error

		grpcErr := status.Error(codes.Unimplemented, "method not implemented")
		s.mockClient.EXPECT().QuerySegments(mock.Anything, mock.AnythingOfType("*querypb.QueryRequest")).
			Return(result, grpcErr)
		s.mockClient.EXPECT().Query(mock.Anything, mock.AnythingOfType("*querypb.QueryRequest")).
			Return(result, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sr, serr := s.worker.QuerySegments(ctx, &querypb.QueryRequest{})

		s.Equal(err, serr)
		s.Equal(result, sr)
	})
}

func (s *RemoteWorkerSuite) TestGetStatistics() {
	s.Run("normal_run", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		var result *internalpb.GetStatisticsResponse
		var err error

		result = &internalpb.GetStatisticsResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}
		s.mockClient.EXPECT().GetStatistics(mock.Anything, mock.AnythingOfType("*querypb.GetStatisticsRequest")).
			Return(result, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sr, serr := s.worker.GetStatistics(ctx, &querypb.GetStatisticsRequest{})

		s.Equal(err, serr)
		s.Equal(result, sr)
	})

	s.Run("client_return_error", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		var result *internalpb.GetStatisticsResponse

		err := errors.New("mocked error")
		s.mockClient.EXPECT().GetStatistics(mock.Anything, mock.AnythingOfType("*querypb.GetStatisticsRequest")).
			Return(result, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sr, serr := s.worker.GetStatistics(ctx, &querypb.GetStatisticsRequest{})

		s.Equal(err, serr)
		s.Equal(result, sr)
	})

	s.Run("client_return_fail_status", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		var result *internalpb.GetStatisticsResponse
		var err error

		result = &internalpb.GetStatisticsResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError},
		}
		s.mockClient.EXPECT().GetStatistics(mock.Anything, mock.AnythingOfType("*querypb.GetStatisticsRequest")).
			Return(result, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sr, serr := s.worker.GetStatistics(ctx, &querypb.GetStatisticsRequest{})

		s.Equal(err, serr)
		s.Equal(result, sr)
	})
}

func (s *RemoteWorkerSuite) TestBasic() {
	s.True(s.worker.IsHealthy())

	s.mockClient.EXPECT().Stop().Return(nil)
	s.worker.Stop()
	s.mockClient.AssertCalled(s.T(), "Stop")
}

func TestRemoteWorker(t *testing.T) {
	suite.Run(t, new(RemoteWorkerSuite))
}

func TestNewRemoteWorker(t *testing.T) {
	client := &mocks.MockQueryNode{}

	w := NewRemoteWorker(client)

	rw, ok := w.(*remoteWorker)
	assert.True(t, ok)
	assert.Equal(t, client, rw.client)
}
