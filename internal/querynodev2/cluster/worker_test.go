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
	"context"
	"io"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type RemoteWorkerSuite struct {
	suite.Suite

	mockClient *mocks.MockQueryNodeClient
	worker     *remoteWorker
}

func (s *RemoteWorkerSuite) SetupTest() {
	s.mockClient = &mocks.MockQueryNodeClient{}
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

	s.Run("legacy_querynode_unimplemented", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		s.mockClient.EXPECT().Delete(mock.Anything, mock.AnythingOfType("*querypb.DeleteRequest")).
			Return(nil, merr.WrapErrServiceUnimplemented(status.Errorf(codes.Unimplemented, "mocked grpc unimplemented")))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := s.worker.Delete(ctx, &querypb.DeleteRequest{})

		s.NoError(err)
	})
}

func (s *RemoteWorkerSuite) TestDeleteBatch() {
	s.Run("normal_run", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		s.mockClient.EXPECT().DeleteBatch(mock.Anything, mock.AnythingOfType("*querypb.DeleteBatchRequest")).
			Return(&querypb.DeleteBatchResponse{Status: merr.Success()}, nil).Once()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		resp, err := s.worker.DeleteBatch(ctx, &querypb.DeleteBatchRequest{
			SegmentIds: []int64{100, 200},
		})
		s.NoError(merr.CheckRPCCall(resp, err))
	})

	s.Run("client_return_error", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		s.mockClient.EXPECT().DeleteBatch(mock.Anything, mock.AnythingOfType("*querypb.DeleteBatchRequest")).
			Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		resp, err := s.worker.DeleteBatch(ctx, &querypb.DeleteBatchRequest{
			SegmentIds: []int64{100, 200},
		})

		s.Error(merr.CheckRPCCall(resp, err))
	})

	s.Run("client_return_fail_status", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		s.mockClient.EXPECT().DeleteBatch(mock.Anything, mock.AnythingOfType("*querypb.DeleteBatchRequest")).
			Return(&querypb.DeleteBatchResponse{
				Status: merr.Status(merr.WrapErrServiceUnavailable("mocked")),
			}, nil).Once()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		resp, err := s.worker.DeleteBatch(ctx, &querypb.DeleteBatchRequest{
			SegmentIds: []int64{100, 200},
		})

		s.Error(merr.CheckRPCCall(resp, err))
	})

	s.Run("batch_delete_unimplemented", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()

		s.mockClient.EXPECT().DeleteBatch(mock.Anything, mock.AnythingOfType("*querypb.DeleteBatchRequest")).
			Return(nil, merr.WrapErrServiceUnimplemented(status.Errorf(codes.Unimplemented, "mocked grpc unimplemented")))
		s.mockClient.EXPECT().Delete(mock.Anything, mock.Anything).Return(merr.Success(), nil).Times(2)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		resp, err := s.worker.DeleteBatch(ctx, &querypb.DeleteBatchRequest{
			SegmentIds: []int64{100, 200},
		})

		s.NoError(merr.CheckRPCCall(resp, err))
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
			Return(result, merr.WrapErrServiceUnimplemented(grpcErr))
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
			Return(result, merr.WrapErrServiceUnimplemented(grpcErr))
		s.mockClient.EXPECT().Query(mock.Anything, mock.AnythingOfType("*querypb.QueryRequest")).
			Return(result, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sr, serr := s.worker.QuerySegments(ctx, &querypb.QueryRequest{})

		s.Equal(err, serr)
		s.Equal(result, sr)
	})
}

func (s *RemoteWorkerSuite) TestQueryStream() {
	s.Run("normal_run", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := streamrpc.NewLocalQueryClient(ctx)
		server := client.CreateServer()

		ids := []int64{10, 11, 12}
		s.mockClient.EXPECT().QueryStreamSegments(
			mock.Anything,
			mock.AnythingOfType("*querypb.QueryRequest"),
		).RunAndReturn(func(ctx context.Context, request *querypb.QueryRequest, option ...grpc.CallOption) (querypb.QueryNode_QueryStreamSegmentsClient, error) {
			client := streamrpc.NewLocalQueryClient(ctx)
			server := client.CreateServer()

			for _, id := range ids {
				err := server.Send(&internalpb.RetrieveResults{
					Status: merr.Success(),
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{Data: []int64{id}},
						},
					},
				})
				s.NoError(err)
			}
			err := server.FinishSend(nil)
			s.NoError(err)
			return client, nil
		})

		go func() {
			err := s.worker.QueryStreamSegments(ctx, &querypb.QueryRequest{}, server)
			if err != nil {
				server.Send(&internalpb.RetrieveResults{
					Status: merr.Status(err),
				})
			}
			server.FinishSend(err)
		}()

		recNum := 0
		for {
			result, err := client.Recv()
			if err == io.EOF {
				break
			}
			s.NoError(err)

			err = merr.Error(result.GetStatus())
			s.NoError(err)

			s.Less(recNum, len(ids))
			s.Equal(result.Ids.GetIntId().Data[0], ids[recNum])
			recNum++
		}
	})

	s.Run("send msg failed", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		clientCtx, clientClose := context.WithCancel(ctx)
		client := streamrpc.NewLocalQueryClient(clientCtx)
		server := client.CreateServer()
		clientClose()

		ids := []int64{10, 11, 12}

		s.mockClient.EXPECT().QueryStreamSegments(
			mock.Anything,
			mock.AnythingOfType("*querypb.QueryRequest"),
		).RunAndReturn(func(ctx context.Context, request *querypb.QueryRequest, option ...grpc.CallOption) (querypb.QueryNode_QueryStreamSegmentsClient, error) {
			for _, id := range ids {
				client := streamrpc.NewLocalQueryClient(ctx)
				server := client.CreateServer()

				err := server.Send(&internalpb.RetrieveResults{
					Status: merr.Success(),
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{Data: []int64{id}},
						},
					},
				})
				s.NoError(err)
			}
			err := server.FinishSend(nil)
			s.NoError(err)
			return client, nil
		})

		err := s.worker.QueryStreamSegments(ctx, &querypb.QueryRequest{}, server)
		s.Error(err)
	})

	s.Run("client_return_error", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := streamrpc.NewLocalQueryClient(ctx)
		server := streamrpc.NewConcurrentQueryStreamServer(client.CreateServer())

		s.mockClient.EXPECT().QueryStreamSegments(
			mock.Anything,
			mock.AnythingOfType("*querypb.QueryRequest"),
		).Return(nil, errors.New("mocked error"))

		go func() {
			err := s.worker.QueryStreamSegments(ctx, &querypb.QueryRequest{}, server)
			server.Send(&internalpb.RetrieveResults{
				Status: merr.Status(err),
			})
		}()

		result, err := client.Recv()
		s.NoError(err)

		err = merr.Error(result.GetStatus())
		// Check result
		s.Error(err)
	})

	s.Run("client_return_fail_status", func() {
		defer func() { s.mockClient.ExpectedCalls = nil }()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := streamrpc.NewLocalQueryClient(ctx)
		server := client.CreateServer()

		s.mockClient.EXPECT().QueryStreamSegments(
			mock.Anything,
			mock.AnythingOfType("*querypb.QueryRequest"),
		).RunAndReturn(func(ctx context.Context, request *querypb.QueryRequest, option ...grpc.CallOption) (querypb.QueryNode_QueryStreamSegmentsClient, error) {
			client := streamrpc.NewLocalQueryClient(ctx)
			server := client.CreateServer()

			err := server.Send(&internalpb.RetrieveResults{
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError},
			})
			s.NoError(err)

			err = server.FinishSend(nil)
			s.NoError(err)
			return client, nil
		})

		go func() {
			err := s.worker.QueryStreamSegments(ctx, &querypb.QueryRequest{}, server)
			server.Send(&internalpb.RetrieveResults{
				Status: merr.Status(err),
			})
		}()

		result, err := client.Recv()
		s.NoError(err)

		err = merr.Error(result.GetStatus())
		// Check result
		s.Error(err)
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

	s.mockClient.EXPECT().Close().Return(nil)
	s.worker.Stop()
	s.mockClient.AssertCalled(s.T(), "Close")
}

func TestRemoteWorker(t *testing.T) {
	suite.Run(t, new(RemoteWorkerSuite))
}

func TestNewRemoteWorker(t *testing.T) {
	client := mocks.NewMockQueryNodeClient(t)
	w := NewRemoteWorker(client)

	rw, ok := w.(*remoteWorker)
	assert.True(t, ok)
	assert.Equal(t, client, rw.client)
}
