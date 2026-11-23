package milvusclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

type ClientSuite struct {
	MockSuiteBase
}

func (s *ClientSuite) TestNewClient() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("Use bufconn dailer, testing case", func() {
		c, err := New(ctx,
			&ClientConfig{
				Address: "bufnet",
				DialOptions: []grpc.DialOption{
					grpc.WithBlock(),
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithContextDialer(s.mockDialer),
				},
			})
		s.NoError(err)
		s.NotNil(c)
	})

	s.Run("empty_addr", func() {
		_, err := New(ctx, &ClientConfig{})
		s.Error(err)
		s.T().Log(err)
	})
}

func TestClient(t *testing.T) {
	suite.Run(t, new(ClientSuite))
}

func (s *ClientSuite) TestMultiSearchDoesNotRetryParameterTooLarge() {
	const parameterTooLarge = 1102
	s.mock.EXPECT().MultiSearch(mock.Anything, mock.Anything).Return(&milvuspb.MultiSearchResponse{
		Status: &commonpb.Status{Code: parameterTooLarge, Reason: "MultiSearch response exceeds maxResultBytes"},
	}, nil).Once()
	response, err := s.client.GetService().MultiSearch(s.T().Context(), &milvuspb.MultiSearchRequest{})
	s.Require().NoError(err)
	s.EqualValues(parameterTooLarge, merr.Code(merr.CheckRPCCall(response, err)))
	s.Empty(response.GetResults())
	s.mock.AssertNumberOfCalls(s.T(), "MultiSearch", 1)
}
