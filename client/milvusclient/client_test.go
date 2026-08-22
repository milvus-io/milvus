package milvusclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	s.Run("custom connection factory", func() {
		var called bool
		c, err := New(ctx, &ClientConfig{
			Address: "bufnet",
			DialOptions: []grpc.DialOption{
				grpc.WithBlock(),
				grpc.WithContextDialer(s.mockDialer),
			},
			ConnectionFactory: func(factoryCtx context.Context, target string, options ConnectionOptions) (*grpc.ClientConn, error) {
				called = true
				s.Equal("bufnet", target)
				s.NotNil(options.TransportCredentials)
				s.Len(options.UnaryInterceptors, 2)
				return defaultConnectionFactory(factoryCtx, target, options)
			},
		})
		s.NoError(err)
		s.NotNil(c)
		s.True(called)
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
