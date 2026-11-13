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

	s.Run("empty_addr", func() {
		_, err := New(ctx, &ClientConfig{})
		s.Error(err)
		s.T().Log(err)
	})

	s.Run("add_tcp_scheme", func() {
		c, err := New(ctx,
			&ClientConfig{
				Address: "xxxxx:19530",
				DialOptions: []grpc.DialOption{
					grpc.WithBlock(),
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithContextDialer(s.mockDialer),
				},
			})
		s.NoError(err)
		s.NotNil(c)
		s.Equal("tcp", c.config.parsedAddress.Scheme)
	})

	s.Run("use_provided_scheme", func() {
		c, err := New(ctx,
			&ClientConfig{
				Address: "xds://xxxxx:19530",
				DialOptions: []grpc.DialOption{
					grpc.WithBlock(),
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithContextDialer(s.mockDialer),
				},
			})
		s.NoError(err)
		s.NotNil(c)
		s.Equal("xds", c.config.parsedAddress.Scheme)
	})
}

func TestClient(t *testing.T) {
	suite.Run(t, new(ClientSuite))
}
