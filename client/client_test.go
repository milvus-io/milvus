package client

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
}

func TestClient(t *testing.T) {
	suite.Run(t, new(ClientSuite))
}
