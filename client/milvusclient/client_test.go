package milvusclient

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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

type trackingConn struct {
	net.Conn
	onClose func()
	once    sync.Once
}

func (c *trackingConn) Close() error {
	c.once.Do(c.onClose)
	return c.Conn.Close()
}

func (s *ClientSuite) TestNewClientClosesConnWhenConnectRPCFails() {
	ctx := context.Background()

	// Count underlying connections handed to gRPC so the test can observe
	// whether the ClientConn is actually closed, not just dereferenced.
	var openConns atomic.Int32
	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		conn, err := s.mockDialer(ctx, addr)
		if err != nil {
			return nil, err
		}
		openConns.Add(1)
		return &trackingConn{Conn: conn, onClose: func() { openConns.Add(-1) }}, nil
	}

	// Drop the Maybe() Connect expectation installed by SetupTest, then make
	// the Connect RPC fail with a non-retryable code (grpc_retry only retries
	// Unavailable/ResourceExhausted, so no backoff delays the test).
	s.mock.ExpectedCalls = nil
	s.mock.Calls = nil
	s.mock.EXPECT().Connect(mock.Anything, mock.AnythingOfType("*milvuspb.ConnectRequest")).
		Return(nil, status.Error(codes.Unauthenticated, "invalid credential")).Once()

	_, err := New(ctx, &ClientConfig{
		Address: "bufnet",
		DialOptions: []grpc.DialOption{
			grpc.WithBlock(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(dialer),
		},
	})
	s.Require().Error(err)

	// New() returns nil on failure, so the caller has no handle to Close the
	// dialed conn; it must have been closed inside connect().
	s.Eventually(func() bool {
		return openConns.Load() == 0
	}, 3*time.Second, 50*time.Millisecond)
}

func TestClient(t *testing.T) {
	suite.Run(t, new(ClientSuite))
}
