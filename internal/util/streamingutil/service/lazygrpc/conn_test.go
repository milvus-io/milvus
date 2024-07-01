package lazygrpc

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestLazyConn(t *testing.T) {
	listener := bufconn.Listen(1024)
	s := grpc.NewServer()
	go s.Serve(listener)
	defer s.Stop()

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	lconn := NewConn(func(ctx context.Context) (*grpc.ClientConn, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			return grpc.DialContext(ctx, "", grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
				return listener.Dial()
			}), grpc.WithTransportCredentials(insecure.NewCredentials()))
		}
	})

	// Get with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	conn, err := lconn.GetConn(ctx)
	assert.Nil(t, conn)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// Get conn after timeout
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err = lconn.GetConn(ctx)
	assert.NotNil(t, conn)
	assert.Nil(t, err)

	// Get with closed.
	lconn.Close()
	conn, err = lconn.GetConn(context.Background())
	assert.ErrorIs(t, err, ErrClosed)
	assert.Nil(t, conn)

	// Get before initialize.
	ticker = time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	lconn = NewConn(func(ctx context.Context) (*grpc.ClientConn, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			return grpc.DialContext(ctx, "", grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
				return listener.Dial()
			}), grpc.WithTransportCredentials(insecure.NewCredentials()))
		}
	})

	// Test WithLazyGRPCServiceCreator
	grpcService := WithServiceCreator(lconn, func(grpc.ClientConnInterface) int {
		return 1
	})
	realService, err := grpcService.GetService(ctx)
	assert.Equal(t, 1, realService)
	assert.NoError(t, err)

	lconn.Close()
	conn, err = lconn.GetConn(context.Background())
	assert.ErrorIs(t, err, ErrClosed)
	assert.Nil(t, conn)
}
