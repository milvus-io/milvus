package lazyconn

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
	lconn := NewLazyGRPCConn(func(ctx context.Context) (*grpc.ClientConn, error) {
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
	conn, err := lconn.Get(ctx)
	assert.Nil(t, conn)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// Get conn after timeout
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err = lconn.Get(ctx)
	assert.NotNil(t, conn)
	assert.Nil(t, err)

	// Get with closed.
	lconn.Close()
	conn, err = lconn.Get(context.Background())
	assert.ErrorIs(t, err, shutdownErr)
	assert.Nil(t, conn)

	// Get before initialize.
	ticker = time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	lconn = NewLazyGRPCConn(func(ctx context.Context) (*grpc.ClientConn, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			return grpc.DialContext(ctx, "", grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
				return listener.Dial()
			}), grpc.WithTransportCredentials(insecure.NewCredentials()))
		}
	})

	lconn.Close()
	conn, err = lconn.Get(context.Background())
	assert.ErrorIs(t, err, shutdownErr)
	assert.Nil(t, conn)
}
