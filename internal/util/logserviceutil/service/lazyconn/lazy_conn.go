package lazyconn

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var shutdownErr = status.NewOnShutdownError("close lazy grpc conn")

// NewLazyGRPCConn creates a new lazy grpc conn.
func NewLazyGRPCConn(dialer func(ctx context.Context) (*grpc.ClientConn, error)) *LazyGRPCConn {
	ctx, cancel := context.WithCancel(context.Background())

	conn := &LazyGRPCConn{
		ctx:         ctx,
		cancel:      cancel,
		initialized: make(chan struct{}),
		ready:       make(chan struct{}),

		dialer: dialer,
	}
	go conn.initialize()
	return conn
}

// LazyGRPCConn is a lazy grpc conn.
// It will dial the underlying grpc conn asynchronously to avoid dependency cycle of milvus component when create grpc client.
// TODO: Remove in future after we refactor the dependency cycle.
type LazyGRPCConn struct {
	ctx         context.Context
	cancel      context.CancelFunc
	initialized chan struct{}
	ready       chan struct{}

	dialer func(ctx context.Context) (*grpc.ClientConn, error)
	conn   *grpc.ClientConn
}

func (c *LazyGRPCConn) initialize() {
	defer close(c.initialized)
	for {
		conn, err := c.dialer(c.ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Info("lazy grpc conn canceled")
				return
			}
			log.Warn("async dial failed, retry...", zap.Error(err))
			continue
		}
		c.conn = conn
		close(c.ready)
		return
	}
}

func (c *LazyGRPCConn) Get(ctx context.Context) (*grpc.ClientConn, error) {
	// If the context is done, return immediately to perform a stable shutdown error after closing.
	if c.ctx.Err() != nil {
		return nil, shutdownErr
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ready:
		return c.conn, nil
	case <-c.ctx.Done():
		return nil, shutdownErr
	}
}

func (c *LazyGRPCConn) Close() {
	c.cancel()
	<-c.initialized

	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			log.Warn("close underlying grpc conn fail", zap.Error(err))
		}
	}
}
