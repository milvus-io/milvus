package lazygrpc

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var ErrClosed = errors.New("lazy grpc conn closed")

// NewConn creates a new lazy grpc conn.
func NewConn(dialer func(ctx context.Context) (*grpc.ClientConn, error)) Conn {
	conn := &connImpl{
		initializationNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		conn:                   syncutil.NewFuture[*grpc.ClientConn](),
		dialer:                 dialer,
	}
	go conn.initialize()
	return conn
}

// Conn is a lazy grpc conn implementation.
// grpc.Dial operation will block until new grpc conn is created at least once.
// Conn will dial the underlying grpc conn asynchronously to avoid dependency cycle of milvus component when create grpc client.
// TODO: Remove in future if we can refactor the dependency cycle.
type Conn interface {
	// GetConn will block until the grpc.ClientConn is ready to use.
	// If the context is done, return immediately with the context.Canceled or Context.DeadlineExceeded error.
	// Return ErrClosed if the lazy grpc conn is closed.
	GetConn(ctx context.Context) (*grpc.ClientConn, error)

	// Close closes the lazy grpc conn.
	// Close the underlying grpc conn if it is already created.
	Close()
}

type connImpl struct {
	initializationNotifier *syncutil.AsyncTaskNotifier[struct{}]
	conn                   *syncutil.Future[*grpc.ClientConn]

	dialer func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *connImpl) initialize() {
	defer c.initializationNotifier.Finish(struct{}{})

	newBackOff := backoff.NewExponentialBackOff()
	newBackOff.InitialInterval = 100 * time.Millisecond
	newBackOff.MaxInterval = 10 * time.Second
	newBackOff.MaxElapsedTime = 0

	backoff.Retry(func() error {
		conn, err := c.dialer(c.initializationNotifier.Context())
		if err != nil {
			if c.initializationNotifier.Context().Err() != nil {
				log.Info("lazy grpc conn canceled", zap.Error(c.initializationNotifier.Context().Err()))
				return nil
			}
			log.Warn("async dial failed, wait for retry...", zap.Error(err))
			return err
		}
		c.conn.Set(conn)
		return nil
	}, newBackOff)
}

func (c *connImpl) GetConn(ctx context.Context) (*grpc.ClientConn, error) {
	// If the context is done, return immediately to perform a stable shutdown error after closing.
	if c.initializationNotifier.Context().Err() != nil {
		return nil, ErrClosed
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.initializationNotifier.Context().Done():
		return nil, ErrClosed
	case <-c.conn.Done():
		return c.conn.Get(), nil
	}
}

func (c *connImpl) Close() {
	c.initializationNotifier.Cancel()
	c.initializationNotifier.BlockUntilFinish()

	if c.conn.Ready() {
		if err := c.conn.Get().Close(); err != nil {
			log.Warn("close underlying grpc conn fail", zap.Error(err))
		}
	}
}
