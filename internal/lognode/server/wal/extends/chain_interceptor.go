package extends

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

var _ wal.AppendInterceptorWithReady = (*chainedInterceptor)(nil)

// appendInterceptorCall is the common function to execute the interceptor.
type appendInterceptorCall = func(ctx context.Context, msg message.MutableMessage, append wal.Append) (message.MessageID, error)

// newChainedInterceptor creates a new chained interceptor.
func newChainedInterceptor(interceptors ...wal.AppendInterceptor) wal.AppendInterceptorWithReady {
	calls := make([]appendInterceptorCall, 0, len(interceptors))
	for _, i := range interceptors {
		calls = append(calls, i.Do)
	}
	return &chainedInterceptor{
		closed:       make(chan struct{}),
		interceptors: interceptors,
		do:           chainUnaryClientInterceptors(calls),
	}
}

// chainedInterceptor chains all interceptors into one.
type chainedInterceptor struct {
	closed       chan struct{}
	interceptors []wal.AppendInterceptor
	do           func(ctx context.Context, msg message.MutableMessage, append wal.Append) (message.MessageID, error)
}

// Ready wait all interceptors to be ready.
func (c *chainedInterceptor) Ready() <-chan struct{} {
	ready := make(chan struct{})
	go func() {
		for _, i := range c.interceptors {
			// check if ready is implemented
			if r, ok := i.(wal.AppendInterceptorWithReady); ok {
				select {
				case <-r.Ready():
				case <-c.closed:
					return
				}
			}
		}
		close(ready)
	}()
	return ready
}

// Do execute the chained interceptors.
func (c *chainedInterceptor) Do(ctx context.Context, msg message.MutableMessage, append wal.Append) (message.MessageID, error) {
	return c.do(ctx, msg, append)
}

// Close close all interceptors.
func (c *chainedInterceptor) Close() {
	close(c.closed)
	for _, i := range c.interceptors {
		i.Close()
	}
}

// chainUnaryClientInterceptors chains all unary client interceptors into one.
func chainUnaryClientInterceptors(interceptorCalls []appendInterceptorCall) appendInterceptorCall {
	if len(interceptorCalls) == 0 {
		// Do nothing if no interceptors.
		return func(ctx context.Context, msg message.MutableMessage, append wal.Append) (message.MessageID, error) {
			return append(ctx, msg)
		}
	} else if len(interceptorCalls) == 1 {
		return interceptorCalls[0]
	} else {
		return func(ctx context.Context, msg message.MutableMessage, invoker wal.Append) (message.MessageID, error) {
			return interceptorCalls[0](ctx, msg, getChainUnaryInvoker(interceptorCalls, 0, invoker))
		}
	}
}

// getChainUnaryInvoker recursively generate the chained unary invoker.
func getChainUnaryInvoker(interceptors []appendInterceptorCall, idx int, finalInvoker wal.Append) wal.Append {
	// all interceptor is called, so return the final invoker.
	if idx == len(interceptors)-1 {
		return finalInvoker
	}
	// recursively generate the chained invoker.
	return func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return interceptors[idx+1](ctx, msg, getChainUnaryInvoker(interceptors, idx+1, finalInvoker))
	}
}
