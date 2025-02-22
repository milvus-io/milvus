package interceptors

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

var _ InterceptorWithReady = (*chainedInterceptor)(nil)

type (
	// appendInterceptorCall is the common function to execute the append interceptor.
	appendInterceptorCall = func(ctx context.Context, msg message.MutableMessage, append Append) (message.MessageID, error)
)

// NewChainedInterceptor creates a new chained interceptor.
func NewChainedInterceptor(interceptors ...Interceptor) InterceptorWithReady {
	appendCalls := make([]appendInterceptorCall, 0, len(interceptors))
	for _, i := range interceptors {
		appendCalls = append(appendCalls, i.DoAppend)
	}
	return &chainedInterceptor{
		closed:       make(chan struct{}),
		interceptors: interceptors,
		appendCall:   chainAppendInterceptors(appendCalls),
	}
}

// chainedInterceptor chains all interceptors into one.
type chainedInterceptor struct {
	closed       chan struct{}
	interceptors []Interceptor
	appendCall   appendInterceptorCall
}

// Ready wait all interceptors to be ready.
func (c *chainedInterceptor) Ready() <-chan struct{} {
	ready := make(chan struct{})
	go func() {
		for _, i := range c.interceptors {
			// check if ready is implemented
			if r, ok := i.(InterceptorWithReady); ok {
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

// DoAppend execute the append operation with all interceptors.
func (c *chainedInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, append Append) (message.MessageID, error) {
	return c.appendCall(ctx, msg, append)
}

// Close close all interceptors.
func (c *chainedInterceptor) Close() {
	close(c.closed)
	for _, i := range c.interceptors {
		i.Close()
	}
}

// chainAppendInterceptors chains all unary client interceptors into one.
func chainAppendInterceptors(interceptorCalls []appendInterceptorCall) appendInterceptorCall {
	if len(interceptorCalls) == 0 {
		// Do nothing if no interceptors.
		return func(ctx context.Context, msg message.MutableMessage, append Append) (message.MessageID, error) {
			return append(ctx, msg)
		}
	} else if len(interceptorCalls) == 1 {
		return interceptorCalls[0]
	}
	return func(ctx context.Context, msg message.MutableMessage, invoker Append) (message.MessageID, error) {
		return interceptorCalls[0](ctx, msg, getChainAppendInvoker(interceptorCalls, 0, invoker))
	}
}

// getChainAppendInvoker recursively generate the chained unary invoker.
func getChainAppendInvoker(interceptors []appendInterceptorCall, idx int, finalInvoker Append) Append {
	// all interceptor is called, so return the final invoker.
	if idx == len(interceptors)-1 {
		return finalInvoker
	}
	// recursively generate the chained invoker.
	return func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return interceptors[idx+1](ctx, msg, getChainAppendInvoker(interceptors, idx+1, finalInvoker))
	}
}
