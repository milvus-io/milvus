package interceptors

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

var _ InterceptorWithReady = (*chainedInterceptor)(nil)

type (
	// appendInterceptorCall is the common function to execute the append interceptor.
	appendInterceptorCall = func(ctx context.Context, msg message.MutableMessage, append Append) (message.MessageID, error)
)

// NewChainedInterceptor creates a new chained interceptor.
func NewChainedInterceptor(interceptors ...Interceptor) InterceptorWithReady {
	return &chainedInterceptor{
		closed:       make(chan struct{}),
		interceptors: interceptors,
		appendCall:   chainAppendInterceptors(interceptors),
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
func chainAppendInterceptors(interceptors []Interceptor) appendInterceptorCall {
	if len(interceptors) == 0 {
		// Do nothing if no interceptors.
		return func(ctx context.Context, msg message.MutableMessage, append Append) (message.MessageID, error) {
			return append(ctx, msg)
		}
	} else if len(interceptors) == 1 {
		if i, ok := interceptors[0].(InterceptorWithMetrics); ok {
			return adaptAppendWithMetricCollecting(i.Name(), interceptors[0].DoAppend)
		}
		return interceptors[0].DoAppend
	}
	return func(ctx context.Context, msg message.MutableMessage, invoker Append) (message.MessageID, error) {
		if i, ok := interceptors[0].(InterceptorWithMetrics); ok {
			return adaptAppendWithMetricCollecting(i.Name(), interceptors[0].DoAppend)(ctx, msg, getChainAppendInvoker(interceptors, 0, invoker))
		}
		return interceptors[0].DoAppend(ctx, msg, getChainAppendInvoker(interceptors, 0, invoker))
	}
}

// getChainAppendInvoker recursively generate the chained unary invoker.
func getChainAppendInvoker(interceptors []Interceptor, idx int, finalInvoker Append) Append {
	// all interceptor is called, so return the final invoker.
	if idx == len(interceptors)-1 {
		return finalInvoker
	}
	// recursively generate the chained invoker.
	return func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		idx := idx + 1
		if i, ok := interceptors[idx].(InterceptorWithMetrics); ok {
			return adaptAppendWithMetricCollecting(i.Name(), i.DoAppend)(ctx, msg, getChainAppendInvoker(interceptors, idx, finalInvoker))
		}
		return interceptors[idx].DoAppend(ctx, msg, getChainAppendInvoker(interceptors, idx, finalInvoker))
	}
}

// adaptAppendWithMetricCollecting adapts the append interceptor with metric collecting.
func adaptAppendWithMetricCollecting(name string, append appendInterceptorCall) appendInterceptorCall {
	return func(ctx context.Context, msg message.MutableMessage, invoker Append) (message.MessageID, error) {
		c := utility.MustGetAppendMetrics(ctx).StartInterceptorCollector(name)
		msgID, err := append(ctx, msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			c.BeforeDone()
			msgID, err := invoker(ctx, msg)
			c.AfterStart()
			return msgID, err
		})
		c.AfterDone()
		c.BeforeFailure(err)
		return msgID, err
	}
}
