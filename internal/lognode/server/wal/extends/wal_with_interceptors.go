package extends

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

// newWALWithInterceptors creates a new wal with interceptors.
func newWALWithInterceptors(l wal.BasicWAL, builders ...wal.InterceptorBuilder) wal.BasicWAL {
	if len(builders) == 0 {
		return l
	}
	// Build all interceptors.
	interceptors := make([]wal.AppendInterceptor, 0, len(builders))
	for _, b := range builders {
		interceptors = append(interceptors, b.Build(l))
	}
	return &walWithInterceptor{
		BasicWAL:    l,
		interceptor: newChainedInterceptor(interceptors...),
	}
}

// walWithInterceptor is a wrapper of wal with interceptors.
type walWithInterceptor struct {
	interceptor wal.AppendInterceptorWithReady
	wal.BasicWAL
}

// Append with interceptors.
func (w *walWithInterceptor) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	// Check if interceptor is ready.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-w.interceptor.Ready():
	}

	// Execute the interceptor and wal append.
	return w.interceptor.Do(ctx, msg, w.BasicWAL.Append)
}

// close all interceptor and underlying wal.
func (w *walWithInterceptor) Close() {
	w.BasicWAL.Close()
	w.interceptor.Close()
}
