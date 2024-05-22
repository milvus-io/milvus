package wal

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

// Append is the common function to append a msg to the wal.
type Append = func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)

// InterceptorBuilder is the interface to build a interceptor.
// 1. InterceptorBuilder is concurrent safe.
// 2. InterceptorBuilder can used to build a interceptor with cross-wal shared resources.
type InterceptorBuilder interface {
	// Build build a interceptor with wal that interceptor will work on.
	Build(wal BasicWAL) AppendInterceptor
}

// AppendInterceptor is the interceptor for Append functions.
// All wal extra operations should be done by these function, such as
// 1. time tick setup.
// 2. unique primary key filter and build.
// 3. index builder.
// AppendInterceptor should be lazy initialized, fast execution.
type AppendInterceptor interface {
	// Execute the append operation with interceptor.
	Do(ctx context.Context, msg message.MutableMessage, append Append) (message.MessageID, error)

	// Close the interceptor release the resources.
	Close()
}

// Some interceptor may need to wait for some resource to be ready or recovery process.
type AppendInterceptorWithReady interface {
	AppendInterceptor

	// Ready check if interceptor is ready.
	// Close of Interceptor would not notify the ready (closed interceptor is not ready).
	// So always apply timeout when waiting for ready.
	// Some append interceptor may be stateful, such as index builder and unique primary key filter,
	// so it need to implement the recovery logic from crash by itself before notifying ready.
	// Append operation will block until ready or canceled.
	// Consumer do not blocked by it.
	Ready() <-chan struct{}
}
