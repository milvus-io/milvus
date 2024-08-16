package interceptors

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

type (
	// Append is the common function to append a msg to the wal.
	Append = func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)
)

type InterceptorBuildParam struct {
	WALImpls walimpls.WALImpls         // The underlying walimpls implementation, can be used anytime.
	WAL      *syncutil.Future[wal.WAL] // The wal final object, can be used after interceptor is ready.
}

// InterceptorBuilder is the interface to build a interceptor.
// 1. InterceptorBuilder is concurrent safe.
// 2. InterceptorBuilder can used to build a interceptor with cross-wal shared resources.
type InterceptorBuilder interface {
	// Build build a interceptor with wal that interceptor will work on.
	// the wal object will be sent to the interceptor builder when the wal is constructed with all interceptors.
	Build(param InterceptorBuildParam) Interceptor
}

type Interceptor interface {
	// AppendInterceptor is the interceptor for Append functions.
	// All wal extra operations should be done by these function, such as
	// 1. time tick setup.
	// 2. unique primary key filter and build.
	// 3. index builder.
	// 4. cache sync up.
	// AppendInterceptor should be lazy initialized and fast execution.
	// Execute the append operation with interceptor.
	DoAppend(ctx context.Context, msg message.MutableMessage, append Append) (message.MessageID, error)

	// Close the interceptor release all the resources.
	Close()
}

// Some interceptor may need to wait for some resource to be ready or recovery process.
type InterceptorWithReady interface {
	Interceptor

	// Ready check if interceptor is ready.
	// Close of Interceptor would not notify the ready (closed interceptor is not ready).
	// So always apply timeout when waiting for ready.
	// Some append interceptor may be stateful, such as index builder and unique primary key filter,
	// so it need to implement the recovery logic from crash by itself before notifying ready.
	// Append operation will block until ready or canceled.
	// Consumer do not blocked by it.
	Ready() <-chan struct{}
}

// Some interceptor may need to perform a graceful close operation.
type InterceptorWithGracefulClose interface {
	Interceptor

	// GracefulClose will be called when the wal begin to close.
	// The interceptor can do some operations before the wal rejects all incoming append operations.
	GracefulClose()
}
