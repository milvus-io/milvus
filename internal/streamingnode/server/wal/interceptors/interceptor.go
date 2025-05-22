package interceptors

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

type (
	// Append is the common function to append a msg to the wal.
	Append = func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)
)

// InterceptorBuildParam is the parameter to build a interceptor.
type InterceptorBuildParam struct {
	ChannelInfo            types.PChannelInfo
	WAL                    *syncutil.Future[wal.WAL]  // The wal final object, can be used after interceptor is ready.
	LastTimeTickMessage    message.ImmutableMessage   // The last time tick message in wal.
	WriteAheadBuffer       *wab.WriteAheadBuffer      // The write ahead buffer for the wal, used to erase the subscription of underlying wal.
	MVCCManager            *mvcc.MVCCManager          // The MVCC manager for the wal, can be used to get the latest mvcc timetick.
	InitialRecoverSnapshot *recovery.RecoverySnapshot // The initial recover snapshot for the wal, used to recover the wal state.
	TxnManager             *txn.TxnManager            // The transaction manager for the wal, used to manage the transactions.
	ShardManager           shards.ShardManager        // The shard manager for the wal, used to manage the shards, segment assignment, partition.
}

// Clear release the resources in the interceptor build param.
func (p *InterceptorBuildParam) Clear() {
	if p.WriteAheadBuffer != nil {
		p.WriteAheadBuffer.Close()
	}
	if p.ShardManager != nil {
		p.ShardManager.Close()
	}
}

// InterceptorBuilder is the interface to build a interceptor.
// 1. InterceptorBuilder is concurrent safe.
// 2. InterceptorBuilder can used to build a interceptor with cross-wal shared resources.
type InterceptorBuilder interface {
	// Build build a interceptor with wal that interceptor will work on.
	// the wal object will be sent to the interceptor builder when the wal is constructed with all interceptors.
	Build(param *InterceptorBuildParam) Interceptor
}

type Interceptor interface {
	// AppendInterceptor is the interceptor for Append functions.
	// All wal extra operations should be done by these function, such as
	// 1. time tick setup.
	// 2. unique primary key filter and build.
	// 3. index builder.
	// 4. cache sync up.
	// !!! AppendInterceptor should be lazy initialized and fast execution.
	// !!! the operation after append should never return error.
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

// InterceptorWithMetrics is the interceptor with metric collecting.
type InterceptorWithMetrics interface {
	Interceptor

	// Name returns the name of the interceptor.
	Name() string
}

// Some interceptor may need to perform a graceful close operation.
type InterceptorWithGracefulClose interface {
	Interceptor

	// GracefulClose will be called when the wal begin to close.
	// The interceptor can do some operations before the wal rejects all incoming append operations.
	GracefulClose()
}
