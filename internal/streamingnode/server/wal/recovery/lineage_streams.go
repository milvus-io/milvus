package recovery

import (
	"context"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// lineageTransformLogStreams opens the TransformLog stream of a split target
// with the client a QueryNode uses. The handler client serves a pchannel of
// this StreamingNode from its local WAL and any other one over gRPC, so a
// target on this node and a target on another node take the same path. The
// client is resolved on use, because it is installed after the WALs open.
type lineageTransformLogStreams struct{}

func (lineageTransformLogStreams) AcquireStream(ctx context.Context, pchannel string) (wal.TransformLogStream, error) {
	accesser := streaming.WAL()
	if accesser == nil {
		return nil, merr.WrapErrServiceNotReadyMsg("streaming client is not initialized")
	}
	return accesser.TransformLogStreamManager().AcquireStream(ctx, pchannel)
}
