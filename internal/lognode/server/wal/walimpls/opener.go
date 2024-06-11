package walimpls

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/logpb"
)

// OpenOption is the option for allocating wal impls instance.
type OpenOption struct {
	Channel *logpb.PChannelInfo // Channel to open.
}

// OpenerImpls is the interface for build WALImpls instance.
type OpenerImpls interface {
	// Open open a WALImpls instance.
	Open(ctx context.Context, opt *OpenOption) (WALImpls, error)

	// Close release the resources.
	Close()
}
