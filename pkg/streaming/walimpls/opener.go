package walimpls

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

// OpenOption is the option for allocating wal impls instance.
type OpenOption struct {
	Channel types.PChannelInfo // Channel to open.
}

// OpenerImpls is the interface for build WALImpls instance.
type OpenerImpls interface {
	// Open open a WALImpls instance.
	Open(ctx context.Context, opt *OpenOption) (WALImpls, error)

	// Close release the resources.
	Close()
}
