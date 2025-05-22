package wal

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

// OpenerBuilder is the interface for build wal opener.
type OpenerBuilder interface {
	// Name of the wal builder, should be a lowercase string.
	Name() string

	Build() (Opener, error)
}

// OpenOption is the option for allocating wal instance.
type OpenOption struct {
	Channel        types.PChannelInfo
	DisableFlusher bool // disable flusher for test, only use in test.
}

// Opener is the interface for build wal instance.
type Opener interface {
	// Open open a wal instance.
	Open(ctx context.Context, opt *OpenOption) (WAL, error)

	// Close closes the opener resources.
	Close()
}
