package wal

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/logpb"
)

// OpenerBuilder is the interface for build wal opener.
type OpenerBuilder interface {
	// Name of the wal builder, should be a lowercase string.
	Name() string

	Build() (Opener, error)
}

// BasicOpenOption is the basic option for allocating wal instance.
// It is used to build BasicWAL instance with BasicOpener.
type BasicOpenOption struct {
	Channel *logpb.PChannelInfo // Channel to open.
}

// BasicOpener is the interface for build BasicWAL instance.
// BasicOpener can be extended to Opener by using `extends.NewOpenerWithBasicOpener`.
type BasicOpener interface {
	// Open open a basicWAL instance.
	Open(ctx context.Context, opt *BasicOpenOption) (BasicWAL, error)

	// Close release the resources.
	Close()
}

// OpenOption is the option for allocating wal instance.
type OpenOption struct {
	BasicOpenOption
	InterceptorBuilders []InterceptorBuilder // Interceptor builders to build when open.
}

// Opener is the interface for build wal instance.
type Opener interface {
	// Open open a wal instance.
	Open(ctx context.Context, opt *OpenOption) (WAL, error)

	// Close closes the opener resources.
	Close()
}
