package transformlog

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type Accesser interface {
	Read(ctx context.Context, opt ReadOption) Scanner
}

type ReadOption struct {
	Name               string
	VChannel           string
	StartAfterTimeTick uint64
}

type Scanner interface {
	Name() string
	Chan() <-chan Event
	Error() error
	Done() <-chan struct{}
	Close() error
}

type Event struct {
	Entry    *streamingpb.TransformLogEntry
	CaughtUp *CaughtUp
}

type CaughtUp struct {
	StartAfterTimeTick uint64
}
