package wal

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

var (
	ErrTransformLogInvalidReadOption   = errors.New("invalid transform log read option")
	ErrTransformLogStartPointTruncated = errors.New("transform log start point is truncated")
	ErrTransformLogVChannelUnavailable = errors.New("transform log vchannel is unavailable")
)

// TransformLogAccesser is the WAL entry for transform log event streams.
type TransformLogAccesser = TransformLogStreamManager

type TransformLogSyncUp struct {
	TimeTick uint64
}

type TransformLogStreamManager interface {
	AcquireStream(ctx context.Context, pchannel string) (TransformLogStream, error)
}

type TransformLogStream interface {
	Subscribe(ctx context.Context, opt TransformLogSubscriptionOption) (TransformLogSubscription, error)
	Done() <-chan struct{}
	Error() error
	Close() error
}

type TransformLogSubscriptionOption struct {
	SubscriptionID     int64
	VChannel           string
	StartAfterTimeTick uint64
	EndTimeTick        uint64
	Handler            TransformLogEventHandler
}

type TransformLogEventHandler interface {
	Handle(event TransformLogStreamEvent) error
	Close()
}

type TransformLogSubscription interface {
	ID() int64
	VChannel() string
	Close() error
}

type TransformLogStreamEvent struct {
	SubscriptionID int64
	VChannel       string
	Entry          *streamingpb.TransformLogEntry
	SyncUp         *TransformLogSyncUp
	Err            error
}

type transformLogErrorAccesser struct {
	err error
}

func NewTransformLogErrorAccesser(err error) TransformLogAccesser {
	return transformLogErrorAccesser{err: err}
}

func (a transformLogErrorAccesser) AcquireStream(context.Context, string) (TransformLogStream, error) {
	return nil, a.err
}
