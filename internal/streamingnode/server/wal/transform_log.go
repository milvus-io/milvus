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
	SubscriptionID int64
	VChannel       string
	// StartAfterTimeTick is exclusive. Losing part of the requested interval
	// terminates this subscription with ErrTransformLogStartPointTruncated.
	StartAfterTimeTick uint64
	EndTimeTick        uint64
	Handler            TransformLogEventHandler
}

// TransformLogEventHandler receives ordered callbacks. A handler must provide
// an independent abort/context that releases any blocked Handle before the
// caller waits for subscription or stream Close. Never call those blocking
// Close methods synchronously from Handle. Remote subscriptions share a receive
// loop: a blocked handler also delays other subscriptions on that RPC.
// Close is a completion notification, not the mechanism for aborting Handle.
// Handle receives ordered entries, watermarks, or a terminal subscription error.
// Events and their payloads are immutable; live dispatch may share a payload
// across subscriptions and Summary. Consumers must copy before modifying it.
// Transport failures remain stream-scoped. A permanent subscription failure
// ends only that subscription and cannot certify progress.
type TransformLogEventHandler interface {
	Handle(event TransformLogStreamEvent)
	Close()
}

// TransformLogSubscription owns resources until Close, including after a
// bounded subscription delivers its EndTimeTick SyncUp. Close joins delivery;
// unlike physical stream Close, it does not impose a timeout on the peer.
type TransformLogSubscription interface {
	ID() int64
	VChannel() string
	Close() error
}

type TransformLogStreamEvent struct {
	SubscriptionID    int64
	VChannel          string
	SubscriptionError error // permanent control event; exclusive with Entry/SyncUp
	Entry             *streamingpb.TransformLogEntry
	SyncUp            *TransformLogSyncUp
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
