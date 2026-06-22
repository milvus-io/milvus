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

// TransformLogAccesser is the WAL read entry for transform log events.
type TransformLogAccesser interface {
	Read(ctx context.Context, opt TransformLogReadOption) TransformLogScanner
}

type TransformLogReadOption struct {
	Name               string
	VChannel           string
	StartAfterTimeTick uint64
	EndTimeTick        uint64
}

type TransformLogScanner interface {
	Name() string
	Chan() <-chan TransformLogEvent
	Error() error
	Done() <-chan struct{}
	Close() error
}

type TransformLogEvent struct {
	Entry    *streamingpb.TransformLogEntry
	CaughtUp *TransformLogCaughtUp
}

type TransformLogCaughtUp struct {
	StartAfterTimeTick uint64
}

type transformLogErrorAccesser struct {
	err error
}

func NewTransformLogErrorAccesser(err error) TransformLogAccesser {
	return transformLogErrorAccesser{err: err}
}

func (a transformLogErrorAccesser) Read(context.Context, TransformLogReadOption) TransformLogScanner {
	return NewTransformLogErrorScanner("", a.err)
}

type transformLogErrorScanner struct {
	name string
	done chan struct{}
	err  error
}

func NewTransformLogErrorScanner(name string, err error) TransformLogScanner {
	done := make(chan struct{})
	close(done)
	return transformLogErrorScanner{name: name, done: done, err: err}
}

func NewEmptyTransformLogScanner(name string) TransformLogScanner {
	return NewTransformLogErrorScanner(name, nil)
}

func (s transformLogErrorScanner) Name() string {
	return s.name
}

func (s transformLogErrorScanner) Chan() <-chan TransformLogEvent {
	ch := make(chan TransformLogEvent)
	close(ch)
	return ch
}

func (s transformLogErrorScanner) Error() error {
	return s.err
}

func (s transformLogErrorScanner) Done() <-chan struct{} {
	return s.done
}

func (s transformLogErrorScanner) Close() error {
	return s.err
}
