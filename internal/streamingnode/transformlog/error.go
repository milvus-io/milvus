package transformlog

import (
	"context"

	"github.com/cockroachdb/errors"
)

var (
	ErrInvalidReadOption   = errors.New("invalid transform log read option")
	ErrStartPointTruncated = errors.New("transform log start point is truncated")
	ErrVChannelUnavailable = errors.New("transform log vchannel is unavailable")
)

type errorAccesser struct {
	err error
}

func NewErrorAccesser(err error) Accesser {
	return errorAccesser{err: err}
}

func (a errorAccesser) Read(context.Context, ReadOption) Scanner {
	return NewErrorScanner("", a.err)
}

type errorScanner struct {
	name string
	done chan struct{}
	err  error
}

func NewErrorScanner(name string, err error) Scanner {
	done := make(chan struct{})
	close(done)
	return errorScanner{name: name, done: done, err: err}
}

func (s errorScanner) Name() string {
	return s.name
}

func (s errorScanner) Chan() <-chan Event {
	ch := make(chan Event)
	close(ch)
	return ch
}

func (s errorScanner) Error() error {
	return s.err
}

func (s errorScanner) Done() <-chan struct{} {
	return s.done
}

func (s errorScanner) Close() error {
	return s.err
}
