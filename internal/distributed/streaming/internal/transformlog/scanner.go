package transformlog

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	transformlogapi "github.com/milvus-io/milvus/internal/streamingnode/transformlog"
	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type Factory = func(ctx context.Context, opt transformlogapi.ReadOption) transformlogapi.Scanner

func NewResumableScanner(ctx context.Context, factory Factory, opt transformlogapi.ReadOption) transformlogapi.Scanner {
	ctx, cancel := context.WithCancel(ctx)
	scanner := &resumableScanner{
		ctx:       ctx,
		cancel:    cancel,
		factory:   factory,
		opt:       opt,
		ch:        make(chan transformlogapi.Event, 16),
		finishErr: syncutil.NewFuture[error](),
		closed:    atomic.NewBool(false),
		logger: mlog.With(
			mlog.String("pchannel", funcutil.ToPhysicalChannel(opt.VChannel)),
			mlog.String("vchannel", opt.VChannel),
			mlog.String("scanner", opt.Name),
		),
	}
	go scanner.resumeLoop()
	return scanner
}

type resumableScanner struct {
	ctx       context.Context
	cancel    context.CancelFunc
	factory   Factory
	opt       transformlogapi.ReadOption
	ch        chan transformlogapi.Event
	finishErr *syncutil.Future[error]
	closed    *atomic.Bool
	logger    *mlog.Logger
}

func (s *resumableScanner) Name() string {
	return s.opt.Name
}

func (s *resumableScanner) Chan() <-chan transformlogapi.Event {
	return s.ch
}

func (s *resumableScanner) Error() error {
	return s.finishErr.Get()
}

func (s *resumableScanner) Done() <-chan struct{} {
	return s.finishErr.Done()
}

func (s *resumableScanner) Close() error {
	s.closed.Store(true)
	s.cancel()
	<-s.Done()
	return s.Error()
}

func (s *resumableScanner) resumeLoop() {
	var finalErr error
	defer func() {
		close(s.ch)
		if s.closed.Load() && errors.Is(finalErr, context.Canceled) {
			finalErr = nil
		}
		s.finishErr.Set(finalErr)
	}()

	nextStartAfter := s.opt.StartAfterTimeTick
	retryBackoff := backoff.NewExponentialBackOff()
	retryBackoff.InitialInterval = 100 * time.Millisecond
	retryBackoff.MaxInterval = 10 * time.Second
	retryBackoff.MaxElapsedTime = 0
	retryBackoff.Reset()

	for {
		if err := s.ctx.Err(); err != nil {
			finalErr = err
			return
		}

		opt := s.opt
		opt.StartAfterTimeTick = nextStartAfter
		underlying := s.factory(s.ctx, opt)
		err := s.forward(underlying, &nextStartAfter)
		if err != nil && !isRetryable(err) {
			finalErr = err
			return
		}
		if err == nil {
			s.logger.Info(context.TODO(), "transform log scanner closed, resume from last delivered timetick", mlog.Uint64("startAfter", nextStartAfter))
		} else {
			s.logger.Warn(context.TODO(), "transform log scanner error, resume from last delivered timetick", mlog.Err(err), mlog.Uint64("startAfter", nextStartAfter))
		}
		if waitErr := s.waitNextRetry(retryBackoff.NextBackOff()); waitErr != nil {
			finalErr = waitErr
			return
		}
	}
}

func (s *resumableScanner) forward(underlying transformlogapi.Scanner, nextStartAfter *uint64) error {
	defer func() {
		_ = underlying.Close()
	}()
	for {
		select {
		case event, ok := <-underlying.Chan():
			if !ok {
				return underlying.Error()
			}
			if !s.sendEvent(event) {
				return s.ctx.Err()
			}
			if event.Entry != nil && event.Entry.GetTimeTick() > *nextStartAfter {
				*nextStartAfter = event.Entry.GetTimeTick()
			}
		case <-underlying.Done():
			return underlying.Error()
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}

func (s *resumableScanner) sendEvent(event transformlogapi.Event) bool {
	select {
	case s.ch <- event:
		return true
	case <-s.ctx.Done():
		return false
	}
}

func (s *resumableScanner) waitNextRetry(duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func isRetryable(err error) bool {
	if err == nil {
		return true
	}
	if errors.IsAny(err, transformlogapi.ErrInvalidReadOption, transformlogapi.ErrStartPointTruncated, transformlogapi.ErrVChannelUnavailable) {
		return false
	}
	var streamingErr *streamingstatus.StreamingError
	if errors.As(err, &streamingErr) {
		return false
	}
	grpcStatus, ok := grpcstatus.FromError(err)
	if !ok {
		return false
	}
	switch grpcStatus.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Canceled:
		return true
	default:
		return false
	}
}
