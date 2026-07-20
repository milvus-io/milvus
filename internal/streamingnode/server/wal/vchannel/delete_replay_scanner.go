package vchannel

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
)

const deleteReplayScannerName = "vchannel-recovery-module-delete-replay"

func newDeleteReplayScanner(
	ctx context.Context,
	manager wal.TransformLogStreamManager,
	pchannel string,
	vchannel string,
	startAfterTimeTick uint64,
	endTimeTick uint64,
) wal.TransformLogScanner {
	if manager == nil {
		return wal.NewTransformLogErrorScanner(deleteReplayScannerName, wal.ErrTransformLogVChannelUnavailable)
	}
	if endTimeTick == 0 {
		return wal.NewEmptyTransformLogScanner(deleteReplayScannerName)
	}
	stream, err := manager.AcquireStream(ctx, pchannel)
	if err != nil {
		return wal.NewTransformLogErrorScanner(deleteReplayScannerName, err)
	}
	scanner := &streamDeleteReplayScanner{
		name:   deleteReplayScannerName,
		stream: stream,
		ch:     make(chan wal.TransformLogEvent, 16),
		done:   make(chan struct{}),
	}
	handler := streamDeleteReplayHandler{scanner: scanner}
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{
		VChannel:           vchannel,
		StartAfterTimeTick: startAfterTimeTick,
		EndTimeTick:        endTimeTick,
		Handler:            handler,
	})
	if err != nil {
		_ = stream.Close()
		return wal.NewTransformLogErrorScanner(deleteReplayScannerName, err)
	}
	scanner.sub = sub
	return scanner
}

type streamDeleteReplayScanner struct {
	name   string
	stream wal.TransformLogStream
	sub    wal.TransformLogSubscription
	ch     chan wal.TransformLogEvent
	done   chan struct{}

	errMu sync.Mutex
	err   error
	once  sync.Once
}

func (s *streamDeleteReplayScanner) Name() string {
	return s.name
}

func (s *streamDeleteReplayScanner) Chan() <-chan wal.TransformLogEvent {
	return s.ch
}

func (s *streamDeleteReplayScanner) Error() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *streamDeleteReplayScanner) Done() <-chan struct{} {
	return s.done
}

func (s *streamDeleteReplayScanner) Close() error {
	if s.sub != nil {
		_ = s.sub.Close()
	}
	if s.stream != nil {
		_ = s.stream.Close()
	}
	s.finish(nil)
	return s.Error()
}

func (s *streamDeleteReplayScanner) setError(err error) {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

func (s *streamDeleteReplayScanner) finish(err error) {
	s.once.Do(func() {
		if err != nil {
			s.setError(err)
		}
		close(s.ch)
		close(s.done)
	})
}

type streamDeleteReplayHandler struct {
	scanner *streamDeleteReplayScanner
}

func (h streamDeleteReplayHandler) Handle(event wal.TransformLogStreamEvent) error {
	if event.Err != nil {
		h.scanner.finish(event.Err)
		return nil
	}
	select {
	case h.scanner.ch <- wal.TransformLogEvent{
		Entry:  event.Entry,
		SyncUp: event.SyncUp,
	}:
		return nil
	case <-h.scanner.done:
		return nil
	}
}

func (h streamDeleteReplayHandler) Close() {
	h.scanner.finish(nil)
}
