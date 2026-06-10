package transformlog

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	transformlogapi "github.com/milvus-io/milvus/internal/streamingnode/transformlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestResumableScannerResumesFromLastDeliveredTimeTick(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var opts []transformlogapi.ReadOption
	scanners := []transformlogapi.Scanner{
		newClosedFakeScanner([]transformlogapi.Event{
			{Entry: &streamingpb.TransformLogEntry{TimeTick: 11}},
		}, grpcstatus.Error(codes.Unavailable, "stream broken")),
		newBlockingFakeScanner([]transformlogapi.Event{
			{Entry: &streamingpb.TransformLogEntry{TimeTick: 12}},
		}),
	}
	scanner := NewResumableScanner(ctx, func(ctx context.Context, opt transformlogapi.ReadOption) transformlogapi.Scanner {
		mu.Lock()
		defer mu.Unlock()
		opts = append(opts, opt)
		return scanners[len(opts)-1]
	}, transformlogapi.ReadOption{
		Name:               "test",
		VChannel:           "by-dev-rootcoord-dml_1_100v0",
		StartAfterTimeTick: 10,
	})
	defer scanner.Close()

	assert.Equal(t, uint64(11), (<-scanner.Chan()).Entry.GetTimeTick())
	assert.Equal(t, uint64(12), (<-scanner.Chan()).Entry.GetTimeTick())
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(opts) == 2 && opts[0].StartAfterTimeTick == 10 && opts[1].StartAfterTimeTick == 11
	}, time.Second, 10*time.Millisecond)
}

func TestResumableScannerStopsOnTerminalTransformLogError(t *testing.T) {
	scanner := NewResumableScanner(context.Background(), func(ctx context.Context, opt transformlogapi.ReadOption) transformlogapi.Scanner {
		return transformlogapi.NewErrorScanner(opt.Name, errors.Wrap(transformlogapi.ErrStartPointTruncated, "truncated"))
	}, transformlogapi.ReadOption{
		Name:               "test",
		VChannel:           "by-dev-rootcoord-dml_1_100v0",
		StartAfterTimeTick: 10,
	})

	select {
	case <-scanner.Done():
	case <-time.After(time.Second):
		t.Fatal("scanner should stop on terminal transform log error")
	}
	assert.ErrorIs(t, scanner.Error(), transformlogapi.ErrStartPointTruncated)
}

type fakeScanner struct {
	ch        chan transformlogapi.Event
	done      chan struct{}
	err       error
	closeOnce sync.Once
}

func newClosedFakeScanner(events []transformlogapi.Event, err error) *fakeScanner {
	scanner := newBlockingFakeScanner(events)
	scanner.err = err
	close(scanner.ch)
	return scanner
}

func newBlockingFakeScanner(events []transformlogapi.Event) *fakeScanner {
	ch := make(chan transformlogapi.Event, len(events))
	for _, event := range events {
		ch <- event
	}
	return &fakeScanner{
		ch:   ch,
		done: make(chan struct{}),
	}
}

func (s *fakeScanner) Name() string {
	return "fake"
}

func (s *fakeScanner) Chan() <-chan transformlogapi.Event {
	return s.ch
}

func (s *fakeScanner) Error() error {
	return s.err
}

func (s *fakeScanner) Done() <-chan struct{} {
	return s.done
}

func (s *fakeScanner) Close() error {
	s.closeOnce.Do(func() {
		select {
		case <-s.done:
		default:
			close(s.done)
		}
	})
	return s.err
}
