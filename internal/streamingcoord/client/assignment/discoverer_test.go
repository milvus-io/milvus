package assignment

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestReportAssignmentErrorFullQueueRespectsContext(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration
		wantErr error
	}{
		{name: "canceled", wantErr: context.Canceled},
		{name: "deadline_exceeded", timeout: 20 * time.Millisecond, wantErr: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			discoverer := &assignmentDiscoverClient{
				lifetime:  typeutil.NewLifetime(),
				requestCh: make(chan *streamingpb.AssignmentDiscoverRequest, 1),
				exitCh:    make(chan struct{}),
			}
			// Keep the queue full without a send loop to exercise backpressure.
			queued := &streamingpb.AssignmentDiscoverRequest{}
			discoverer.requestCh <- queued
			defer close(discoverer.exitCh)
			service := &AssignmentServiceImpl{
				lifetime:   typeutil.NewLifetime(),
				cond:       syncutil.NewContextCond(&sync.Mutex{}),
				discoverer: discoverer,
			}

			ctx, cancel := context.WithCancel(context.Background())
			if tc.timeout > 0 {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), tc.timeout)
			}
			defer cancel()
			result := make(chan error, 1)
			started := make(chan struct{})
			go func() {
				close(started)
				result <- service.ReportAssignmentError(ctx, types.PChannelInfo{Name: "c1", Term: 1}, merr.WrapErrNodeNotMatch(1, 2))
			}()
			<-started
			if tc.timeout == 0 {
				cancel()
			}

			select {
			case err := <-result:
				require.ErrorIs(t, err, tc.wantErr)
			case <-time.After(5 * time.Second):
				t.Fatal("assignment error reporting ignored the request context")
			}
			require.Len(t, discoverer.requestCh, 1)
			assert.Same(t, queued, <-discoverer.requestCh)
		})
	}
}

func TestReportAssignmentErrorDeduplicatesChannelTerm(t *testing.T) {
	stream := mock_streamingpb.NewMockStreamingCoordAssignmentService_AssignmentDiscoverClient(t)
	var reports []types.PChannelInfo
	stream.EXPECT().Send(mock.Anything).RunAndReturn(func(req *streamingpb.AssignmentDiscoverRequest) error {
		if report := req.GetReportError(); report != nil {
			reports = append(reports, types.NewPChannelInfoFromProto(report.GetPchannel()))
		}
		return nil
	})
	stream.EXPECT().CloseSend().Return(nil).Once()

	discoverer := &assignmentDiscoverClient{
		lifetime:              typeutil.NewLifetime(),
		streamClient:          stream,
		requestCh:             make(chan *streamingpb.AssignmentDiscoverRequest, 8),
		exitCh:                make(chan struct{}),
		lastErrorReportedTerm: make(map[string]int64),
	}
	discoverer.wg.Add(1)
	go discoverer.sendLoop()
	for _, channel := range []types.PChannelInfo{
		{Name: "c1", Term: 2},
		{Name: "c1", Term: 2},
		{Name: "c1", Term: 1},
		{Name: "c1", Term: 3},
		{Name: "c2", Term: 1},
	} {
		require.NoError(t, discoverer.ReportAssignmentError(context.Background(), channel, merr.WrapErrNodeNotMatch(1, 2)))
	}
	discoverer.Close()

	assert.Equal(t, []types.PChannelInfo{
		{Name: "c1", Term: 2},
		{Name: "c1", Term: 3},
		{Name: "c2", Term: 1},
	}, reports)
}

func TestReportAssignmentErrorRetriesClosedDiscoverer(t *testing.T) {
	for _, replace := range []bool{true, false} {
		name := "replacement_ready"
		if !replace {
			name = "replacement_deadline"
		}
		t.Run(name, func(t *testing.T) {
			oldDiscoverer := &assignmentDiscoverClient{
				lifetime:  typeutil.NewLifetime(),
				requestCh: make(chan *streamingpb.AssignmentDiscoverRequest, 1),
				exitCh:    make(chan struct{}),
			}
			oldDiscoverer.requestCh <- &streamingpb.AssignmentDiscoverRequest{}
			service := &AssignmentServiceImpl{
				lifetime:   typeutil.NewLifetime(),
				cond:       syncutil.NewContextCond(&sync.Mutex{}),
				discoverer: oldDiscoverer,
			}
			timeout := 5 * time.Second
			if !replace {
				timeout = time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			observedCtx := &enqueueWaitContext{Context: ctx, entered: make(chan struct{})}
			channel := types.PChannelInfo{Name: "c1", Term: 11}
			result := make(chan error, 1)
			go func() {
				result <- service.ReportAssignmentError(observedCtx, channel, merr.WrapErrNodeNotMatch(1, 2))
			}()

			// The first Done call is the old discoverer's enqueue select, so
			// closing its stream here deterministically rejects that enqueue.
			select {
			case <-observedCtx.entered:
			case <-ctx.Done():
				t.Fatal("report did not reach the old discoverer")
			}
			close(oldDiscoverer.exitCh)
			var replacement *assignmentDiscoverClient
			if replace {
				replacement = &assignmentDiscoverClient{
					lifetime:  typeutil.NewLifetime(),
					requestCh: make(chan *streamingpb.AssignmentDiscoverRequest, 1),
					exitCh:    make(chan struct{}),
				}
				service.cond.LockAndBroadcast()
				service.discoverer = replacement
				service.cond.L.Unlock()
			}
			select {
			case err := <-result:
				if replace {
					require.NoError(t, err)
					require.Len(t, replacement.requestCh, 1)
					report := (<-replacement.requestCh).GetReportError()
					require.Equal(t, channel, types.NewPChannelInfoFromProto(report.GetPchannel()))
				} else {
					require.ErrorIs(t, err, context.DeadlineExceeded)
				}
			case <-time.After(6 * time.Second):
				t.Fatal("assignment reporting ignored the original deadline")
			}
			require.Len(t, oldDiscoverer.requestCh, 1)
		})
	}
}

func TestReportAssignmentErrorClosedDiscovererWithQueueRoom(t *testing.T) {
	// A closed stream never sends what is queued, so room in the queue must not
	// turn the rejection into a lost report.
	discoverer := &assignmentDiscoverClient{
		lifetime:  typeutil.NewLifetime(),
		requestCh: make(chan *streamingpb.AssignmentDiscoverRequest, 1),
		exitCh:    make(chan struct{}),
	}
	close(discoverer.exitCh)
	for range 100 {
		err := discoverer.ReportAssignmentError(context.Background(), types.PChannelInfo{Name: "c1", Term: 1}, merr.WrapErrNodeNotMatch(1, 2))
		require.True(t, status.AsStreamingError(err).IsOnShutdown())
		require.Empty(t, discoverer.requestCh)
	}
}

func TestReportAssignmentErrorStoppedDiscovererRespectsDeadline(t *testing.T) {
	// A stopped discoverer may still appear available until its receive loop
	// exits. Wait for its replacement without reselecting the rejected client.
	discoverer := &assignmentDiscoverClient{
		lifetime: typeutil.NewLifetime(),
		exitCh:   make(chan struct{}),
	}
	discoverer.lifetime.SetState(typeutil.LifetimeStateStopped)
	service := &AssignmentServiceImpl{
		lifetime:   typeutil.NewLifetime(),
		cond:       syncutil.NewContextCond(&sync.Mutex{}),
		discoverer: discoverer,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- service.ReportAssignmentError(ctx, types.PChannelInfo{Name: "c1", Term: 1}, merr.WrapErrNodeNotMatch(1, 2))
	}()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("closed discoverer retry ignored the original deadline")
	}
}

// enqueueWaitContext exposes when reporting reaches a blocking select without
// changing cancellation or deadline behavior.
type enqueueWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *enqueueWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}
