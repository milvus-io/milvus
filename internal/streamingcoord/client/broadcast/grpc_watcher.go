package broadcast

import (
	"context"
	"io"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type grpcWatcherBuilder struct {
	broadcastService lazygrpc.Service[streamingpb.StreamingCoordBroadcastServiceClient]
}

func (b *grpcWatcherBuilder) Build(ctx context.Context) (Watcher, error) {
	service, err := b.broadcastService.GetService(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get broadcast service")
	}
	bt := syncutil.NewAsyncTaskNotifier[struct{}]()
	// TODO: Here we make a broken stream by passing a context.
	// Implement a graceful closing should be better.
	streamCtx, cancel := context.WithCancel(context.Background())
	svr, err := service.Watch(streamCtx)
	if err != nil {
		cancel()
		return nil, errors.Wrap(err, "failed to create broadcast watcher server client")
	}
	w := &grpcWatcherClient{
		lifetime:           typeutil.NewLifetime(),
		backgroundTask:     bt,
		streamServerCancel: cancel,
		streamClient:       svr,
		input:              make(chan *message.BroadcastEvent),
		output:             make(chan *message.BroadcastEvent),
		sendExitCh:         make(chan struct{}),
		recvExitCh:         make(chan struct{}),
	}
	w.SetLogger(logger)
	go w.executeBackgroundTask()
	return w, nil
}

type grpcWatcherClient struct {
	log.Binder
	lifetime           *typeutil.Lifetime
	backgroundTask     *syncutil.AsyncTaskNotifier[struct{}]
	streamServerCancel context.CancelFunc
	streamClient       streamingpb.StreamingCoordBroadcastService_WatchClient
	input              chan *message.BroadcastEvent
	output             chan *message.BroadcastEvent
	recvExitCh         chan struct{}
	sendExitCh         chan struct{}
}

func (c *grpcWatcherClient) ObserveResourceKeyEvent(ctx context.Context, ev *message.BroadcastEvent) error {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return errWatcherClosed
	}
	defer c.lifetime.Done()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.backgroundTask.Context().Done():
		return c.backgroundTask.Context().Err()
	case c.input <- ev:
		return nil
	}
}

func (c *grpcWatcherClient) EventChan() <-chan *message.BroadcastEvent {
	return c.output
}

func (c *grpcWatcherClient) gracefulClose() error {
	c.lifetime.SetState(typeutil.LifetimeStateStopped)
	// cancel the background task and wait for all request to finish
	c.backgroundTask.Cancel()
	c.lifetime.Wait()

	select {
	case <-c.backgroundTask.FinishChan():
		return nil
	case <-time.After(100 * time.Millisecond):
		return context.DeadlineExceeded
	}
}

func (c *grpcWatcherClient) Close() {
	// Try to make a graceful close.
	if err := c.gracefulClose(); err != nil {
		c.Logger().Warn("failed to close the broadcast watcher gracefully, a froce closing will be applied", zap.Error(err))
	}
	c.streamServerCancel()
	c.backgroundTask.BlockUntilFinish()
}

func (c *grpcWatcherClient) executeBackgroundTask() {
	defer func() {
		close(c.output)
		c.backgroundTask.Finish(struct{}{})
	}()

	go c.recvLoop()
	go c.sendLoop()
	<-c.recvExitCh
	<-c.sendExitCh
}

// sendLoop send the incoming event to the remote server.
// If the input channel is closed, it will send a close message to the remote server and return.
func (c *grpcWatcherClient) sendLoop() (err error) {
	defer func() {
		if err != nil {
			c.Logger().Warn("send arm of stream closed by unexpected error", zap.Error(err))
		} else {
			c.Logger().Info("send arm of stream closed")
		}
		if err := c.streamClient.CloseSend(); err != nil {
			c.Logger().Warn("failed to close send", zap.Error(err))
		}
		close(c.sendExitCh)
	}()

	for {
		select {
		case <-c.backgroundTask.Context().Done():
			// send close message stop the loop.
			// then the server will close the recv arm and return io.EOF.
			// recv arm can be closed after that.
			return c.streamClient.Send(&streamingpb.BroadcastWatchRequest{
				Command: &streamingpb.BroadcastWatchRequest_Close{
					Close: &streamingpb.CloseBroadcastWatchRequest{},
				},
			})
		case ev := <-c.input:
			if err := c.streamClient.Send(&streamingpb.BroadcastWatchRequest{
				Command: &streamingpb.BroadcastWatchRequest_CreateEventWatch{
					CreateEventWatch: &streamingpb.BroadcastCreateEventWatchRequest{
						Event: ev,
					},
				},
			}); err != nil {
				return err
			}
		}
	}
}

// recvLoop receive the event from the remote server.
func (c *grpcWatcherClient) recvLoop() (err error) {
	defer func() {
		if err != nil {
			c.Logger().Warn("recv arm of stream closed by unexpected error", zap.Error(err))
		} else {
			c.Logger().Info("recv arm of stream closed")
		}
		close(c.recvExitCh)
	}()

	for {
		resp, err := c.streamClient.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch resp := resp.Response.(type) {
		case *streamingpb.BroadcastWatchResponse_EventDone:
			select {
			case c.output <- resp.EventDone.Event:
			case <-c.backgroundTask.Context().Done():
				c.Logger().Info("recv arm close when send event to output channel, skip wait for io.EOF")
				return nil
			}
		case *streamingpb.BroadcastWatchResponse_Close:
			// nothing to do now, just wait io.EOF.
		default:
			c.Logger().Warn("unknown response type", zap.Any("response", resp))
		}
	}
}
