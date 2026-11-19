package manager

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// Follow resolves the writer again after every lost stream. Each subscription
// starts at a new full snapshot; no cursor survives the writer's term.
func (c *WALReplicaClient) Follow(ctx context.Context, channel types.PChannelInfo, apply func(context.Context, *streamingpb.WALReplicaState, bool) error, reports func(context.Context) (*streamingpb.WALQueryViewReport, error)) {
	delay := backoff.NewExponentialBackOff()
	delay.InitialInterval = 100 * time.Millisecond
	delay.MaxInterval = 3 * time.Second
	delay.MaxElapsedTime = 0
	delay.Reset()
	for ctx.Err() == nil {
		state, err := c.Discover(ctx)
		if err == nil {
			var writer *types.PChannelInfoAssigned
			writer, err = WriterFor(state, channel.Name)
			if err == nil {
				err = c.followWriter(ctx, writer, channel.WALReplicaID, apply, reports, delay.Reset)
			}
		}
		if ctx.Err() != nil {
			return
		}
		mlog.RatedWarn(ctx, 5, "reconnecting WAL reader to writer", mlog.String("channel", channel.Name), mlog.Err(err))
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay.NextBackOff()):
		}
	}
}

func (c *WALReplicaClient) followWriter(ctx context.Context, writer *types.PChannelInfoAssigned, id int64, apply func(context.Context, *streamingpb.WALReplicaState, bool) error, reports func(context.Context) (*streamingpb.WALQueryViewReport, error), connected func()) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.Watch(ctx, writer, id)
	if err != nil {
		return err
	}
	response, err := stream.Recv()
	if err != nil {
		return err
	}
	if response.GetSnapshot() == nil {
		return status.NewInvalidArgument("missing initial WAL replica snapshot")
	}
	if err := apply(ctx, response.GetSnapshot(), true); err != nil {
		return err
	}
	connected()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer cancel()
		for {
			report, err := reports(ctx)
			if err != nil {
				return
			}
			if err := stream.Send(&streamingpb.WatchWALReplicaStateRequest{Request: &streamingpb.WatchWALReplicaStateRequest_ViewReport{ViewReport: report}}); err != nil {
				return
			}
		}
	}()
	defer func() { cancel(); <-done }()
	for {
		response, err = stream.Recv()
		if err != nil {
			return err
		}
		if response.GetChanges() == nil {
			return status.NewInvalidArgument("expected WAL replica state changes")
		}
		if err := apply(ctx, response.GetChanges(), false); err != nil {
			return err
		}
	}
}
