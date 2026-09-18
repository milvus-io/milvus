package flusherimpl

import (
	"context"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/flushcommon/pipeline"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
)

// newDataSyncServiceWrapper creates a new data sync service wrapper.
func newDataSyncServiceWrapper(
	channelName string,
	input chan<- *msgstream.MsgPack,
	ds *pipeline.DataSyncService,
	channelCheckpointTimeTick uint64,
) *dataSyncServiceWrapper {
	handler := adaptor.NewBaseMsgPackAdaptorHandler()
	return &dataSyncServiceWrapper{
		channelName:               channelName,
		input:                     input,
		handler:                   handler,
		ds:                        ds,
		channelCheckpointTimeTick: channelCheckpointTimeTick,
	}
}

// dataSyncServiceWrapper wraps DataSyncService and related input channel.
type dataSyncServiceWrapper struct {
	channelName               string
	channelCheckpointTimeTick uint64
	input                     chan<- *msgstream.MsgPack
	handler                   *adaptor.BaseMsgPackAdaptorHandler
	ds                        *pipeline.DataSyncService
	// ackedCheckpointTimeTick is the largest channel checkpoint time tick
	// DataCoord has acked for this service. It is written by the
	// checkpoint-updater callback and read by the dispatch goroutine, which
	// alone decides to close a drained fenced source.
	ackedCheckpointTimeTick atomic.Uint64
}

// ObserveAckedCheckpoint records an acked channel checkpoint. The recorded
// value only ever grows: acks for different checkpoint rounds may complete
// out of order.
func (ds *dataSyncServiceWrapper) ObserveAckedCheckpoint(timestamp uint64) {
	for {
		current := ds.ackedCheckpointTimeTick.Load()
		if timestamp <= current || ds.ackedCheckpointTimeTick.CompareAndSwap(current, timestamp) {
			return
		}
	}
}

// AckedCheckpoint returns the largest acked channel checkpoint time tick, 0 if
// none was acked since the service was built.
func (ds *dataSyncServiceWrapper) AckedCheckpoint() uint64 {
	return ds.ackedCheckpointTimeTick.Load()
}

// Start starts the data sync service.
func (ds *dataSyncServiceWrapper) Start() {
	ds.ds.Start()
}

// HandleMessage handles the incoming message.
func (ds *dataSyncServiceWrapper) HandleMessage(ctx context.Context, msg message.ImmutableMessage) error {
	ds.handler.GenerateMsgPack(msg)
	for ds.handler.PendingMsgPack.Len() > 0 {
		next := ds.handler.PendingMsgPack.Next()
		nextTsMsg := msgstream.MustBuildMsgPackFromConsumeMsgPack(next, adaptor.UnmashalerDispatcher)

		// filter out the message less than vchannel level checkpoint.
		if nextTsMsg.EndTs < ds.channelCheckpointTimeTick {
			ds.handler.Logger.Debug(ctx, "skip the message less than vchannel checkpoint",
				mlog.Uint64("timestamp", nextTsMsg.EndTs),
				mlog.Uint64("checkpoint", ds.channelCheckpointTimeTick),
			)
			ds.handler.PendingMsgPack.UnsafeAdvance()
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ds.input <- nextTsMsg:
			// The input channel will never get stuck because the data sync service will consume the message continuously.
			ds.handler.PendingMsgPack.UnsafeAdvance()
		}
	}
	return nil
}

// Close close the input channel and gracefully close the data sync service.
func (ds *dataSyncServiceWrapper) Close() {
	// The input channel should be closed first, otherwise the flowgraph in datasync service will be blocked.
	close(ds.input)
	ds.ds.GracefullyClose()
	resource.Resource().WriteBufferManager().RemoveChannel(ds.channelName)
}
