package flusherimpl

import (
	"context"

	"github.com/milvus-io/milvus/internal/flushcommon/pipeline"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message/adaptor"
)

// newDataSyncServiceWrapper creates a new data sync service wrapper.
func newDataSyncServiceWrapper(input chan<- *msgstream.MsgPack, ds *pipeline.DataSyncService) *dataSyncServiceWrapper {
	handler := adaptor.NewBaseMsgPackAdaptorHandler()
	return &dataSyncServiceWrapper{
		input:   input,
		handler: handler,
		ds:      ds,
	}
}

// dataSyncServiceWrapper wraps DataSyncService and related input channel.
type dataSyncServiceWrapper struct {
	input   chan<- *msgstream.MsgPack
	handler *adaptor.BaseMsgPackAdaptorHandler
	ds      *pipeline.DataSyncService
}

// Start starts the data sync service.
func (ds *dataSyncServiceWrapper) Start() {
	ds.ds.Start()
}

// HandleMessage handles the incoming message.
func (ds *dataSyncServiceWrapper) HandleMessage(ctx context.Context, msg message.ImmutableMessage) error {
	ds.handler.GenerateMsgPack(msg)
	for ds.handler.PendingMsgPack.Len() > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ds.input <- ds.handler.PendingMsgPack.Next():
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
}
