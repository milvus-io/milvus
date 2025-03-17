package flusherimpl

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/pipeline"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/stats"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
)

// flusherComponents is the components of the flusher.
type flusherComponents struct {
	wal               wal.WAL
	broker            broker.Broker
	cpUpdater         *util.ChannelCheckpointUpdater
	chunkManager      storage.ChunkManager
	dataServices      map[string]*dataSyncServiceWrapper
	checkpointManager *pchannelCheckpointManager
	logger            *log.MLogger
}

// StartMessageID returns the start message id of the flusher after recovering.
func (impl *flusherComponents) StartMessageID() message.MessageID {
	return impl.checkpointManager.StartMessageID()
}

// WhenCreateCollection handles the create collection message.
func (impl *flusherComponents) WhenCreateCollection(createCollectionMsg message.ImmutableCreateCollectionMessageV1) {
	if _, ok := impl.dataServices[createCollectionMsg.VChannel()]; ok {
		impl.logger.Info("the data sync service of current vchannel is built, skip it", zap.String("vchannel", createCollectionMsg.VChannel()))
		// May repeated consumed, so we ignore the message.
		return
	}
	createCollectionRequest, err := createCollectionMsg.Body()
	if err != nil {
		panic("the message body is not CreateCollectionRequest")
	}
	msgChan := make(chan *msgstream.MsgPack, 10)

	schema := &schemapb.CollectionSchema{}
	if err := proto.Unmarshal(createCollectionRequest.GetSchema(), schema); err != nil {
		panic("failed to unmarshal collection schema")
	}
	ds := pipeline.NewEmptyStreamingNodeDataSyncService(
		context.Background(), // There's no any rpc in this function, so the context is not used here.
		&util.PipelineParams{
			Ctx:                context.Background(),
			Broker:             impl.broker,
			SyncMgr:            resource.Resource().SyncManager(),
			ChunkManager:       impl.chunkManager,
			WriteBufferManager: resource.Resource().WriteBufferManager(),
			CheckpointUpdater:  impl.cpUpdater,
			Allocator:          idalloc.NewMAllocator(resource.Resource().IDAllocator()),
			MsgHandler:         newMsgHandler(resource.Resource().WriteBufferManager()),
		},
		msgChan,
		&datapb.VchannelInfo{
			CollectionID: createCollectionMsg.Header().GetCollectionId(),
			ChannelName:  createCollectionMsg.VChannel(),
			SeekPosition: &msgpb.MsgPosition{
				ChannelName: createCollectionMsg.VChannel(),
				// from the last confirmed message id, you can read all messages which timetick is greater or equal than current message id.
				MsgID:     adaptor.MustGetMQWrapperIDFromMessage(createCollectionMsg.LastConfirmedMessageID()).Serialize(),
				MsgGroup:  "", // Not important any more.
				Timestamp: createCollectionMsg.TimeTick(),
			},
		},
		schema,
		func(t syncmgr.Task, err error) {
			if err != nil || t == nil {
				return
			}
			if tt, ok := t.(*syncmgr.SyncTask); ok {
				insertLogs, _, _, _ := tt.Binlogs()
				resource.Resource().SegmentAssignStatsManager().UpdateOnSync(tt.SegmentID(), stats.SyncOperationMetrics{
					BinLogCounterIncr:     1,
					BinLogFileCounterIncr: uint64(len(insertLogs)),
				})
			}
		},
		nil,
	)
	impl.addNewDataSyncService(createCollectionMsg, msgChan, ds)
}

// WhenDropCollection handles the drop collection message.
func (impl *flusherComponents) WhenDropCollection(vchannel string) {
	// flowgraph is removed by data sync service it self.
	if ds, ok := impl.dataServices[vchannel]; ok {
		ds.Close()
		delete(impl.dataServices, vchannel)
		impl.logger.Info("drop data sync service", zap.String("vchannel", vchannel))
	}
	impl.checkpointManager.DropVChannel(vchannel)
}

// HandleMessage handles the plain message.
func (impl *flusherComponents) HandleMessage(ctx context.Context, msg message.ImmutableMessage) error {
	vchannel := msg.VChannel()
	if vchannel == "" {
		return impl.broadcastToAllDataSyncService(ctx, msg)
	}
	if _, ok := impl.dataServices[vchannel]; !ok {
		return nil
	}
	return impl.dataServices[vchannel].HandleMessage(ctx, msg)
}

// broadcastToAllDataSyncService broadcasts the message to all data sync services.
func (impl *flusherComponents) broadcastToAllDataSyncService(ctx context.Context, msg message.ImmutableMessage) error {
	for _, ds := range impl.dataServices {
		if err := ds.HandleMessage(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

// addNewDataSyncService adds a new data sync service to the components when new collection is created.
func (impl *flusherComponents) addNewDataSyncService(
	createCollectionMsg message.ImmutableCreateCollectionMessageV1,
	input chan<- *msgstream.MsgPack,
	ds *pipeline.DataSyncService,
) {
	impl.checkpointManager.AddVChannel(createCollectionMsg.VChannel(), createCollectionMsg.LastConfirmedMessageID())
	newDS := newDataSyncServiceWrapper(input, ds)
	newDS.Start()
	impl.dataServices[createCollectionMsg.VChannel()] = newDS
	impl.logger.Info("create data sync service done", zap.String("vchannel", createCollectionMsg.VChannel()))
}

// Close release all the resources of components.
func (impl *flusherComponents) Close() {
	for vchannel, ds := range impl.dataServices {
		ds.Close()
		impl.logger.Info("data sync service closed for flusher closing", zap.String("vchannel", vchannel))
	}
	impl.cpUpdater.Close()
	impl.checkpointManager.Close()
}

// recover recover the components of the flusher.
func (impl *flusherComponents) recover(ctx context.Context, recoverInfos map[string]*datapb.GetChannelRecoveryInfoResponse) error {
	futures := make(map[string]*conc.Future[interface{}], len(recoverInfos))
	for vchannel, recoverInfo := range recoverInfos {
		recoverInfo := recoverInfo
		future := GetExecPool().Submit(func() (interface{}, error) {
			return impl.buildDataSyncServiceWithRetry(ctx, recoverInfo)
		})
		futures[vchannel] = future
	}
	dataServices := make(map[string]*dataSyncServiceWrapper, len(futures))
	for vchannel, future := range futures {
		ds, err := future.Await()
		if err != nil {
			return err
		}
		dataServices[vchannel] = ds.(*dataSyncServiceWrapper)
	}
	impl.dataServices = dataServices
	for vchannel, ds := range dataServices {
		ds.Start()
		impl.logger.Info("start data sync service when recovering", zap.String("vchannel", vchannel))
	}
	return nil
}

// buildDataSyncServiceWithRetry builds the data sync service with retry.
func (impl *flusherComponents) buildDataSyncServiceWithRetry(ctx context.Context, recoverInfo *datapb.GetChannelRecoveryInfoResponse) (*dataSyncServiceWrapper, error) {
	var ds *dataSyncServiceWrapper
	err := retry.Do(ctx, func() error {
		var err error
		ds, err = impl.buildDataSyncService(ctx, recoverInfo)
		return err
	}, retry.AttemptAlways())
	if err != nil {
		return nil, err
	}
	return ds, nil
}

// buildDataSyncService builds the data sync service with given recovery info.
func (impl *flusherComponents) buildDataSyncService(ctx context.Context, recoverInfo *datapb.GetChannelRecoveryInfoResponse) (*dataSyncServiceWrapper, error) {
	// Build and add pipeline.
	input := make(chan *msgstream.MsgPack, 10)
	ds, err := pipeline.NewStreamingNodeDataSyncService(ctx,
		&util.PipelineParams{
			Ctx:                context.Background(),
			Broker:             impl.broker,
			SyncMgr:            resource.Resource().SyncManager(),
			ChunkManager:       impl.chunkManager,
			WriteBufferManager: resource.Resource().WriteBufferManager(),
			CheckpointUpdater:  impl.cpUpdater,
			Allocator:          idalloc.NewMAllocator(resource.Resource().IDAllocator()),
			MsgHandler:         newMsgHandler(resource.Resource().WriteBufferManager()),
		},
		&datapb.ChannelWatchInfo{Vchan: recoverInfo.GetInfo(), Schema: recoverInfo.GetSchema()},
		input,
		func(t syncmgr.Task, err error) {
			if err != nil || t == nil {
				return
			}
			if tt, ok := t.(*syncmgr.SyncTask); ok {
				insertLogs, _, _, _ := tt.Binlogs()
				resource.Resource().SegmentAssignStatsManager().UpdateOnSync(tt.SegmentID(), stats.SyncOperationMetrics{
					BinLogCounterIncr:     1,
					BinLogFileCounterIncr: uint64(len(insertLogs)),
				})
			}
		},
		nil,
	)
	if err != nil {
		return nil, err
	}
	return newDataSyncServiceWrapper(input, ds), nil
}
