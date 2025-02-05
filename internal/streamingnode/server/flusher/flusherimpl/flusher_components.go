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
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/stats"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

// buildFlusherComponents builds the components of the flusher.
func (impl *WALFlusherImpl) buildFlusherComponents(ctx context.Context, l wal.WAL) (*flusherComponents, error) {
	// Get all existed vchannels of the pchannel.
	vchannels, err := impl.getVchannels(ctx, l.Channel().Name)
	if err != nil {
		impl.logger.Warn("get vchannels failed", zap.Error(err))
		return nil, err
	}
	impl.logger.Info("fetch vchannel done", zap.Int("vchannelNum", len(vchannels)))

	// Get all the recovery info of the recoverable vchannels.
	recoverInfos, err := impl.getRecoveryInfos(ctx, vchannels)
	if err != nil {
		impl.logger.Warn("get recovery info failed", zap.Error(err))
		return nil, err
	}
	impl.logger.Info("fetch recovery info done", zap.Int("recoveryInfoNum", len(recoverInfos)))

	// build up components
	dc, err := resource.Resource().DataCoordClient().GetWithContext(ctx)
	if err != nil {
		impl.logger.Warn("flusher recovery is canceled before data coord client ready", zap.Error(err))
		return nil, err
	}
	impl.logger.Info("data coord client ready")

	broker := broker.NewCoordBroker(dc, paramtable.GetNodeID())
	chunkManager := resource.Resource().ChunkManager()
	syncMgr := syncmgr.NewSyncManager(chunkManager)
	wbMgr := writebuffer.NewManager(syncMgr)
	wbMgr.Start()
	cpUpdater := util.NewChannelCheckpointUpdater(broker)
	go cpUpdater.Start()

	fc := &flusherComponents{
		wal:          l,
		broker:       broker,
		fgMgr:        pipeline.NewFlowgraphManager(),
		syncMgr:      syncMgr,
		wbMgr:        wbMgr,
		cpUpdater:    cpUpdater,
		chunkManager: chunkManager,
		dataServices: make(map[string]*dataService),
		logger:       impl.logger,
	}
	impl.logger.Info("flusher components intiailizing done")
	if err := fc.recover(ctx, recoverInfos); err != nil {
		impl.logger.Warn("flusher recovery is canceled before recovery done, recycle the resource", zap.Error(err))
		fc.Close()
		impl.logger.Info("flusher recycle the resource done")
		return nil, err
	}
	impl.logger.Info("flusher recovery done")
	return fc, nil
}

type dataService struct {
	input          chan<- *msgstream.MsgPack
	handler        *adaptor.BaseMsgPackAdaptorHandler
	ds             *pipeline.DataSyncService
	startMessageID message.MessageID
}

func (ds *dataService) HandleMessage(ctx context.Context, msg message.ImmutableMessage) error {
	ds.handler.GenerateMsgPack(msg)
	for ds.handler.PendingMsgPack.Len() > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ds.input <- ds.handler.PendingMsgPack.Next():
			ds.handler.PendingMsgPack.UnsafeAdvance()
		}
	}
	return nil
}

func (ds *dataService) Close() {
	close(ds.input)
	ds.ds.GracefullyClose()
}

// flusherComponents is the components of the flusher.
type flusherComponents struct {
	wal          wal.WAL
	broker       broker.Broker
	fgMgr        pipeline.FlowgraphManager
	syncMgr      syncmgr.SyncManager
	wbMgr        writebuffer.BufferManager
	cpUpdater    *util.ChannelCheckpointUpdater
	chunkManager storage.ChunkManager
	dataServices map[string]*dataService
	logger       *log.MLogger
}

// GetMinimumStartMessage gets the minimum start message of all the data services.
func (impl *flusherComponents) GetMinimumStartMessage() message.MessageID {
	var startMessageID message.MessageID
	for _, ds := range impl.dataServices {
		if startMessageID == nil || ds.startMessageID.LT(startMessageID) {
			startMessageID = ds.startMessageID
		}
	}
	return startMessageID
}

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
			SyncMgr:            impl.syncMgr,
			ChunkManager:       impl.chunkManager,
			WriteBufferManager: impl.wbMgr,
			CheckpointUpdater:  impl.cpUpdater,
			Allocator:          idalloc.NewMAllocator(resource.Resource().IDAllocator()),
			MsgHandler:         newMsgHandler(impl.wbMgr),
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
	ds.Start()
	impl.logger.Info("create data sync service done", zap.String("vchannel", createCollectionMsg.VChannel()))
}

// WhenDropCollection handles the drop collection message.
func (impl *flusherComponents) WhenDropCollection(vchannel string) {
	// flowgraph is removed by data sync service it self.
	if ds, ok := impl.dataServices[vchannel]; ok {
		ds.Close()
		delete(impl.dataServices, vchannel)
		impl.logger.Info("drop data sync service", zap.String("vchannel", vchannel))
	}
}

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

func (impl *flusherComponents) broadcastToAllDataSyncService(ctx context.Context, msg message.ImmutableMessage) error {
	for _, ds := range impl.dataServices {
		if err := ds.HandleMessage(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

func (impl *flusherComponents) addNewDataSyncService(
	createCollectionMsg message.ImmutableCreateCollectionMessageV1,
	input chan<- *msgstream.MsgPack,
	ds *pipeline.DataSyncService,
) {
	impl.dataServices[createCollectionMsg.VChannel()] = &dataService{
		input:          input,
		handler:        adaptor.NewBaseMsgPackAdaptorHandler(),
		ds:             ds,
		startMessageID: createCollectionMsg.LastConfirmedMessageID(),
	}
	impl.fgMgr.AddFlowgraph(ds)
}

// Close release all the resources of components.
func (impl *flusherComponents) Close() {
	impl.fgMgr.ClearFlowgraphs()
	impl.wbMgr.Stop()
	impl.cpUpdater.Close()
	for _, ds := range impl.dataServices {
		ds.Close()
	}
	impl.syncMgr.Close()
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
	dataServices := make(map[string]*dataService, len(futures))
	var firstErr error
	for vchannel, future := range futures {
		ds, err := future.Await()
		if err == nil {
			dataServices[vchannel] = ds.(*dataService)
			continue
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	impl.dataServices = dataServices
	return firstErr
}

func (impl *flusherComponents) buildDataSyncServiceWithRetry(ctx context.Context, recoverInfo *datapb.GetChannelRecoveryInfoResponse) (*dataService, error) {
	var ds *dataService
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

func (impl *flusherComponents) buildDataSyncService(ctx context.Context, recoverInfo *datapb.GetChannelRecoveryInfoResponse) (*dataService, error) {
	// Build and add pipeline.
	input := make(chan *msgstream.MsgPack, 10)
	ds, err := pipeline.NewStreamingNodeDataSyncService(ctx,
		&util.PipelineParams{
			Ctx:                context.Background(),
			Broker:             impl.broker,
			SyncMgr:            impl.syncMgr,
			ChunkManager:       impl.chunkManager,
			WriteBufferManager: impl.wbMgr,
			CheckpointUpdater:  impl.cpUpdater,
			Allocator:          idalloc.NewMAllocator(resource.Resource().IDAllocator()),
			MsgHandler:         newMsgHandler(impl.wbMgr),
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
	return &dataService{
		input:          input,
		handler:        adaptor.NewBaseMsgPackAdaptorHandler(),
		ds:             ds,
		startMessageID: adaptor.MustGetMessageIDFromMQWrapperIDBytes(impl.wal.WALName(), recoverInfo.GetInfo().GetSeekPosition().GetMsgID()),
	}, nil
}
