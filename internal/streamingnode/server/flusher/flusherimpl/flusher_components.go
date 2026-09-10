package flusherimpl

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/pipeline"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// flusherComponents is the components of the flusher.
type flusherComponents struct {
	wal          wal.WAL
	broker       broker.Broker
	cpUpdater    *util.ChannelCheckpointUpdater
	chunkManager storage.ChunkManager
	dataServices map[string]*dataSyncServiceWrapper
	// fenced holds, per source vchannel, the largest SplitShard fence tick
	// (T_switch) seen for it: the time tick at or after which its data sync
	// service has drained every message the fence-time dd_node sealed and
	// may close itself. A same-task re-fence carries a larger tick than the
	// one before it, so the drain point is the largest one ever observed,
	// not the first. Seeded from the recovery snapshot on restart (see
	// fencedTicksFromSnapshot) so a source fenced before a restart still
	// closes once its checkpoint catches up.
	fenced                     map[string]uint64
	logger                     *mlog.Logger
	recoveryCheckPointTimeTick uint64 // The time tick of the recovery storage.
	rs                         recovery.RecoveryStorage
}

// fencedTicksFromSnapshot collects the fence tick (VChannelMeta.SplitTimeTick,
// i.e. T_switch) of every SPLITTED vchannel recorded in the recovery
// snapshot. It seeds flusherComponents.fenced on restart: a source vchannel
// that was fenced before the restart, and whose data sync service survived
// it (recovered because it was not yet drained), must still close itself
// once its checkpoint passes T_switch.
func fencedTicksFromSnapshot(vchannels map[string]*streamingpb.VChannelMeta) map[string]uint64 {
	fenced := make(map[string]uint64, len(vchannels))
	for vchannel, meta := range vchannels {
		if meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED {
			fenced[vchannel] = meta.GetSplitTimeTick()
		}
	}
	return fenced
}

// WhenCreateCollection handles the create collection message.
func (impl *flusherComponents) WhenCreateCollection(ctx context.Context, createCollectionMsg message.ImmutableCreateCollectionMessageV1) error {
	return impl.spawnGenesisDataSyncService(ctx, createCollectionMsg, createCollectionMsg.Header().GetCollectionId())
}

// WhenCreateVChannel handles the target replica of a SplitShard broadcast,
// the genesis of a shard split target vchannel: it spawns a data sync service
// for the new vchannel exactly as a create collection genesis does.
func (impl *flusherComponents) WhenCreateVChannel(ctx context.Context, splitShardMsg message.ImmutableSplitShardMessageV2) error {
	return impl.spawnGenesisDataSyncService(ctx, splitShardMsg, splitShardMsg.Header().GetCollectionId())
}

// spawnGenesisDataSyncService spawns the data sync service of a freshly created
// vchannel from its genesis message (create collection or create vchannel).
func (impl *flusherComponents) spawnGenesisDataSyncService(ctx context.Context, genesisMsg message.ImmutableMessage, collectionID int64) error {
	// because we need to get the schema from the recovery storage, we need to observe the message at recovery storage first.
	if err := impl.rs.ObserveMessage(ctx, genesisMsg); err != nil {
		return err
	}
	if _, ok := impl.dataServices[genesisMsg.VChannel()]; ok {
		impl.logger.Info(ctx, "the data sync service of current vchannel is built, skip it", mlog.FieldVChannel(genesisMsg.VChannel()))
		// May repeated consumed, so we ignore the message.
		return nil
	}
	if genesisMsg.TimeTick() <= impl.recoveryCheckPointTimeTick {
		// It should already be recovered from the recovery storage.
		// if it's not in recovery storage, it means the genesis is already dropped.
		// so we can skip it.
		impl.logger.Info(ctx, "the vchannel genesis message is older than the recovery checkpoint, skip it",
			mlog.FieldVChannel(genesisMsg.VChannel()),
			mlog.Uint64("timeTick", genesisMsg.TimeTick()),
			mlog.Uint64("recoveryCheckPointTimeTick", impl.recoveryCheckPointTimeTick))
		return nil
	}

	msgChan := make(chan *msgstream.MsgPack, 10)
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
			SchemaManager:      newVersionedSchemaManager(genesisMsg.VChannel(), impl.rs),
			FlushSourceModeNotifier: resource.Resource().SegmentStatsManager().
				UpdateFlushSourceMode,
		},
		msgChan,
		&datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  genesisMsg.VChannel(),
			SeekPosition: &msgpb.MsgPosition{
				ChannelName: genesisMsg.VChannel(),
				// from the last confirmed message id, you can read all messages which timetick is greater or equal than current message id.
				MsgID:     adaptor.MustGetMQWrapperIDFromMessage(genesisMsg.LastConfirmedMessageID()).Serialize(),
				MsgGroup:  "", // Not important any more.
				Timestamp: genesisMsg.TimeTick(),
				WALName:   commonpb.WALName(genesisMsg.WALName()),
			},
		},
		func(t syncmgr.Task, err error) {
			if err != nil || t == nil {
				return
			}
			if tt, ok := t.(*syncmgr.SyncTask); ok {
				insertLogs, _, _, _ := tt.Binlogs()
				resource.Resource().SegmentStatsManager().UpdateOnSync(tt.SegmentID(), stats.SyncOperationMetrics{
					BinLogCounterIncr:     1,
					BinLogFileCounterIncr: uint64(len(insertLogs)),
				})
			}
		},
		nil,
	)
	impl.addNewDataSyncService(ctx, genesisMsg, msgChan, ds)
	return nil
}

// WhenDropCollection handles the drop collection message.
func (impl *flusherComponents) WhenDropCollection(ctx context.Context, vchannel string) {
	// flowgraph is removed by data sync service it self.
	if ds, ok := impl.dataServices[vchannel]; ok {
		ds.Close()
		delete(impl.dataServices, vchannel)
		impl.logger.Info(ctx, "drop data sync service", mlog.FieldVChannel(vchannel))
	}
}

// RecordFence records the fence tick (T_switch) of a SplitShard source
// replica dispatched to vchannel, before the replica is forwarded to its
// data sync service. A same-task re-fence can carry a larger tick than a
// prior one -- the largest tick ever observed is kept, since the data sync
// service isn't drained until its checkpoint passes every fence it was
// handed, not merely the first.
func (impl *flusherComponents) RecordFence(vchannel string, tick uint64) {
	if impl.fenced == nil {
		impl.fenced = make(map[string]uint64)
	}
	if tick > impl.fenced[vchannel] {
		impl.fenced[vchannel] = tick
	}
}

// CloseIfDrained closes and removes the data sync service of vchannel once
// its checkpoint has caught up with the fence tick recorded for it, i.e.
// once it has synced every message the fence-time dd_node sealed. It is a
// no-op when vchannel was never fenced, or its checkpoint has not reached
// the fence tick yet, and idempotent afterwards: once the data sync service
// is removed, later calls (including the same or a smaller checkpoint) find
// nothing left to close and log nothing.
//
// This is deliberately not reached from the AlterCollection replica that
// retires the vchannel -- on a secondary that replica can arrive before the
// fenced segments are flushed, and closing there would drop unflushed data.
// The data sync service closes itself only once its own checkpoint proves
// it is drained.
func (impl *flusherComponents) CloseIfDrained(ctx context.Context, vchannel string, timestamp uint64) {
	fenceTick := impl.fenced[vchannel]
	if fenceTick == 0 || timestamp < fenceTick {
		return
	}
	ds, ok := impl.dataServices[vchannel]
	if !ok {
		return
	}
	ds.Close()
	delete(impl.dataServices, vchannel)
	delete(impl.fenced, vchannel)
	impl.logger.Info(ctx, "closed the fenced source's data sync service once its checkpoint passed the fence",
		mlog.FieldVChannel(vchannel), mlog.Uint64("fenceTimeTick", fenceTick), mlog.Uint64("checkpointTimeTick", timestamp))
}

// HandleMessage handles the plain message.
func (impl *flusherComponents) HandleMessage(ctx context.Context, msg message.ImmutableMessage) error {
	// AlterReplicateConfig is a coordinator-only message, skip it in flusher.
	if msg.MessageType() == message.MessageTypeAlterReplicateConfig {
		return nil
	}
	vchannel := msg.VChannel()
	if vchannel == "" || msg.IsPChannelLevel() {
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
	ctx context.Context,
	genesisMsg message.ImmutableMessage,
	input chan<- *msgstream.MsgPack,
	ds *pipeline.DataSyncService,
) {
	newDS := newDataSyncServiceWrapper(genesisMsg.VChannel(), input, ds, genesisMsg.TimeTick())
	newDS.Start()
	impl.dataServices[genesisMsg.VChannel()] = newDS
	impl.logger.Info(ctx, "create data sync service done", mlog.FieldVChannel(genesisMsg.VChannel()))
}

// Close release all the resources of components.
func (impl *flusherComponents) Close() {
	for vchannel, ds := range impl.dataServices {
		ds.Close()
		impl.logger.Info(context.TODO(), "data sync service closed for flusher closing", mlog.FieldVChannel(vchannel))
	}
	impl.cpUpdater.Close()
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
	var lastErr error
	for vchannel, future := range futures {
		ds, err := future.Await()
		if err != nil {
			lastErr = err
			continue
		}
		dataServices[vchannel] = ds.(*dataSyncServiceWrapper)
	}
	if lastErr != nil {
		// release all the data sync services if operation is canceled.
		// otherwise, the write buffer will leak.
		for _, ds := range dataServices {
			ds.Close()
		}
		impl.logger.Warn(ctx, "failed to build data sync service, may be canceled when recovering", mlog.Err(lastErr))
		return lastErr
	}
	impl.dataServices = dataServices
	for vchannel, ds := range dataServices {
		ds.Start()
		impl.logger.Info(ctx, "start data sync service when recovering", mlog.FieldVChannel(vchannel))
	}
	return nil
}

// buildDataSyncServiceWithRetry builds the data sync service with retry.
func (impl *flusherComponents) buildDataSyncServiceWithRetry(ctx context.Context, recoverInfo *datapb.GetChannelRecoveryInfoResponse) (*dataSyncServiceWrapper, error) {
	// Flush all the growing segment that is not created by streaming.
	for _, segment := range recoverInfo.SegmentsNotCreatedByStreaming {
		logger := impl.logger.With(
			mlog.FieldCollectionID(segment.CollectionId),
			mlog.FieldVChannel(recoverInfo.GetInfo().GetChannelName()),
			mlog.FieldPartitionID(segment.PartitionId),
			mlog.FieldSegmentID(segment.SegmentId),
		)
		if err := retry.Do(ctx, func() error {
			msg := message.NewFlushMessageBuilderV2().
				WithVChannel(recoverInfo.GetInfo().GetChannelName()).
				WithHeader(&message.FlushMessageHeader{
					CollectionId: segment.CollectionId,
					PartitionId:  segment.PartitionId,
					SegmentId:    segment.SegmentId,
				}).
				WithBody(&message.FlushMessageBody{}).MustBuildMutable()
			appendResult, err := impl.wal.Append(utility.WithFlushFromOldArch(ctx), msg)
			if err != nil {
				logger.Warn(ctx, "fail to append flush message for segments that not created by streaming service into wal", mlog.Err(err))
				return err
			}
			logger.Info(ctx, "append flush message for segments that not created by streaming service into wal", mlog.Stringer("msgID", appendResult.MessageID), mlog.Uint64("timeTick", appendResult.TimeTick))
			return nil
		}, retry.AttemptAlways(), retry.RetryErr(func(error) bool { return true })); err != nil {
			return nil, err
		}
	}

	var ds *dataSyncServiceWrapper
	err := retry.Do(ctx, func() error {
		var err error
		ds, err = impl.buildDataSyncService(ctx, recoverInfo)
		return err
	}, retry.AttemptAlways(), retry.RetryErr(func(error) bool { return true }))
	if err != nil {
		return nil, err
	}
	return ds, nil
}

// buildDataSyncService builds the data sync service with given recovery info.
func (impl *flusherComponents) buildDataSyncService(ctx context.Context, recoverInfo *datapb.GetChannelRecoveryInfoResponse) (*dataSyncServiceWrapper, error) {
	// Build and add pipeline.
	input := make(chan *msgstream.MsgPack, 10)
	schemaManager := newVersionedSchemaManager(recoverInfo.GetInfo().GetChannelName(), impl.rs)
	schema := schemaManager.GetSchema(0)
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
			SchemaManager:      newVersionedSchemaManager(recoverInfo.GetInfo().GetChannelName(), impl.rs),
			FlushSourceModeNotifier: resource.Resource().SegmentStatsManager().
				UpdateFlushSourceMode,
		},
		&datapb.ChannelWatchInfo{Vchan: recoverInfo.GetInfo(), Schema: schema},
		input,
		func(t syncmgr.Task, err error) {
			if err != nil || t == nil {
				return
			}
			if tt, ok := t.(*syncmgr.SyncTask); ok {
				insertLogs, _, _, _ := tt.Binlogs()
				resource.Resource().SegmentStatsManager().UpdateOnSync(tt.SegmentID(), stats.SyncOperationMetrics{
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
	return newDataSyncServiceWrapper(recoverInfo.Info.ChannelName, input, ds, recoverInfo.Info.GetSeekPosition().GetTimestamp()), nil
}
