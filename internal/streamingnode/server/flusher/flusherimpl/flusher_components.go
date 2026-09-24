package flusherimpl

import (
	"context"
	"sync"

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
	// mu guards dataServices and fenced. Every mutation of either map, and
	// every Close of a data sync service, happens on the flusher's own
	// dispatch goroutine (WhenCreateCollection, WhenCreateVChannel,
	// WhenDropCollection, RecordFence, HandleMessage and the drained-source
	// close it runs first), which is also the only goroutine that sends into a
	// service's input channel -- so a send can never meet a closed channel.
	// The lock exists for the one reader on another goroutine:
	// ObserveAckedCheckpoint, the checkpoint-updater callback, which only
	// looks a service up to record its acked checkpoint and never closes or
	// removes anything.
	mu           sync.Mutex
	wal          wal.WAL
	broker       broker.Broker
	cpUpdater    *util.ChannelCheckpointUpdater
	chunkManager storage.ChunkManager
	dataServices map[string]*dataSyncServiceWrapper
	// fenced holds, per source vchannel, the close gate of its data sync
	// service: the own time tick of the first SplitShard source record
	// dispatched to it. Once the service's acked checkpoint reaches it, the
	// dd_node has processed that record -- which seals every growing segment
	// of the vchannel -- and the dispatch goroutine closes the service
	// (closeDrainedFencedSources). A same-task re-fence dispatched later
	// seals nothing new, so it does not move the gate.
	//
	// The gate is the seal record's tick, deliberately NOT T_switch. They
	// differ when the first fence append failed without persisting: the
	// shard manager keeps the fence at the first attempt's tick (T_switch),
	// and the first record in the WAL is the re-drive at a later tick. Gating
	// on T_switch would close the service as soon as time ticks between the
	// two carried its checkpoint past T_switch, before the re-drive -- the only
	// seal record -- reached the dd_node, leaving the fenced segments growing
	// forever. Seeded from the recovery snapshot on restart (see
	// fencedTicksFromSnapshot) so a source fenced before a restart still
	// closes once its checkpoint catches up.
	fenced map[string]uint64
	// persistedCheckpoint reads the channel checkpoint DataCoord holds for a
	// vchannel. It is asked only for an ack that would open a fence gate (see
	// ObserveCheckpointAck). Nil -- a component built without a DataCoord view,
	// as unit tests do -- takes the ack at its word.
	persistedCheckpoint        persistedCheckpointFunc
	logger                     *mlog.Logger
	recoveryCheckPointTimeTick uint64 // The time tick of the recovery storage.
	rs                         recovery.RecoveryStorage
}

// fencedTicksFromSnapshot collects the close gate (see flusherComponents.fenced)
// of every SPLITTED vchannel recorded in the recovery snapshot. It seeds
// flusherComponents.fenced on restart: a source vchannel that was fenced
// before the restart, and whose data sync service survived it (recovered
// because it was not yet drained), must still close itself once drained.
//
// The snapshot does not name the seal record's tick directly; the gate is
// reseeded by recovery.SplitFenceGate, the function the recovery storage also
// judges a source drained by (vchannelRecoveryInfo.DrainedPastFence). The two
// are not computed from the same meta, though: the recovery storage reseeds
// from the catalog meta before the replay, this one from the post-replay
// snapshot, whose CheckpointTimeTick a replayed source-side DDL record may
// have bumped. This gate is therefore at or after the recovery storage's,
// which is the safe direction: the recovery storage never waits on a source
// longer than the service stays open, and a later close gate only keeps a
// draining service open a little longer.
func fencedTicksFromSnapshot(vchannels map[string]*streamingpb.VChannelMeta) map[string]uint64 {
	fenced := make(map[string]uint64, len(vchannels))
	for vchannel, meta := range vchannels {
		if gate := recovery.SplitFenceGate(meta); gate != 0 {
			fenced[vchannel] = gate
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
	if impl.hasDataSyncService(genesisMsg.VChannel()) {
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
			SeekPosition: genesisSeekPosition(genesisMsg.VChannel(), genesisMsg.LastConfirmedMessageID(), genesisMsg.TimeTick()),
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

// genesisSeekPosition is the seek position of a vchannel's data sync service
// spawned from its genesis message: the genesis's last confirmed message id and
// its time tick. The recovery of a shard split target that datacoord has no
// position for yet rebuilds the same position (splitTargetGenesisRecoveryInfo).
func genesisSeekPosition(vchannel string, lastConfirmed message.MessageID, timeTick uint64) *msgpb.MsgPosition {
	return &msgpb.MsgPosition{
		ChannelName: vchannel,
		// from the last confirmed message id, you can read all messages which timetick is greater or equal than current message id.
		MsgID:     adaptor.MustGetMQWrapperIDFromMessage(lastConfirmed).Serialize(),
		MsgGroup:  "", // Not important any more.
		Timestamp: timeTick,
		WALName:   commonpb.WALName(lastConfirmed.WALName()),
	}
}

// WhenDropCollection handles the drop collection message.
func (impl *flusherComponents) WhenDropCollection(ctx context.Context, vchannel string) {
	// Runs on the dispatch goroutine, like every other removal and close. The
	// close itself runs outside the lock: flowgraph is removed by data sync
	// service it self, and it can block draining, which must not hold up the
	// checkpoint-updater callback's lookup.
	impl.mu.Lock()
	ds, ok := impl.dataServices[vchannel]
	if ok {
		delete(impl.dataServices, vchannel)
	}
	// The fence tick is dropped with the vchannel itself, whether or not a
	// data sync service is still around: once the collection is gone the
	// recorded T_switch has no consumer left, and keeping it would let a
	// later vchannel of the same name (only reachable by a replay of the
	// same collection id) be closed by a stale fence the moment it is
	// spawned.
	delete(impl.fenced, vchannel)
	impl.mu.Unlock()
	if !ok {
		return
	}
	ds.Close()
	impl.logger.Info(ctx, "drop data sync service", mlog.FieldVChannel(vchannel))
}

// RecordFence records the close gate of vchannel's data sync service: the own
// tick of a SplitShard source record dispatched to it, before the record is
// forwarded (see flusherComponents.fenced for why the record's tick and not
// T_switch). Only the FIRST record's tick is kept: a same-task re-fence seals
// nothing the first record did not, and waiting for its later tick could keep
// the source's data sync service open past the point it already drained. A
// re-fence that arrives after the service was closed records a tick with no
// service behind it, which the next closeDrainedFencedSources pass drops.
func (impl *flusherComponents) RecordFence(vchannel string, tick uint64) {
	impl.mu.Lock()
	defer impl.mu.Unlock()
	if impl.fenced == nil {
		impl.fenced = make(map[string]uint64)
	}
	if recorded, ok := impl.fenced[vchannel]; ok && recorded != 0 {
		return
	}
	impl.fenced[vchannel] = tick
}

// ObserveAckedCheckpoint records that DataCoord has acked vchannel's channel
// checkpoint at timestamp. It is the checkpoint-updater callback's only effect
// on the flusher components and it only records state: it never closes or
// removes a data sync service. Deciding that a fenced source has drained and
// closing it is left to the dispatch goroutine (closeDrainedFencedSources),
// because that goroutine is the one sending into the service's input channel;
// closing it from here raced a broadcast that had already snapshotted the
// service and panicked with a send on a closed channel.
//
// An ack for a vchannel without a data sync service (already closed, dropped,
// or an RPC that completed after the teardown) is ignored.
func (impl *flusherComponents) ObserveAckedCheckpoint(vchannel string, timestamp uint64) {
	if ds := impl.getDataSyncService(vchannel); ds != nil {
		ds.ObserveAckedCheckpoint(timestamp)
	}
}

// persistedCheckpointFunc returns the time tick of the channel checkpoint
// DataCoord holds for vchannel.
type persistedCheckpointFunc func(ctx context.Context, vchannel string) (uint64, error)

// ObserveCheckpointAck handles DataCoord's Success answer to the update of
// vchannel's channel checkpoint to requested. It runs on the checkpoint
// updater's goroutine.
//
// Success does not say that requested is what DataCoord stored: for a
// collection with TEXT fields the stored position is clamped to the earliest
// growing segment of the channel (meta.GetMinGrowingSegmentCheckpoint), and
// the answer carries no position. The write buffer pins its checkpoint under
// every sync task that carries data, but a sealed segment whose buffer is
// already empty gets a flush-only task with no start position, which pins
// nothing: the checkpoint can pass the fence while that segment is still
// Growing in DataCoord, so the update that reaches the fence gate is clamped
// below it. Closing the source on such an ack ends its checkpoint reports, and
// DataCoord's stored checkpoint then stays before T_switch: the drain predicate
// and the flush state of the collection never turn true.
//
// So an ack that would open a fence gate is replaced by the checkpoint
// DataCoord actually holds, which costs one RPC per split source when nothing
// was clamped. While DataCoord's checkpoint is short of the gate the service
// stays open, keeps reporting, and the next ack asks again. A failed read
// records nothing, for the same reason.
func (impl *flusherComponents) ObserveCheckpointAck(ctx context.Context, vchannel string, requested uint64) {
	impl.mu.Lock()
	gate := impl.fenced[vchannel]
	impl.mu.Unlock()
	if gate == 0 || requested < gate || impl.persistedCheckpoint == nil {
		impl.ObserveAckedCheckpoint(vchannel, requested)
		return
	}
	persisted, err := impl.persistedCheckpoint(ctx, vchannel)
	if err != nil {
		impl.logger.Warn(ctx, "failed to read the fenced source's channel checkpoint from datacoord, keep its data sync service open until the next ack",
			mlog.FieldVChannel(vchannel),
			mlog.Uint64("fenceTimeTick", gate),
			mlog.Uint64("requestedTimeTick", requested),
			mlog.Err(err))
		return
	}
	if persisted < gate {
		impl.logger.Warn(ctx, "datacoord holds the fenced source's channel checkpoint before the fence although it acked one past it, keep its data sync service open",
			mlog.FieldVChannel(vchannel),
			mlog.Uint64("fenceTimeTick", gate),
			mlog.Uint64("requestedTimeTick", requested),
			mlog.Uint64("persistedTimeTick", persisted))
	}
	impl.ObserveAckedCheckpoint(vchannel, min(requested, persisted))
}

// drainedFencedSource is a fenced source whose data sync service has been
// removed from the components and is about to be closed.
type drainedFencedSource struct {
	vchannel          string
	fenceTimeTick     uint64
	checkpointAckedAt uint64
	ds                *dataSyncServiceWrapper
}

// closeDrainedFencedSources closes and removes the data sync service of every
// fenced source whose acked checkpoint has caught up with its fence tick, i.e.
// that has synced every message the fence-time dd_node sealed. A fenced
// vchannel that has no data sync service (never spawned here, or already
// closed) has nothing left to close and its fence entry is dropped, which
// keeps this scan bounded by the sources still draining.
//
// It MUST run on the flusher's dispatch goroutine, before a message is handed
// to any data sync service: that goroutine is the only sender into a
// service's input channel, so closing here can never race a send, and a
// service closed here is already gone from dataServices when the message
// that triggered the check is dispatched. Close blocks the dispatch goroutine
// while the flow graph drains, exactly as the drop-collection teardown does.
//
// The acked checkpoint compared here is one DataCoord holds, not merely one
// it answered Success to (ObserveCheckpointAck).
//
// The trigger is the acked checkpoint (ObserveAckedCheckpoint) rather than
// the AlterCollection replica that retires the vchannel: on a secondary that
// replica can arrive before the fenced segments are flushed, and closing
// there would drop unflushed data. Time ticks are broadcast to every data
// sync service and keep being dispatched while the source drains, so the
// check runs again after every checkpoint ack.
func (impl *flusherComponents) closeDrainedFencedSources(ctx context.Context) {
	impl.mu.Lock()
	if len(impl.fenced) == 0 {
		impl.mu.Unlock()
		return
	}
	var drained []drainedFencedSource
	for vchannel, fenceTick := range impl.fenced {
		ds, ok := impl.dataServices[vchannel]
		if !ok {
			delete(impl.fenced, vchannel)
			continue
		}
		acked := ds.AckedCheckpoint()
		if fenceTick == 0 || acked < fenceTick {
			continue
		}
		delete(impl.dataServices, vchannel)
		delete(impl.fenced, vchannel)
		drained = append(drained, drainedFencedSource{vchannel: vchannel, fenceTimeTick: fenceTick, checkpointAckedAt: acked, ds: ds})
	}
	impl.mu.Unlock()

	for _, source := range drained {
		source.ds.Close()
		impl.logger.Info(ctx, "closed the fenced source's data sync service once its checkpoint passed the fence",
			mlog.FieldVChannel(source.vchannel),
			mlog.Uint64("fenceTimeTick", source.fenceTimeTick),
			mlog.Uint64("checkpointTimeTick", source.checkpointAckedAt))
	}
}

// HandleMessage handles the plain message.
func (impl *flusherComponents) HandleMessage(ctx context.Context, msg message.ImmutableMessage) error {
	// AlterReplicateConfig is a coordinator-only message, skip it in flusher.
	if msg.MessageType() == message.MessageTypeAlterReplicateConfig {
		return nil
	}
	// Close every drained fenced source before this message is handed to any
	// data sync service; see closeDrainedFencedSources for why it has to be
	// here, on the dispatch goroutine.
	impl.closeDrainedFencedSources(ctx)
	vchannel := msg.VChannel()
	if vchannel == "" || msg.IsPChannelLevel() {
		return impl.broadcastToAllDataSyncService(ctx, msg)
	}
	ds := impl.getDataSyncService(vchannel)
	if ds == nil {
		return nil
	}
	return ds.HandleMessage(ctx, msg)
}

// hasDataSyncService reports whether vchannel already has a data sync service.
func (impl *flusherComponents) hasDataSyncService(vchannel string) bool {
	impl.mu.Lock()
	defer impl.mu.Unlock()
	_, ok := impl.dataServices[vchannel]
	return ok
}

// getDataSyncService returns the data sync service of vchannel, nil if none.
func (impl *flusherComponents) getDataSyncService(vchannel string) *dataSyncServiceWrapper {
	impl.mu.Lock()
	defer impl.mu.Unlock()
	return impl.dataServices[vchannel]
}

// snapshotDataServices returns a point-in-time copy of every data sync
// service, so a broadcast can call each one without holding the lock for the
// whole iteration.
func (impl *flusherComponents) snapshotDataServices() []*dataSyncServiceWrapper {
	impl.mu.Lock()
	defer impl.mu.Unlock()
	services := make([]*dataSyncServiceWrapper, 0, len(impl.dataServices))
	for _, ds := range impl.dataServices {
		services = append(services, ds)
	}
	return services
}

// broadcastToAllDataSyncService broadcasts the message to all data sync services.
func (impl *flusherComponents) broadcastToAllDataSyncService(ctx context.Context, msg message.ImmutableMessage) error {
	for _, ds := range impl.snapshotDataServices() {
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
	impl.mu.Lock()
	impl.dataServices[genesisMsg.VChannel()] = newDS
	impl.mu.Unlock()
	impl.logger.Info(ctx, "create data sync service done", mlog.FieldVChannel(genesisMsg.VChannel()))
}

// Close release all the resources of components.
func (impl *flusherComponents) Close() {
	// Snapshot-and-clear under the lock (uniform with every other access to
	// these maps, even though nothing else should still be racing with a
	// shutting-down flusher by the time this runs), then close each data
	// sync service outside it.
	impl.mu.Lock()
	dataServices := impl.dataServices
	impl.dataServices = make(map[string]*dataSyncServiceWrapper)
	impl.fenced = make(map[string]uint64)
	impl.mu.Unlock()

	for vchannel, ds := range dataServices {
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
	// Runs before any concurrency starts (the checkpoint updater goroutine
	// is already running by this point, but nothing has fed it a checkpoint
	// yet), but takes the lock anyway for uniformity with every other access.
	impl.mu.Lock()
	impl.dataServices = dataServices
	impl.mu.Unlock()
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
