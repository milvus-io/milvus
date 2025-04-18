package recovery

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// RecoverRecoveryStorage creates a new recovery storage.
func RecoverRecoveryStorage(
	ctx context.Context,
	recoveryStreamBuilder RecoveryStreamBuilder,
	lastTimeTickMessage message.ImmutableMessage,
) (*RecoveryStorage, *RecoverSnapshot, error) {
	cfg := newConfig()
	info, err := recoverRecoveryInfoFromMeta(ctx, recoveryStreamBuilder.WALName(), recoveryStreamBuilder.Channel(), lastTimeTickMessage)
	if err != nil {
		return nil, nil, err
	}
	rs := &RecoveryStorage{
		backgroundTaskNotifier:    syncutil.NewAsyncTaskNotifier[struct{}](),
		cfg:                       cfg,
		mu:                        sync.Mutex{},
		channel:                   recoveryStreamBuilder.Channel(),
		segments:                  newSegmentRecoveryInfoFromSegmentAssignmentMeta(info.Segments),
		vchannels:                 newVChannelRecoveryInfoFromVChannelMeta(info.VChannels),
		checkpoint:                info.Checkpoint,
		vchannelCheckpointManager: nil,
		dirtyCounter:              0,
		persistNotifier:           make(chan struct{}, 1),
	}
	rs.SetLogger(resource.Resource().Logger().With(zap.String("channel", recoveryStreamBuilder.Channel().String())))

	// recover the state from wal and start the background task to persist the state.
	snapshot, err := rs.recoverFromStream(ctx, recoveryStreamBuilder, lastTimeTickMessage)
	if err != nil {
		return nil, nil, err
	}
	go rs.backgroundTask()
	return rs, snapshot, nil
}

// RecoveryStorage is a component that manages the recovery info for the streaming service.
// It will consume the message from the wal, consume the message in wal, and update the checkpoint for it.
type RecoveryStorage struct {
	log.Binder
	backgroundTaskNotifier    *syncutil.AsyncTaskNotifier[struct{}]
	cfg                       *config
	mu                        sync.Mutex
	channel                   types.PChannelInfo
	segments                  map[int64]*segmentRecoveryInfo
	vchannels                 map[string]*vchannelRecoveryInfo
	checkpoint                *WALCheckpoint
	vchannelCheckpointManager *vchannelCheckpointManager // Because current data path at datasyncservice doen's promise
	// the checkpoint is closed enough, so we need to manage the checkpoint of every vchannel here.
	// make the pchannel checkpoint and write ahead checkpoint different.
	// TODO: remove this in future.
	dirtyCounter int // records the message count since last persist snapshot.
	// used to trigger the recovery persist operation.
	persistNotifier chan struct{}
}

// recoverFromStream recovers the recovery storage from the recovery stream.
func (r *RecoveryStorage) recoverFromStream(
	ctx context.Context,
	recoveryStreamBuilder RecoveryStreamBuilder,
	lastTimeTickMessage message.ImmutableMessage,
) (*RecoverSnapshot, error) {
	rs := recoveryStreamBuilder.Build(BuildRecoveryStreamParam{
		StartCheckpoint: r.checkpoint.WriteAheadCheckpoint,
		EndTimeTick:     lastTimeTickMessage.TimeTick(),
	})
	defer rs.Close()
L:
	for {
		select {
		case <-ctx.Done():
			return nil, errors.Wrap(ctx.Err(), "failed to recover from wal")
		case msg, ok := <-rs.Chan():
			if !ok {
				// The recovery stream is reach the end, we can stop the recovery.
				break L
			}
			r.observeMessage(msg)
		}
	}
	if rs.Error() != nil {
		return nil, errors.Wrap(rs.Error(), "failed to read the recovery info from wal")
	}
	snapshot := r.getSnapshot()
	snapshot.TxnBuffer = rs.TxnBuffer()
	return snapshot, nil
}

// FlushCheckpoint returns the checkpoint of the recovery storage.
func (r *RecoveryStorage) FlushCheckpoint() message.MessageID {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.vchannelCheckpointManager == nil {
		panic("FlushCheckpoint should always be called after InitVChannelCheckpoint")
	}
	if r.checkpoint.FlushCheckpoint == nil {
		return r.vchannelCheckpointManager.MinimumCheckpoint()
	}
	return r.checkpoint.FlushCheckpoint
}

// getSnapshot returns the snapshot of the recovery storage.
// Use this function to get the snapshot after recovery is finished,
// and use the snapshot to recover all write ahead components.
func (r *RecoveryStorage) getSnapshot() *RecoverSnapshot {
	segments := make(map[int64]*streamingpb.SegmentAssignmentMeta, len(r.segments))
	vchannels := make(map[string]*streamingpb.VChannelMeta, len(r.vchannels))
	for segmentID, segment := range r.segments {
		segments[segmentID] = proto.Clone(segment.meta).(*streamingpb.SegmentAssignmentMeta)
	}
	for channelName, vchannel := range r.vchannels {
		vchannels[channelName] = proto.Clone(vchannel.meta).(*streamingpb.VChannelMeta)
	}
	return &RecoverSnapshot{
		VChannels:          vchannels,
		SegmentAssignments: segments,
		Checkpoint:         r.checkpoint.Clone(),
	}
}

// InitVChannelCheckpoint initializes the vchannel checkpoint manager.
func (r *RecoveryStorage) InitVChannelCheckpoint(exists map[string]message.MessageID) {
	vchannelCheckpointManager := newVChannelCheckpointManager(exists)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.vchannelCheckpointManager = vchannelCheckpointManager
}

func (r *RecoveryStorage) AddVChannelCheckpoint(vchannel string, checkpoint message.MessageID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.vchannelCheckpointManager == nil {
		panic("AddVChannelCheckpoint should always be called after InitVChannelCheckpoint")
	}

	err := r.vchannelCheckpointManager.Add(vchannel, checkpoint)
	if err != nil {
		r.Logger().Warn("failed to add vchannel checkpoint", zap.String("vchannel", vchannel), zap.Error(err))
		return
	}
	if newMinimum := r.vchannelCheckpointManager.MinimumCheckpoint(); r.checkpoint.FlushCheckpoint == nil || r.checkpoint.FlushCheckpoint.LT(newMinimum) {
		r.checkpoint.FlushCheckpoint = newMinimum
	}
}

// UpdateVChannelCheckpoint updates the checkpoint of a vchannel.
func (r *RecoveryStorage) UpdateVChannelCheckpoint(vchannel string, checkpoint message.MessageID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.vchannelCheckpointManager == nil {
		panic("UpdateVChannelCheckpoint should always be called after InitVChannelCheckpoint")
	}

	oldMinimum := r.vchannelCheckpointManager.MinimumCheckpoint()
	err := r.vchannelCheckpointManager.Update(vchannel, checkpoint)
	if err != nil {
		r.Logger().Warn("failed to update vchannel checkpoint", zap.String("vchannel", vchannel), zap.Error(err))
		return
	}
	if newMinimum := r.vchannelCheckpointManager.MinimumCheckpoint(); oldMinimum == nil || oldMinimum.LT(newMinimum) {
		r.checkpoint.FlushCheckpoint = newMinimum
	}
}

func (r *RecoveryStorage) DropVChannelCheckpoint(vchannel string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.vchannelCheckpointManager == nil {
		panic("AddVChannelCheckpoint should always be called after InitVChannelCheckpoint")
	}

	err := r.vchannelCheckpointManager.Drop(vchannel)
	if err != nil {
		r.Logger().Warn("failed to drop vchannel checkpoint", zap.String("vchannel", vchannel), zap.Error(err))
	}
}

// ObserveMessage is called when a new message is observed.
func (r *RecoveryStorage) ObserveMessage(msg message.ImmutableMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.observeMessage(msg)
}

// Close closes the recovery storage and wait the background task stop.
func (r *RecoveryStorage) Close() {
	r.backgroundTaskNotifier.Cancel()
	r.backgroundTaskNotifier.BlockUntilFinish()
}

// notifyPersist notifies a persist operation.
func (r *RecoveryStorage) notifyPersist() {
	select {
	case r.persistNotifier <- struct{}{}:
	default:
	}
}

// consumeDirtySnapshot consumes the dirty state and returns a snapshot to persist.
func (r *RecoveryStorage) consumeDirtySnapshot() *RecoverSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	segments := make(map[int64]*streamingpb.SegmentAssignmentMeta)
	vchannels := make(map[string]*streamingpb.VChannelMeta)
	for _, segment := range r.segments {
		dirtySnapshot, shouldBeRemoved := segment.ConsumeDirtyAndGetSnapshot()
		if shouldBeRemoved {
			delete(r.segments, segment.meta.SegmentId)
		}
		if dirtySnapshot != nil {
			segments[segment.meta.SegmentId] = dirtySnapshot
		}
	}
	for _, vchannel := range r.vchannels {
		dirtySnapshot, shouldBeRemoved := vchannel.ConsumeDirtyAndGetSnapshot()
		if shouldBeRemoved {
			delete(r.vchannels, vchannel.meta.Vchannel)
		}
		if dirtySnapshot != nil {
			vchannels[vchannel.meta.Vchannel] = dirtySnapshot
		}
	}
	// clear the dirty counter.
	r.dirtyCounter = 0
	return &RecoverSnapshot{
		VChannels:          vchannels,
		SegmentAssignments: segments,
		Checkpoint:         r.checkpoint.Clone(),
	}
}

func (r *RecoveryStorage) observeMessage(msg message.ImmutableMessage) {
	r.handleMessage(msg)

	checkpointUpdates := !r.checkpoint.WriteAheadCheckpoint.EQ(msg.LastConfirmedMessageID())
	r.checkpoint.WriteAheadCheckpointTimeTick = msg.TimeTick()
	r.checkpoint.WriteAheadCheckpoint = msg.LastConfirmedMessageID()

	if checkpointUpdates {
		// only count the dirty if last confrimed message id is updated.
		// we alwasy recover from that point, the writeaheadtimetick is just a redundant information.
		r.dirtyCounter++
	}
	if r.dirtyCounter > r.cfg.maxDirtyMessages {
		r.notifyPersist()
	}
}

// The incoming message id is always sorted with timetick.
func (r *RecoveryStorage) handleMessage(msg message.ImmutableMessage) {
	switch msg.MessageType() {
	case message.MessageTypeInsert:
		immutableMsg := message.MustAsImmutableInsertMessageV1(msg)
		r.handleInsert(immutableMsg)
	case message.MessageTypeDelete:
		immutableMsg := message.MustAsImmutableDeleteMessageV1(msg)
		r.handleDelete(immutableMsg)
	case message.MessageTypeCreateSegment:
		immutableMsg := message.MustAsImmutableCreateSegmentMessageV2(msg)
		r.handleCreateSegment(immutableMsg)
	case message.MessageTypeFlush:
		immutableMsg := message.MustAsImmutableFlushMessageV2(msg)
		r.handleFlush(immutableMsg)
	case message.MessageTypeManualFlush:
		immutableMsg := message.MustAsImmutableManualFlushMessageV2(msg)
		r.handleManualFlush(immutableMsg)
	case message.MessageTypeCreateCollection:
		immutableMsg := message.MustAsImmutableCreateCollectionMessageV1(msg)
		r.handleCreateCollection(immutableMsg)
	case message.MessageTypeDropCollection:
		immutableMsg := message.MustAsImmutableDropCollectionMessageV1(msg)
		r.handleDropCollection(immutableMsg)
	case message.MessageTypeCreatePartition:
		immutableMsg := message.MustAsImmutableCreatePartitionMessageV1(msg)
		r.handleCreatePartition(immutableMsg)
	case message.MessageTypeDropPartition:
		immutableMsg := message.MustAsImmutableDropPartitionMessageV1(msg)
		r.handleDropPartition(immutableMsg)
	case message.MessageTypeTxn:
		immutableMsg := message.AsImmutableTxnMessage(msg)
		r.handleTxn(immutableMsg)
	case message.MessageTypeImport:
		immutableMsg := message.MustAsImmutableImportMessageV1(msg)
		r.handleImport(immutableMsg)
	case message.MessageTypeSchemaChange:
		immutableMsg := message.MustAsImmutableCollectionSchemaChangeV2(msg)
		r.handleSchemaChange(immutableMsg)
	case message.MessageTypeTimeTick:
		// nothing, the time tick message make no recovery operation.
	default:
		panic("unreachable: some message type can not be consumed, there's a critical bug.")
	}
}

// handleInsert handles the insert message.
func (r *RecoveryStorage) handleInsert(msg message.ImmutableInsertMessageV1) {
	for _, partition := range msg.Header().GetPartitions() {
		if segment, ok := r.segments[partition.SegmentAssignment.SegmentId]; ok {
			segment.ObserveInsert(msg.TimeTick(), partition)
		}
	}
}

func (r *RecoveryStorage) handleDelete(msg message.ImmutableDeleteMessageV1) {
	// nothing, current delete operation is managed by flowgraph, not recovery storage.
}

// handleCreateSegment handles the create segment message.
func (r *RecoveryStorage) handleCreateSegment(msg message.ImmutableCreateSegmentMessageV2) {
	segments := newSegmentRecoveryInfoFromCreateSegmentMessage(msg)
	for _, segment := range segments {
		r.segments[segment.meta.SegmentId] = segment
	}
}

// handleFlush handles the flush message.
func (r *RecoveryStorage) handleFlush(msg message.ImmutableFlushMessageV2) {
	body := msg.MustBody()
	for _, segmentID := range body.SegmentId {
		if segmentRecoveryInfo, ok := r.segments[segmentID]; ok {
			segmentRecoveryInfo.ObserveFlush(msg.TimeTick())
		}
	}
}

// handleManualFlush handles the manual flush message.
func (r *RecoveryStorage) handleManualFlush(msg message.ImmutableManualFlushMessageV2) {
	r.flushAllSegmentOfCollection(msg.Header().CollectionId, msg.TimeTick())
}

func (r *RecoveryStorage) flushAllSegmentOfCollection(collectionID int64, timetick uint64) {
	for _, segment := range r.segments {
		if segment.meta.CollectionId == collectionID {
			segment.ObserveFlush(timetick)
		}
	}
}

func (r *RecoveryStorage) flushAllSegmentOfPartition(partitionID int64, timetick uint64) {
	for _, segment := range r.segments {
		if segment.meta.PartitionId == partitionID {
			segment.ObserveFlush(timetick)
		}
	}
}

// handleCreateCollection handles the create collection message.
func (r *RecoveryStorage) handleCreateCollection(msg message.ImmutableCreateCollectionMessageV1) {
	if _, ok := r.vchannels[msg.VChannel()]; ok {
		return
	}
	r.vchannels[msg.VChannel()] = newVChannelRecoveryInfoFromCreateCollectionMessage(msg)
}

// handleDropCollection handles the drop collection message.
func (r *RecoveryStorage) handleDropCollection(msg message.ImmutableDropCollectionMessageV1) {
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; !ok || vchannelInfo.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
		return
	}
	r.vchannels[msg.VChannel()].ObserveDropCollection(msg)
	// flush all existing segments.
	r.flushAllSegmentOfCollection(msg.Header().CollectionId, msg.TimeTick())
}

// handleCreatePartition handles the create partition message.
func (r *RecoveryStorage) handleCreatePartition(msg message.ImmutableCreatePartitionMessageV1) {
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; !ok || vchannelInfo.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
		return
	}
	r.vchannels[msg.VChannel()].ObserveCreatePartition(msg)
}

// handleDropPartition handles the drop partition message.
func (r *RecoveryStorage) handleDropPartition(msg message.ImmutableDropPartitionMessageV1) {
	r.vchannels[msg.VChannel()].ObserveDropPartition(msg)
	// flush all existing segments.
	r.flushAllSegmentOfPartition(msg.Header().PartitionId, msg.TimeTick())
}

// handleTxn handles the txn message.
func (r *RecoveryStorage) handleTxn(msg message.ImmutableTxnMessage) {
	msg.RangeOver(func(im message.ImmutableMessage) error {
		r.handleMessage(im)
		return nil
	})
}

// handleImport handles the import message.
func (r *RecoveryStorage) handleImport(msg message.ImmutableImportMessageV1) {
	return
}

func (r *RecoveryStorage) handleSchemaChange(msg message.ImmutableSchemaChangeMessageV2) {
	// when schema change happens, we need to flush all segments in the collection.
	// TODO: persist the schema change into recoveryinfo.
	r.flushAllSegmentOfCollection(msg.Header().CollectionId, msg.TimeTick())
}
