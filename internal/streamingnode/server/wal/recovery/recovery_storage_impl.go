package recovery

import (
	"context"
	"sync"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

const (
	componentRecoveryStorage = "recovery-storage"

	recoveryStorageStatePersistRecovering = "persist-recovering"
	recoveryStorageStateStreamRecovering  = "stream-recovering"
	recoveryStorageStateWorking           = "working"
)

// RecoverRecoveryStorage creates a new recovery storage.
func RecoverRecoveryStorage(
	ctx context.Context,
	recoveryStreamBuilder RecoveryStreamBuilder,
	lastTimeTickMessage message.ImmutableMessage,
) (*RecoveryStorage, *RecoverySnapshot, error) {
	rs := newRecoveryStorage(recoveryStreamBuilder.Channel())
	if err := rs.recoverRecoveryInfoFromMeta(ctx, recoveryStreamBuilder.WALName(), recoveryStreamBuilder.Channel(), lastTimeTickMessage); err != nil {
		rs.Logger().Warn("recovery storage failed", zap.Error(err))
		return nil, nil, err
	}
	// recover the state from wal and start the background task to persist the state.
	snapshot, err := rs.recoverFromStream(ctx, recoveryStreamBuilder, lastTimeTickMessage)
	if err != nil {
		rs.Logger().Warn("recovery storage failed", zap.Error(err))
		return nil, nil, err
	}
	// recovery storage start work.
	rs.SetLogger(resource.Resource().Logger().With(
		log.FieldComponent(componentRecoveryStorage),
		zap.String("channel", recoveryStreamBuilder.Channel().String()),
		zap.String("state", recoveryStorageStateWorking)))
	go rs.backgroundTask()
	return rs, snapshot, nil
}

// newRecoveryStorage creates a new recovery storage.
func newRecoveryStorage(channel types.PChannelInfo) *RecoveryStorage {
	cfg := newConfig()
	return &RecoveryStorage{
		backgroundTaskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		cfg:                    cfg,
		mu:                     sync.Mutex{},
		channel:                channel,
		dirtyCounter:           0,
		persistNotifier:        make(chan struct{}, 1),
		gracefulClosed:         false,
	}
}

// RecoveryStorage is a component that manages the recovery info for the streaming service.
// It will consume the message from the wal, consume the message in wal, and update the checkpoint for it.
type RecoveryStorage struct {
	log.Binder
	backgroundTaskNotifier *syncutil.AsyncTaskNotifier[struct{}]
	cfg                    *config
	mu                     sync.Mutex
	channel                types.PChannelInfo
	segments               map[int64]*segmentRecoveryInfo
	vchannels              map[string]*vchannelRecoveryInfo
	checkpoint             *WALCheckpoint
	dirtyCounter           int // records the message count since last persist snapshot.
	// used to trigger the recovery persist operation.
	persistNotifier chan struct{}
	gracefulClosed  bool
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
// A snapshot is always a consistent state (fully consume a message or a txn message) of the recovery storage.
func (r *RecoveryStorage) consumeDirtySnapshot() *RecoverySnapshot {
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
	return &RecoverySnapshot{
		VChannels:          vchannels,
		SegmentAssignments: segments,
		Checkpoint:         r.checkpoint.Clone(),
	}
}

// observeMessage observes a message and update the recovery storage.
func (r *RecoveryStorage) observeMessage(msg message.ImmutableMessage) {
	if msg.TimeTick() <= r.checkpoint.TimeTick {
		if r.Logger().Level().Enabled(zap.DebugLevel) {
			r.Logger().Debug("skip the message before the checkpoint",
				log.FieldMessage(msg),
				zap.Uint64("checkpoint", r.checkpoint.TimeTick),
				zap.Uint64("incoming", msg.TimeTick()),
			)
		}
		return
	}
	r.handleMessage(msg)

	checkpointUpdates := !r.checkpoint.MessageID.EQ(msg.LastConfirmedMessageID())
	r.checkpoint.TimeTick = msg.TimeTick()
	r.checkpoint.MessageID = msg.LastConfirmedMessageID()

	if checkpointUpdates {
		// only count the dirty if last confirmed message id is updated.
		// we always recover from that point, the writeaheadtimetick is just a redundant information.
		r.dirtyCounter++
	}
	if r.dirtyCounter > r.cfg.maxDirtyMessages {
		r.notifyPersist()
	}
}

// The incoming message id is always sorted with timetick.
func (r *RecoveryStorage) handleMessage(msg message.ImmutableMessage) {
	if msg.VChannel() != "" && msg.MessageType() != message.MessageTypeCreateCollection &&
		msg.MessageType() != message.MessageTypeDropCollection && r.vchannels[msg.VChannel()] == nil {
		r.detectInconsistency(msg, "vchannel not found")
	}

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
		if segment, ok := r.segments[partition.SegmentAssignment.SegmentId]; ok && segment.IsGrowing() {
			segment.ObserveInsert(msg.TimeTick(), partition)
			if r.Logger().Level().Enabled(zap.DebugLevel) {
				r.Logger().Debug("insert entity", log.FieldMessage(msg), zap.Uint64("segmentRows", segment.Rows()), zap.Uint64("segmentBinary", segment.BinarySize()))
			}
		} else {
			r.detectInconsistency(msg, "segment not found")
		}
	}
}

// handleDelete handles the delete message.
func (r *RecoveryStorage) handleDelete(msg message.ImmutableDeleteMessageV1) {
	// nothing, current delete operation is managed by flowgraph, not recovery storage.
	if r.Logger().Level().Enabled(zap.DebugLevel) {
		r.Logger().Debug("delete entity", log.FieldMessage(msg))
	}
}

// handleCreateSegment handles the create segment message.
func (r *RecoveryStorage) handleCreateSegment(msg message.ImmutableCreateSegmentMessageV2) {
	segment := newSegmentRecoveryInfoFromCreateSegmentMessage(msg)
	r.segments[segment.meta.SegmentId] = segment
	r.Logger().Info("create segment", log.FieldMessage(msg))
}

// handleFlush handles the flush message.
func (r *RecoveryStorage) handleFlush(msg message.ImmutableFlushMessageV2) {
	header := msg.Header()
	if segment, ok := r.segments[header.SegmentId]; ok {
		segment.ObserveFlush(msg.TimeTick())
		r.Logger().Info("flush segment", log.FieldMessage(msg), zap.Uint64("rows", segment.Rows()), zap.Uint64("binarySize", segment.BinarySize()))
	}
}

// handleManualFlush handles the manual flush message.
func (r *RecoveryStorage) handleManualFlush(msg message.ImmutableManualFlushMessageV2) {
	segments := make(map[int64]struct{}, len(msg.Header().SegmentIds))
	for _, segmentID := range msg.Header().SegmentIds {
		segments[segmentID] = struct{}{}
	}
	r.flushSegments(msg, segments)
}

// flushSegments flushes the segments in the recovery storage.
func (r *RecoveryStorage) flushSegments(msg message.ImmutableMessage, sealSegmentIDs map[int64]struct{}) {
	segmentIDs := make([]int64, 0)
	rows := make([]uint64, 0)
	binarySize := make([]uint64, 0)
	for _, segment := range r.segments {
		if _, ok := sealSegmentIDs[segment.meta.SegmentId]; ok {
			segment.ObserveFlush(msg.TimeTick())
			segmentIDs = append(segmentIDs, segment.meta.SegmentId)
			rows = append(rows, segment.Rows())
			binarySize = append(binarySize, segment.BinarySize())
		}
	}
	if len(segmentIDs) != len(sealSegmentIDs) {
		r.detectInconsistency(msg, "flush segments not exist", zap.Int64s("wanted", lo.Keys(sealSegmentIDs)), zap.Int64s("actually", segmentIDs))
	}
	r.Logger().Info("flush all segments of collection by manual flush", log.FieldMessage(msg), zap.Uint64s("rows", rows), zap.Uint64s("binarySize", binarySize))
}

// handleCreateCollection handles the create collection message.
func (r *RecoveryStorage) handleCreateCollection(msg message.ImmutableCreateCollectionMessageV1) {
	if _, ok := r.vchannels[msg.VChannel()]; ok {
		return
	}
	r.vchannels[msg.VChannel()] = newVChannelRecoveryInfoFromCreateCollectionMessage(msg)
	r.Logger().Info("create collection", log.FieldMessage(msg))
}

// handleDropCollection handles the drop collection message.
func (r *RecoveryStorage) handleDropCollection(msg message.ImmutableDropCollectionMessageV1) {
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; !ok || vchannelInfo.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
		return
	}
	r.vchannels[msg.VChannel()].ObserveDropCollection(msg)
	// flush all existing segments.
	r.flushAllSegmentOfCollection(msg, msg.Header().CollectionId)
	r.Logger().Info("drop collection", log.FieldMessage(msg))
}

// flushAllSegmentOfCollection flushes all segments of the collection.
func (r *RecoveryStorage) flushAllSegmentOfCollection(msg message.ImmutableMessage, collectionID int64) {
	segmentIDs := make([]int64, 0)
	rows := make([]uint64, 0)
	for _, segment := range r.segments {
		if segment.meta.CollectionId == collectionID {
			segment.ObserveFlush(msg.TimeTick())
			segmentIDs = append(segmentIDs, segment.meta.SegmentId)
			rows = append(rows, segment.Rows())
		}
	}
	r.Logger().Info("flush all segments of collection", log.FieldMessage(msg), zap.Int64s("segmentIDs", segmentIDs), zap.Uint64s("rows", rows))
}

// handleCreatePartition handles the create partition message.
func (r *RecoveryStorage) handleCreatePartition(msg message.ImmutableCreatePartitionMessageV1) {
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; !ok || vchannelInfo.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
		return
	}
	r.vchannels[msg.VChannel()].ObserveCreatePartition(msg)
	r.Logger().Info("create partition", log.FieldMessage(msg))
}

// handleDropPartition handles the drop partition message.
func (r *RecoveryStorage) handleDropPartition(msg message.ImmutableDropPartitionMessageV1) {
	r.vchannels[msg.VChannel()].ObserveDropPartition(msg)
	// flush all existing segments.
	r.flushAllSegmentOfPartition(msg, msg.Header().CollectionId, msg.Header().PartitionId)
	r.Logger().Info("drop partition", log.FieldMessage(msg))
}

// flushAllSegmentOfPartition flushes all segments of the partition.
func (r *RecoveryStorage) flushAllSegmentOfPartition(msg message.ImmutableMessage, collectionID int64, partitionID int64) {
	segmentIDs := make([]int64, 0)
	rows := make([]uint64, 0)
	for _, segment := range r.segments {
		if segment.meta.PartitionId == partitionID {
			segment.ObserveFlush(msg.TimeTick())
			segmentIDs = append(segmentIDs, segment.meta.SegmentId)
			rows = append(rows, segment.Rows())
		}
	}
	r.Logger().Info("flush all segments of partition", log.FieldMessage(msg), zap.Int64s("segmentIDs", segmentIDs), zap.Uint64s("rows", rows))
}

// handleTxn handles the txn message.
func (r *RecoveryStorage) handleTxn(msg message.ImmutableTxnMessage) {
	msg.RangeOver(func(im message.ImmutableMessage) error {
		r.handleMessage(im)
		return nil
	})
}

// handleImport handles the import message.
func (r *RecoveryStorage) handleImport(_ message.ImmutableImportMessageV1) {
}

// handleSchemaChange handles the schema change message.
func (r *RecoveryStorage) handleSchemaChange(msg message.ImmutableSchemaChangeMessageV2) {
	// when schema change happens, we need to flush all segments in the collection.
	// TODO: add the flush segment list into schema change message.
	// TODO: persist the schema change into recoveryinfo.
	r.flushAllSegmentOfCollection(msg, msg.Header().CollectionId)
}

// detectInconsistency detects the inconsistency in the recovery storage.
func (r *RecoveryStorage) detectInconsistency(msg message.ImmutableMessage, reason string, extra ...zap.Field) {
	fields := make([]zap.Field, 0, len(extra)+2)
	fields = append(fields, log.FieldMessage(msg), zap.String("reason", reason))
	fields = append(fields, extra...)
	// The log is not fatal in some cases.
	// because our meta is not atomic-updated, so these error may be logged if crashes when meta updated partially.
	r.Logger().Warn("inconsistency detected", fields...)
}
