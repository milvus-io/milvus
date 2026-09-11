package shards

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
)

type CollectionSchemaInfo struct {
	VChannel string
	Schema   *schemapb.CollectionSchema
}

// CheckIfCollectionCanBeCreated checks if a collection can be created.
// It returns false if the collection cannot be created.
func (m *shardManagerImpl) CheckIfCollectionCanBeCreated(collectionID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.checkIfCollectionCanBeCreated(collectionID)
}

// checkIfCollectionCanBeCreated checks if a collection can be created.
func (m *shardManagerImpl) checkIfCollectionCanBeCreated(collectionID int64) error {
	if _, ok := m.collections[collectionID]; ok {
		return ErrCollectionExists
	}
	return nil
}

// CheckIfCollectionExists checks if a collection can be dropped.
func (m *shardManagerImpl) CheckIfCollectionExists(collectionID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.checkIfCollectionExists(collectionID)
}

// checkIfCollectionExists checks if a collection exists.
func (m *shardManagerImpl) checkIfCollectionExists(collectionID int64) error {
	if _, ok := m.collections[collectionID]; !ok {
		return ErrCollectionNotFound
	}
	return nil
}

// CheckIfVChannelCanBeWritten checks if the given vchannel of the collection
// still accepts new DML.
//
// The vchannel is named, not just the collection: m.collections is keyed by
// collection id -- one entry per collection per pchannel -- so an entry may
// describe a DIFFERENT vchannel of the same collection than the one the message
// targets. Answering from that entry would report the wrong shard's fence state
// in both directions: letting writes through onto a fenced shard, or rejecting
// writes to a live one.
func (m *shardManagerImpl) CheckIfVChannelCanBeWritten(collectionID int64, vchannel string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.checkIfVChannelCanBeWritten(collectionID, vchannel)
}

// GetSplitFence returns the fence recorded for the named vchannel: T_switch and
// the task that placed it. Zero values when the vchannel is unknown or not
// fenced.
//
// Answered from the tombstone alone, because that is the only place a fence is
// ever recorded: SplitShard tears the registration down as it fences, so a
// fenced vchannel has no entry in m.collections to read from -- and the entry
// that may sit under its collection id belongs to a successor, whose fence
// state is not this vchannel's.
//
// The task id is what lets a caller tell ITS OWN retry from another task's
// fence. Without it a rehash landing on a source an automatic split already
// fenced would read the rejection as "my own fence holds", roll forward, and
// give two tasks the same source.
func (m *shardManagerImpl) GetSplitFence(collectionID int64, vchannel string) SplitFence {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.fencedVChannels[vchannel]
}

// CheckIfVChannelCanBeCreated checks if the named vchannel can be registered on
// this pchannel.
//
// Re-registering the SAME vchannel is a no-op replay and reported as such. A
// DIFFERENT vchannel of the same collection is the dangerous case and is
// reported as an error rather than silently skipped: the entry is keyed by
// collection id, so the newcomer would find the slot taken, skip its own
// registration, and inherit the incumbent's state -- including a SPLITTED
// source's fence, which leaves the new shard permanently unwritable with
// nothing but a warning in the log. The split coordinator must retire a source
// (the routing commit that delists it) before a successor lands on its
// pchannel, and this is where that contract is enforced.
func (m *shardManagerImpl) CheckIfVChannelCanBeCreated(collectionID int64, vchannel string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.checkIfVChannelCanBeCreated(collectionID, vchannel)
}

func (m *shardManagerImpl) checkIfVChannelCanBeCreated(collectionID int64, vchannel string) error {
	collectionInfo, ok := m.collections[collectionID]
	if !ok {
		return nil
	}
	if collectionInfo.VChannel == vchannel {
		return ErrCollectionExists
	}
	return errors.Wrapf(ErrVChannelConflict,
		"collection %d is registered on this pchannel as vchannel %s, cannot register %s",
		collectionID, collectionInfo.VChannel, vchannel)
}

// checkIfVChannelCanBeWritten checks if the given vchannel of the collection still accepts new DML.
//
// Three answers, and which one is given decides whether the client's write
// survives:
//
//   - nil -- this pchannel holds the vchannel and it is live.
//   - ErrVChannelFenced -- the vchannel was fenced by a split. The write belongs
//     to a shard that exists; the caller's route is one routing commit behind.
//     It becomes SHARD_FENCED, and the proxy refreshes and retries.
//   - ErrCollectionNotFound -- this pchannel has never held the vchannel. No
//     refresh sends the write anywhere, so it is terminal.
//
// The fenced answer is decided by NAME, not by the registration: the fence
// removes the registration as it is placed, so a fenced vchannel has none, and
// the entry that may sit under its collection id belongs to a successor. A
// registration therefore means exactly one thing here -- this pchannel holds
// that vchannel and it is live.
//
// A different vchannel in the registration is NOT enough on its own to report a
// fence: a newcomer whose registration was refused while a live incumbent holds
// the slot is a wrong route, not a successor, and must stay terminal.
func (m *shardManagerImpl) checkIfVChannelCanBeWritten(collectionID int64, vchannel string) error {
	if collectionInfo, ok := m.collections[collectionID]; ok && collectionInfo.VChannel == vchannel {
		return nil
	}
	if _, fenced := m.fencedVChannels[vchannel]; fenced {
		return ErrVChannelFenced
	}
	return ErrCollectionNotFound
}

// SplitShard fences the source vchannel of a split and frees this pchannel's
// registration slot, when a SplitShard message is written into the wal.
//
// Two things happen here, and they have to be one critical section: a reader
// must never see the registration gone without the tombstone in its place.
//
//   - the fence is recorded by NAME in fencedVChannels. That tombstone is what
//     answers a stale proxy route afterwards -- SHARD_FENCED, refresh, retry --
//     and what returns T_switch to a re-sent fence.
//   - the registration is torn down: every partition manager of the collection
//     is flushed and dropped, and the entry removed. Nothing on the source
//     needs it any more -- no DML follows the fence, and the growing segments
//     were sealed by FlushAndFenceSegmentAllocUntil while the message was being
//     built (there is no separate ManualFlush; this message IS the seal record).
//     Dropping it here is what frees the slot immediately, so a successor
//     vchannel of the same collection can be registered on this pchannel
//     without waiting for a routing commit to come back and reclaim it.
//
// A second fence record of the SAME task raises the tombstone's tick instead:
// T_switch is the tick of the task's LATEST fence record. The broadcaster
// re-drives a split whose source landed but whose task was not yet persisted,
// and every fence record of one task seals the same data -- the vchannel took
// no DML in between -- so the later tick is both safe and the one DataCoord
// recorded. A fence record of ANOTHER task is refused on the append path; it
// must never move a fence it did not place.
func (m *shardManagerImpl) SplitShard(msg message.ImmutableSplitShardMessageV2) {
	collectionID := msg.Header().CollectionId
	vchannel := msg.VChannel()
	taskID := msg.Header().GetSplitTaskId()
	logger := m.Logger().With(mlog.FieldMessage(msg))

	m.mu.Lock()
	defer m.mu.Unlock()

	if fence, ok := m.fencedVChannels[vchannel]; ok {
		if fence.TaskID != taskID {
			logger.Warn(context.TODO(), "split shard skipped: the vchannel is already fenced by another task",
				mlog.Int64("collectionID", collectionID),
				mlog.Int64("fencedByTaskID", fence.TaskID),
				mlog.Int64("splitTaskID", taskID))
			return
		}
		if msg.TimeTick() > fence.TimeTick {
			fence.TimeTick = msg.TimeTick()
			m.fencedVChannels[vchannel] = fence
			logger.Info(context.TODO(), "fence time tick raised by a later fence record of the same task",
				mlog.Int64("collectionID", collectionID),
				mlog.Int64("splitTaskID", taskID),
				mlog.Uint64("timetick", msg.TimeTick()))
		}
		return
	}

	collectionInfo, ok := m.collections[collectionID]
	if !ok {
		logger.Warn(context.TODO(), "collection not exists when splitting shard", mlog.Int64("collectionID", collectionID))
		return
	}
	if collectionInfo.VChannel != vchannel {
		// The entry is keyed by collection id, so a replayed or late fence must
		// not fence a successor vchannel that has since taken over the slot.
		logger.Warn(context.TODO(), "split shard skipped: this pchannel now hosts another vchannel of the collection",
			mlog.String("registered", collectionInfo.VChannel))
		return
	}
	m.fencedVChannels[vchannel] = SplitFence{TimeTick: msg.TimeTick(), TaskID: taskID}
	partitionIDs, segmentIDs := m.removeCollectionLocked(collectionID, collectionInfo, logger)
	logger.Info(context.TODO(), "vchannel is fenced by shard split, its registration is released",
		mlog.Int64("collectionID", collectionID),
		mlog.Int64("splitTaskID", taskID),
		mlog.Uint64("timetick", msg.TimeTick()),
		mlog.Int64s("partitionIDs", partitionIDs),
		mlog.Int64s("segmentIDs", segmentIDs))
}

// CreateCollection creates a new partition manager when create collection message is written into wal.
// After CreateCollection is called, the ddl and dml on the collection can be applied.
func (m *shardManagerImpl) CreateCollection(msg message.ImmutableCreateCollectionMessageV1) {
	logger := m.Logger().With(mlog.FieldMessage(msg))
	schema := schemaOfCreateBody(msg.MustBody())
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createCollectionLocked(msg.Header().CollectionId, msg.Header().PartitionIds, msg.VChannel(),
		msg.TimeTick(), schema, logger)
}

// schemaOfCreateBody resolves the schema a CreateCollection-shaped body
// carries, in either of its two forms: the CollectionSchema message, or the
// pre-2.6.1 serialized Schema bytes. Nil when the body carries neither. Both
// genesis bodies (the CreateCollection message, and the split target replica's
// SplitShardMessageBody.Genesis) share the shape and admit both forms, so both
// must resolve the schema the same way: a registration that reads only one form
// registers a nil schema for the other, and every versioned insert then fails
// with ErrCollectionSchemaNotFound until a restart rebuilds the entry from the
// persisted meta.
func schemaOfCreateBody(body *message.CreateCollectionRequest) *schemapb.CollectionSchema {
	if schema := body.GetCollectionSchema(); schema != nil {
		return schema
	}
	if len(body.GetSchema()) > 0 {
		return messageutil.MustGetSchemaFromCreateCollectionMessageBody(body)
	}
	return nil
}

// CreateVChannel registers a shard split target vchannel. The TARGET replica of
// the split broadcast is the genesis message of the target vchannel, and its
// body carries the genesis in the CreateCollection body shape, so it registers
// the collection for DML and segment assignment on this pchannel exactly as
// CreateCollection does.
func (m *shardManagerImpl) CreateVChannel(msg message.ImmutableSplitShardMessageV2) {
	logger := m.Logger().With(mlog.FieldMessage(msg))
	collectionID := msg.Header().CollectionId
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.checkIfVChannelCanBeCreated(collectionID, msg.VChannel()); err != nil {
		if errors.Is(err, ErrVChannelConflict) {
			// The append path refuses this (CheckIfVChannelCanBeCreated), so
			// reaching here means a replay of a genesis whose source has since
			// been retired and replaced. Log loudly rather than skip quietly:
			// the newcomer is left without a registration and the shard is
			// unwritable until the WAL is next recovered.
			logger.Error(context.TODO(), "cannot register vchannel, another vchannel of the collection holds this pchannel", mlog.Err(err))
			return
		}
		// ErrCollectionExists: the same vchannel is already registered, an
		// ordinary idempotent replay.
		logger.Info(context.TODO(), "vchannel already registered, skip the genesis")
		return
	}
	m.createCollectionLocked(collectionID, msg.Header().PartitionIds, msg.VChannel(),
		msg.TimeTick(), schemaOfCreateBody(msg.MustBody().GetGenesis()), logger)
}

// createCollectionLocked registers the collection and its partition managers on
// this pchannel for DML and segment assignment. The caller must hold m.mu.
func (m *shardManagerImpl) createCollectionLocked(collectionID int64, partitionIDs []int64, vchannel string, timetick uint64, schema *schemapb.CollectionSchema, logger *mlog.Logger) {
	if err := m.checkIfCollectionCanBeCreated(collectionID); err != nil {
		logger.Warn(context.TODO(), "collection already exists")
		return
	}

	collectionInfo := newCollectionInfo(vchannel, partitionIDs)
	// Set schema when creating collection.
	if schema != nil {
		collectionInfo.setSchema(&streamingpb.CollectionSchemaOfVChannel{
			Schema:             schema,
			CheckpointTimeTick: timetick,
			State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
		})
	}
	m.collections[collectionID] = collectionInfo

	for partitionID := range collectionInfo.PartitionIDs {
		uniqueKey := PartitionUniqueKey{CollectionID: collectionID, PartitionID: partitionID}
		if _, ok := m.partitionManagers[uniqueKey]; ok {
			logger.Warn(context.TODO(), "partition already exists", mlog.Int64("partitionID", partitionID))
			continue
		}
		m.partitionManagers[uniqueKey] = newPartitionSegmentManager(
			m.ctx,
			m.Logger(),
			m.wal,
			m.pchannel,
			vchannel,
			collectionID,
			partitionID,
			make(map[int64]*segmentAllocManager),
			m.txnManager,
			timetick,
			m.metrics,
		)
	}
	logger.Info(context.TODO(), "collection created in segment assignment service", mlog.Int64s("partitionIDs", partitionIDs))
	m.updateMetrics()
}

// DropCollection drops the collection and all the partitions and segments belong to it when drop collection message is written into wal.
// After DropCollection is called, no more segments can be assigned to the collection.
// Any dml and ddl for the collection will be rejected.
func (m *shardManagerImpl) DropCollection(msg message.ImmutableDropCollectionMessageV1) {
	collectionID := msg.Header().CollectionId
	logger := m.Logger().With(mlog.FieldMessage(msg))

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.checkIfCollectionExists(collectionID); err != nil {
		logger.Warn(context.TODO(), "collection not exists")
		return
	}

	partitionIDs, segmentIDs := m.removeCollectionLocked(collectionID, m.collections[collectionID], logger)
	logger.Info(context.TODO(), "collection removed", mlog.Int64s("partitionIDs", partitionIDs), mlog.Int64s("segmentIDs", segmentIDs))
}

// removeCollectionLocked releases the collection's registration on this
// pchannel: every partition manager is flushed and dropped, and the entry
// itself removed. It returns what it removed, for the caller to log. The caller
// must hold m.mu and must have resolved collectionInfo out of m.collections.
//
// Flushing rather than discarding keeps every teardown honest: the source of a
// split has been sealed and fenced long before it gets here and should have
// nothing growing left, so a segment that somehow survived is flushed, not
// dropped.
func (m *shardManagerImpl) removeCollectionLocked(collectionID int64, collectionInfo *CollectionInfo, logger *mlog.Logger) (partitionIDs []int64, segmentIDs []int64) {
	delete(m.collections, collectionID)
	partitionIDs = make([]int64, 0, len(collectionInfo.PartitionIDs))
	segmentIDs = make([]int64, 0, len(collectionInfo.PartitionIDs))
	for partitionID := range collectionInfo.PartitionIDs {
		uniqueKey := PartitionUniqueKey{CollectionID: collectionID, PartitionID: partitionID}
		pm, ok := m.partitionManagers[uniqueKey]
		if !ok {
			logger.Warn(context.TODO(), "partition not exists", mlog.Int64("partitionID", partitionID))
			continue
		}
		partitionIDs = append(partitionIDs, partitionID)
		segmentIDs = append(segmentIDs, pm.FlushAndDropPartition(policy.PolicyCollectionRemoved())...)
		delete(m.partitionManagers, uniqueKey)
	}
	m.updateMetrics()
	return partitionIDs, segmentIDs
}

// AlterCollection handles the alter collection message.
// It updates the schema if present, all within one critical region before WAL append.
func (m *shardManagerImpl) AlterCollection(msg message.MutableAlterCollectionMessageV2) ([]int64, error) {
	header := msg.Header()
	collectionID := header.CollectionId
	timetick := msg.TimeTick()
	logger := m.Logger().With(mlog.FieldMessage(msg))

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.checkIfCollectionExists(collectionID); err != nil {
		logger.Warn(context.TODO(), "collection not found when altering collection", mlog.Int64("collectionID", collectionID))
		return nil, err
	}

	// For schema changes: flush/fence segment allocation and update in-memory schema
	// atomically within this critical region. Both operations share the same
	// IsSchemaChange gate so live path and recovery replay stay consistent.
	var segmentIDs []int64
	if messageutil.IsSchemaChange(header) {
		var err error
		segmentIDs, err = m.flushAndFenceSegmentAllocUntil(collectionID, timetick)
		if err != nil {
			return nil, err
		}
		logger.Info(context.TODO(), "flushed segments on schema change", mlog.Int64s("segmentIDs", segmentIDs))

		schema := msg.MustBody().Updates.Schema
		if schema == nil {
			// UpdateMask says schema changed but the body carries no schema —
			// malformed message; fail fast to avoid nil-pointer dereferences
			// in downstream GetSchema paths.
			logger.Error(context.TODO(), "schema change indicated by UpdateMask but schema body is nil",
				mlog.Int64("collectionID", collectionID))
			return nil, status.NewInvalidArgument("schema change message has nil schema body")
		}
		collectionInfo := m.collections[collectionID]
		collectionInfo.setSchema(&streamingpb.CollectionSchemaOfVChannel{
			Schema:             schema,
			CheckpointTimeTick: timetick,
			State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
		})
		logger.Info(context.TODO(), "updated collection schema in shard manager",
			mlog.Int64("collectionID", collectionID),
			mlog.Int32("schemaVersion", schema.GetVersion()),
			mlog.Uint64("checkpointTimeTick", timetick))
	}

	return segmentIDs, nil
}

// CheckWritableAndSchemaVersion answers both of the insert path's admission
// questions under one read lock. The writable check runs first, so a collection
// this pchannel does not hold is reported as such rather than as a schema
// mismatch.
func (m *shardManagerImpl) CheckWritableAndSchemaVersion(vchannel string, header *message.InsertMessageHeader) (int32, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if err := m.checkIfVChannelCanBeWritten(header.GetCollectionId(), vchannel); err != nil {
		return -1, err
	}
	return m.checkIfCollectionSchemaVersionMatch(header)
}

// CheckIfCollectionSchemaVersionMatch answers the schema-version half alone.
//
// Not on the ShardManager interface: the write path always asks it together with
// the writable check, and asking separately is what cost the second lock
// acquisition. It stays exported so the schema-version rule -- which has more
// cases than the writable one -- can be tested on its own.
func (m *shardManagerImpl) CheckIfCollectionSchemaVersionMatch(header *message.InsertMessageHeader) (int32, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.checkIfCollectionSchemaVersionMatch(header)
}

func (m *shardManagerImpl) checkIfCollectionSchemaVersionMatch(header *message.InsertMessageHeader) (int32, error) {
	collectionID := header.GetCollectionId()
	collectionInfo, ok := m.collections[collectionID]
	if !ok {
		m.Logger().Warn(context.TODO(), "collection not found", mlog.Int64("collectionID", collectionID))
		return -1, ErrCollectionNotFound
	}
	// Missing schemaVersion means the proxy did not set it (old proxy or old SDK).
	// Skip the schema presence and version checks for backward compatibility during
	// rolling upgrades, where a legacy collection may still have Schema == nil when
	// an old proxy writes.
	if header.SchemaVersion == nil {
		return collectionInfo.SchemaVersion(), nil
	}

	if collectionInfo.Schema == nil || collectionInfo.Schema.GetSchema() == nil {
		m.Logger().Warn(context.TODO(), "collection schema not found", mlog.Int64("collectionID", collectionID))
		return -1, ErrCollectionSchemaNotFound
	}

	collectionSchemaVersion := collectionInfo.SchemaVersion()
	if collectionSchemaVersion != header.GetSchemaVersion() {
		m.Logger().Warn(context.TODO(), "collection schema version not match", mlog.Int64("collectionID", collectionID),
			mlog.Int32("schemaVersion", header.GetSchemaVersion()),
			mlog.Int32("collectionSchemaVersion", collectionSchemaVersion))
		return collectionSchemaVersion, ErrCollectionSchemaVersionNotMatch
	}

	return collectionSchemaVersion, nil
}

func (m *shardManagerImpl) GetCollectionSchema(collectionID int64, schemaVersion int32) (*schemapb.CollectionSchema, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	collectionInfo, ok := m.collections[collectionID]
	if !ok {
		return nil, ErrCollectionNotFound
	}
	if collectionInfo.Schema == nil || collectionInfo.Schema.GetSchema() == nil {
		return nil, ErrCollectionSchemaNotFound
	}
	collectionSchemaVersion := collectionInfo.SchemaVersion()
	if schemaVersion != latestCollectionSchemaVersion && collectionSchemaVersion != schemaVersion {
		return nil, ErrCollectionSchemaVersionNotMatch
	}

	return proto.Clone(collectionInfo.Schema.GetSchema()).(*schemapb.CollectionSchema), nil
}

// GetPrimaryKeyDescriptor returns immutable PK schema data without cloning the
// complete collection schema.
func (m *shardManagerImpl) GetPrimaryKeyDescriptor(collectionID int64, schemaVersion int32) (PrimaryKeyDescriptor, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	collectionInfo, ok := m.collections[collectionID]
	if !ok {
		return PrimaryKeyDescriptor{}, ErrCollectionNotFound
	}
	if collectionInfo.Schema == nil || collectionInfo.Schema.GetSchema() == nil {
		return PrimaryKeyDescriptor{}, ErrCollectionSchemaNotFound
	}
	collectionSchemaVersion := collectionInfo.SchemaVersion()
	if schemaVersion != latestCollectionSchemaVersion && collectionSchemaVersion != schemaVersion {
		return PrimaryKeyDescriptor{}, ErrCollectionSchemaVersionNotMatch
	}
	if collectionInfo.primaryKey != nil {
		return *collectionInfo.primaryKey, nil
	}
	return primaryKeyDescriptorFromSchema(collectionInfo.Schema.GetSchema())
}

func (m *shardManagerImpl) GetAllCollectionSchemaInfos() map[int64]CollectionSchemaInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	infos := make(map[int64]CollectionSchemaInfo)
	for collectionID, collectionInfo := range m.collections {
		if collectionInfo.Schema == nil || collectionInfo.Schema.GetSchema() == nil {
			continue
		}
		infos[collectionID] = CollectionSchemaInfo{
			VChannel: collectionInfo.VChannel,
			Schema:   proto.Clone(collectionInfo.Schema.GetSchema()).(*schemapb.CollectionSchema),
		}
	}
	return infos
}
