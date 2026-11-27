package shards

import (
	"context"

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

// CollectionVChannel returns the vchannel the collection is registered with.
func (m *shardManagerImpl) CollectionVChannel(collectionID int64) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info, ok := m.collections[collectionID]
	if !ok {
		return "", false
	}
	return info.VChannel, true
}

// SplitFenceChecker is implemented by a shard manager that can tell whether a
// vchannel is the source of a shard split. It is kept out of ShardManager so
// that the interface and its mock stay as they are; use IsSplitFenced.
type SplitFenceChecker interface {
	IsSplitFenced(vchannel string) bool
}

// IsSplitFenced reports whether the vchannel is the source of a shard split on
// the pchannel of m. A shard manager that cannot tell answers false.
func IsSplitFenced(m ShardManager, vchannel string) bool {
	checker, ok := m.(SplitFenceChecker)
	return ok && checker.IsSplitFenced(vchannel)
}

// IsSplitFenced reports whether a collection is registered with the vchannel
// on this pchannel and the vchannel is the source of a shard split.
func (m *shardManagerImpl) IsSplitFenced(vchannel string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, info := range m.collections {
		if info.VChannel == vchannel {
			return info.SplitFenced()
		}
	}
	return false
}

// FenceForSplit makes the vchannel the source of a shard split.
func (m *shardManagerImpl) FenceForSplit(collectionID int64, vchannel string, splitTaskID int64, timetick uint64) ([]int64, bool, error) {
	logger := m.Logger().With(mlog.FieldCollectionID(collectionID), mlog.FieldVChannel(vchannel),
		mlog.Int64("splitTaskID", splitTaskID), mlog.Uint64("timetick", timetick))

	m.mu.Lock()
	defer m.mu.Unlock()

	info, ok := m.collections[collectionID]
	if !ok || info.VChannel != vchannel {
		logger.Warn(m.ctx, "collection is not registered with the vchannel when fencing it for a shard split")
		return nil, false, ErrCollectionNotFound
	}
	if info.SplitFenced() {
		if info.SplitTaskID != splitTaskID {
			logger.Warn(m.ctx, "vchannel is already fenced by another shard split", mlog.Int64("fencedBy", info.SplitTaskID))
			return nil, false, ErrShardFenced
		}
		// A copy of the same split that is appended again. Nothing was written
		// since the first fence, so there is nothing to seal.
		logger.Info(m.ctx, "vchannel is already fenced by this shard split")
		return nil, true, nil
	}
	segmentIDs, err := m.flushAndFenceSegmentAllocUntil(collectionID, timetick)
	if err != nil {
		return nil, false, err
	}
	info.SplitTaskID = splitTaskID
	// An insert may be waiting for a segment whose create message is refused
	// from now on. Wake it; its next attempt is refused as fenced.
	for partitionID := range info.PartitionIDs {
		if pm, ok := m.partitionManagers[PartitionUniqueKey{CollectionID: collectionID, PartitionID: partitionID}]; ok {
			pm.CancelPendingAlloc()
		}
	}
	logger.Info(m.ctx, "vchannel fenced by a shard split, it accepts no more DML", mlog.Int64s("segmentIDs", segmentIDs))
	return segmentIDs, false, nil
}

// CreateCollection creates a new partition manager when create collection message is written into wal.
// After CreateCollection is called, the ddl and dml on the collection can be applied.
func (m *shardManagerImpl) CreateCollection(msg message.ImmutableCreateCollectionMessageV1) {
	collectionID := msg.Header().CollectionId
	partitionIDs := msg.Header().PartitionIds
	vchannel := msg.VChannel()
	timetick := msg.TimeTick()
	body := msg.MustBody()
	schema := body.GetCollectionSchema()
	if schema == nil && len(body.GetSchema()) > 0 {
		schema = messageutil.MustGetSchemaFromCreateCollectionMessageBody(body)
	}
	logger := m.Logger().With(mlog.FieldMessage(msg))

	m.mu.Lock()
	defer m.mu.Unlock()

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
		).withCollectionInfo(collectionInfo)
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

	collectionInfo := m.collections[collectionID]
	delete(m.collections, collectionID)
	// remove all partition and segment
	partitionIDs := make([]int64, 0, len(collectionInfo.PartitionIDs))
	segmentIDs := make([]int64, 0, len(collectionInfo.PartitionIDs))
	for partitionID := range collectionInfo.PartitionIDs {
		uniqueKey := PartitionUniqueKey{CollectionID: collectionID, PartitionID: partitionID}
		pm, ok := m.partitionManagers[uniqueKey]
		if !ok {
			logger.Warn(context.TODO(), "partition not exists", mlog.Int64("partitionID", partitionID))
			continue
		}
		// Flush all segments and fence assign to the partition manager.
		segments := pm.FlushAndDropPartition(policy.PolicyCollectionRemoved())
		partitionIDs = append(partitionIDs, partitionID)
		segmentIDs = append(segmentIDs, segments...)
		delete(m.partitionManagers, uniqueKey)
	}
	logger.Info(context.TODO(), "collection removed", mlog.Int64s("partitionIDs", partitionIDs), mlog.Int64s("segmentIDs", segmentIDs))
	m.updateMetrics()
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

	// Only encoding/schema changes flush and fence segment allocation. Metadata
	// updates still replace the current schema snapshot under the same lock.
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
	}
	collectionInfo := m.collections[collectionID]
	if schema := messageutil.ApplyCollectionSchemaUpdates(collectionInfo.Schema.GetSchema(), header, msg.MustBody()); schema != nil {
		collectionInfo.setSchema(&streamingpb.CollectionSchemaOfVChannel{
			Schema:             schema,
			CheckpointTimeTick: timetick,
			State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
		})
	}

	return segmentIDs, nil
}

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
	// An insert is refused before anything else is checked if the vchannel is
	// the source of a shard split: its rows belong to the new shards.
	if collectionInfo.SplitFenced() {
		return -1, ErrShardFenced
	}
	// Missing schemaVersion means the proxy did not set it (old proxy or old SDK).
	// Skip the schema presence and version checks for backward compatibility during
	// rolling upgrades. A collection without a schema is unreachable on supported
	// paths: CreateCollection carries the schema and vchannel recovery meta
	// persists it. With partial-update CAS enabled, partialupdate rejects such an
	// insert before the WAL append: the row-lock hook does so before this check for
	// a non-transactional insert, and the partialupdate interceptor after it for a
	// transaction body or a replicated insert (see the 20260601 partial-update
	// optimistic lock design doc).
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

func (m *shardManagerImpl) GetCollectionSchemaSnapshot(collectionID int64) *schemapb.CollectionSchema {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if collection := m.collections[collectionID]; collection != nil {
		return collection.Schema.GetSchema()
	}
	return nil
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

// GetWriteSchema captures the same schema used by insert admission. Recovery's
// observer may still be behind this write-path state after an ALTER append.
func (m *shardManagerImpl) GetWriteSchema(collectionID int64, vchannel string) (*streamingpb.CollectionSchemaOfVChannel, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	collection, ok := m.collections[collectionID]
	if !ok || collection.VChannel != vchannel {
		return nil, status.NewUnrecoverableError("collection %d is not present on vchannel %s", collectionID, vchannel)
	}
	if collection.Schema.GetSchema() == nil {
		return nil, status.NewInner("write schema for collection %d is not ready", collectionID)
	}
	return proto.Clone(collection.Schema).(*streamingpb.CollectionSchemaOfVChannel), nil
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

// GetAllCollectionSchemaInfos returns a new map containing immutable schema
// snapshots. Callers must not mutate the schemas or their nested values.
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
			Schema:   collectionInfo.Schema.GetSchema(),
		}
	}
	return infos
}
