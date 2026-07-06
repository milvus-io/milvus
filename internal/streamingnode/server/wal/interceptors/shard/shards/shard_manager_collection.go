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

// CheckIfVChannelCanBeWritten checks if the vchannel of the collection still accepts new DML.
func (m *shardManagerImpl) CheckIfVChannelCanBeWritten(collectionID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.checkIfVChannelCanBeWritten(collectionID)
}

// GetSplitTimeTick returns T_switch (the time tick the collection's vchannel was
// fenced at by shard split), or 0 if the collection is unknown or not fenced.
func (m *shardManagerImpl) GetSplitTimeTick(collectionID int64) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	if info, ok := m.collections[collectionID]; ok {
		return info.SplitTimeTick
	}
	return 0
}

// checkIfVChannelCanBeWritten checks if the vchannel of the collection still accepts new DML.
func (m *shardManagerImpl) checkIfVChannelCanBeWritten(collectionID int64) error {
	collectionInfo, ok := m.collections[collectionID]
	if !ok {
		return ErrCollectionNotFound
	}
	if collectionInfo.State == streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED {
		return ErrVChannelFenced
	}
	return nil
}

// SplitShard marks the vchannel of the collection as splitted (fenced) when
// a SplitShard message is written into the wal.
// The growing segments have been sealed by the ManualFlush message written
// right before the SplitShard message; here only the fence state flips.
func (m *shardManagerImpl) SplitShard(msg message.ImmutableSplitShardMessageV2) {
	collectionID := msg.Header().CollectionId
	logger := m.Logger().With(mlog.FieldMessage(msg))

	m.mu.Lock()
	defer m.mu.Unlock()

	collectionInfo, ok := m.collections[collectionID]
	if !ok {
		logger.Warn(context.TODO(), "collection not exists when splitting shard", mlog.Int64("collectionID", collectionID))
		return
	}
	if collectionInfo.State == streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED {
		// idempotent, only the first split message takes effect.
		return
	}
	collectionInfo.State = streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED
	// record T_switch so an already-fenced re-fence can return it; the split
	// coordinator recovers T_switch from here after a crash that lost it.
	collectionInfo.SplitTimeTick = msg.TimeTick()
	logger.Info(context.TODO(), "vchannel is fenced by shard split",
		mlog.Int64("collectionID", collectionID),
		mlog.Int64("splitTaskID", msg.Header().GetSplitTaskId()),
		mlog.Uint64("timetick", msg.TimeTick()))
}

// CreateCollection creates a new partition manager when create collection message is written into wal.
// After CreateCollection is called, the ddl and dml on the collection can be applied.
func (m *shardManagerImpl) CreateCollection(msg message.ImmutableCreateCollectionMessageV1) {
	logger := m.Logger().With(mlog.FieldMessage(msg))
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createCollectionLocked(msg.Header().CollectionId, msg.Header().PartitionIds, msg.VChannel(),
		msg.TimeTick(), msg.MustBody().GetCollectionSchema(), logger)
}

// CreateVChannel registers a shard split target vchannel. CreateVChannel is
// the genesis message of the target vchannel and shares the CreateCollection
// body shape, so it registers the collection for DML and segment assignment
// on this pchannel exactly as CreateCollection does.
func (m *shardManagerImpl) CreateVChannel(msg message.ImmutableCreateVChannelMessageV2) {
	logger := m.Logger().With(mlog.FieldMessage(msg))
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createCollectionLocked(msg.Header().CollectionId, msg.Header().PartitionIds, msg.VChannel(),
		msg.TimeTick(), msg.MustBody().GetCollectionSchema(), logger)
}

// createCollectionLocked registers the collection and its partition managers on
// this pchannel for DML and segment assignment. The caller must hold m.mu.
func (m *shardManagerImpl) createCollectionLocked(collectionID int64, partitionIDs []int64, vchannel string, timetick uint64, schema *schemapb.CollectionSchema, logger *mlog.Logger) {
	if err := m.checkIfCollectionCanBeCreated(collectionID); err != nil {
		logger.Warn(context.TODO(), "collection already exists")
		return
	}

	collectionInfo := newCollectionInfo(vchannel, partitionIDs)
	// Set schema when creating collection
	if schema != nil {
		collectionInfo.Schema = &streamingpb.CollectionSchemaOfVChannel{
			Schema:             schema,
			CheckpointTimeTick: timetick,
			State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
		}
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
		collectionInfo.Schema = &streamingpb.CollectionSchemaOfVChannel{
			Schema:             schema,
			CheckpointTimeTick: timetick,
			State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
		}
		logger.Info(context.TODO(), "updated collection schema in shard manager",
			mlog.Int64("collectionID", collectionID),
			mlog.Int32("schemaVersion", schema.GetVersion()),
			mlog.Uint64("checkpointTimeTick", timetick))
	}

	return segmentIDs, nil
}

func (m *shardManagerImpl) CheckIfCollectionSchemaVersionMatch(header *message.InsertMessageHeader) (int32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

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
	m.mu.Lock()
	defer m.mu.Unlock()

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

func (m *shardManagerImpl) GetAllCollectionSchemaInfos() map[int64]CollectionSchemaInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

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
