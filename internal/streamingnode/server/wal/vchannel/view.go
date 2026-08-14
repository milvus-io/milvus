package vchannel

import (
	"math"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
)

// NewVChannelViewFromMeta creates a VChannelView from persisted vchannel meta.
func NewVChannelViewFromMeta(meta *streamingpb.VChannelMeta, configs ...runtimeConfig) *VChannelView {
	return NewVChannelView(
		meta,
		meta.GetCheckpointTimeTick(),
		false,
		firstRuntimeConfig(configs),
	)
}

func newVChannelViewFromOwnedMeta(meta *streamingpb.VChannelMeta) *VChannelView {
	return newVChannelView(meta, meta.GetCheckpointTimeTick(), false, runtimeConfig{})
}

func NewVChannelView(
	meta *streamingpb.VChannelMeta,
	persistedMetaTimeTick uint64,
	dirty bool,
	config runtimeConfig,
) *VChannelView {
	return newVChannelView(proto.Clone(meta).(*streamingpb.VChannelMeta), persistedMetaTimeTick, dirty, config)
}

func newVChannelView(
	meta *streamingpb.VChannelMeta,
	persistedMetaTimeTick uint64,
	dirty bool,
	config runtimeConfig,
) *VChannelView {
	return &VChannelView{
		meta:                  meta,
		persistedMetaTimeTick: persistedMetaTimeTick,
		dirty:                 dirty,
		schemaDirty:           dirty,
		metaAndData:           config.metaAndData,
	}
}

// NewVChannelMetaFromCreateCollectionMessage creates a new vchannel meta from a create collection message.
func NewVChannelMetaFromCreateCollectionMessage(msg message.ImmutableCreateCollectionMessageV1) *streamingpb.VChannelMeta {
	partitions := make([]*streamingpb.PartitionInfoOfVChannel, 0, len(msg.Header().PartitionIds))
	for _, partitionId := range msg.Header().PartitionIds {
		partitions = append(partitions, &streamingpb.PartitionInfoOfVChannel{
			PartitionId: partitionId,
			State:       streamingpb.PartitionState_PARTITION_STATE_NORMAL,
		})
	}
	body := msg.MustBody()
	schema := messageutil.MustGetSchemaFromCreateCollectionMessageBody(body)
	return &streamingpb.VChannelMeta{
		Vchannel: msg.VChannel(),
		State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: msg.Header().CollectionId,
			Partitions:   partitions,
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             schema,
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					CheckpointTimeTick: msg.TimeTick(),
				},
			},
		},
		CheckpointTimeTick: msg.TimeTick(),
	}
}

// VChannelView tracks the metadata and durability state of a vchannel.
type VChannelView struct {
	mu sync.Mutex

	meta                             *streamingpb.VChannelMeta
	persistedMetaTimeTick            uint64
	dirty                            bool // whether the current vchannel recovery info still needs catalog persistence.
	schemaDirty                      bool
	pendingDirtySnapshot             *streamingpb.VChannelMeta
	pendingDirtySnapshotSavesSchemas bool

	metaAndData bool
}

// NewVChannelViewFromCreateCollectionMessage creates a vchannel-level recovery
// view from a CreateCollection WAL message.
func NewVChannelViewFromCreateCollectionMessage(msg message.ImmutableCreateCollectionMessageV1) *VChannelView {
	return NewVChannelView(NewVChannelMetaFromCreateCollectionMessage(msg), 0, true, runtimeConfig{})
}

func (info *VChannelView) ObserveCreateCollectionMessageV1(msg message.ImmutableCreateCollectionMessageV1) (*VChannelView, bool) {
	decision := info.ObserveExistingCreateCollectionMessageV1(msg)
	if decision != existingCreateCollectionStartNew {
		return nil, false
	}
	info.mu.Lock()
	metaAndData := info.metaAndData
	info.mu.Unlock()
	return NewVChannelView(NewVChannelMetaFromCreateCollectionMessage(msg), 0, true, runtimeConfig{metaAndData: metaAndData}), true
}

func (info *VChannelView) AssignmentMeta() *streamingpb.VChannelMeta {
	info.mu.Lock()
	defer info.mu.Unlock()
	return proto.Clone(info.meta).(*streamingpb.VChannelMeta)
}

func (info *VChannelView) WritePathRecoveryState() (moduleapi.VChannelWritePathRecoveryState, bool) {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL || info.meta.GetCollectionInfo() == nil {
		return moduleapi.VChannelWritePathRecoveryState{}, false
	}
	collection := info.meta.GetCollectionInfo()
	state := moduleapi.VChannelWritePathRecoveryState{
		VChannel:     info.meta.GetVchannel(),
		CollectionID: collection.GetCollectionId(),
		PartitionIDs: make([]int64, 0, len(collection.GetPartitions())),
	}
	for _, partition := range collection.GetPartitions() {
		state.PartitionIDs = append(state.PartitionIDs, partition.GetPartitionId())
	}
	if schemas := collection.GetSchemas(); len(schemas) > 0 && schemas[len(schemas)-1].GetSchema() != nil {
		state.Schema = proto.Clone(schemas[len(schemas)-1].GetSchema()).(*schemapb.CollectionSchema)
	}
	return state, true
}

func (info *VChannelView) Name() string {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.meta.GetVchannel()
}

func (info *VChannelView) HasDirty() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.dirty
}

func (info *VChannelView) markMetaPersistedLocked(timetick uint64) {
	if timetick > info.persistedMetaTimeTick {
		info.persistedMetaTimeTick = timetick
	}
}

func (info *VChannelView) MarkSnapshotPersisted(snapshot *streamingpb.VChannelMeta) {
	info.mu.Lock()
	defer info.mu.Unlock()
	info.markMetaPersistedLocked(snapshot.GetCheckpointTimeTick())
	if info.pendingDirtySnapshot != nil && proto.Equal(info.pendingDirtySnapshot, snapshot) {
		if info.pendingDirtySnapshotSavesSchemas && vchannelSchemasEqual(info.meta, snapshot) {
			info.schemaDirty = false
		}
		info.pendingDirtySnapshot = nil
		info.pendingDirtySnapshotSavesSchemas = false
	}
	info.dirty = !proto.Equal(info.meta, snapshot)
}

func (info *VChannelView) TryFinalizeTombstone(dataCheckpointTimeTick uint64) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.maybeMarkTombstonedLocked(dataCheckpointTimeTick)
}

func (info *VChannelView) HasReadyTombstoneFinalize(dataCheckpointTimeTick uint64) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.vchannelTombstoneFinalizeReadyLocked(dataCheckpointTimeTick) {
		return true
	}
	for _, partition := range info.meta.GetCollectionInfo().GetPartitions() {
		if info.partitionTombstoneFinalizeReadyLocked(partition, dataCheckpointTimeTick) {
			return true
		}
	}
	return false
}

func (info *VChannelView) SwitchIntoMetaAndData() {
	info.mu.Lock()
	defer info.mu.Unlock()
	info.metaAndData = true
}

// IsActive returns true if the vchannel is active.
func (info *VChannelView) IsActive() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_NORMAL
}

type existingCreateCollectionDecision int

const (
	existingCreateCollectionIgnored existingCreateCollectionDecision = iota
	existingCreateCollectionStartNew
	existingCreateCollectionInconsistent
)

func (info *VChannelView) ObserveExistingCreateCollectionMessageV1(msg message.ImmutableCreateCollectionMessageV1) existingCreateCollectionDecision {
	info.mu.Lock()
	defer info.mu.Unlock()
	timetick := msg.TimeTick()
	if shouldSkipTombstonedVChannelMeta(info.meta, timetick) {
		return existingCreateCollectionIgnored
	}
	if info.canStartNewCollectionAtLocked(timetick) {
		return existingCreateCollectionStartNew
	}
	if !info.canReplayAtLocked(timetick) {
		return existingCreateCollectionIgnored
	}
	return existingCreateCollectionInconsistent
}

func (info *VChannelView) canStartNewCollectionAtLocked(timetick uint64) bool {
	return info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED &&
		info.meta.GetTombstoneTimeTick() > 0 &&
		timetick > info.meta.GetTombstoneTimeTick()
}

func (info *VChannelView) canReplayAtLocked(timetick uint64) bool {
	if shouldSkipTombstonedVChannelMeta(info.meta, timetick) {
		return false
	}
	if info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
		return true
	}
	return timetick <= info.meta.GetCheckpointTimeTick()
}

func (info *VChannelView) CanObserveActiveAt(timetick uint64) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.canReplayAtLocked(timetick) &&
		info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_NORMAL
}

func (info *VChannelView) canReplayExistingPartitionAtLocked(partitionID int64, timetick uint64) bool {
	if partitionID == common.AllPartitionsID {
		return true
	}
	return info.canReplayPartitionAtLocked(partitionID, timetick) && info.hasPartitionMetaLocked(partitionID)
}

func (info *VChannelView) canReplayPartitionAtLocked(partitionID int64, timetick uint64) bool {
	for _, partition := range info.meta.GetCollectionInfo().GetPartitions() {
		if partition.GetPartitionId() != partitionID {
			continue
		}
		switch partition.GetState() {
		case streamingpb.PartitionState_PARTITION_STATE_NORMAL:
			return true
		case streamingpb.PartitionState_PARTITION_STATE_DROPPED:
			return timetick <= partition.GetTombstoneTimeTick()
		default:
			return false
		}
	}
	return true
}

func (info *VChannelView) hasPartitionMetaLocked(partitionID int64) bool {
	for _, partition := range info.meta.GetCollectionInfo().GetPartitions() {
		if partition.GetPartitionId() == partitionID {
			return true
		}
	}
	return false
}

func (info *VChannelView) CreateSegmentSchema(partitionID int64, timetick uint64) *schemapb.CollectionSchema {
	info.mu.Lock()
	defer info.mu.Unlock()
	if !info.canReplayAtLocked(timetick) || !info.canReplayPartitionAtLocked(partitionID, timetick) {
		return nil
	}
	if !info.hasPartitionMetaLocked(partitionID) {
		return nil
	}
	_, schema := info.GetSchemaLocked(timetick)
	return schema
}

func (info *VChannelView) TombstonedCleanupPlan(
	metaPhysicalTimeTick uint64,
	dataPhysicalTimeTick uint64,
	persistedDataTimeTick uint64,
) (dropSnapshot *streamingpb.VChannelMeta, cleanupPartitions map[int64]uint64) {
	info.mu.Lock()
	defer info.mu.Unlock()

	if info.vchannelTombstonedCleanupReadyLocked(metaPhysicalTimeTick, dataPhysicalTimeTick, persistedDataTimeTick) {
		return proto.Clone(info.meta).(*streamingpb.VChannelMeta), nil
	}
	if info.dirty {
		return nil, nil
	}
	for _, partition := range info.meta.GetCollectionInfo().GetPartitions() {
		if partitionTombstonedCleanupReady(partition, metaPhysicalTimeTick, dataPhysicalTimeTick) &&
			info.persistedMetaTimeTick >= partition.GetTombstoneTimeTick() &&
			persistedDataTimeTick >= partition.GetTombstoneTimeTick() {
			if cleanupPartitions == nil {
				cleanupPartitions = make(map[int64]uint64)
			}
			cleanupPartitions[partition.GetPartitionId()] = partition.GetTombstoneTimeTick()
		}
	}
	if len(cleanupPartitions) == 0 {
		return nil, nil
	}
	return nil, cleanupPartitions
}

func (info *VChannelView) vchannelTombstonedCleanupReadyLocked(
	metaPhysicalTimeTick uint64,
	dataPhysicalTimeTick uint64,
	persistedDataTimeTick uint64,
) bool {
	tombstoneTimeTick := info.meta.GetTombstoneTimeTick()
	return info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED &&
		tombstoneTimeTick > 0 &&
		!info.dirty &&
		info.persistedMetaTimeTick >= tombstoneTimeTick &&
		persistedDataTimeTick >= tombstoneTimeTick &&
		metaPhysicalTimeTick > tombstoneTimeTick &&
		dataPhysicalTimeTick > tombstoneTimeTick
}

func (info *VChannelView) VChannelDropCleanupSnapshot(tombstoneTimeTick uint64, persistedDataTimeTick uint64) *streamingpb.VChannelMeta {
	info.mu.Lock()
	defer info.mu.Unlock()

	if info.meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED ||
		tombstoneTimeTick == 0 ||
		info.meta.GetTombstoneTimeTick() != tombstoneTimeTick ||
		info.dirty ||
		info.persistedMetaTimeTick < tombstoneTimeTick ||
		persistedDataTimeTick < tombstoneTimeTick {
		return nil
	}
	return proto.Clone(info.meta).(*streamingpb.VChannelMeta)
}

func (info *VChannelView) ApplyPartitionCleanup(partitionIDs map[int64]uint64) bool {
	info.mu.Lock()
	defer info.mu.Unlock()

	cleanup := info.partitionCleanupPlanLocked(partitionIDs)
	if len(cleanup) == 0 {
		return false
	}
	partitions := info.meta.GetCollectionInfo().GetPartitions()
	remaining := partitions[:0]
	for _, partition := range partitions {
		if _, ok := cleanup[partition.GetPartitionId()]; ok {
			continue
		}
		remaining = append(remaining, partition)
	}
	info.meta.CollectionInfo.Partitions = remaining
	info.dirty = true
	return true
}

func (info *VChannelView) PartitionCleanupPlan(partitionIDs map[int64]uint64) map[int64]uint64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.partitionCleanupPlanLocked(partitionIDs)
}

func (info *VChannelView) partitionCleanupPlanLocked(partitionIDs map[int64]uint64) map[int64]uint64 {
	actualCleanup := make(map[int64]uint64)
	for _, partition := range info.meta.GetCollectionInfo().GetPartitions() {
		if tombstoneTimeTick, ok := partitionIDs[partition.GetPartitionId()]; ok &&
			partition.GetState() == streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED &&
			partition.GetTombstoneTimeTick() == tombstoneTimeTick {
			actualCleanup[partition.GetPartitionId()] = tombstoneTimeTick
		}
	}
	return actualCleanup
}

// GetSchema returns the schema of the vchannel at the given timetick.
// return nil if the schema is not found.
func (info *VChannelView) GetSchema(timetick uint64) (int, *schemapb.CollectionSchema) {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.GetSchemaLocked(timetick)
}

func (info *VChannelView) GetSchemaLocked(timetick uint64) (int, *schemapb.CollectionSchema) {
	if info.meta.GetCollectionInfo() == nil {
		return -1, nil
	}
	if timetick == 0 {
		// timetick 0 means the latest schema.
		timetick = math.MaxUint64
	}

	for i := len(info.meta.CollectionInfo.Schemas) - 1; i >= 0; i-- {
		schema := info.meta.CollectionInfo.Schemas[i]
		if schema.CheckpointTimeTick <= timetick {
			return i, schema.Schema
		}
	}
	return -1, nil
}

func (info *VChannelView) ObserveSchemaChangeMessageV2(msg message.ImmutableSchemaChangeMessageV2) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	if msg.TimeTick() <= info.meta.CheckpointTimeTick {
		return false
	}
	if info.meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
		return false
	}
	if info.hasSchemaVersionLocked(msg.TimeTick(), msg.MustBody().Schema) {
		return false
	}

	info.meta.CollectionInfo.Schemas = append(info.meta.CollectionInfo.Schemas, &streamingpb.CollectionSchemaOfVChannel{
		Schema:             msg.MustBody().Schema,
		State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
		CheckpointTimeTick: msg.TimeTick(),
	})
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
	info.schemaDirty = true
	return true
}

func (info *VChannelView) ObserveAlterCollectionMessageV2(msg message.ImmutableAlterCollectionMessageV2) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	if msg.TimeTick() <= info.meta.CheckpointTimeTick {
		return false
	}
	if info.meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
		return false
	}
	schemaChange := messageutil.IsSchemaChange(msg.Header())
	if schemaChange {
		schema := msg.MustBody().Updates.Schema
		if info.hasSchemaVersionLocked(msg.TimeTick(), schema) {
			return false
		}
		info.meta.CollectionInfo.Schemas = append(info.meta.CollectionInfo.Schemas, &streamingpb.CollectionSchemaOfVChannel{
			Schema:             schema,
			State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
			CheckpointTimeTick: msg.TimeTick(),
		})
		info.schemaDirty = true
	}
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
	return true
}

func (info *VChannelView) hasSchemaVersionLocked(timetick uint64, schema *schemapb.CollectionSchema) bool {
	for _, existing := range info.meta.GetCollectionInfo().GetSchemas() {
		if existing.GetCheckpointTimeTick() == timetick && proto.Equal(existing.GetSchema(), schema) {
			return true
		}
	}
	return false
}

func (info *VChannelView) ObserveDropCollectionMessageV1(msg message.ImmutableDropCollectionMessageV1) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	if msg.TimeTick() <= info.meta.CheckpointTimeTick {
		return false
	}
	if isVChannelClosed(info.meta.GetState()) {
		// make it idempotent, only the first drop collection message can be observed.
		return false
	}
	info.meta.State = streamingpb.VChannelState_VCHANNEL_STATE_DROPPED
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
	return true
}

func (info *VChannelView) ObserveTruncateCollectionMessageV2(msg message.ImmutableTruncateCollectionMessageV2) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	if msg.TimeTick() <= info.meta.CheckpointTimeTick {
		return false
	}
	if info.meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
		return false
	}
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
	return true
}

func (info *VChannelView) ObserveDropPartitionMessageV1(msg message.ImmutableDropPartitionMessageV1) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	if msg.TimeTick() <= info.meta.CheckpointTimeTick {
		return false
	}
	for _, partition := range info.meta.CollectionInfo.Partitions {
		if partition.PartitionId == msg.Header().PartitionId {
			// make it idempotent, only the first drop partition message can be observed.
			if !isPartitionNormal(partition.GetState()) {
				return false
			}
			partition.State = streamingpb.PartitionState_PARTITION_STATE_DROPPED
			partition.TombstoneTimeTick = msg.TimeTick()
			info.meta.CheckpointTimeTick = msg.TimeTick()
			info.dirty = true
			return true
		}
	}
	return false
}

func (info *VChannelView) ObserveCreatePartitionMessageV1(msg message.ImmutableCreatePartitionMessageV1) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	if msg.TimeTick() <= info.meta.CheckpointTimeTick {
		return false
	}
	if info.meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
		return false
	}
	for _, partition := range info.meta.CollectionInfo.Partitions {
		if partition.PartitionId == msg.Header().PartitionId {
			if partition.GetState() != streamingpb.PartitionState_PARTITION_STATE_NORMAL {
				partition.State = streamingpb.PartitionState_PARTITION_STATE_NORMAL
				partition.TombstoneTimeTick = 0
				info.meta.CheckpointTimeTick = msg.TimeTick()
				info.dirty = true
				return true
			}
			return false
		}
	}
	info.meta.CollectionInfo.Partitions = append(info.meta.CollectionInfo.Partitions, &streamingpb.PartitionInfoOfVChannel{
		PartitionId: msg.Header().PartitionId,
		State:       streamingpb.PartitionState_PARTITION_STATE_NORMAL,
	})
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
	return true
}

// ConsumeDirtyAndGetSnapshot returns the current stable dirty snapshot of the
// vchannel recovery info. It is not a queue pop: until MarkSnapshotPersisted is
// called, repeated calls keep returning the same in-flight snapshot.
func (info *VChannelView) ConsumeDirtyAndGetSnapshot() (*streamingpb.VChannelMeta, bool) {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.pendingDirtySnapshot != nil {
		return proto.Clone(info.pendingDirtySnapshot).(*streamingpb.VChannelMeta), info.pendingDirtySnapshotSavesSchemas
	}
	if !info.dirty {
		return nil, false
	}
	info.pendingDirtySnapshot = proto.Clone(info.meta).(*streamingpb.VChannelMeta)
	info.pendingDirtySnapshotSavesSchemas = info.schemaDirty
	return proto.Clone(info.pendingDirtySnapshot).(*streamingpb.VChannelMeta), info.pendingDirtySnapshotSavesSchemas
}

func vchannelSchemasEqual(left, right *streamingpb.VChannelMeta) bool {
	leftSchemas := left.GetCollectionInfo().GetSchemas()
	rightSchemas := right.GetCollectionInfo().GetSchemas()
	if len(leftSchemas) != len(rightSchemas) {
		return false
	}
	for i := range leftSchemas {
		if !proto.Equal(leftSchemas[i], rightSchemas[i]) {
			return false
		}
	}
	return true
}

func (info *VChannelView) maybeMarkTombstonedLocked(dataCheckpointTimeTick uint64) bool {
	changed := false
	if info.vchannelTombstoneFinalizeReadyLocked(dataCheckpointTimeTick) {
		tombstoneTimeTick := info.meta.GetCheckpointTimeTick()
		info.meta.State = streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED
		info.meta.TombstoneTimeTick = tombstoneTimeTick
		info.dirty = true
		changed = true
	}
	for _, partition := range info.meta.GetCollectionInfo().GetPartitions() {
		if !info.partitionTombstoneFinalizeReadyLocked(partition, dataCheckpointTimeTick) {
			continue
		}
		partition.State = streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED
		info.dirty = true
		changed = true
	}
	return changed
}

func (info *VChannelView) vchannelTombstoneFinalizeReadyLocked(dataCheckpointTimeTick uint64) bool {
	tombstoneTimeTick := info.meta.GetCheckpointTimeTick()
	return info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED &&
		tombstoneTimeTick > 0 &&
		dataCheckpointTimeTick >= tombstoneTimeTick
}

func (info *VChannelView) partitionTombstoneFinalizeReadyLocked(partition *streamingpb.PartitionInfoOfVChannel, dataCheckpointTimeTick uint64) bool {
	tombstoneTimeTick := partition.GetTombstoneTimeTick()
	return partition.GetState() == streamingpb.PartitionState_PARTITION_STATE_DROPPED &&
		tombstoneTimeTick > 0 &&
		dataCheckpointTimeTick >= tombstoneTimeTick
}

func isVChannelClosed(state streamingpb.VChannelState) bool {
	return state == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED ||
		state == streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED
}

func isPartitionNormal(state streamingpb.PartitionState) bool {
	return state == streamingpb.PartitionState_PARTITION_STATE_NORMAL
}

func shouldSkipTombstonedVChannelMeta(meta *streamingpb.VChannelMeta, timetick uint64) bool {
	return meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED &&
		meta.GetTombstoneTimeTick() > 0 &&
		timetick <= meta.GetTombstoneTimeTick()
}

func partitionTombstonedCleanupReady(meta *streamingpb.PartitionInfoOfVChannel, metaPhysicalTimeTick uint64, dataPhysicalTimeTick uint64) bool {
	tombstoneTimeTick := meta.GetTombstoneTimeTick()
	return meta.GetState() == streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED &&
		tombstoneTimeTick > 0 &&
		metaPhysicalTimeTick > tombstoneTimeTick &&
		dataPhysicalTimeTick > tombstoneTimeTick
}
