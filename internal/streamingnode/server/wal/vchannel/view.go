package vchannel

import (
	"math"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
)

// NewVChannelViewFromMeta creates a VChannelView from persisted vchannel meta.
func NewVChannelViewFromMeta(meta *streamingpb.VChannelMeta) *VChannelView {
	return NewVChannelView(
		meta,
		meta.GetCheckpointTimeTick(),
		false,
	)
}

func newVChannelViewFromOwnedMeta(meta *streamingpb.VChannelMeta) *VChannelView {
	return newVChannelView(meta, meta.GetCheckpointTimeTick(), false)
}

func NewVChannelView(
	meta *streamingpb.VChannelMeta,
	persistedMetaTimeTick uint64,
	dirty bool,
) *VChannelView {
	return newVChannelView(proto.Clone(meta).(*streamingpb.VChannelMeta), persistedMetaTimeTick, dirty)
}

func newVChannelView(
	meta *streamingpb.VChannelMeta,
	persistedMetaTimeTick uint64,
	dirty bool,
) *VChannelView {
	return &VChannelView{
		meta:                          meta,
		persistedMetaTimeTick:         persistedMetaTimeTick,
		persistedMaterializedTimeTick: meta.GetTransformMaterializedTimeTick(),
		dirty:                         dirty,
		schemaDirty:                   dirty,
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
	persistedMaterializedTimeTick    uint64 // the transform materialization frontier already stored in the catalog.
	dirty                            bool   // whether the current vchannel recovery info still needs catalog persistence.
	schemaDirty                      bool
	pendingDirtySnapshot             *streamingpb.VChannelMeta
	pendingDirtySnapshotSavesSchemas bool
}

// NewVChannelViewFromCreateCollectionMessage creates a vchannel-level recovery
// view from a CreateCollection WAL message.
func NewVChannelViewFromCreateCollectionMessage(msg message.ImmutableCreateCollectionMessageV1) *VChannelView {
	return NewVChannelView(NewVChannelMetaFromCreateCollectionMessage(msg), 0, true)
}

func (info *VChannelView) ObserveCreateCollectionMessageV1(msg message.ImmutableCreateCollectionMessageV1) (*VChannelView, bool) {
	decision := info.ObserveExistingCreateCollectionMessageV1(msg)
	if decision != existingCreateCollectionStartNew {
		return nil, false
	}
	return NewVChannelView(NewVChannelMetaFromCreateCollectionMessage(msg), 0, true), true
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

func (info *VChannelView) markMetaPersistedLocked(timetick uint64) {
	if timetick > info.persistedMetaTimeTick {
		info.persistedMetaTimeTick = timetick
	}
}

// SetTransformMaterializedTimeTick advances the transform materialization
// frontier held in the vchannel meta and marks the snapshot dirty, so the
// frontier persists with the next catalog checkpoint. It is called by the
// transform consumer after every committed materialization batch.
func (info *VChannelView) SetTransformMaterializedTimeTick(timetick uint64) {
	info.mu.Lock()
	defer info.mu.Unlock()
	if timetick > info.meta.GetTransformMaterializedTimeTick() {
		info.meta.TransformMaterializedTimeTick = timetick
		info.dirty = true
	}
}

// PersistedMaterializedTimeTick returns the transform materialization frontier
// already stored in the recovery catalog.
func (info *VChannelView) PersistedMaterializedTimeTick() uint64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.persistedMaterializedTimeTick
}

// MaterializedTimeTick returns the transform materialization frontier held in
// the vchannel meta, i.e. the frontier that is (or will be) persisted with the
// next catalog checkpoint. It is the largest timetick whose delete data of
// this vchannel is durably materialized as L0 output; the vchannel-level flush
// checkpoint must not advance past it while delete records are outstanding.
func (info *VChannelView) MaterializedTimeTick() uint64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.meta.GetTransformMaterializedTimeTick()
}

func (info *VChannelView) MarkSnapshotPersisted(snapshot *streamingpb.VChannelMeta) {
	info.mu.Lock()
	defer info.mu.Unlock()
	info.markMetaPersistedLocked(snapshot.GetCheckpointTimeTick())
	if materialized := snapshot.GetTransformMaterializedTimeTick(); materialized > info.persistedMaterializedTimeTick {
		info.persistedMaterializedTimeTick = materialized
	}
	if info.pendingDirtySnapshot != nil && proto.Equal(info.pendingDirtySnapshot, snapshot) {
		if info.pendingDirtySnapshotSavesSchemas && vchannelSchemasEqual(info.meta, snapshot) {
			info.schemaDirty = false
		}
		info.pendingDirtySnapshot = nil
		info.pendingDirtySnapshotSavesSchemas = false
	}
	info.dirty = !proto.Equal(info.meta, snapshot)
}

func (info *VChannelView) TryFinalizeTombstone(checkpointTimeTick uint64) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.maybeMarkTombstonedLocked(checkpointTimeTick)
}

// HasCleanupCandidate reports whether the retained replay fence may need a
// future tombstone transition or catalog cleanup.
func (info *VChannelView) HasCleanupCandidate() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	if isVChannelClosed(info.meta.GetState()) {
		return true
	}
	for _, partition := range info.meta.GetCollectionInfo().GetPartitions() {
		if partition.GetState() == streamingpb.PartitionState_PARTITION_STATE_DROPPED ||
			partition.GetState() == streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED {
			return true
		}
	}
	return false
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
	if info.canStartNewCollectionAtLocked(timetick) {
		return existingCreateCollectionStartNew
	}
	if !info.canObserveAtLocked(timetick) {
		return existingCreateCollectionIgnored
	}
	return existingCreateCollectionInconsistent
}

func (info *VChannelView) canStartNewCollectionAtLocked(timetick uint64) bool {
	return info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED &&
		info.meta.GetCheckpointTimeTick() > 0 &&
		timetick > info.meta.GetCheckpointTimeTick()
}

// shouldObserveLocked is the one-pass observation watermark: every message is
// delivered exactly once from the WAL, so anything at or before the consumed
// checkpoint is skipped.
func (info *VChannelView) shouldObserveLocked(timetick uint64) bool {
	return timetick > info.meta.GetCheckpointTimeTick()
}

func (info *VChannelView) canObserveAtLocked(timetick uint64) bool {
	switch info.meta.GetState() {
	case streamingpb.VChannelState_VCHANNEL_STATE_NORMAL:
		// A normal vchannel observes every message.
		return true
	case streamingpb.VChannelState_VCHANNEL_STATE_DROPPED:
		// A dropped vchannel observes only through its checkpoint; data
		// beyond it belongs to the dropped lifetime and is filtered.
		return timetick <= info.meta.GetCheckpointTimeTick()
	default:
		// TOMBSTONED: data has caught up, the retained meta is a pure
		// observation filter, so nothing is observed (a new collection may
		// start beyond the checkpoint, see canStartNewCollectionAtLocked).
		return false
	}
}

func (info *VChannelView) canObservePartitionAtLocked(partitionID int64, timetick uint64) bool {
	for _, partition := range info.meta.GetCollectionInfo().GetPartitions() {
		if partition.GetPartitionId() != partitionID {
			continue
		}
		switch partition.GetState() {
		case streamingpb.PartitionState_PARTITION_STATE_NORMAL:
			return true
		case streamingpb.PartitionState_PARTITION_STATE_DROPPED:
			return timetick <= partition.GetCheckpointTimeTick()
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
	if !info.canObserveAtLocked(timetick) || !info.canObservePartitionAtLocked(partitionID, timetick) {
		return nil
	}
	if !info.hasPartitionMetaLocked(partitionID) {
		return nil
	}
	_, schema := info.GetSchemaLocked(timetick)
	return schema
}

func (info *VChannelView) TombstonedCleanupPlan(
	physicalTimeTick uint64,
	persistedMaterializedTimeTick uint64,
) (dropSnapshot *streamingpb.VChannelMeta, cleanupPartitions map[int64]uint64) {
	info.mu.Lock()
	defer info.mu.Unlock()

	if info.vchannelTombstonedCleanupReadyLocked(physicalTimeTick, persistedMaterializedTimeTick) {
		return proto.Clone(info.meta).(*streamingpb.VChannelMeta), nil
	}
	if info.dirty {
		return nil, nil
	}
	for _, partition := range info.meta.GetCollectionInfo().GetPartitions() {
		if partitionTombstonedCleanupReady(partition, physicalTimeTick) &&
			info.persistedMetaTimeTick >= partition.GetCheckpointTimeTick() &&
			persistedMaterializedTimeTick >= partition.GetCheckpointTimeTick() {
			if cleanupPartitions == nil {
				cleanupPartitions = make(map[int64]uint64)
			}
			cleanupPartitions[partition.GetPartitionId()] = partition.GetCheckpointTimeTick()
		}
	}
	if len(cleanupPartitions) == 0 {
		return nil, nil
	}
	return nil, cleanupPartitions
}

func (info *VChannelView) vchannelTombstonedCleanupReadyLocked(
	physicalTimeTick uint64,
	persistedMaterializedTimeTick uint64,
) bool {
	checkpointTimeTick := info.meta.GetCheckpointTimeTick()
	return info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED &&
		checkpointTimeTick > 0 &&
		!info.dirty &&
		info.persistedMetaTimeTick >= checkpointTimeTick &&
		persistedMaterializedTimeTick >= checkpointTimeTick &&
		physicalTimeTick > checkpointTimeTick
}

func (info *VChannelView) VChannelDropCleanupSnapshot(checkpointTimeTick uint64, persistedMaterializedTimeTick uint64) *streamingpb.VChannelMeta {
	info.mu.Lock()
	defer info.mu.Unlock()

	if info.meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED ||
		checkpointTimeTick == 0 ||
		info.meta.GetCheckpointTimeTick() != checkpointTimeTick ||
		info.dirty ||
		info.persistedMetaTimeTick < checkpointTimeTick ||
		persistedMaterializedTimeTick < checkpointTimeTick {
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
			partition.GetCheckpointTimeTick() == tombstoneTimeTick {
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
	if !info.shouldObserveLocked(msg.TimeTick()) {
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
	if !info.shouldObserveLocked(msg.TimeTick()) {
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
	if !info.shouldObserveLocked(msg.TimeTick()) {
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
	if !info.shouldObserveLocked(msg.TimeTick()) {
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
	if !info.shouldObserveLocked(msg.TimeTick()) {
		return false
	}
	for _, partition := range info.meta.CollectionInfo.Partitions {
		if partition.PartitionId == msg.Header().PartitionId {
			// make it idempotent, only the first drop partition message can be observed.
			if !isPartitionNormal(partition.GetState()) {
				return false
			}
			partition.State = streamingpb.PartitionState_PARTITION_STATE_DROPPED
			partition.CheckpointTimeTick = msg.TimeTick()
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
	if !info.shouldObserveLocked(msg.TimeTick()) {
		return false
	}
	if info.meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
		return false
	}
	for _, partition := range info.meta.CollectionInfo.Partitions {
		if partition.PartitionId == msg.Header().PartitionId {
			if partition.GetState() != streamingpb.PartitionState_PARTITION_STATE_NORMAL {
				partition.State = streamingpb.PartitionState_PARTITION_STATE_NORMAL
				partition.CheckpointTimeTick = 0
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

func (info *VChannelView) maybeMarkTombstonedLocked(checkpointTimeTick uint64) bool {
	changed := false
	if info.vchannelTombstoneFinalizeReadyLocked(checkpointTimeTick) {
		info.meta.State = streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED
		info.dirty = true
		changed = true
	}
	for _, partition := range info.meta.GetCollectionInfo().GetPartitions() {
		if !info.partitionTombstoneFinalizeReadyLocked(partition, checkpointTimeTick) {
			continue
		}
		partition.State = streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED
		info.dirty = true
		changed = true
	}
	return changed
}

func (info *VChannelView) vchannelTombstoneFinalizeReadyLocked(checkpointTimeTick uint64) bool {
	return info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED &&
		info.meta.GetCheckpointTimeTick() > 0 &&
		checkpointTimeTick >= info.meta.GetCheckpointTimeTick()
}

func (info *VChannelView) partitionTombstoneFinalizeReadyLocked(partition *streamingpb.PartitionInfoOfVChannel, checkpointTimeTick uint64) bool {
	return partition.GetState() == streamingpb.PartitionState_PARTITION_STATE_DROPPED &&
		partition.GetCheckpointTimeTick() > 0 &&
		checkpointTimeTick >= partition.GetCheckpointTimeTick()
}

func isVChannelClosed(state streamingpb.VChannelState) bool {
	return state == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED ||
		state == streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED
}

func isPartitionNormal(state streamingpb.PartitionState) bool {
	return state == streamingpb.PartitionState_PARTITION_STATE_NORMAL
}

func partitionTombstonedCleanupReady(meta *streamingpb.PartitionInfoOfVChannel, physicalTimeTick uint64) bool {
	checkpointTimeTick := meta.GetCheckpointTimeTick()
	return meta.GetState() == streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED &&
		checkpointTimeTick > 0 &&
		physicalTimeTick > checkpointTimeTick
}
