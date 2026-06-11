package vchannel

import (
	"math"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
)

// newVChannelViewFromMeta creates a vChannelView from persisted vchannel meta.
func newVChannelViewFromMeta(meta *streamingpb.VChannelMeta, configs ...runtimeConfig) *vChannelView {
	return newVChannelView(
		meta,
		meta.GetCheckpointTimeTick(),
		false,
		firstRuntimeConfig(configs),
	)
}

func newVChannelView(
	meta *streamingpb.VChannelMeta,
	persistedMetaTimeTick uint64,
	dirty bool,
	config runtimeConfig,
) *vChannelView {
	meta = proto.Clone(meta).(*streamingpb.VChannelMeta)
	return &vChannelView{
		meta:                  meta,
		persistedMetaTimeTick: persistedMetaTimeTick,
		dirty:                 dirty,
		metaAndData:           config.metaAndData,
	}
}

// newVChannelMetaFromCreateCollectionMessage creates a new vchannel meta from a create collection message.
func newVChannelMetaFromCreateCollectionMessage(msg message.ImmutableCreateCollectionMessageV1) *streamingpb.VChannelMeta {
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
		LatestDataVersion:  &viewpb.DataVersion{},
		GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
	}
}

// vChannelView tracks the metadata and durability state of a vchannel.
type vChannelView struct {
	mu sync.Mutex

	meta                  *streamingpb.VChannelMeta
	persistedMetaTimeTick uint64
	dirty                 bool // whether the current vchannel recovery info still needs catalog persistence.
	pendingDirtySnapshot  *streamingpb.VChannelMeta

	metaAndData bool
}

func (info *vChannelView) AssignmentMeta() *streamingpb.VChannelMeta {
	info.mu.Lock()
	defer info.mu.Unlock()
	return proto.Clone(info.meta).(*streamingpb.VChannelMeta)
}

func (info *vChannelView) Name() string {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.meta.GetVchannel()
}

func (info *vChannelView) HasDirty() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.dirty
}

func (info *vChannelView) metaTimeTick() uint64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.persistedMetaTimeTick
}

func (info *vChannelView) MetaBarrier() walcheckpoint.Barrier {
	return walcheckpoint.BarrierFunc(info.metaTimeTick)
}

func (info *vChannelView) markMetaPersistedLocked(timetick uint64) {
	if timetick > info.persistedMetaTimeTick {
		info.persistedMetaTimeTick = timetick
	}
}

func (info *vChannelView) MarkSnapshotPersisted(snapshot *streamingpb.VChannelMeta) {
	info.mu.Lock()
	defer info.mu.Unlock()
	info.markMetaPersistedLocked(snapshot.GetCheckpointTimeTick())
	if info.pendingDirtySnapshot != nil && proto.Equal(info.pendingDirtySnapshot, snapshot) {
		info.pendingDirtySnapshot = nil
	}
	info.dirty = !proto.Equal(info.meta, snapshot)
}

func (info *vChannelView) TryFinalizeTombstone(dataCheckpointTimeTick uint64) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.maybeMarkTombstonedLocked(dataCheckpointTimeTick)
}

func (info *vChannelView) HasReadyTombstoneFinalize(dataCheckpointTimeTick uint64) bool {
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

func (info *vChannelView) SwitchIntoMetaAndData() {
	info.mu.Lock()
	defer info.mu.Unlock()
	info.metaAndData = true
}

// IsActive returns true if the vchannel is active.
func (info *vChannelView) IsActive() bool {
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

func (info *vChannelView) ObserveExistingCreateCollectionMessageV1(msg message.ImmutableCreateCollectionMessageV1) (existingCreateCollectionDecision, moduleapi.ObserveResult) {
	info.mu.Lock()
	defer info.mu.Unlock()
	timetick := msg.TimeTick()
	if shouldSkipTombstonedVChannelMeta(info.meta, timetick) {
		return existingCreateCollectionIgnored, emptyObserveResult()
	}
	if info.canStartNewCollectionAtLocked(timetick) {
		return existingCreateCollectionStartNew, emptyObserveResult()
	}
	if !info.canReplayAtLocked(timetick) {
		return existingCreateCollectionIgnored, emptyObserveResult()
	}
	return existingCreateCollectionInconsistent, moduleapi.ObserveResult{Meta: walcheckpoint.BarrierFunc(info.metaTimeTick)}
}

func (info *vChannelView) canStartNewCollectionAtLocked(timetick uint64) bool {
	return info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED &&
		info.meta.GetTombstoneTimeTick() > 0 &&
		timetick > info.meta.GetTombstoneTimeTick()
}

func (info *vChannelView) canReplayAtLocked(timetick uint64) bool {
	if shouldSkipTombstonedVChannelMeta(info.meta, timetick) {
		return false
	}
	if info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
		return true
	}
	return timetick <= info.meta.GetCheckpointTimeTick()
}

func (info *vChannelView) CanObserveActiveAt(timetick uint64) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.canReplayAtLocked(timetick) &&
		info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_NORMAL
}

func (info *vChannelView) canReplayExistingPartitionAtLocked(partitionID int64, timetick uint64) bool {
	if partitionID == common.AllPartitionsID {
		return true
	}
	return info.canReplayPartitionAtLocked(partitionID, timetick) && info.hasPartitionMetaLocked(partitionID)
}

func (info *vChannelView) canReplayPartitionAtLocked(partitionID int64, timetick uint64) bool {
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

func (info *vChannelView) hasPartitionMetaLocked(partitionID int64) bool {
	for _, partition := range info.meta.GetCollectionInfo().GetPartitions() {
		if partition.GetPartitionId() == partitionID {
			return true
		}
	}
	return false
}

func (info *vChannelView) CreateSegmentSchema(partitionID int64, timetick uint64) *schemapb.CollectionSchema {
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

func (info *vChannelView) TombstonedCleanupPlan(
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

func (info *vChannelView) vchannelTombstonedCleanupReadyLocked(
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

func (info *vChannelView) VChannelDropCleanupSnapshot(tombstoneTimeTick uint64, persistedDataTimeTick uint64) *streamingpb.VChannelMeta {
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

func (info *vChannelView) ApplyPartitionCleanup(partitionIDs map[int64]uint64) bool {
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

func (info *vChannelView) PartitionCleanupPlan(partitionIDs map[int64]uint64) map[int64]uint64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.partitionCleanupPlanLocked(partitionIDs)
}

func (info *vChannelView) partitionCleanupPlanLocked(partitionIDs map[int64]uint64) map[int64]uint64 {
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
func (info *vChannelView) GetSchema(timetick uint64) (int, *schemapb.CollectionSchema) {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.GetSchemaLocked(timetick)
}

func (info *vChannelView) GetSchemaLocked(timetick uint64) (int, *schemapb.CollectionSchema) {
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

func (info *vChannelView) ObserveSchemaChangeMessageV2(msg message.ImmutableSchemaChangeMessageV2) moduleapi.ObserveResult {
	info.mu.Lock()
	defer info.mu.Unlock()
	if msg.TimeTick() <= info.meta.CheckpointTimeTick {
		return emptyObserveResult()
	}
	if info.meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
		return emptyObserveResult()
	}
	if info.hasSchemaVersionLocked(msg.TimeTick(), msg.MustBody().Schema) {
		return emptyObserveResult()
	}

	info.meta.CollectionInfo.Schemas = append(info.meta.CollectionInfo.Schemas, &streamingpb.CollectionSchemaOfVChannel{
		Schema:             msg.MustBody().Schema,
		State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
		CheckpointTimeTick: msg.TimeTick(),
	})
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
	return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
}

func (info *vChannelView) ObserveAlterCollectionMessageV2(msg message.ImmutableAlterCollectionMessageV2) moduleapi.ObserveResult {
	info.mu.Lock()
	defer info.mu.Unlock()
	if msg.TimeTick() <= info.meta.CheckpointTimeTick {
		return emptyObserveResult()
	}
	if info.meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
		return emptyObserveResult()
	}
	schemaChange := messageutil.IsSchemaChange(msg.Header())
	if schemaChange {
		schema := msg.MustBody().Updates.Schema
		if info.hasSchemaVersionLocked(msg.TimeTick(), schema) {
			return emptyObserveResult()
		}
		info.meta.CollectionInfo.Schemas = append(info.meta.CollectionInfo.Schemas, &streamingpb.CollectionSchemaOfVChannel{
			Schema:             schema,
			State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
			CheckpointTimeTick: msg.TimeTick(),
		})
	}
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
	return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
}

func (info *vChannelView) hasSchemaVersionLocked(timetick uint64, schema *schemapb.CollectionSchema) bool {
	for _, existing := range info.meta.GetCollectionInfo().GetSchemas() {
		if existing.GetCheckpointTimeTick() == timetick && proto.Equal(existing.GetSchema(), schema) {
			return true
		}
	}
	return false
}

func (info *vChannelView) ObserveDropCollectionMessageV1(msg message.ImmutableDropCollectionMessageV1) moduleapi.ObserveResult {
	info.mu.Lock()
	defer info.mu.Unlock()
	if msg.TimeTick() <= info.meta.CheckpointTimeTick {
		return emptyObserveResult()
	}
	if isVChannelClosed(info.meta.GetState()) {
		// make it idempotent, only the first drop collection message can be observed.
		return emptyObserveResult()
	}
	info.meta.State = streamingpb.VChannelState_VCHANNEL_STATE_DROPPED
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
	return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
}

func (info *vChannelView) ObserveTruncateCollectionMessageV2(msg message.ImmutableTruncateCollectionMessageV2) moduleapi.ObserveResult {
	info.mu.Lock()
	defer info.mu.Unlock()
	if msg.TimeTick() <= info.meta.CheckpointTimeTick {
		return emptyObserveResult()
	}
	if info.meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
		return emptyObserveResult()
	}
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
	return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
}

func (info *vChannelView) ObserveDropPartitionMessageV1(msg message.ImmutableDropPartitionMessageV1) moduleapi.ObserveResult {
	info.mu.Lock()
	defer info.mu.Unlock()
	if msg.TimeTick() <= info.meta.CheckpointTimeTick {
		return emptyObserveResult()
	}
	for _, partition := range info.meta.CollectionInfo.Partitions {
		if partition.PartitionId == msg.Header().PartitionId {
			// make it idempotent, only the first drop partition message can be observed.
			if !isPartitionNormal(partition.GetState()) {
				return emptyObserveResult()
			}
			partition.State = streamingpb.PartitionState_PARTITION_STATE_DROPPED
			partition.TombstoneTimeTick = msg.TimeTick()
			info.meta.CheckpointTimeTick = msg.TimeTick()
			info.dirty = true
			return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
		}
	}
	return emptyObserveResult()
}

func (info *vChannelView) ObserveCreatePartitionMessageV1(msg message.ImmutableCreatePartitionMessageV1) moduleapi.ObserveResult {
	info.mu.Lock()
	defer info.mu.Unlock()
	if msg.TimeTick() <= info.meta.CheckpointTimeTick {
		return emptyObserveResult()
	}
	if info.meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
		return emptyObserveResult()
	}
	for _, partition := range info.meta.CollectionInfo.Partitions {
		if partition.PartitionId == msg.Header().PartitionId {
			if partition.GetState() != streamingpb.PartitionState_PARTITION_STATE_NORMAL {
				partition.State = streamingpb.PartitionState_PARTITION_STATE_NORMAL
				partition.TombstoneTimeTick = 0
				info.meta.CheckpointTimeTick = msg.TimeTick()
				info.dirty = true
				return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
			}
			return emptyObserveResult()
		}
	}
	info.meta.CollectionInfo.Partitions = append(info.meta.CollectionInfo.Partitions, &streamingpb.PartitionInfoOfVChannel{
		PartitionId: msg.Header().PartitionId,
		State:       streamingpb.PartitionState_PARTITION_STATE_NORMAL,
	})
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
	return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
}

// ConsumeDirtyAndGetSnapshot returns the current stable dirty snapshot of the
// vchannel recovery info. It is not a queue pop: until MarkSnapshotPersisted is
// called, repeated calls keep returning the same in-flight snapshot.
func (info *vChannelView) ConsumeDirtyAndGetSnapshot() *streamingpb.VChannelMeta {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.pendingDirtySnapshot != nil {
		return proto.Clone(info.pendingDirtySnapshot).(*streamingpb.VChannelMeta)
	}
	if !info.dirty {
		return nil
	}
	info.pendingDirtySnapshot = proto.Clone(info.meta).(*streamingpb.VChannelMeta)
	return proto.Clone(info.pendingDirtySnapshot).(*streamingpb.VChannelMeta)
}

func (info *vChannelView) maybeMarkTombstonedLocked(dataCheckpointTimeTick uint64) bool {
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

func (info *vChannelView) vchannelTombstoneFinalizeReadyLocked(dataCheckpointTimeTick uint64) bool {
	tombstoneTimeTick := info.meta.GetCheckpointTimeTick()
	return info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED &&
		tombstoneTimeTick > 0 &&
		dataCheckpointTimeTick >= tombstoneTimeTick
}

func (info *vChannelView) partitionTombstoneFinalizeReadyLocked(partition *streamingpb.PartitionInfoOfVChannel, dataCheckpointTimeTick uint64) bool {
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
