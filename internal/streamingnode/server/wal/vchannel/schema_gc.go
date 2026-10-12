package vchannel

import (
	"slices"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// SchemaCleanupPlan first tombstones unreachable versions, then returns only
// persisted tombstones for deletion on a later cleanup pass. The replay floor
// is the published WAL checkpoint, never the checkpoint being saved this round.
func (info *VChannelView) SchemaCleanupPlan(
	replayFloor uint64,
	segments []*streamingpb.SegmentAssignmentMeta,
) (*streamingpb.VChannelMeta, bool) {
	info.mu.Lock()
	defer info.mu.Unlock()
	schemas := info.meta.GetCollectionInfo().GetSchemas()
	if len(info.pendingDrops) > 0 || len(schemas) <= 1 || replayFloor == 0 {
		return nil, false
	}
	// Base-only changes do not delay schema GC on a busy channel. A schema
	// snapshot still in flight, however, cannot prove tombstone durability.
	if !info.schemaDirty && !info.pendingDirtySnapshotSavesSchemas {
		var dropped []*streamingpb.CollectionSchemaOfVChannel
		for _, schema := range schemas {
			if schema.GetState() == streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED {
				dropped = append(dropped, &streamingpb.CollectionSchemaOfVChannel{
					CheckpointTimeTick: schema.GetCheckpointTimeTick(),
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED,
				})
			}
		}
		if len(dropped) > 0 {
			return &streamingpb.VChannelMeta{
				Vchannel:       info.meta.GetVchannel(),
				CollectionInfo: &streamingpb.CollectionInfoOfVChannel{Schemas: dropped},
			}, false
		}
	}
	keep := make([]bool, len(schemas))
	// Retain the version covering the replay floor, plus every newer version.
	floorIndex, _ := info.GetSchemaLocked(replayFloor)
	if floorIndex < 0 {
		return nil, false
	}
	for i := floorIndex; i < len(schemas); i++ {
		keep[i] = true
	}
	for _, segment := range segments {
		if i, _ := info.GetSchemaLocked(segment.GetStat().GetCreateSegmentTimeTick()); i >= 0 {
			keep[i] = true
		}
		// Migrated segments may have a synthetic allocation timestamp. Keep
		// their encoding version too, until their recovery metadata is removed.
		for i := len(schemas) - 1; i >= 0; i-- {
			if schemas[i].GetState() == streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL &&
				schemas[i].GetSchema().GetVersion() == segment.GetSchemaVersion() {
				keep[i] = true
				break
			}
		}
	}
	changed := false
	for i, schema := range schemas {
		if !keep[i] && schema.GetState() == streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL {
			schema.State = streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED
			changed = true
		}
	}
	if changed {
		info.dirty = true
		info.schemaDirty = true
	}
	return nil, changed
}

// MarkSchemaCleanupPersisted removes only the captured identities. Concurrent
// schema additions and their dirty state remain owned by the normal save path.
func (info *VChannelView) MarkSchemaCleanupPersisted(snapshot *streamingpb.VChannelMeta) {
	info.mu.Lock()
	defer info.mu.Unlock()
	removed := make(map[uint64]struct{}, len(snapshot.GetCollectionInfo().GetSchemas()))
	for _, schema := range snapshot.GetCollectionInfo().GetSchemas() {
		removed[schema.GetCheckpointTimeTick()] = struct{}{}
	}
	remove := func(meta *streamingpb.VChannelMeta) {
		meta.CollectionInfo.Schemas = slices.DeleteFunc(meta.CollectionInfo.Schemas,
			func(schema *streamingpb.CollectionSchemaOfVChannel) bool {
				_, ok := removed[schema.GetCheckpointTimeTick()]
				return ok && schema.GetState() == streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED
			})
	}
	remove(info.meta)
	for _, drop := range info.pendingDrops {
		remove(drop.before)
	}
}
