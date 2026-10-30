package vchannel

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// GetSchemaVersion returns an owned snapshot of an exact historical schema.
// In particular, zero identifies version zero, never the latest schema.
func (m *VChannelRecoveryModule) GetSchemaVersion(collectionID int64, version int32) (*streamingpb.CollectionSchemaOfVChannel, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.vchannelView == nil {
		return nil, status.NewInner("schema history for vchannel %s is not ready", m.vchannel)
	}
	return m.vchannelView.GetSchemaVersion(collectionID, version)
}

func (info *VChannelView) GetSchemaVersion(collectionID int64, version int32) (*streamingpb.CollectionSchemaOfVChannel, error) {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.meta.GetCollectionInfo().GetCollectionId() != collectionID || info.meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL || info.closingLocked(0) {
		return nil, status.NewUnrecoverableError("collection %d is not active on vchannel %s", collectionID, info.meta.GetVchannel())
	}
	schemas := info.meta.GetCollectionInfo().GetSchemas()
	if entry := info.canonicalReadSchemaLocked(version); entry != nil {
		return proto.Clone(entry).(*streamingpb.CollectionSchemaOfVChannel), nil
	}
	if len(schemas) == 0 || schemas[len(schemas)-1].GetSchema().GetVersion() < version {
		return nil, status.NewInner("schema version %d has not been observed on vchannel %s", version, info.meta.GetVchannel())
	}
	return nil, status.NewUnrecoverableError("schema version %d is no longer available on vchannel %s", version, info.meta.GetVchannel())
}

// canonicalReadSchemaLocked is the compatibility mapping for pre-versioned
// semantic updates (for example legacy TTL changes). The last retained normal
// payload of that version is the upgrade's read identity. All new semantic
// updates advance SchemaVersion, so this mapping stays immutable afterwards.
// Segment encoding additionally uses its allocation boundary, not this alias.
func (info *VChannelView) canonicalReadSchemaLocked(version int32) *streamingpb.CollectionSchemaOfVChannel {
	history := info.meta.GetCollectionInfo().GetSchemas()
	for i := len(history) - 1; i >= 0; i-- {
		entry := history[i]
		if entry.GetSchema() != nil && entry.GetSchema().GetVersion() == version && entry.GetState() == streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL {
			return entry
		}
	}
	return nil
}
