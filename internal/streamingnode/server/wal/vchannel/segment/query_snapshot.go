package segment

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func (info *SegmentView) VisibleSnapshot(vchannel string, dataVersion qviews.DataVersion) (walview.VisibleSegment, bool) {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.meta.GetVchannel() != vchannel || !info.visibleAtDataVersionLocked(dataVersion) {
		return walview.VisibleSegment{}, false
	}
	meta := proto.Clone(info.durableMeta).(*streamingpb.SegmentAssignmentMeta)
	var insertMessages []message.ImmutableMessage
	for _, chunk := range info.pendingFlushChunks {
		insertMessages = append(insertMessages, chunk.Messages()...)
	}
	insertMessages = append(insertMessages, info.pending.Messages()...)
	for i, msg := range insertMessages {
		insertMessages[i] = walview.CopyMessage(msg)
	}
	var schema *schemapb.CollectionSchema
	if info.schema != nil {
		schema = proto.Clone(info.schema).(*schemapb.CollectionSchema)
	}
	visible := walview.VisibleSegment{
		SegmentID:           meta.GetSegmentId(),
		PartitionID:         meta.GetPartitionId(),
		Schema:              schema,
		Assignment:          meta,
		SealedAtDataVersion: meta.GetSealedAtDataVersion(),
		Data: walview.SegmentSnapshotData{
			PersistedStorage: meta.GetPersistedStorage(),
			InsertMessages:   insertMessages,
		},
	}
	return visible, true
}

func (info *SegmentView) FlushedSegmentSnapshot(vchannel string, dataVersion qviews.DataVersion) (walview.FlushedSegment, bool) {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.meta.GetVchannel() != vchannel ||
		info.meta.GetState() != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED ||
		info.meta.GetSealedAtDataVersion() == nil ||
		qviews.FromProtoDataVersion(info.meta.GetSealedAtDataVersion()).GT(dataVersion) {
		return walview.FlushedSegment{}, false
	}
	return walview.FlushedSegment{
		SegmentID:           info.meta.GetSegmentId(),
		PartitionID:         info.meta.GetPartitionId(),
		FlushTimeTick:       info.meta.GetCheckpointTimeTick(),
		SealedAtDataVersion: qviews.FromProtoDataVersion(info.meta.GetSealedAtDataVersion()),
	}, true
}

func (info *SegmentView) visibleAtDataVersionLocked(dataVersion qviews.DataVersion) bool {
	switch info.meta.GetState() {
	case streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING:
		return true
	case streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED:
		if info.meta.GetSealedAtDataVersion() == nil {
			return false
		}
		return qviews.FromProtoDataVersion(info.meta.GetSealedAtDataVersion()).GT(dataVersion)
	default:
		return false
	}
}

func (info *SegmentView) SealedDataVersion(vchannel string) (qviews.DataVersion, bool) {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.meta.GetVchannel() != vchannel ||
		info.meta.GetState() != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED ||
		info.meta.GetSealedAtDataVersion() == nil {
		return qviews.DataVersion{}, false
	}
	return qviews.FromProtoDataVersion(info.meta.GetSealedAtDataVersion()), true
}
