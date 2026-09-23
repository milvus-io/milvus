package walview

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// VChannelWALView is the vchannel-owned WAL input view for one query view.
type VChannelWALView struct {
	PChannel     string
	VChannel     string
	CollectionID int64

	BaseGrowingTimeTick   uint64
	BaseTransformTimeTick uint64

	LoadInfoVersion uint64
	PartitionIDs    []int64
	LoadFields      []*messagespb.LoadFieldConfig
	IndexInfos      []*indexpb.IndexInfo
	Schema          *schemapb.CollectionSchema

	SegmentSnapshot                VisibleSegmentSnapshot
	TransformLogStream             wal.TransformLogStream
	DeleteReplayStartAfterTimeTick uint64
}

// VisibleSegmentSnapshot is the historical growing-side insert state captured at a WAL observe point.
type VisibleSegmentSnapshot struct {
	CollectionID        int64
	VChannel            string
	DataVersion         qviews.DataVersion
	BaseGrowingTimeTick uint64
	Segments            []VisibleSegment
	FlushedSegments     []FlushedSegment
}

// VisibleSegment is a query-visible segment and its shallow-copied data handles.
type VisibleSegment struct {
	SegmentID   int64
	PartitionID int64

	Schema *schemapb.CollectionSchema

	Assignment          *streamingpb.SegmentAssignmentMeta
	SealedAtDataVersion *viewpb.DataVersion

	Data SegmentSnapshotData
}

// SegmentSnapshotData contains persisted and in-memory insert data for a visible segment.
type SegmentSnapshotData struct {
	PersistedStorage *streamingpb.L1SegmentPersistedStorage
	InsertMessages   []message.ImmutableMessage
}

// FlushedSegment is a non-queryable segment marker kept only to make WAL replay
// idempotent for flushed segments that are already covered by QueryNode at the
// query view data version.
type FlushedSegment struct {
	SegmentID           int64
	PartitionID         int64
	FlushTimeTick       uint64
	SealedAtDataVersion qviews.DataVersion
}

// VChannelResourceEvent is the ordered live input delivered after a
// VChannelWALView capture.
type VChannelResourceEvent struct {
	Message       message.ImmutableMessage
	SegmentSealed *SegmentSealedEvent
}

// SegmentSealedEvent reports the DataVersion assigned when a flushed growing
// segment becomes sealed.
type SegmentSealedEvent struct {
	SegmentID           int64
	VChannel            string
	SealedAtDataVersion qviews.DataVersion
}
