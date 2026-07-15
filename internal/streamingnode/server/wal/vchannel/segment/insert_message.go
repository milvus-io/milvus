package segment

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type segmentInsertMessage = walview.SegmentInsertMessage

func forEachSegmentInsertMessage(
	raw message.ImmutableMessage,
	segmentID int64,
	visit func(segmentInsertMessage) error,
) error {
	return walview.ForEachSegmentInsertMessage(raw, segmentID, visit)
}
