package segment

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type writeOnlyInsertBuffer struct {
	entries      []message.ImmutableMessage
	fromTimeTick uint64
	toTimeTick   uint64
	rows         uint64
	binarySize   uint64
}

func (b *writeOnlyInsertBuffer) append(msg message.ImmutableInsertMessageV1, assignment *messagespb.PartitionSegmentAssignment) {
	b.appendMessage(msg, assignment.GetRows(), assignment.GetBinarySize())
}

func (b *writeOnlyInsertBuffer) appendMessage(msg message.ImmutableMessage, rows uint64, binarySize uint64) {
	timetick := msg.TimeTick()
	if len(b.entries) == 0 {
		b.fromTimeTick = timetick
	}
	b.toTimeTick = timetick
	b.rows += rows
	b.binarySize += binarySize
	b.entries = append(b.entries, msg)
}

func (b writeOnlyInsertBuffer) DataTimeTick() uint64 {
	return b.toTimeTick
}

func (b writeOnlyInsertBuffer) Messages() []message.ImmutableMessage {
	if len(b.entries) == 0 {
		return nil
	}
	return cloneGrowingSegmentInsertMessages(b.entries)
}

func (b *writeOnlyInsertBuffer) flushPack(meta *streamingpb.SegmentAssignmentMeta, schema *schemapb.CollectionSchema) *flushPack {
	return &flushPack{
		Meta:         proto.Clone(meta).(*streamingpb.SegmentAssignmentMeta),
		CollectionID: meta.GetCollectionId(),
		PartitionID:  meta.GetPartitionId(),
		SegmentID:    meta.GetSegmentId(),
		VChannel:     meta.GetVchannel(),
		FromTimeTick: b.fromTimeTick,
		ToTimeTick:   b.toTimeTick,
		Schema:       schema,
		Rows:         b.rows,
		BinarySize:   b.binarySize,
		Inserts:      b.entries,
	}
}

func (b *writeOnlyInsertBuffer) reset() {
	*b = writeOnlyInsertBuffer{}
}

func (b *writeOnlyInsertBuffer) takeAll() writeOnlyInsertBuffer {
	chunk := *b
	b.reset()
	return chunk
}

func cloneGrowingSegmentInsertMessages(entries []message.ImmutableMessage) []message.ImmutableMessage {
	if len(entries) == 0 {
		return nil
	}
	cloned := make([]message.ImmutableMessage, len(entries))
	copy(cloned, entries)
	return cloned
}
