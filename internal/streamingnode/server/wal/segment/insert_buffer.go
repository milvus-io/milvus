package segment

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type writeOnlyInsertBuffer struct {
	entries      []insertEntry
	fromTimeTick uint64
	toTimeTick   uint64
	rows         uint64
	binarySize   uint64
}

func (b *writeOnlyInsertBuffer) append(msg message.ImmutableInsertMessageV1, assignment *messagespb.PartitionSegmentAssignment) {
	b.appendWithTimeTick(msg, assignment, msg.TimeTick())
}

func (b *writeOnlyInsertBuffer) appendWithTimeTick(msg message.ImmutableInsertMessageV1, assignment *messagespb.PartitionSegmentAssignment, timetick uint64) {
	if len(b.entries) == 0 {
		b.fromTimeTick = timetick
	}
	b.toTimeTick = timetick
	b.rows += assignment.GetRows()
	b.binarySize += assignment.GetBinarySize()
	b.entries = append(b.entries, insertEntry{
		timeTick:   timetick,
		assignment: clonePartitionSegmentAssignment(assignment),
		request:    cloneInsertRequest(msg.MustBody()),
	})
}

func (b writeOnlyInsertBuffer) DataTimeTick() uint64 {
	return b.toTimeTick
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
		Inserts:      cloneGrowingSegmentInsertEntries(b.entries),
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

func cloneGrowingSegmentInsertEntries(entries []insertEntry) []insertEntry {
	if len(entries) == 0 {
		return nil
	}
	cloned := make([]insertEntry, 0, len(entries))
	for _, entry := range entries {
		cloned = append(cloned, insertEntry{
			timeTick:   entry.timeTick,
			assignment: clonePartitionSegmentAssignment(entry.assignment),
			request:    cloneInsertRequest(entry.request),
		})
	}
	return cloned
}
