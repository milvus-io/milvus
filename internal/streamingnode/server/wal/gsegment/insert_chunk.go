package gsegment

import "github.com/milvus-io/milvus/pkg/v3/streaming/util/message"

const (
	defaultExpectedSize = 16 * 1024 * 1024 // 16MB

)

// newInsertChunk creates a new ChunkInsertBuffer.
func newInsertChunk(expectedSize int64) *InsertChunk {
	if expectedSize <= 0 {
		expectedSize = defaultExpectedSize
	}
	return &InsertChunk{
		expectedSize: expectedSize,
	}
}

// InsertChunk is a buffer that stores insert messages and pops them in order of time tick.
type InsertChunk struct {
	msgs              []message.ImmutableInsertMessageV1 // inter message is order by timetick.
	size              int64                              // size of the buffer.
	expectedSize      int64                              // expected size of the buffer.
	startFromTimeTick uint64                             // start from time tick.
	endToTimeTick     uint64                             // end to time tick.
}

// IsEmpty returns true if the buffer is empty.
func (b *InsertChunk) IsEmpty() bool {
	return len(b.msgs) == 0
}

// AvailableSize returns the available size of the buffer.
func (b *InsertChunk) AvailableSize() int64 {
	return b.expectedSize - b.size
}

// Push pushes a message into the buffer.
func (b *InsertChunk) Push(msg message.ImmutableMessage) {
	insertMsg := message.MustAsImmutableInsertMessageV1(msg)
	b.msgs = append(b.msgs, insertMsg)
	b.size += int64(msg.EstimateSize())
	if b.startFromTimeTick == 0 {
		b.startFromTimeTick = msg.TimeTick()
	}
	b.endToTimeTick = msg.TimeTick()
}

func (b *InsertChunk) Drain() []message.ImmutableInsertMessageV1 {
	msgs := b.msgs
	b.msgs = nil
	return msgs
}
