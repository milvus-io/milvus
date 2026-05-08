package goplog

import "github.com/milvus-io/milvus/pkg/v3/streaming/util/message"

// DeleteChunk is a buffer that stores delete messages and pops them in order of time tick.
type DeleteChunk struct {
	msgs              []message.ImmutableDeleteMessageV1 // inter message is order by timetick.
	size              int64                              // size of the buffer.
	startFromTimeTick uint64                             // start from time tick.
	endToTimeTick     uint64                             // end to time tick.
}

// IsEmpty returns true if the buffer is empty.
func (b *DeleteChunk) IsEmpty() bool {
	return len(b.msgs) == 0
}

// Push pushes a message into the buffer.
func (b *DeleteChunk) Push(msg message.ImmutableMessage) {
	deleteMsg := message.MustAsImmutableDeleteMessageV1(msg)
	b.msgs = append(b.msgs, deleteMsg)
	b.size += int64(msg.EstimateSize())
	if b.startFromTimeTick == 0 {
		b.startFromTimeTick = msg.TimeTick()
	}
	b.endToTimeTick = msg.TimeTick()
}
