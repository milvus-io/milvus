package wab

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// WriteAheadBufferReader is used to read messages from WriteAheadBuffer.
type WriteAheadBufferReader struct {
	nextOffset    int
	lastTimeTick  uint64
	snapshot      []messageWithOffset
	underlyingBuf *WriteAheadBuffer
}

// Next returns the next message in the buffer.
func (r *WriteAheadBufferReader) Next(ctx context.Context) (message.ImmutableMessage, error) {
	// Consume snapshot first.
	if msg := r.nextFromSnapshot(); msg != nil {
		return msg, nil
	}

	snapshot, err := r.underlyingBuf.createSnapshotFromOffset(ctx, r.nextOffset, r.lastTimeTick)
	if err != nil {
		return nil, err
	}
	r.snapshot = snapshot
	return r.nextFromSnapshot(), nil
}

// nextFromSnapshot returns the next message from the snapshot.
func (r *WriteAheadBufferReader) nextFromSnapshot() message.ImmutableMessage {
	if len(r.snapshot) == 0 {
		return nil
	}
	nextMsg := r.snapshot[0]
	newNextOffset := nextMsg.Offset + 1
	if newNextOffset < r.nextOffset {
		panic("unreachable: next offset should be monotonically increasing")
	}
	r.nextOffset = newNextOffset
	r.lastTimeTick = nextMsg.Message.TimeTick()
	r.snapshot = r.snapshot[1:]
	return nextMsg.Message
}
