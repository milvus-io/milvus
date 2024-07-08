package utility

import "github.com/milvus-io/milvus/pkg/streaming/util/message"

// NewImmutableMessageQueue create a new immutable message queue.
func NewImmutableMessageQueue() *ImmutableMessageQueue {
	return &ImmutableMessageQueue{
		pendings: make([][]message.ImmutableMessage, 0),
		cnt:      0,
	}
}

// ImmutableMessageQueue is a queue of messages.
type ImmutableMessageQueue struct {
	pendings [][]message.ImmutableMessage
	cnt      int
}

// Len return the queue size.
func (pq *ImmutableMessageQueue) Len() int {
	return pq.cnt
}

// Add add a slice of message as pending one
func (pq *ImmutableMessageQueue) Add(msgs []message.ImmutableMessage) {
	if len(msgs) == 0 {
		return
	}
	pq.pendings = append(pq.pendings, msgs)
	pq.cnt += len(msgs)
}

// Next return the next message in pending queue.
func (pq *ImmutableMessageQueue) Next() message.ImmutableMessage {
	if len(pq.pendings) != 0 && len(pq.pendings[0]) != 0 {
		return pq.pendings[0][0]
	}
	return nil
}

// UnsafeAdvance do a advance without check.
// !!! Should only be called `Next` do not return nil.
func (pq *ImmutableMessageQueue) UnsafeAdvance() {
	if len(pq.pendings[0]) == 1 {
		pq.pendings = pq.pendings[1:]
		pq.cnt--
		return
	}
	pq.pendings[0] = pq.pendings[0][1:]
	pq.cnt--
}
