package utility

import (
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type PendingQueue struct {
	*typeutil.MultipartQueue[message.ImmutableMessage]
	bytes int
}

func NewPendingQueue() *PendingQueue {
	return &PendingQueue{
		MultipartQueue: typeutil.NewMultipartQueue[message.ImmutableMessage](),
	}
}

func (q *PendingQueue) Bytes() int {
	return q.bytes
}

func (q *PendingQueue) Add(msg []message.ImmutableMessage) {
	for _, m := range msg {
		q.bytes += m.EstimateSize()
	}
	q.MultipartQueue.Add(msg)
}

func (q *PendingQueue) AddOne(msg message.ImmutableMessage) {
	q.bytes += msg.EstimateSize()
	q.MultipartQueue.AddOne(msg)
}

func (q *PendingQueue) UnsafeAdvance() {
	q.bytes -= q.MultipartQueue.Next().EstimateSize()
	q.MultipartQueue.UnsafeAdvance()
}
