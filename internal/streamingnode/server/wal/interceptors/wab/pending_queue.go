package wab

import (
	"io"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

// ErrEvicted is returned when the expected message has been evicted.
var ErrEvicted = errors.New("message has been evicted")

// messageWithOffset is a message with an offset as a unique continuous identifier.
type messageWithOffset struct {
	Message  message.ImmutableMessage
	Offset   int
	Eviction time.Time
}

// newPendingQueue creates a new pendingQueue with given configuration
func newPendingQueue(capacity int, keepAlive time.Duration, lastConfirmedMessage message.ImmutableMessage) *pendingQueue {
	pq := &pendingQueue{
		lastTimeTick: 0,
		latestOffset: -1,
		buf:          make([]messageWithOffset, 0, 10),
		size:         0,
		capacity:     capacity,
		keepAlive:    keepAlive,
	}
	pq.Push([]message.ImmutableMessage{lastConfirmedMessage})
	return pq
}

// pendingQueue is a buffer that stores messages in order of time tick.
// pendingQueue only keep the persisted messages in the buffer.
type pendingQueue struct {
	lastTimeTick uint64
	latestOffset int
	buf          []messageWithOffset
	size         int
	capacity     int
	keepAlive    time.Duration
}

// Push adds messages to the buffer.
func (q *pendingQueue) Push(msgs []message.ImmutableMessage) {
	now := time.Now()
	for _, msg := range msgs {
		q.pushOne(msg, now)
	}
	q.evict(now)
}

// Evict removes messages that have been in the buffer for longer than the keepAlive duration.
func (q *pendingQueue) Evict() {
	q.evict(time.Now())
}

// CurrentOffset returns the next offset of the buffer.
func (q *pendingQueue) CurrentOffset() int {
	return q.latestOffset
}

// push adds a message to the buffer.
func (q *pendingQueue) pushOne(msg message.ImmutableMessage, now time.Time) {
	if (msg.MessageType() == message.MessageTypeTimeTick && msg.TimeTick() < q.lastTimeTick) ||
		(msg.MessageType() != message.MessageTypeTimeTick && msg.TimeTick() <= q.lastTimeTick) {
		// only timetick message can be repeated with the last time tick.
		panic("message time tick is not in ascending order")
	}
	q.latestOffset++
	q.buf = append(q.buf, messageWithOffset{
		Offset:   q.latestOffset,
		Message:  msg,
		Eviction: now.Add(q.keepAlive),
	})
	q.size += msg.EstimateSize()
	q.lastTimeTick = msg.TimeTick()
}

// CreateSnapshotFromOffset creates a snapshot of the buffer from the given offset.
// The continuous slice of messages after [offset, ...] will be returned.
func (q *pendingQueue) CreateSnapshotFromOffset(offset int) ([]messageWithOffset, error) {
	if offset > q.latestOffset {
		if offset != q.latestOffset+1 {
			panic("unreachable: bug here, the offset is not continuous")
		}
		// If the given version is a version that has not been generated yet, we reach the end of the buffer.
		// Return io.EOF to perform a block operation.
		return nil, io.EOF
	}
	if len(q.buf) == 0 || offset < q.buf[0].Offset {
		// The expected version is out of range, the expected messages has been evicted.
		// So return ErrEvicted to indicate a unrecoverable operation.
		return nil, ErrEvicted
	}

	// Find the offset of the expected offset in the buffer.
	idx := offset - q.buf[0].Offset
	return q.makeSnapshot(idx), nil
}

// CreateSnapshotFromExclusiveTimeTick creates a snapshot of the buffer from the given timetick.
// The coutinous slice of messages after (timeTick, ...] will be returned.
func (q *pendingQueue) CreateSnapshotFromExclusiveTimeTick(timeTick uint64) ([]messageWithOffset, error) {
	if timeTick >= q.lastTimeTick {
		// If the given timetick is a timetick that has not been generated yet, we reach the end of the buffer.
		// Return io.EOF to perform a block operation.
		return nil, io.EOF
	}
	if len(q.buf) == 0 || timeTick < q.buf[0].Message.TimeTick() {
		// The expected timetick is out of range, the expected messages may evict.
		// So return ErrEvicted to indicate a unrecoverable operation.
		return nil, ErrEvicted
	}

	// Find the offset of the expected timetick in the buffer.
	idx := lowerboundOfMessageList(q.buf, timeTick)
	return q.makeSnapshot(idx), nil
}

// makeSnapshot creates a snapshot of the buffer from the given offset.
func (q *pendingQueue) makeSnapshot(idx int) []messageWithOffset {
	snapshot := make([]messageWithOffset, len(q.buf)-idx) // we need a extra position to set a time tick message.
	copy(snapshot, q.buf[idx:])
	return snapshot
}

// evict removes messages that have been in the buffer for longer than the keepAlive duration.
func (q *pendingQueue) evict(now time.Time) {
	releaseUntilIdx := -1
	needRelease := 0
	if q.size > q.capacity {
		needRelease = q.size - q.capacity
	}
	for i := 0; i < len(q.buf); i++ {
		if q.buf[i].Eviction.Before(now) || needRelease > 0 {
			releaseUntilIdx = i
			needRelease -= q.buf[i].Message.EstimateSize()
		} else {
			break
		}
	}

	preservedIdx := releaseUntilIdx + 1
	if preservedIdx > 0 {
		for i := 0; i < preservedIdx; i++ {
			// reset the message as zero to release the resource.
			q.size -= q.buf[i].Message.EstimateSize()
			q.buf[i] = messageWithOffset{}
		}
		q.buf = q.buf[preservedIdx:]
	}
}

// lowerboundOfMessageList returns the lowerbound of the message list.
func lowerboundOfMessageList(data []messageWithOffset, timetick uint64) int {
	// perform a lowerbound search here.
	left, right := 0, len(data)-1
	result := -1
	for left <= right {
		mid := (left + right) / 2
		if data[mid].Message.TimeTick() > timetick {
			result = mid
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	return result
}
