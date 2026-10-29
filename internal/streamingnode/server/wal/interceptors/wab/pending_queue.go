package wab

import (
	"io"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// ErrEvicted is returned when the expected message has been evicted.
var (
	ErrEvicted = errors.New("message has been evicted")
	ErrClosed  = errors.New("write ahead buffer is closed")
)

// messageWithOffset is a message with an offset as a unique continuous identifier.
type messageWithOffset struct {
	Message  message.ImmutableMessage
	Offset   int
	Eviction time.Time
}

// newPendingQueue creates a new pendingQueue with given configuration
func newPendingQueue(capacity int, keepAlive time.Duration, lastConfirmedMessage message.ImmutableMessage) *pendingQueue {
	pq := &pendingQueue{
		lastTimeTick:    0,
		readableAfter:   lastConfirmedMessage.TimeTick(),
		consumedThrough: lastConfirmedMessage.TimeTick(),
		capacity:        capacity,
		keepAlive:       keepAlive,
		latestOffset:    -1,
		buf:             make([]messageWithOffset, 0, 10),
		size:            0,
	}
	pq.Push([]message.ImmutableMessage{lastConfirmedMessage})
	return pq
}

// pendingQueue is a buffer that stores messages in order of time tick.
// It retains data and scoped confirmations until RecoveryStorage consumes them.
type pendingQueue struct {
	readableAfter   uint64 // Actual eviction boundary, independent of consumption.
	consumedThrough uint64 // Only records at or below this boundary may be evicted.
	capacity        int
	keepAlive       time.Duration
	lastTimeTick    uint64
	latestOffset    int
	buf             []messageWithOffset
	size            int
}

// Len returns the length of the buffer.
func (q *pendingQueue) Len() int {
	return len(q.buf)
}

// Size returns the size of the buffer.
func (q *pendingQueue) Size() int {
	return q.size
}

// EarliestTimeTick returns the earliest time tick of the buffer.
func (q *pendingQueue) EarliestTimeTick() uint64 {
	if len(q.buf) == 0 {
		return q.lastTimeTick
	}
	return q.buf[0].Message.TimeTick()
}

// Push adds messages to the buffer.
func (q *pendingQueue) Push(msgs []message.ImmutableMessage) {
	now := time.Now()
	for _, msg := range msgs {
		q.pushOne(msg, now)
	}
}

// LastTimeTick returns the last time tick of the buffer.
func (q *pendingQueue) LastTimeTick() uint64 {
	return q.lastTimeTick
}

// CurrentOffset returns the next offset of the buffer.
func (q *pendingQueue) CurrentOffset() int {
	return q.latestOffset
}

// push adds a message to the buffer.
func (q *pendingQueue) pushOne(msg message.ImmutableMessage, now time.Time) {
	isSync := msg.MessageType() == message.MessageTypeTimeTick || msg.MessageType() == message.MessageTypeMVCCBarrier
	if (isSync && msg.TimeTick() < q.lastTimeTick) || (!isSync && msg.TimeTick() <= q.lastTimeTick) {
		// Confirmations may share the last data message's TimeTick.
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
// The continuous slice of messages after (timeTick, ...] will be returned.
func (q *pendingQueue) CreateSnapshotFromExclusiveTimeTick(timeTick uint64) ([]messageWithOffset, error) {
	if timeTick < q.readableAfter {
		return nil, ErrEvicted
	}
	if timeTick >= q.lastTimeTick {
		// If the given timetick is a timetick that has not been generated yet, we reach the end of the buffer.
		// Return io.EOF to perform a block operation.
		return nil, io.EOF
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

// ReleaseThrough makes a consumed prefix eligible for cache eviction.
// Consumption alone does not discard cached records.
func (q *pendingQueue) ReleaseThrough(tick uint64) {
	q.consumedThrough = max(q.consumedThrough, tick)
	q.Evict()
}

func (q *pendingQueue) Evict() {
	q.evict(time.Now())
}

// evict retains the original capacity/TTL policy, constrained by consumption.
// The durable catchup anchor is retained separately by WriteAheadBuffer.
func (q *pendingQueue) evict(now time.Time) {
	n := 0
	for n < len(q.buf) {
		msg := q.buf[n]
		if msg.Message.TimeTick() > q.consumedThrough {
			break
		}
		if q.size <= q.capacity && !msg.Eviction.Before(now) {
			break
		}
		q.readableAfter = max(q.readableAfter, msg.Message.TimeTick())
		q.size -= msg.Message.EstimateSize()
		q.buf[n] = messageWithOffset{}
		n++
	}
	q.buf = q.buf[n:]
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
