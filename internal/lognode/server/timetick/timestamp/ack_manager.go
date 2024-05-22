package timestamp

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// AckManager manages the timestampAck.
type AckManager struct {
	mu                 sync.Mutex
	timestampAllocator Allocator
	notAckHeap         typeutil.Heap[*Timestamp] // a minimum heap of timestampAck to search minimum timestamp in list.
}

// NewTimestampAckManager creates a new timestampAckHelper.
func NewTimestampAckManager(allocator Allocator) *AckManager {
	return &AckManager{
		mu:                 sync.Mutex{},
		timestampAllocator: allocator,
		notAckHeap:         typeutil.NewHeap[*Timestamp](&timestampWithAckArray{}),
	}
}

// Allocate allocates a timestamp.
// Concurrent safe to call with Sync and Allocate.
func (ta *AckManager) Allocate(ctx context.Context) (*Timestamp, error) {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	// allocate one from underlying allocator first.
	ts, err := ta.timestampAllocator.Allocate(ctx)
	if err != nil {
		return nil, err
	}

	// create new timestampAck for ack process.
	// add ts to heap wait for ack.
	tsWithAck := newTimestampAck(ts)
	ta.notAckHeap.Push(tsWithAck)
	return tsWithAck, nil
}

// Sync syncs the recorder with allocator, and get the last all acknowledged info.
// Concurrent safe to call with Allocate.
func (ta *AckManager) Sync(ctx context.Context) ([]*AckDetail, error) {
	// local timestamp may out of date, sync the underlying allocator before get last all acknowledged.
	ta.timestampAllocator.Sync()

	// AllocateOne may be uncalled in long term, and the recorder may be out of date.
	// Do a Allocate and Ack, can sync up the recorder with internal timetick.TimestampAllocator latest time.
	tsWithAck, err := ta.Allocate(ctx)
	if err != nil {
		return nil, err
	}
	tsWithAck.Ack(OptSync())

	// update a new snapshot of acknowledged timestamps after sync up.
	return ta.popUntilLastAllAcknowledged(), nil
}

// popUntilLastAllAcknowledged pops the timestamps until the one that all timestamps before it have been acknowledged.
func (ta *AckManager) popUntilLastAllAcknowledged() []*AckDetail {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	// pop all acknowledged timestamps.
	details := make([]*AckDetail, 0, 5)
	for ta.notAckHeap.Len() > 0 && ta.notAckHeap.Peek().acknowledged.Load() {
		ack := ta.notAckHeap.Pop()
		details = append(details, ack.ackDetail())
	}
	return details
}
