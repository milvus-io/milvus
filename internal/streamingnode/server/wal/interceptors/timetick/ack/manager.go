package ack

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// AckManager manages the timestampAck.
type AckManager struct {
	mu                     sync.Mutex
	notAckHeap             typeutil.Heap[*Acker] // a minimum heap of timestampAck to search minimum timestamp in list.
	lastConfirmedMessageID message.MessageID
}

// NewAckManager creates a new timestampAckHelper.
func NewAckManager() *AckManager {
	return &AckManager{
		mu:         sync.Mutex{},
		notAckHeap: typeutil.NewHeap[*Acker](&timestampWithAckArray{}),
	}
}

// Allocate allocates a timestamp.
// Concurrent safe to call with Sync and Allocate.
func (ta *AckManager) Allocate(ctx context.Context) (*Acker, error) {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	// allocate one from underlying allocator first.
	ts, err := resource.Resource().TSOAllocator().AllocOne()
	if err != nil {
		return nil, err
	}

	// create new timestampAck for ack process.
	// add ts to heap wait for ack.
	tsWithAck := newAcker(uint64(ts), ta.lastConfirmedMessageID)
	ta.notAckHeap.Push(tsWithAck)
	return tsWithAck, nil
}

// SyncAndGetAcknowledged syncs the ack records with allocator, and get the last all acknowledged info.
// Concurrent safe to call with Allocate.
func (ta *AckManager) SyncAndGetAcknowledged(ctx context.Context) ([]*AckDetail, error) {
	// local timestamp may out of date, sync the underlying allocator before get last all acknowledged.
	resource.Resource().TSOAllocator().Sync()

	// Allocate may be uncalled in long term, and the recorder may be out of date.
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

// AdvanceLastConfirmedMessageID update the last confirmed message id.
func (ta *AckManager) AdvanceLastConfirmedMessageID(msgID message.MessageID) {
	if msgID == nil {
		return
	}

	ta.mu.Lock()
	if ta.lastConfirmedMessageID == nil || ta.lastConfirmedMessageID.LT(msgID) {
		ta.lastConfirmedMessageID = msgID
	}
	ta.mu.Unlock()
}
