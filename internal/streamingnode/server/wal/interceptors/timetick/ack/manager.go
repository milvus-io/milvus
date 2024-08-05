package ack

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/atomic"
)

// AckManager manages the timestampAck.
type AckManager struct {
	mu                    sync.Mutex
	lastAllocatedTimeTick uint64
	notAckHeap            typeutil.Heap[*Acker] // a minimum heap of timestampAck to search minimum allocated timestamp in list.
	ackHeap               typeutil.Heap[*Acker] // a minimum heap of timestampAck to search minimum ack timestamp in list.
	acknowledgedDetails   sortedDetails
	lastConfirmedManager  *lastConfirmedManager
}

// NewAckManager creates a new timestampAckHelper.
func NewAckManager(lastConfirmedMessageID message.MessageID) *AckManager {
	return &AckManager{
		mu:                    sync.Mutex{},
		lastAllocatedTimeTick: 0,
		notAckHeap:            typeutil.NewHeap[*Acker](&ackersOrderByTimestamp{}),
		ackHeap:               typeutil.NewHeap[*Acker](&ackersOrderByEndTimestamp{}),
		lastConfirmedManager:  newLastConfirmedManager(lastConfirmedMessageID),
	}
}

// Allocate allocates a timestamp.
// Concurrent safe to call with Sync and Allocate.
func (ta *AckManager) Allocate(ctx context.Context) (*Acker, error) {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	// allocate one from underlying allocator first.
	ts, err := resource.Resource().TSOAllocator().Allocate(ctx)
	if err != nil {
		return nil, err
	}
	ta.lastAllocatedTimeTick = ts

	// create new timestampAck for ack process.
	// add ts to heap wait for ack.
	acker := &Acker{
		acknowledged: atomic.NewBool(false),
		detail:       newAckDetail(ts, ta.lastConfirmedManager.GetLastConfirmedMessageID()),
		manager:      ta,
	}
	ta.notAckHeap.Push(acker)
	return acker, nil
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

	ta.mu.Lock()
	defer ta.mu.Unlock()
	details := ta.acknowledgedDetails
	ta.acknowledgedDetails = make(sortedDetails, 0, 5)
	return details, nil
}

// ack marks the timestamp as acknowledged.
func (ta *AckManager) ack(acker *Acker) {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	acker.detail.EndTimestamp = ta.lastAllocatedTimeTick
	ta.ackHeap.Push(acker)
	ta.popUntilLastAllAcknowledged()
}

// popUntilLastAllAcknowledged pops the timestamps until the one that all timestamps before it have been acknowledged.
func (ta *AckManager) popUntilLastAllAcknowledged() {
	// pop all acknowledged timestamps.
	acknowledgedDetails := make(sortedDetails, 0, 5)
	for ta.notAckHeap.Len() > 0 && ta.notAckHeap.Peek().acknowledged.Load() {
		ack := ta.notAckHeap.Pop()
		acknowledgedDetails = append(acknowledgedDetails, ack.ackDetail())
	}
	if len(acknowledgedDetails) == 0 {
		return
	}

	// update last confirmed message id.
	lastConfirmedTimeTick := acknowledgedDetails[len(acknowledgedDetails)-1].BeginTimestamp

	// pop all EndTimestamp is less than lastConfirmedTimeTick.
	// The message which EndTimetick less than lastConfirmedTimeTick has all been commited into wal.
	// So the MessageID of the messages is dense and continuous.
	confirmedDetails := make(sortedDetails, 0, 5)
	for ta.ackHeap.Len() > 0 && ta.ackHeap.Peek().detail.EndTimestamp < lastConfirmedTimeTick {
		ack := ta.ackHeap.Pop()
		confirmedDetails = append(confirmedDetails, ack.ackDetail())
	}
	ta.lastConfirmedManager.AddConfirmedDetails(confirmedDetails, lastConfirmedTimeTick)
	// TODO: cache update operation is also performed here.

	ta.acknowledgedDetails = append(ta.acknowledgedDetails, acknowledgedDetails...)
}
