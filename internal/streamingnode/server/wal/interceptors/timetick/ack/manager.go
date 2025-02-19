package ack

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// AckManager manages the timestampAck.
type AckManager struct {
	mu                    sync.Mutex
	lastAllocatedTimeTick uint64                // The last allocated time tick, the latest timestamp allocated by the allocator.
	lastConfirmedTimeTick uint64                // The last confirmed time tick, the message which time tick less than lastConfirmedTimeTick has been committed into wal.
	notAckHeap            typeutil.Heap[*Acker] // A minimum heap of timestampAck to search minimum allocated but not ack timestamp in list.
	// Actually, the notAckHeap can be replaced by a list because of the the allocate operation is protected by mutex,
	// keep it as a heap to make the code more readable.
	ackHeap typeutil.Heap[*Acker] // A minimum heap of timestampAck to search minimum ack timestamp in list.
	// It is used to detect the concurrent operation to find the last confirmed message id.
	acknowledgedDetails  sortedDetails         // All ack details which time tick less than lastConfirmedTimeTick will be temporarily kept here until sync operation happens.
	lastConfirmedManager *lastConfirmedManager // The last confirmed message id manager.
	metrics              *metricsutil.TimeTickMetrics
}

// NewAckManager creates a new timestampAckHelper.
func NewAckManager(
	lastConfirmedTimeTick uint64,
	lastConfirmedMessageID message.MessageID,
	metrics *metricsutil.TimeTickMetrics,
) *AckManager {
	return &AckManager{
		mu:                    sync.Mutex{},
		lastAllocatedTimeTick: 0,
		notAckHeap:            typeutil.NewHeap[*Acker](&ackersOrderByTimestamp{}),
		ackHeap:               typeutil.NewHeap[*Acker](&ackersOrderByEndTimestamp{}),
		lastConfirmedTimeTick: lastConfirmedTimeTick,
		lastConfirmedManager:  newLastConfirmedManager(lastConfirmedMessageID),
		metrics:               metrics,
	}
}

// AllocateWithBarrier allocates a timestamp with a barrier.
func (ta *AckManager) AllocateWithBarrier(ctx context.Context, barrierTimeTick uint64) (*Acker, error) {
	// Just make a barrier to the underlying allocator.
	if err := resource.Resource().TSOAllocator().BarrierUntil(ctx, barrierTimeTick); err != nil {
		return nil, err
	}
	return ta.Allocate(ctx)
}

// Allocate allocates a timestamp.
// Concurrent safe to call with Sync and Allocate.
func (ta *AckManager) Allocate(ctx context.Context) (*Acker, error) {
	metricsGuard := ta.metrics.StartAllocateTimeTick()
	ta.mu.Lock()
	defer ta.mu.Unlock()

	// allocate one from underlying allocator first.
	ts, err := resource.Resource().TSOAllocator().Allocate(ctx)
	if err != nil {
		metricsGuard.Done(0, err)
		return nil, err
	}
	ta.lastAllocatedTimeTick = ts

	// create new timestampAck for ack process.
	// add ts to heap wait for ack.
	acker := &Acker{
		acknowledged: false,
		detail:       newAckDetail(ts, ta.lastConfirmedManager.GetLastConfirmedMessageID()),
		manager:      ta,
	}
	ta.notAckHeap.Push(acker)
	metricsGuard.Done(ts, err)
	return acker, nil
}

// SyncAndGetAcknowledged syncs the ack records with allocator, and get the last all acknowledged info.
// Concurrent safe to call with Allocate.
func (ta *AckManager) SyncAndGetAcknowledged(ctx context.Context) ([]*AckDetail, error) {
	// local timestamp may out of date, sync the underlying allocator before get last all acknowledged.
	resource.Resource().TSOAllocator().SyncIfExpired(50 * time.Millisecond)

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

	acker.acknowledged = true
	acker.detail.EndTimestamp = ta.lastAllocatedTimeTick
	ta.ackHeap.Push(acker)
	ta.metrics.CountAcknowledgeTimeTick(acker.ackDetail().IsSync)
	ta.popUntilLastAllAcknowledged()
}

// popUntilLastAllAcknowledged pops the timestamps until the one that all timestamps before it have been acknowledged.
func (ta *AckManager) popUntilLastAllAcknowledged() {
	// pop all acknowledged timestamps.
	acknowledgedDetails := make(sortedDetails, 0, 5)
	for ta.notAckHeap.Len() > 0 && ta.notAckHeap.Peek().acknowledged {
		ack := ta.notAckHeap.Pop()
		acknowledgedDetails = append(acknowledgedDetails, ack.ackDetail())
	}
	if len(acknowledgedDetails) == 0 {
		return
	}

	// update last confirmed time tick.
	ta.lastConfirmedTimeTick = acknowledgedDetails[len(acknowledgedDetails)-1].BeginTimestamp
	ta.metrics.UpdateLastConfirmedTimeTick(ta.lastConfirmedTimeTick)

	// pop all EndTimestamp is less than lastConfirmedTimeTick.
	// All the messages which EndTimetick less than lastConfirmedTimeTick have been committed into wal.
	// So the MessageID of those messages is dense and continuous.
	confirmedDetails := make(sortedDetails, 0, 5)
	for ta.ackHeap.Len() > 0 && ta.ackHeap.Peek().detail.EndTimestamp < ta.lastConfirmedTimeTick {
		ack := ta.ackHeap.Pop()
		confirmedDetails = append(confirmedDetails, ack.ackDetail())
	}
	ta.lastConfirmedManager.AddConfirmedDetails(confirmedDetails, ta.lastConfirmedTimeTick)
	// TODO: cache update operation is also performed here.

	ta.acknowledgedDetails = append(ta.acknowledgedDetails, acknowledgedDetails...)
}
