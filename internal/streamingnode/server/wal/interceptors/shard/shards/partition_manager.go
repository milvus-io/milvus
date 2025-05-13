package shards

import (
	"context"
	"math"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/utils"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// newPartitionSegmentManager creates a new partition segment assign manager.
func newPartitionSegmentManager(
	ctx context.Context,
	logger *log.MLogger,
	wal *syncutil.Future[wal.WAL],
	pchannel types.PChannelInfo,
	vchannel string,
	collectionID int64,
	paritionID int64,
	segments map[int64]*segmentAllocManager,
	txnManager TxnManager,
	fencedAssignTimeTick uint64,
	metrics *metricsutil.SegmentAssignMetrics,
) *partitionManager {
	for _, segment := range segments {
		if segment.CreateSegmentTimeTick() > fencedAssignTimeTick {
			fencedAssignTimeTick = segment.CreateSegmentTimeTick()
		}
	}
	m := &partitionManager{
		ctx:                  ctx,
		txnManager:           txnManager,
		wal:                  wal,
		pchannel:             pchannel,
		vchannel:             vchannel,
		collectionID:         collectionID,
		partitionID:          paritionID,
		onAllocating:         nil,
		segments:             segments,
		fencedAssignTimeTick: fencedAssignTimeTick,
		metrics:              metrics,
	}
	m.SetLogger(logger.With(zap.String("vchannel", vchannel), zap.Int64("collectionID", collectionID), zap.Int64("partitionID", paritionID)))
	return m
}

// partitionManager is a assign manager of determined partition on determined vchannel.
type partitionManager struct {
	log.Binder

	ctx                  context.Context
	txnManager           TxnManager // the txn manager is used to manage the transaction of the segment.
	wal                  *syncutil.Future[wal.WAL]
	pchannel             types.PChannelInfo
	vchannel             string
	collectionID         int64
	partitionID          int64
	onAllocating         chan struct{}                  // indicates that if the partition manager is on-allocating a new segment.
	segments             map[int64]*segmentAllocManager // there will be very few segments in this list.
	fencedAssignTimeTick uint64                         // the time tick that the assign operation is fenced.
	metrics              *metricsutil.SegmentAssignMetrics
}

// AddSegment adds a segment to the partition segment manager.
func (m *partitionManager) AddSegment(s *segmentAllocManager) {
	if m.onAllocating == nil {
		panic("critical bug: onAllocating is nil when receive a create segment message")
	}
	close(m.onAllocating)
	m.onAllocating = nil
	if s.CreateSegmentTimeTick() <= m.fencedAssignTimeTick {
		panic("critical bug: create segment time tick is less than fenced assign time tick")
	}
	m.segments[s.GetSegmentID()] = s
	m.metrics.ObserveCreateSegment()
}

// GetSegmentManager returns the segment manager of the given segment ID.
func (m *partitionManager) GetSegmentManager(segmentID int64) *segmentAllocManager {
	return m.segments[segmentID]
}

// AssignSegment assigns a segment for a assign segment request.
func (m *partitionManager) AssignSegment(req *AssignSegmentRequest) (*AssignSegmentResult, error) {
	// !!! We have promised that the fencedAssignTimeTick is always less than new incoming insert request by Barrier TimeTick of ManualFlush.
	// So it's just a promise check here.
	// If the request time tick is less than the fenced time tick, the assign operation is fenced.
	// A special error will be returned to indicate the assign operation is fenced.
	if req.TimeTick <= m.fencedAssignTimeTick {
		return nil, ErrFencedAssign
	}
	return m.assignSegment(req)
}

// WaitPendingGrowingSegmentReady waits until the growing segment is ready.
func (m *partitionManager) WaitPendingGrowingSegmentReady() <-chan struct{} {
	if m.onAllocating != nil {
		return m.onAllocating
	}
	ready := make(chan struct{})
	close(ready)
	return ready
}

// FlushAndDropPartition flushes all segments in the partition.
// !!! caller should ensure that the returned segment is flushed by other message (not FlushMessage), such as DropPartition, DropCollection.
func (m *partitionManager) FlushAndDropPartition(policy policy.SealPolicy) []int64 {
	m.fencedAssignTimeTick = math.MaxInt64
	if m.onAllocating != nil {
		close(m.onAllocating)
		m.onAllocating = nil
	}

	segmentIDs := make([]int64, 0, len(m.segments))
	for _, segment := range m.segments {
		segment.Flush(policy)
		m.metrics.ObserveSegmentFlushed(
			string(segment.SealPolicy().Policy),
			int64(segment.GetFlushedStat().Insert.Rows),
			int64(segment.GetFlushedStat().Insert.BinarySize),
		)
		segmentIDs = append(segmentIDs, segment.GetSegmentID())
	}
	m.segments = make(map[int64]*segmentAllocManager)
	return segmentIDs
}

// FlushAndFenceSegmentUntil flush all segment that contains the message or create segment message is less than the incoming timetick.
// !!! caller should ensure that the returned segment is flushed by other message (not FlushMessage), such as ManualFlushMessage, SchemaChange.
func (m *partitionManager) FlushAndFenceSegmentUntil(timeTick uint64) []int64 {
	// no-op if the incoming time tick is less than the fenced time tick.
	if timeTick <= m.fencedAssignTimeTick {
		return nil
	}

	segmentIDs := make([]int64, 0, len(m.segments))
	for _, segment := range m.segments {
		segment.Flush(policy.PolicyFenced(timeTick))
		m.metrics.ObserveSegmentFlushed(
			string(segment.SealPolicy().Policy),
			int64(segment.GetFlushedStat().Insert.Rows),
			int64(segment.GetFlushedStat().Insert.BinarySize),
		)
		segmentIDs = append(segmentIDs, segment.GetSegmentID())
	}
	m.segments = make(map[int64]*segmentAllocManager)

	// fence the assign operation until the incoming time tick or latest assigned timetick.
	// The new incoming assignment request will be fenced.
	// So all the insert operation before the fenced time tick cannot added to the growing segment (no more insert can be applied on it).
	// In other words, all insert operation before the fenced time tick will be sealed
	if timeTick > m.fencedAssignTimeTick {
		m.fencedAssignTimeTick = timeTick
	}
	return segmentIDs
}

// AsyncFlushSegment flushes the segments into the wal asynchronously.
func (m *partitionManager) AsyncFlushSegment(signal utils.SealSegmentSignal) error {
	sm, ok := m.segments[signal.SegmentBelongs.SegmentID]
	if !ok {
		return ErrSegmentNotFound
	}

	if !sm.IsFlushed() {
		sm.Flush(signal.SealPolicy)
		m.metrics.ObserveSegmentFlushed(
			string(sm.SealPolicy().Policy),
			int64(sm.GetFlushedStat().Insert.Rows),
			int64(sm.GetFlushedStat().Insert.BinarySize),
		)
		m.asyncFlushSegment(m.ctx, sm)
	}
	return nil
}

// MustRemoveFlushedSegment removes the flushed segment from the segment manager.
func (m *partitionManager) MustRemoveFlushedSegment(segmentID int64) {
	if !m.segments[segmentID].IsFlushed() {
		panic("critical bug: segment is not flushed before removing")
	}
	delete(m.segments, segmentID)
}

// assignSegment assigns a segment for a assign segment request and return should trigger a seal operation.
func (m *partitionManager) assignSegment(req *AssignSegmentRequest) (*AssignSegmentResult, error) {
	// Alloc segment for insert at allocated segments.
	var lastErr error
	for _, segment := range m.segments {
		result, err := segment.AllocRows(req)
		if err == nil {
			return result, nil
		}
		if errors.IsAny(err, ErrTooLargeInsert, ErrTimeTickTooOld) {
			// Return error directly.
			// If the insert message is too large to hold by single segment, it can not be inserted anymore.
			lastErr = err
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}

	// There is no segment can be allocated for the insert request.
	// Ask a new pending segment to insert.
	m.asyncAllocSegment()
	return nil, ErrWaitForNewSegment
}
