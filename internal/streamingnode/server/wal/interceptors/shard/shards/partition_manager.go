package shards

import (
	"context"
	"math"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/utils"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// newPartitionSegmentManager creates a new partition segment assign manager.
func newPartitionSegmentManager(
	ctx context.Context,
	logger *mlog.Logger,
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
	// Workers bind partition fields per task; idle partitions share the parent logger.
	m.SetLogger(logger)
	return m
}

// partitionManager is a assign manager of determined partition on determined vchannel.
type partitionManager struct {
	mlog.Binder

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
		m.flushSegment(segment, policy)
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
		m.flushSegment(segment, policy.PolicyFenced(timeTick))
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

	if m.flushSegment(sm, signal.SealPolicy) {
		m.asyncFlushSegment(m.ctx, sm)
	}
	return nil
}

// flushSegment records metrics only on the first transition to flushed.
// A segment may remain in the partition while its async Flush message is pending.
func (m *partitionManager) flushSegment(segment *segmentAllocManager, sealPolicy policy.SealPolicy) bool {
	if segment.IsFlushed() {
		return false
	}
	segment.Flush(sealPolicy)
	m.metrics.ObserveSegmentFlushed(
		string(segment.SealPolicy().Policy),
		int64(segment.GetFlushedStat().Modified.Rows),
		int64(segment.GetFlushedStat().Modified.BinarySize),
	)
	return true
}

// MustRemoveFlushedSegment removes the flushed segment from the segment manager.
func (m *partitionManager) MustRemoveFlushedSegment(segmentID int64) {
	if !m.segments[segmentID].IsFlushed() {
		panic("critical bug: segment is not flushed before removing")
	}
	delete(m.segments, segmentID)
}

// prepareAssignment selects a candidate without accounting or references.
// The caller holds shardManagerImpl.mu through final batch accounting. This
// selection is not a reservation; StatsManager rechecks the entire batch.
func (m *partitionManager) prepareAssignment(req *AssignSegmentRequest) (*segmentAllocManager, error) {
	if req.TimeTick <= m.fencedAssignTimeTick {
		return nil, ErrFencedAssign
	}
	var lastErr error
	for _, segment := range m.segments {
		// Preserve the legacy single-assignment TimeTick precedence, including
		// flushed segments still awaiting their asynchronous Flush message.
		if req.TimeTick <= segment.CreateSegmentTimeTick() {
			lastErr = ErrTimeTickTooOld
			continue
		}
		if segment.inner.State != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING {
			continue
		}
		if resource.Resource().SegmentStatsManager().CanAllocRows(segment.GetSegmentID()) {
			return segment, nil
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	m.asyncAllocSegment(req.SchemaVersion, req.RequiresStorageV3)
	return nil, ErrWaitForNewSegment
}
