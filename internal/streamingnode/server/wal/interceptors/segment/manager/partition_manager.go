package manager

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var ErrFencedAssign = errors.New("fenced assign")

// newPartitionSegmentManager creates a new partition segment assign manager.
func newPartitionSegmentManager(
	wal *syncutil.Future[wal.WAL],
	pchannel types.PChannelInfo,
	vchannel string,
	collectionID int64,
	paritionID int64,
	segments []*segmentAllocManager,
	allocSegmentWorker *allocSegmentWorker,
	metrics *metricsutil.SegmentAssignMetrics,
) *partitionSegmentManager {
	return &partitionSegmentManager{
		mu: sync.Mutex{},
		logger: resource.Resource().Logger().With(
			log.FieldComponent("segment-assigner"),
			zap.String("pchannel", pchannel.String()),
			zap.String("vchannel", vchannel),
			zap.Int64("collectionID", collectionID),
			zap.Int64("partitionID", paritionID)),
		wal:                wal,
		pchannel:           pchannel,
		vchannel:           vchannel,
		collectionID:       collectionID,
		paritionID:         paritionID,
		segments:           segments,
		allocSegmentWorker: allocSegmentWorker,
		metrics:            metrics,
	}
}

// partitionSegmentManager is a assign manager of determined partition on determined vchannel.
type partitionSegmentManager struct {
	mu                         sync.Mutex
	logger                     *log.MLogger
	wal                        *syncutil.Future[wal.WAL]
	pchannel                   types.PChannelInfo
	vchannel                   string
	collectionID               int64
	paritionID                 int64
	pendingAllocSegmentRequest *syncutil.Future[message.ImmutableCreateSegmentMessageV2] // the pending segment that is on-allocating.
	segments                   []*segmentAllocManager                                    // there will be very few segments in this list.
	fencedAssignTimeTick       uint64                                                    // the time tick that the assign operation is fenced.
	allocSegmentWorker         *allocSegmentWorker
	metrics                    *metricsutil.SegmentAssignMetrics
}

func (m *partitionSegmentManager) CollectionID() int64 {
	return m.collectionID
}

// AssignSegment assigns a segment for a assign segment request.
func (m *partitionSegmentManager) AssignSegment(ctx context.Context, req *AssignSegmentRequest) (*AssignSegmentResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// !!! We have promised that the fencedAssignTimeTick is always less than new incoming insert request by Barrier TimeTick of ManualFlush.
	// So it's just a promise check here.
	// If the request time tick is less than the fenced time tick, the assign operation is fenced.
	// A special error will be returned to indicate the assign operation is fenced.
	if req.TimeTick <= m.fencedAssignTimeTick {
		return nil, ErrFencedAssign
	}
	return m.assignSegment(ctx, req)
}

// SealAndFenceSegmentUntil seal all segment that contains the message less than the incoming timetick.
func (m *partitionSegmentManager) SealAndFenceSegmentUntil(timeTick uint64) (sealedSegments []*segmentAllocManager) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// no-op if the incoming time tick is less than the fenced time tick.
	if timeTick <= m.fencedAssignTimeTick {
		return
	}

	for _, segment := range m.segments {
		segment.WithSealPolicy(policy.PolicyFenced(timeTick))
	}
	segments := m.segments
	m.segments = make([]*segmentAllocManager, 0)

	// fence the assign operation until the incoming time tick or latest assigned timetick.
	// The new incoming assignment request will be fenced.
	// So all the insert operation before the fenced time tick cannot added to the growing segment (no more insert can be applied on it).
	// In other words, all insert operation before the fenced time tick will be sealed
	if timeTick > m.fencedAssignTimeTick {
		m.fencedAssignTimeTick = timeTick
	}
	return segments
}

// GetAndRemoveSegment seals the segment with the specified segmentID.
func (m *partitionSegmentManager) GetAndRemoveSegment(segmentID int64) *segmentAllocManager {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, segment := range m.segments {
		if segment.GetSegmentID() == segmentID {
			m.segments = append(m.segments[:id], m.segments[id+1:]...)
			return segment
		}
	}
	return nil
}

// CollectAllAndClear collects all segments that can be sealed and clear the manager.
func (m *partitionSegmentManager) CollectAllAndClear(sealPolicy policy.SealPolicy) []*segmentAllocManager {
	m.mu.Lock()
	defer m.mu.Unlock()

	canBeSealed := make([]*segmentAllocManager, 0, len(m.segments))
	for _, segment := range m.segments {
		canBeSealed = append(canBeSealed, segment.WithSealPolicy(sealPolicy))
	}
	m.segments = make([]*segmentAllocManager, 0)
	return canBeSealed
}

// assignSegment assigns a segment for a assign segment request and return should trigger a seal operation.
func (m *partitionSegmentManager) assignSegment(ctx context.Context, req *AssignSegmentRequest) (*AssignSegmentResult, error) {
	m.checkIfPendingSegmentReady()
	hitTimeTickTooOld := false
	// Alloc segment for insert at allocated segments.
	for _, segment := range m.segments {
		result, err := segment.AllocRows(ctx, req)
		if err == nil {
			return result, nil
		}
		if errors.IsAny(err, ErrTooLargeInsert) {
			// Return error directly.
			// If the insert message is too large to hold by single segment, it can not be inserted anymore.
			return nil, err
		}
		if errors.Is(err, ErrTimeTickTooOld) {
			hitTimeTickTooOld = true
		}
	}
	// There is no segment can be allocated for the insert request.
	// Ask a new peding segment to insert.
	m.asyncAllocNewGrowingSegment()
	if m.pendingAllocSegmentRequest != nil {
		// wait for the pending segment to be ready.
		utility.ReplaceRedoWait(ctx, m.pendingAllocSegmentRequest.Done())
	}
	// If the timetick is too old for existing segment, it can not be inserted even allocate new growing segment,
	// (new growing segment's timetick is always greater than the old gorwing segmet's timetick).
	// Return directly to avoid unnecessary growing segment allocation.
	if hitTimeTickTooOld {
		return nil, ErrTimeTickTooOld
	}
	return nil, ErrWaitForNewSegment
}

func (m *partitionSegmentManager) checkIfPendingSegmentReady() {
	// check if there's a pending segment.
	if m.pendingAllocSegmentRequest != nil && m.pendingAllocSegmentRequest.Ready() {
		createSegmentMessage := m.pendingAllocSegmentRequest.Get()
		newSegmentManager := newSegmentAllocManager(m.pchannel, createSegmentMessage, m.metrics)
		m.logger.Info(
			"pending segment is ready",
			zap.Int64("segmentID", newSegmentManager.GetSegmentID()),
			zap.String("createSegmentMessageID", createSegmentMessage.MessageID().String()),
			zap.Uint64("createSegmentTimeTick", createSegmentMessage.TimeTick()),
		)
		m.segments = append(m.segments, newSegmentManager)
		m.pendingAllocSegmentRequest = nil
	}
}

// asyncAllocNewGrowingSegment allocates a new growing segment asynchronously.
func (m *partitionSegmentManager) asyncAllocNewGrowingSegment() {
	if m.pendingAllocSegmentRequest == nil {
		m.pendingAllocSegmentRequest = m.allocSegmentWorker.AsyncAllocNewGrowingSegment(
			&AsyncAllocRequest{
				CollectionID: m.collectionID,
				PartitionID:  m.paritionID,
				VChannel:     m.vchannel,
			})
	}
}
