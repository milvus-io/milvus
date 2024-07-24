package manager

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/stats"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

// newPartitionSegmentManager creates a new partition segment assign manager.
func newPartitionSegmentManager(
	pchannel types.PChannelInfo,
	vchannel string,
	collectionID int64,
	paritionID int64,
	segments []*segmentAllocManager,
) *partitionSegmentManager {
	return &partitionSegmentManager{
		mu: sync.Mutex{},
		logger: log.With(
			zap.Any("pchannel", pchannel),
			zap.String("vchannel", vchannel),
			zap.Int64("collectionID", collectionID),
			zap.Int64("partitionID", paritionID)),
		pchannel:     pchannel,
		vchannel:     vchannel,
		collectionID: collectionID,
		paritionID:   paritionID,
		segments:     segments,
	}
}

// partitionSegmentManager is a assign manager of determined partition on determined vchannel.
type partitionSegmentManager struct {
	mu           sync.Mutex
	logger       *log.MLogger
	pchannel     types.PChannelInfo
	vchannel     string
	collectionID int64
	paritionID   int64
	segments     []*segmentAllocManager // there will be very few segments in this list.
}

func (m *partitionSegmentManager) CollectionID() int64 {
	return m.collectionID
}

// AssignSegment assigns a segment for a assign segment request.
func (m *partitionSegmentManager) AssignSegment(ctx context.Context, insert stats.InsertMetrics) (*AssignSegmentResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.assignSegment(ctx, insert)
}

// CollectShouldBeSealed try to collect all segments that should be sealed.
func (m *partitionSegmentManager) CollectShouldBeSealed() []*segmentAllocManager {
	m.mu.Lock()
	defer m.mu.Unlock()

	shouldBeSealedSegments := make([]*segmentAllocManager, 0, len(m.segments))
	segments := make([]*segmentAllocManager, 0, len(m.segments))
	for _, segment := range m.segments {
		// A already sealed segment may be came from recovery.
		if segment.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_SEALED {
			shouldBeSealedSegments = append(shouldBeSealedSegments, segment)
			m.logger.Info("segment has been sealed, remove it from assignment",
				zap.Int64("segmentID", segment.GetSegmentID()),
				zap.String("state", segment.GetState().String()),
				zap.Any("stat", segment.GetStat()),
			)
			continue
		}
		// policy hitted segment should be removed from assignment manager.
		if m.hitSealPolicy(segment) {
			shouldBeSealedSegments = append(shouldBeSealedSegments, segment)
			continue
		}
		segments = append(segments, segment)
	}
	m.segments = segments
	return shouldBeSealedSegments
}

// CollectAllDirtySegments collects all segments in the manager.
func (m *partitionSegmentManager) CollectAllDirtySegments() []*segmentAllocManager {
	m.mu.Lock()
	defer m.mu.Unlock()
	dirtySegments := make([]*segmentAllocManager, 0, len(m.segments))
	for _, segment := range m.segments {
		if segment.IsDirtyEnough() {
			dirtySegments = append(dirtySegments, segment)
		}
	}
	m.segments = make([]*segmentAllocManager, 0)
	return dirtySegments
}

// CollectAllCanBeSealed collects all segments that can be sealed.
func (m *partitionSegmentManager) CollectAllCanBeSealed() []*segmentAllocManager {
	m.mu.Lock()
	defer m.mu.Unlock()
	canBeSealed := make([]*segmentAllocManager, 0, len(m.segments))
	for _, segment := range m.segments {
		if segment.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING ||
			segment.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_SEALED {
			canBeSealed = append(canBeSealed, segment)
		}
	}
	m.segments = make([]*segmentAllocManager, 0)
	return canBeSealed
}

// hitSealPolicy checks if the segment should be sealed by policy.
func (m *partitionSegmentManager) hitSealPolicy(segmentMeta *segmentAllocManager) bool {
	stat := segmentMeta.GetStat()
	for _, p := range policy.GetSegmentAsyncSealPolicy() {
		if result := p.ShouldBeSealed(stat); result.ShouldBeSealed {
			m.logger.Info("segment should be sealed by policy",
				zap.Int64("segmentID", segmentMeta.GetSegmentID()),
				zap.String("policy", result.PolicyName),
				zap.Any("stat", stat),
				zap.Any("extraInfo", result.ExtraInfo),
			)
			return true
		}
	}
	return false
}

// allocNewGrowingSegment allocates a new growing segment.
// After this operation, the growing segment can be seen at datacoord.
func (m *partitionSegmentManager) allocNewGrowingSegment(ctx context.Context) (*segmentAllocManager, error) {
	// A pending segment may be already created when failure or recovery.
	pendingSegment := m.findPendingSegmentInMeta()
	if pendingSegment == nil {
		// if there's no pending segment, create a new pending segment.
		var err error
		if pendingSegment, err = m.createNewPendingSegment(ctx); err != nil {
			return nil, err
		}
	}

	// Transfer the pending segment into growing state.
	// Alloc the growing segment at datacoord first.
	resp, err := resource.Resource().DataCoordClient().AllocSegment(ctx, &datapb.AllocSegmentRequest{
		CollectionId: pendingSegment.GetCollectionID(),
		PartitionId:  pendingSegment.GetPartitionID(),
		SegmentId:    pendingSegment.GetSegmentID(),
		Vchannel:     pendingSegment.GetVChannel(),
	})
	if merr.CheckRPCCall(resp, err); err != nil {
		return nil, errors.Wrap(err, "failed to alloc growing segment at datacoord")
	}

	// Getnerate growing segment limitation.
	limitation := policy.GetSegmentLimitationPolicy().GenerateLimitation()

	// Commit it into streaming node meta.
	// growing segment can be assigned now.
	tx := pendingSegment.BeginModification()
	tx.IntoGrowing(&limitation)
	if err := tx.Commit(ctx); err != nil {
		return nil, errors.Wrapf(err, "failed to commit modification of segment assignment into growing, segmentID: %d", pendingSegment.GetSegmentID())
	}
	m.logger.Info(
		"generate new growing segment",
		zap.Int64("segmentID", pendingSegment.GetSegmentID()),
		zap.String("limitationPolicy", limitation.PolicyName),
		zap.Uint64("segmentBinarySize", limitation.SegmentSize),
		zap.Any("extraInfo", limitation.ExtraInfo),
	)
	return pendingSegment, nil
}

// findPendingSegmentInMeta finds a pending segment in the meta list.
func (m *partitionSegmentManager) findPendingSegmentInMeta() *segmentAllocManager {
	// Found if there's already a pending segment.
	for _, segment := range m.segments {
		if segment.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_PENDING {
			return segment
		}
	}
	return nil
}

// createNewPendingSegment creates a new pending segment.
// pending segment only have a segment id, it's not a real segment,
// and will be transfer into growing state until registering to datacoord.
// The segment id is always allocated from rootcoord to avoid repeated.
// Pending state is used to avoid growing segment leak at datacoord.
func (m *partitionSegmentManager) createNewPendingSegment(ctx context.Context) (*segmentAllocManager, error) {
	// Allocate new segment id and create ts from remote.
	segmentID, err := resource.Resource().IDAllocator().Allocate(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to allocate segment id")
	}
	meta := newSegmentAllocManager(m.pchannel, m.collectionID, m.paritionID, int64(segmentID), m.vchannel)
	tx := meta.BeginModification()
	if err := tx.Commit(ctx); err != nil {
		return nil, errors.Wrap(err, "failed to commit segment assignment modification")
	}
	m.segments = append(m.segments, meta)
	return meta, nil
}

// assignSegment assigns a segment for a assign segment request and return should trigger a seal operation.
func (m *partitionSegmentManager) assignSegment(ctx context.Context, insert stats.InsertMetrics) (*AssignSegmentResult, error) {
	// Alloc segment for insert at previous segments.
	for _, segment := range m.segments {
		inserted, ack := segment.AllocRows(ctx, insert)
		if inserted {
			return &AssignSegmentResult{SegmentID: segment.GetSegmentID(), Acknowledge: ack}, nil
		}
	}

	// If not inserted, ask a new growing segment to insert.
	newGrowingSegment, err := m.allocNewGrowingSegment(ctx)
	if err != nil {
		return nil, err
	}
	if inserted, ack := newGrowingSegment.AllocRows(ctx, insert); inserted {
		return &AssignSegmentResult{SegmentID: newGrowingSegment.GetSegmentID(), Acknowledge: ack}, nil
	}
	return nil, errors.Errorf("too large insert message, cannot hold in empty growing segment, stats: %+v", insert)
}
