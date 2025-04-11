package manager

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
	metrics *metricsutil.SegmentAssignMetrics,
) *partitionSegmentManager {
	return &partitionSegmentManager{
		mu: sync.Mutex{},
		logger: resource.Resource().Logger().With(
			log.FieldComponent("segment-assigner"),
			zap.Any("pchannel", pchannel),
			zap.Any("pchannel", pchannel),
			zap.String("vchannel", vchannel),
			zap.Int64("collectionID", collectionID),
			zap.Int64("partitionID", paritionID)),
		wal:          wal,
		pchannel:     pchannel,
		vchannel:     vchannel,
		collectionID: collectionID,
		paritionID:   paritionID,
		segments:     segments,
		metrics:      metrics,
	}
}

// partitionSegmentManager is a assign manager of determined partition on determined vchannel.
type partitionSegmentManager struct {
	mu                   sync.Mutex
	logger               *log.MLogger
	wal                  *syncutil.Future[wal.WAL]
	pchannel             types.PChannelInfo
	vchannel             string
	collectionID         int64
	paritionID           int64
	segments             []*segmentAllocManager // there will be very few segments in this list.
	fencedAssignTimeTick uint64                 // the time tick that the assign operation is fenced.
	metrics              *metricsutil.SegmentAssignMetrics
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

// CollectAllSegmentsAndClear collects all segments in the manager and clear the manager.
func (m *partitionSegmentManager) CollectAllSegmentsAndClear() []*segmentAllocManager {
	m.mu.Lock()
	defer m.mu.Unlock()

	segments := m.segments
	m.segments = make([]*segmentAllocManager, 0)
	return segments
}

// CollectAllCanBeSealedAndClear collects all segments that can be sealed and clear the manager.
func (m *partitionSegmentManager) CollectAllCanBeSealedAndClear(sealPolicy policy.SealPolicy) []*segmentAllocManager {
	m.mu.Lock()
	defer m.mu.Unlock()

	canBeSealed := make([]*segmentAllocManager, 0, len(m.segments))
	for _, segment := range m.segments {
		if segment.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING ||
			segment.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_SEALED {
			canBeSealed = append(canBeSealed, segment.WithSealPolicy(sealPolicy))
		}
	}
	m.segments = make([]*segmentAllocManager, 0)
	return canBeSealed
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
	mix, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := mix.AllocSegment(ctx, &datapb.AllocSegmentRequest{
		CollectionId:         pendingSegment.GetCollectionID(),
		PartitionId:          pendingSegment.GetPartitionID(),
		SegmentId:            pendingSegment.GetSegmentID(),
		Vchannel:             pendingSegment.GetVChannel(),
		StorageVersion:       pendingSegment.GetStorageVersion(),
		IsCreatedByStreaming: true,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return nil, errors.Wrap(err, "failed to alloc growing segment at datacoord")
	}
	msg, err := message.NewCreateSegmentMessageBuilderV2().
		WithVChannel(pendingSegment.GetVChannel()).
		WithHeader(&message.CreateSegmentMessageHeader{}).
		WithBody(&message.CreateSegmentMessageBody{
			CollectionId: pendingSegment.GetCollectionID(),
			Segments: []*messagespb.CreateSegmentInfo{{
				// We only execute one segment creation operation at a time.
				// But in future, we need to modify the segment creation operation to support batch creation.
				// Because the partition-key based collection may create huge amount of segments at the same time.
				PartitionId:    pendingSegment.GetPartitionID(),
				SegmentId:      pendingSegment.GetSegmentID(),
				StorageVersion: pendingSegment.GetStorageVersion(),
			}},
		}).BuildMutable()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create new segment message, segmentID: %d", pendingSegment.GetSegmentID())
	}
	// Send CreateSegmentMessage into wal.
	msgID, err := m.wal.Get().Append(ctx, msg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to send create segment message into wal, segmentID: %d", pendingSegment.GetSegmentID())
	}

	// Getnerate growing segment limitation.
	limitation := getSegmentLimitationPolicy().GenerateLimitation()

	// Commit it into streaming node meta.
	// growing segment can be assigned now.
	tx := pendingSegment.BeginModification()
	tx.IntoGrowing(&limitation, msgID.TimeTick)
	if err := tx.Commit(ctx); err != nil {
		return nil, errors.Wrapf(err, "failed to commit modification of segment assignment into growing, segmentID: %d", pendingSegment.GetSegmentID())
	}
	m.logger.Info("generate new growing segment",
		zap.Int64("segmentID", pendingSegment.GetSegmentID()),
		zap.String("messageID", msgID.MessageID.String()),
		zap.Uint64("timetick", msgID.TimeTick),
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
	storageVersion := storage.StorageV1
	if paramtable.Get().CommonCfg.EnableStorageV2.GetAsBool() {
		storageVersion = storage.StorageV2
	}
	meta := newSegmentAllocManager(m.pchannel, m.collectionID, m.paritionID, int64(segmentID), m.vchannel, m.metrics, storageVersion)
	tx := meta.BeginModification()
	tx.IntoPending()
	if err := tx.Commit(ctx); err != nil {
		return nil, errors.Wrap(err, "failed to commit segment assignment modification")
	}
	m.segments = append(m.segments, meta)
	return meta, nil
}

// assignSegment assigns a segment for a assign segment request and return should trigger a seal operation.
func (m *partitionSegmentManager) assignSegment(ctx context.Context, req *AssignSegmentRequest) (*AssignSegmentResult, error) {
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

	// If the timetick is too old for existing segment, it can not be inserted even allocate new growing segment,
	// (new growing segment's timetick is always greater than the old gorwing segmet's timetick).
	// Return directly to avoid unnecessary growing segment allocation.
	if hitTimeTickTooOld {
		return nil, ErrTimeTickTooOld
	}

	// If not inserted, ask a new growing segment to insert.
	newGrowingSegment, err := m.allocNewGrowingSegment(ctx)
	if err != nil {
		return nil, err
	}
	return newGrowingSegment.AllocRows(ctx, req)
}
