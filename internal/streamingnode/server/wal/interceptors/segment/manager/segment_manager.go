package manager

import (
	"context"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/stats"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

const dirtyThreshold = 30 * 1024 * 1024 // 30MB

// newSegmentAllocManagerFromProto creates a new segment assignment meta from proto.
func newSegmentAllocManagerFromProto(
	pchannel types.PChannelInfo,
	inner *streamingpb.SegmentAssignmentMeta,
) *segmentAllocManager {
	stat := stats.NewSegmentStatFromProto(inner.Stat)
	// Growing segment's stat should be registered to stats manager.
	// Async sealed policy will use it.
	if inner.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING {
		resource.Resource().SegmentAssignStatsManager().RegisterNewGrowingSegment(stats.SegmentBelongs{
			CollectionID: inner.GetCollectionId(),
			PartitionID:  inner.GetPartitionId(),
			SegmentID:    inner.GetSegmentId(),
			PChannel:     pchannel.Name,
			VChannel:     inner.GetVchannel(),
		}, inner.GetSegmentId(), stat)
		stat = nil
	}
	return &segmentAllocManager{
		pchannel:      pchannel,
		inner:         inner,
		immutableStat: stat,
		ackSem:        atomic.NewInt32(0),
		txnSem:        atomic.NewInt32(0),
		dirtyBytes:    0,
	}
}

// newSegmentAllocManager creates a new segment assignment meta.
func newSegmentAllocManager(
	pchannel types.PChannelInfo,
	collectionID int64,
	partitionID int64,
	segmentID int64,
	vchannel string,
) *segmentAllocManager {
	return &segmentAllocManager{
		pchannel: pchannel,
		inner: &streamingpb.SegmentAssignmentMeta{
			CollectionId: collectionID,
			PartitionId:  partitionID,
			SegmentId:    segmentID,
			Vchannel:     vchannel,
			State:        streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_PENDING,
			Stat:         nil,
		},
		immutableStat: nil, // immutable stat can be seen after sealed.
		ackSem:        atomic.NewInt32(0),
		dirtyBytes:    0,
		txnSem:        atomic.NewInt32(0),
	}
}

// segmentAllocManager is the meta of segment assignment,
// only used to recover the assignment status on streaming node.
// !!! Not Concurrent Safe
// The state transfer is as follows:
// Pending -> Growing -> Sealed -> Flushed.
//
// The recovery process is as follows:
//
// | State | DataCoord View | Writable | WAL Status | Recovery |
// |-- | -- | -- | -- | -- |
// | Pending | Not exist | No | Not exist | 1. Check datacoord if exist; transfer into growing if exist. |
// | Growing | Exist | Yes | Insert Message Exist; Seal Message Not Exist | nothing |
// | Sealed  | Exist | No | Insert Message Exist; Seal Message Maybe Exist | Resend a Seal Message and transfer into Flushed. |
// | Flushed | Exist | No | Insert Message Exist; Seal Message Exist | Already physically deleted, nothing to do |
type segmentAllocManager struct {
	pchannel      types.PChannelInfo
	inner         *streamingpb.SegmentAssignmentMeta
	immutableStat *stats.SegmentStats // after sealed or flushed, the stat is immutable and cannot be seen by stats manager.
	ackSem        *atomic.Int32       // the ackSem is increased when segment allocRows, decreased when the segment is acked.
	dirtyBytes    uint64              // records the dirty bytes that didn't persist.
	txnSem        *atomic.Int32       // the runnint txn count of the segment.
}

// GetCollectionID returns the collection id of the segment assignment meta.
func (s *segmentAllocManager) GetCollectionID() int64 {
	return s.inner.GetCollectionId()
}

// GetPartitionID returns the partition id of the segment assignment meta.
func (s *segmentAllocManager) GetPartitionID() int64 {
	return s.inner.GetPartitionId()
}

// GetSegmentID returns the segment id of the segment assignment meta.
func (s *segmentAllocManager) GetSegmentID() int64 {
	return s.inner.GetSegmentId()
}

// GetVChannel returns the vchannel of the segment assignment meta.
func (s *segmentAllocManager) GetVChannel() string {
	return s.inner.GetVchannel()
}

// State returns the state of the segment assignment meta.
func (s *segmentAllocManager) GetState() streamingpb.SegmentAssignmentState {
	return s.inner.GetState()
}

// Stat get the stat of segments.
// Pending segment will return nil.
// Growing segment will return a snapshot.
// Sealed segment will return the final.
func (s *segmentAllocManager) GetStat() *stats.SegmentStats {
	if s.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING {
		return resource.Resource().SegmentAssignStatsManager().GetStatsOfSegment(s.GetSegmentID())
	}
	return s.immutableStat
}

// AckSem returns the ack sem.
func (s *segmentAllocManager) AckSem() int32 {
	return s.ackSem.Load()
}

// TxnSem returns the txn sem.
func (s *segmentAllocManager) TxnSem() int32 {
	return s.txnSem.Load()
}

// AllocRows ask for rows from current segment.
// Only growing and not fenced segment can alloc rows.
func (s *segmentAllocManager) AllocRows(ctx context.Context, req *AssignSegmentRequest) (bool, *atomic.Int32) {
	// if the segment is not growing or reach limit, return false directly.
	if s.inner.GetState() != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING {
		return false, nil
	}
	inserted := resource.Resource().SegmentAssignStatsManager().AllocRows(s.GetSegmentID(), req.InsertMetrics)
	if !inserted {
		return false, nil
	}
	s.dirtyBytes += req.InsertMetrics.BinarySize
	s.ackSem.Inc()

	// register the txn session cleanup to the segment.
	if req.TxnSession != nil {
		s.txnSem.Inc()
		req.TxnSession.RegisterCleanup(func() { s.txnSem.Dec() }, req.TimeTick)
	}

	// persist stats if too dirty.
	s.persistStatsIfTooDirty(ctx)
	return inserted, s.ackSem
}

// Snapshot returns the snapshot of the segment assignment meta.
func (s *segmentAllocManager) Snapshot() *streamingpb.SegmentAssignmentMeta {
	copied := proto.Clone(s.inner).(*streamingpb.SegmentAssignmentMeta)
	copied.Stat = stats.NewProtoFromSegmentStat(s.GetStat())
	return copied
}

// IsDirtyEnough returns if the dirty bytes is enough to persist.
func (s *segmentAllocManager) IsDirtyEnough() bool {
	// only growing segment can be dirty.
	return s.inner.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING && s.dirtyBytes >= dirtyThreshold
}

// PersisteStatsIfTooDirty persists the stats if the dirty bytes is too large.
func (s *segmentAllocManager) persistStatsIfTooDirty(ctx context.Context) {
	if s.inner.GetState() != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING {
		return
	}
	if s.dirtyBytes < dirtyThreshold {
		return
	}
	if err := resource.Resource().StreamingNodeCatalog().SaveSegmentAssignments(ctx, s.pchannel.Name, []*streamingpb.SegmentAssignmentMeta{
		s.Snapshot(),
	}); err != nil {
		log.Warn("failed to persist stats of segment", zap.Int64("segmentID", s.GetSegmentID()), zap.Error(err))
	}
	s.dirtyBytes = 0
}

// BeginModification begins the modification of the segment assignment meta.
// Do a copy of the segment assignment meta, update the remote meta storage, than modifies the original.
func (s *segmentAllocManager) BeginModification() *mutableSegmentAssignmentMeta {
	copied := s.Snapshot()
	return &mutableSegmentAssignmentMeta{
		original:     s,
		modifiedCopy: copied,
	}
}

// mutableSegmentAssignmentMeta is the mutable version of segment assignment meta.
type mutableSegmentAssignmentMeta struct {
	original     *segmentAllocManager
	modifiedCopy *streamingpb.SegmentAssignmentMeta
}

// IntoGrowing transfers the segment assignment meta into growing state.
func (m *mutableSegmentAssignmentMeta) IntoGrowing(limitation *policy.SegmentLimitation) {
	if m.modifiedCopy.State != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_PENDING {
		panic("tranfer state to growing from non-pending state")
	}
	m.modifiedCopy.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING
	now := time.Now().UnixNano()
	m.modifiedCopy.Stat = &streamingpb.SegmentAssignmentStat{
		MaxBinarySize:                    limitation.SegmentSize,
		CreateTimestampNanoseconds:       now,
		LastModifiedTimestampNanoseconds: now,
	}
}

// IntoSealed transfers the segment assignment meta into sealed state.
func (m *mutableSegmentAssignmentMeta) IntoSealed() {
	if m.modifiedCopy.State != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING {
		panic("tranfer state to sealed from non-growing state")
	}
	m.modifiedCopy.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_SEALED
}

// IntoFlushed transfers the segment assignment meta into flushed state.
// Will be delted physically when transfer into flushed state.
func (m *mutableSegmentAssignmentMeta) IntoFlushed() {
	if m.modifiedCopy.State != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_SEALED {
		panic("tranfer state to flushed from non-sealed state")
	}
	m.modifiedCopy.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED
}

// Commit commits the modification.
func (m *mutableSegmentAssignmentMeta) Commit(ctx context.Context) error {
	if err := resource.Resource().StreamingNodeCatalog().SaveSegmentAssignments(ctx, m.original.pchannel.Name, []*streamingpb.SegmentAssignmentMeta{
		m.modifiedCopy,
	}); err != nil {
		return err
	}
	if m.original.GetState() != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING &&
		m.modifiedCopy.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING {
		// if the state transferred into growing, register the stats to stats manager.
		resource.Resource().SegmentAssignStatsManager().RegisterNewGrowingSegment(stats.SegmentBelongs{
			CollectionID: m.original.GetCollectionID(),
			PartitionID:  m.original.GetPartitionID(),
			SegmentID:    m.original.GetSegmentID(),
			PChannel:     m.original.pchannel.Name,
			VChannel:     m.original.GetVChannel(),
		}, m.original.GetSegmentID(), stats.NewSegmentStatFromProto(m.modifiedCopy.Stat))
	} else if m.original.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING &&
		m.modifiedCopy.GetState() != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING {
		// if the state transferred from growing into others, remove the stats from stats manager.
		m.original.immutableStat = resource.Resource().SegmentAssignStatsManager().UnregisterSealedSegment(m.original.GetSegmentID())
	}
	m.original.inner = m.modifiedCopy
	return nil
}
