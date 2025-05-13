package shards

import (
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/utils"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

// newSegmentAllocManagerFromProto creates a new segment assignment meta from proto.
// if the segment is growing, the stat should be registered to stats manager,
// so it will be returned.
// it will be registered into stats manager as an atomic operation at recover shard manager.
func newSegmentAllocManagerFromProto(pchannel types.PChannelInfo, inner *streamingpb.SegmentAssignmentMeta) *segmentAllocManager {
	if inner.State != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING {
		panic("segment state should be growing")
	}
	return &segmentAllocManager{
		pchannel: pchannel,
		inner:    inner,
		ackSem:   atomic.NewInt32(0),
		txnSem:   atomic.NewInt32(0),
	}
}

// newSegmentAllocManager creates a new segment assignment meta.
func newSegmentAllocManager(pchannel types.PChannelInfo, msg message.ImmutableCreateSegmentMessageV2) *segmentAllocManager {
	meta := recovery.NewSegmentAssignmentMetaFromCreateSegmentMessage(msg)
	stat := utils.NewSegmentStatFromProto(meta.Stat)
	h := msg.Header()
	resource.Resource().SegmentStatsManager().RegisterNewGrowingSegment(utils.SegmentBelongs{
		PChannel:     pchannel.ChannelID().Name,
		VChannel:     msg.VChannel(),
		CollectionID: h.CollectionId,
		PartitionID:  h.PartitionId,
		SegmentID:    h.SegmentId,
	}, stat)
	return &segmentAllocManager{
		pchannel: pchannel,
		inner:    meta,
		ackSem:   atomic.NewInt32(0),
		txnSem:   atomic.NewInt32(0),
	}
}

// segmentAllocManager is the allocation manager of segment assignment,
type segmentAllocManager struct {
	pchannel types.PChannelInfo
	inner    *streamingpb.SegmentAssignmentMeta
	ackSem   *atomic.Int32 // the ackSem is increased when segment allocRows, decreased when the segment is acked.
	txnSem   *atomic.Int32 // the running txn count of the segment, !!! it's lost after wal recovery.

	// only can be seen after the segment is flushed.
	flushedStats *stats.SegmentStats
	sealPolicy   policy.SealPolicy
}

// Flush marks the segment assignment meta as flushed, and unregister the its stat from the stats manager.
func (s *segmentAllocManager) Flush(policy policy.SealPolicy) {
	if s.inner.State != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING {
		return
	}
	s.sealPolicy = policy
	s.inner.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED
	s.flushedStats = resource.Resource().SegmentStatsManager().UnregisterSealedSegment(s.GetSegmentID())
}

// SealPolicy returns the seal policy of the segment assignment meta.
func (s *segmentAllocManager) SealPolicy() policy.SealPolicy {
	if s.inner.State != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED {
		panic("SealPolicy should be called after Flush")
	}
	return s.sealPolicy
}

// CreateSegmentTimeTick returns the create segment time tick of the segment assignment meta.
func (s *segmentAllocManager) CreateSegmentTimeTick() uint64 {
	return s.inner.Stat.CreateSegmentTimeTick
}

// GetStaFromRecovery returns the segment assignment stat of the segment assignment meta.
func (s *segmentAllocManager) GetStatFromRecovery() *stats.SegmentStats {
	if s.inner.State != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING {
		panic("GetStatFromRecovery should be called before Flush")
	}
	return utils.NewSegmentStatFromProto(s.inner.Stat)
}

// GetFlushedStat returns the segment assignment stat of the segment assignment meta.
func (s *segmentAllocManager) GetFlushedStat() *stats.SegmentStats {
	// stats is managed by the SegmentAssignStatsManager before it is sealed.
	if s.inner.State != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED {
		panic("GetStat should be called after Flush")
	}
	return s.flushedStats
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

// AckSem returns the ack sem.
func (s *segmentAllocManager) AckSem() int32 {
	return s.ackSem.Load()
}

// TxnSem returns the txn sem.
func (s *segmentAllocManager) TxnSem() int32 {
	return s.txnSem.Load()
}

// IsFlushed returns true if the segment assignment meta is flushed.
func (s *segmentAllocManager) IsFlushed() bool {
	return s.inner.State == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED
}

// AllocRows ask for rows from current segment.
// Only growing and not fenced segment can alloc rows.
func (s *segmentAllocManager) AllocRows(req *AssignSegmentRequest) (*AssignSegmentResult, error) {
	if req.TimeTick <= s.inner.Stat.CreateSegmentTimeTick {
		// The incoming insert request's timetick is less than the segment's create time tick,
		// return ErrTimeTickTooOld and reallocate new timetick.
		return nil, ErrTimeTickTooOld
	}
	if s.inner.State != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING {
		// The segment is not growing, return ErrWaitForNewSegment.
		return nil, ErrNotGrowing
	}

	err := resource.Resource().SegmentStatsManager().AllocRows(s.GetSegmentID(), req.InsertMetrics)
	if err != nil {
		return nil, err
	}
	s.ackSem.Inc()

	// register the txn session cleanup to the segment.
	if req.TxnSession != nil {
		s.txnSem.Inc()
		req.TxnSession.RegisterCleanup(func() { s.txnSem.Dec() }, req.TimeTick)
	}

	// persist stats if too dirty.
	return &AssignSegmentResult{
		SegmentID:   s.GetSegmentID(),
		Acknowledge: s.ackSem,
	}, nil
}
