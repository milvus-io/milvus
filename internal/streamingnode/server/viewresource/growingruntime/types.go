package growingruntime

import (
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func settingsFromWALView(view walview.VChannelWALView) *viewpb.QueryViewSettings {
	return settingsFromAlterLoadConfig(view.LoadConfig.GetHeader())
}

func settingsFromAlterLoadConfig(header *messagespb.AlterLoadConfigMessageHeader) *viewpb.QueryViewSettings {
	if header == nil {
		return &viewpb.QueryViewSettings{}
	}
	fields := make([]int64, 0, len(header.GetLoadFields()))
	for _, field := range header.GetLoadFields() {
		fields = append(fields, field.GetFieldId())
	}
	return &viewpb.QueryViewSettings{
		RequiredPartitions: append([]int64{}, header.GetPartitionIds()...),
		RequiredFields:     fields,
	}
}

// Builder converts WAL-side growing state into queryable csegment-backed
// resources for the requested latest DataVersion.
type Builder interface {
	NewRuntime() (*Runtime, error)
}

// Runtime is the csegment-backed growing side prepared for one DataVersion.
type Runtime struct {
	mu                       sync.RWMutex
	closed                   bool
	collection               *segcore.CCollection
	segments                 map[int64]*growingSegment
	segmentIDs               []int64
	truncateDataVersion      qviews.DataVersion
	hasTruncateDataVersion   bool
	closeOnce                sync.Once
	appliedGrowingTimeTick   atomic.Uint64
	appliedTransformTimeTick atomic.Uint64
}

func newRuntime() *Runtime {
	return &Runtime{
		segments: make(map[int64]*growingSegment),
	}
}

func (r *Runtime) AppliedGrowingTimeTick() uint64 {
	if r == nil {
		return 0
	}
	return r.appliedGrowingTimeTick.Load()
}

func (r *Runtime) AppliedTransformTimeTick() uint64 {
	if r == nil {
		return 0
	}
	return r.appliedTransformTimeTick.Load()
}

func (r *Runtime) Segment(segmentID int64) (segcore.CSegment, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	segment := r.segments[segmentID]
	r.mu.RUnlock()
	if segment == nil {
		return nil, false
	}
	return segment.csegment()
}

func (r *Runtime) SegmentFlushed(segmentID int64) bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	segment := r.segments[segmentID]
	r.mu.RUnlock()
	return segment != nil && segment.isFlushed()
}

func (r *Runtime) SegmentIDs() []int64 {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]int64(nil), r.segmentIDs...)
}

func (r *Runtime) Truncate(minDataVersion qviews.DataVersion) {
	if r == nil {
		return
	}
	r.mu.Lock()
	if !r.hasTruncateDataVersion || minDataVersion.GT(r.truncateDataVersion) {
		r.truncateDataVersion = minDataVersion
		r.hasTruncateDataVersion = true
	}
	segmentsToRelease := make([]*growingSegment, 0)
	for segmentID, segment := range r.segments {
		if segment.shouldRelease(r.truncateDataVersion) {
			segmentsToRelease = append(segmentsToRelease, segment)
			r.removeSegmentMetadataLocked(segmentID)
		}
	}
	r.mu.Unlock()
	for _, segment := range segmentsToRelease {
		segment.release()
	}
}

func (r *Runtime) Advance(minDataVersion qviews.DataVersion) {
	r.Truncate(minDataVersion)
}

func (r *Runtime) removeSegmentMetadataLocked(segmentID int64) {
	delete(r.segments, segmentID)
	for i, id := range r.segmentIDs {
		if id == segmentID {
			r.segmentIDs = append(r.segmentIDs[:i], r.segmentIDs[i+1:]...)
			return
		}
	}
}

func (r *Runtime) Close() {
	if r == nil {
		return
	}
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()

		r.mu.Lock()
		segments := make([]*growingSegment, 0, len(r.segments))
		for _, segment := range r.segments {
			segments = append(segments, segment)
		}
		r.segments = nil
		r.segmentIDs = nil
		collection := r.collection
		r.collection = nil
		r.mu.Unlock()
		for _, segment := range segments {
			segment.release()
		}
		if collection != nil {
			collection.Release()
		}
	})
}
