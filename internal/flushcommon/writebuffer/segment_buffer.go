package writebuffer

import (
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// segmentBuffer is one segment's unflushed data. The insert side lives behind
// the segmentPayload interface — the one axis on which flush modes differ —
// while the delta side is always owned Go memory (deletes are local on every
// path; the L0 machinery is untouched by payload modes).
//
// Unlike the pre-payload design, a segmentBuffer PERSISTS across flush rounds:
// Snapshot moves rows out of the payload instead of the buffer leaving the
// map, so the payload's in-flight floors stay reachable for GetCheckpoint. The
// buffer leaves wb.buffers only when its segment's final (flush/drop) task
// commits.
type segmentBuffer struct {
	segmentID int64

	payload     segmentPayload
	deltaBuffer *DeltaBuffer
}

func newSegmentBuffer(segmentID int64, collSchema *schemapb.CollectionSchema) (*segmentBuffer, error) {
	insertBuffer, err := NewInsertBuffer(collSchema)
	if err != nil {
		return nil, err
	}
	return &segmentBuffer{
		segmentID:   segmentID,
		payload:     newOwnedPayload(insertBuffer),
		deltaBuffer: NewDeltaBuffer(),
	}, nil
}

// newSegmentBufferWithPayload builds a segmentBuffer around an already-chosen
// payload implementation (the ref path; the owned path keeps newSegmentBuffer).
func newSegmentBufferWithPayload(segmentID int64, payload segmentPayload) *segmentBuffer {
	return &segmentBuffer{
		segmentID:   segmentID,
		payload:     payload,
		deltaBuffer: NewDeltaBuffer(),
	}
}

func (buf *segmentBuffer) IsFull() bool {
	return buf.payload.IsFull() || buf.deltaBuffer.IsFull()
}

// IsEmpty reports whether this buffer holds no BUFFERED data at all. In-flight
// floors do not count: they pin the checkpoint (EarliestPosition), but an
// empty buffer must not be selected by sync policies.
func (buf *segmentBuffer) IsEmpty() bool {
	payloadEmpty := buf.payload == nil ||
		(buf.payload.UnflushedRows() == 0 && buf.payload.UnflushedBytes() == 0 &&
			buf.payload.MinTimestamp() == math.MaxUint64)
	deltaEmpty := buf.deltaBuffer == nil ||
		(buf.deltaBuffer.IsEmpty() && buf.deltaBuffer.startPos == nil)
	return payloadEmpty && deltaEmpty
}

// yieldDelta moves the buffered delete data out and resets the delta buffer
// for the next round. It returns the yielded data together with the time range
// and start position it was buffered under (both read BEFORE the reset).
func (buf *segmentBuffer) yieldDelta() (*storage.DeleteData, *TimeRange, *msgpb.MsgPosition) {
	delta := buf.deltaBuffer.Yield()
	timeRange := buf.deltaBuffer.GetTimeRange()
	startPos := buf.deltaBuffer.startPos
	buf.deltaBuffer = NewDeltaBuffer()
	return delta, timeRange, startPos
}

func (buf *segmentBuffer) MinTimestamp() typeutil.Timestamp {
	insertTs := buf.payload.MinTimestamp()
	deltaTs := buf.deltaBuffer.MinTimestamp()

	if insertTs < deltaTs {
		return insertTs
	}
	return deltaTs
}

// EarliestPosition is the replay origin of everything this segment has not yet
// flush-committed: buffered inserts and deletes, plus the payload's in-flight
// snapshot floors.
func (buf *segmentBuffer) EarliestPosition() *msgpb.MsgPosition {
	return getEarliestCheckpoint(buf.payload.EarliestPosition(), buf.deltaBuffer.startPos)
}

// MemorySize returns total resident memory of the buffered (not in-flight)
// insert payload & delta buffer.
func (buf *segmentBuffer) MemorySize() int64 {
	return buf.payload.UnflushedBytes() + buf.deltaBuffer.size
}

// TimeRange is a range of timestamp contains the min-timestamp and max-timestamp
type TimeRange struct {
	timestampMin typeutil.Timestamp
	timestampMax typeutil.Timestamp
}

func NewTimeRange(min, max typeutil.Timestamp) *TimeRange {
	return &TimeRange{
		timestampMin: min,
		timestampMax: max,
	}
}

func (tr *TimeRange) GetMinTimestamp() typeutil.Timestamp {
	return tr.timestampMin
}

func (tr *TimeRange) GetMaxTimestamp() typeutil.Timestamp {
	return tr.timestampMax
}

func (tr *TimeRange) Merge(other *TimeRange) {
	if other.timestampMin < tr.timestampMin {
		tr.timestampMin = other.timestampMin
	}
	if other.timestampMax > tr.timestampMax {
		tr.timestampMax = other.timestampMax
	}
}

func getEarliestCheckpoint(cps ...*msgpb.MsgPosition) *msgpb.MsgPosition {
	var result *msgpb.MsgPosition
	for _, cp := range cps {
		if cp == nil {
			continue
		}
		if result == nil {
			result = cp
			continue
		}

		if cp.GetTimestamp() < result.GetTimestamp() {
			result = cp
		}
	}
	return result
}
