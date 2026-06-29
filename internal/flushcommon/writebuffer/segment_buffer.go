package writebuffer

import (
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type segmentBuffer struct {
	segmentID int64

	insertBuffer *InsertBuffer
	deltaBuffer  *DeltaBuffer
}

func newSegmentBuffer(segmentID int64, collSchema *schemapb.CollectionSchema) (*segmentBuffer, error) {
	insertBuffer, err := NewInsertBuffer(collSchema)
	if err != nil {
		return nil, err
	}
	return &segmentBuffer{
		segmentID:    segmentID,
		insertBuffer: insertBuffer,
		deltaBuffer:  NewDeltaBuffer(),
	}, nil
}

func (buf *segmentBuffer) IsFull() bool {
	return (buf.insertBuffer != nil && buf.insertBuffer.IsFull()) ||
		(buf.deltaBuffer != nil && buf.deltaBuffer.IsFull())
}

func (buf *segmentBuffer) Yield() (insert []*storage.InsertData, bm25stats map[int64]*storage.BM25Stats, delete *storage.DeleteData, schema *schemapb.CollectionSchema) {
	if buf.insertBuffer != nil {
		insert = buf.insertBuffer.Yield()
		bm25stats = buf.insertBuffer.YieldStats()
		schema = buf.insertBuffer.collSchema
	}
	if buf.deltaBuffer != nil {
		delete = buf.deltaBuffer.Yield()
	}
	return
}

func (buf *segmentBuffer) MinTimestamp() typeutil.Timestamp {
	insertTs := buf.insertBuffer.MinTimestamp()
	deltaTs := buf.deltaBuffer.MinTimestamp()
	if insertTs < deltaTs {
		return insertTs
	}
	return deltaTs
}

func (buf *segmentBuffer) EarliestPosition() *msgpb.MsgPosition {
	var insertStartPos, deltaStartPos *msgpb.MsgPosition
	if buf.insertBuffer != nil {
		insertStartPos = buf.insertBuffer.startPos
	}
	if buf.deltaBuffer != nil {
		deltaStartPos = buf.deltaBuffer.startPos
	}
	return getEarliestCheckpoint(insertStartPos, deltaStartPos)
}

func (buf *segmentBuffer) GetTimeRange() *TimeRange {
	result := &TimeRange{
		timestampMin: math.MaxUint64,
		timestampMax: 0,
	}
	if buf.insertBuffer != nil {
		result.Merge(buf.insertBuffer.GetTimeRange())
	}
	if buf.deltaBuffer != nil {
		result.Merge(buf.deltaBuffer.GetTimeRange())
	}

	return result
}

// MemorySize returns total memory size of insert buffer and delete buffers.
func (buf *segmentBuffer) MemorySize() int64 {
	var size int64
	if buf.insertBuffer != nil {
		size += buf.insertBuffer.size
	}
	if buf.deltaBuffer != nil {
		size += buf.deltaBuffer.size
	}
	return size
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
