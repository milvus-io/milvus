package stats

import (
	"time"

	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
)

// SegmentStats is the usage stats of a segment.
type SegmentStats struct {
	Insert           InsertMetrics
	MaxBinarySize    uint64    // MaxBinarySize of current segment should be assigned, it's a fixed value when segment is transfer int growing.
	CreateTime       time.Time // created timestamp of this segment, it's a fixed value when segment is created, not a tso.
	LastModifiedTime time.Time // LastWriteTime is the last write time of this segment, it's not a tso, just a local time.
	BinLogCounter    uint64    // BinLogCounter is the counter of binlog, it's an async stat not real time.
	ReachLimit       bool      // ReachLimit is a flag to indicate the segment reach the limit once.
}

// NewSegmentStatFromProto creates a new segment assignment stat from proto.
func NewSegmentStatFromProto(statProto *streamingpb.SegmentAssignmentStat) *SegmentStats {
	if statProto == nil {
		return nil
	}
	return &SegmentStats{
		Insert: InsertMetrics{
			Rows:       statProto.InsertedRows,
			BinarySize: statProto.InsertedBinarySize,
		},
		MaxBinarySize:    statProto.MaxBinarySize,
		CreateTime:       time.Unix(0, statProto.CreateTimestampNanoseconds),
		BinLogCounter:    statProto.BinlogCounter,
		LastModifiedTime: time.Unix(0, statProto.LastModifiedTimestampNanoseconds),
	}
}

// NewProtoFromSegmentStat creates a new proto from segment assignment stat.
func NewProtoFromSegmentStat(stat *SegmentStats) *streamingpb.SegmentAssignmentStat {
	if stat == nil {
		return nil
	}
	return &streamingpb.SegmentAssignmentStat{
		MaxBinarySize:                    stat.MaxBinarySize,
		InsertedRows:                     stat.Insert.Rows,
		InsertedBinarySize:               stat.Insert.BinarySize,
		CreateTimestampNanoseconds:       stat.CreateTime.UnixNano(),
		BinlogCounter:                    stat.BinLogCounter,
		LastModifiedTimestampNanoseconds: stat.LastModifiedTime.UnixNano(),
	}
}

// FlushOperationMetrics is the metrics of flush operation.
type FlushOperationMetrics struct {
	BinLogCounter uint64
}

// AllocRows alloc space of rows on current segment.
// Return true if the segment is assigned.
func (s *SegmentStats) AllocRows(m InsertMetrics) bool {
	if m.BinarySize > s.BinaryCanBeAssign() {
		s.ReachLimit = true
		return false
	}

	s.Insert.Collect(m)
	s.LastModifiedTime = time.Now()
	return true
}

// BinaryCanBeAssign returns the capacity of binary size can be inserted.
func (s *SegmentStats) BinaryCanBeAssign() uint64 {
	return s.MaxBinarySize - s.Insert.BinarySize
}

// UpdateOnFlush updates the stats of segment on flush.
func (s *SegmentStats) UpdateOnFlush(f FlushOperationMetrics) {
	s.BinLogCounter = f.BinLogCounter
}

// Copy copies the segment stats.
func (s *SegmentStats) Copy() *SegmentStats {
	s2 := *s
	return &s2
}
