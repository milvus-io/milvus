package stats

import (
	"time"

	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
)

// SegmentStats is the usage stats of a segment.
// The SegmentStats is imprecise, so it is not promised to be recoverable for performance.
type SegmentStats struct {
	Insert            InsertMetrics
	MaxBinarySize     uint64    // MaxBinarySize of current segment should be assigned, it's a fixed value when segment is transfer int growing.
	CreateTime        time.Time // created timestamp of this segment, it's a fixed value when segment is created, not a tso.
	LastModifiedTime  time.Time // LastWriteTime is the last write time of this segment, it's not a tso, just a local time.
	BinLogCounter     uint64    // BinLogCounter is the counter of binlog (equal to the binlog file count of primary key), it's an async stat not real time.
	BinLogFileCounter uint64    // BinLogFileCounter is the counter of binlog files, it's an async stat not real time.
	ReachLimit        bool      // ReachLimit is a flag to indicate the segment reach the limit once.
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

// SyncOperationMetrics is the metrics of sync operation.
type SyncOperationMetrics struct {
	BinLogCounterIncr     uint64 // the counter increment of bin log
	BinLogFileCounterIncr uint64 // the counter increment of bin log file
}

// AllocRows alloc space of rows on current segment.
// Return true if the segment is assigned.
func (s *SegmentStats) AllocRows(m InsertMetrics) bool {
	if m.BinarySize > s.BinaryCanBeAssign() {
		if s.Insert.BinarySize > 0 {
			// if the binary size is not empty, it means the segment cannot hold more data, mark it as reach limit.
			s.ReachLimit = true
		}
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

// ShouldBeSealed returns if the segment should be sealed.
func (s *SegmentStats) ShouldBeSealed() bool {
	return s.ReachLimit
}

// IsEmpty returns if the segment is empty.
func (s *SegmentStats) IsEmpty() bool {
	return s.Insert.Rows == 0
}

// UpdateOnSync updates the stats of segment on sync.
func (s *SegmentStats) UpdateOnSync(f SyncOperationMetrics) {
	s.BinLogCounter += f.BinLogCounterIncr
	s.BinLogFileCounter += f.BinLogFileCounterIncr
}

// Copy copies the segment stats.
func (s *SegmentStats) Copy() *SegmentStats {
	s2 := *s
	return &s2
}
