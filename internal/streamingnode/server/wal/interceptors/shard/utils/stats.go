package utils

import (
	"fmt"
	"math"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// PartitionUniqueKey is the unique key of a partition.
type PartitionUniqueKey struct {
	CollectionID int64
	PartitionID  int64 // -1 means all partitions, see common.AllPartitionsID.
}

// IsAllPartitions returns true if the partition is all partitions.
func (k *PartitionUniqueKey) IsAllPartitions() bool {
	return k.PartitionID == common.AllPartitionsID
}

// SegmentBelongs is the info of segment belongs to a channel.
type SegmentBelongs struct {
	PChannel     string
	VChannel     string
	CollectionID int64
	PartitionID  int64
	SegmentID    int64
}

// PartitionUniqueKey returns the partition unique key of the segment belongs.
func (s *SegmentBelongs) PartitionUniqueKey() PartitionUniqueKey {
	return PartitionUniqueKey{
		CollectionID: s.CollectionID,
		PartitionID:  s.PartitionID,
	}
}

// SegmentStats is the usage stats of a segment.
type SegmentStats struct {
	Modified              ModifiedMetrics
	RuntimeFlushSize      uint64    // runtime-only size used by StreamingNode flush HWM/LWM decisions; not persisted into recovery meta.
	MaxRows               uint64    // MaxRows of current segment should be assigned, it's a fixed value when segment is transfer int growing.
	MaxBinarySize         uint64    // MaxBinarySize of current segment should be assigned, it's a fixed value when segment is transfer int growing.
	MaxFullSegmentSize    uint64    // optional hard ceiling on the segment's actual whole-row bytes (0 = disabled). Not persisted: on recovery it is recomputed from the current config, so lowering the ceiling tightens existing growing segments (safe) and raising it relaxes them.
	CreateTime            time.Time // created timestamp of this segment, it's a fixed value when segment is created, not a tso.
	LastModifiedTime      time.Time // LastWriteTime is the last write time of this segment, it's not a tso, just a local time.
	CreateSegmentTimeTick uint64
	BinLogCounter         uint64 // BinLogCounter is the counter of binlog (equal to the binlog file count of primary key), it's an async stat not real time.
	BinLogFileCounter     uint64 // BinLogFileCounter is the counter of binlog files, it's an async stat not real time.
	ReachLimit            bool   // ReachLimit is a flag to indicate the segment reach the limit once.
	Level                 datapb.SegmentLevel
}

// NewSegmentStatFromProto creates a new segment assignment stat from proto.
func NewSegmentStatFromProto(statProto *streamingpb.SegmentAssignmentStat) *SegmentStats {
	if statProto == nil {
		return nil
	}
	lv := datapb.SegmentLevel_L1
	if statProto.Level != datapb.SegmentLevel_Legacy {
		lv = statProto.Level
	}
	if lv != datapb.SegmentLevel_L0 && lv != datapb.SegmentLevel_L1 {
		panic(fmt.Sprintf("invalid level: %s", lv))
	}
	maxRows := uint64(math.MaxUint64)
	if statProto.MaxRows != 0 {
		maxRows = statProto.MaxRows
	}
	return &SegmentStats{
		Modified: ModifiedMetrics{
			Rows:       statProto.ModifiedRows,
			BinarySize: statProto.ModifiedBinarySize,
		},
		MaxRows:               maxRows,
		MaxBinarySize:         statProto.MaxBinarySize,
		MaxFullSegmentSize:    maxFullSegmentSizeBytes(),
		CreateTime:            time.Unix(statProto.CreateTimestamp, 0),
		CreateSegmentTimeTick: statProto.CreateSegmentTimeTick,
		BinLogCounter:         statProto.BinlogCounter,
		LastModifiedTime:      time.Unix(statProto.LastModifiedTimestamp, 0),
		Level:                 lv,
	}
}

// maxFullSegmentSizeBytes returns the configured hard whole-row ceiling in
// bytes (0 = disabled).
func maxFullSegmentSizeBytes() uint64 {
	value := paramtable.Get().DataCoordCfg.MaxFullSegmentSize.GetAsInt64()
	if value <= 0 {
		return 0
	}
	return uint64(value) * 1024 * 1024
}

// NewProtoFromSegmentStat creates a new proto from segment assignment stat.
func NewProtoFromSegmentStat(stat *SegmentStats) *streamingpb.SegmentAssignmentStat {
	if stat == nil {
		return nil
	}
	return &streamingpb.SegmentAssignmentStat{
		MaxRows:               stat.MaxRows,
		MaxBinarySize:         stat.MaxBinarySize,
		ModifiedRows:          stat.Modified.Rows,
		ModifiedBinarySize:    stat.Modified.BinarySize,
		CreateTimestamp:       stat.CreateTime.Unix(),
		CreateSegmentTimeTick: stat.CreateSegmentTimeTick,
		BinlogCounter:         stat.BinLogCounter,
		LastModifiedTimestamp: stat.LastModifiedTime.Unix(),
		Level:                 stat.Level,
	}
}

// AllocRows alloc space of rows on current segment.
// Return true if the segment is assigned.
func (s *SegmentStats) AllocRows(m ModifiedMetrics) bool {
	if !s.canAssign(m) {
		if s.Modified.BinarySize > 0 {
			// if the binary size is not empty, it means the segment cannot hold more data, mark it as reach limit.
			s.ReachLimit = true
		}
		return false
	}

	s.Modified.Collect(m)
	s.LastModifiedTime = time.Now()
	return true
}

// canAssign checks whether the message fits the segment. A segment can no
// longer accept inserts when it reaches its seal budget (in the active size
// metric), its row cap, or — when enabled — its whole-row ceiling.
func (s *SegmentStats) canAssign(m ModifiedMetrics) bool {
	if m.Rows > s.rowsCanBeAssign() {
		return false
	}
	if incomingSealBudget(m) > s.SealBudgetCanBeAssign() {
		return false
	}
	if s.MaxFullSegmentSize > 0 && m.BinarySize > s.wholeRowCeilingCanBeAssign() {
		return false
	}
	return true
}

// rowsCanBeAssign returns the capacity of rows can be inserted. A zero MaxRows
// means unbounded (matching the MaxUint64 default applied on recovery).
func (s *SegmentStats) rowsCanBeAssign() uint64 {
	if s.MaxRows == 0 {
		return math.MaxUint64
	}
	return s.MaxRows - s.Modified.Rows
}

// AllocRuntimeFlushSize records runtime-only size growth for flush HWM/LWM decisions.
func (s *SegmentStats) AllocRuntimeFlushSize(size uint64) {
	if size > math.MaxUint64-s.RuntimeFlushSize {
		s.RuntimeFlushSize = math.MaxUint64
		return
	}
	s.RuntimeFlushSize += size
}

// FlushSize returns the size used by runtime flush decisions.
func (s *SegmentStats) FlushSize() uint64 {
	if s.RuntimeFlushSize > 0 {
		return s.RuntimeFlushSize
	}
	return s.Modified.BinarySize
}

// incomingSealBudget returns the bytes a message consumes against the seal
// budget. The seal-specific accumulator (SealSize) is authoritative when
// present; otherwise the whole-row payload size is used (wholeRow metric, L0
// delete messages, or messages whose vector column could not be measured).
func incomingSealBudget(m ModifiedMetrics) uint64 {
	if m.SealSize > 0 {
		return m.SealSize
	}
	return m.BinarySize
}

// SealBudgetCanBeAssign returns the capacity of the seal budget in the active
// size metric's unit. Falls back to accumulated whole-row bytes when the
// seal-specific accumulator is empty (e.g. after recovery, where SealSize is
// not persisted). The result is saturated to 0 — never underflowed — so a
// recovered segment whose whole-row bytes already reached the main-column
// budget is treated as full (seals) instead of becoming unbounded.
func (s *SegmentStats) SealBudgetCanBeAssign() uint64 {
	used := s.Modified.SealSize
	if used == 0 {
		used = s.Modified.BinarySize
	}
	if used >= s.MaxBinarySize {
		return 0
	}
	return s.MaxBinarySize - used
}

// BackfillSealSizeFromSchema reconstructs the seal-size accumulator of a
// recovered segment from the schema fallback (rows × mainIndexPerRecord).
// SealSize is not persisted in recovery meta; without a backfill the budget
// check would compare a main-column budget against whole-row bytes and either
// underflow (losing the size seal) or over-restrict. Sparse/ArrayOfVector-only
// schemas have no dense vector field and are left as-is (they seal via the
// saturated whole-row fallback in SealBudgetCanBeAssign).
func BackfillSealSizeFromSchema(s *SegmentStats, schema *schemapb.CollectionSchema) {
	if s == nil || schema == nil || s.Modified.SealSize > 0 || s.Modified.Rows == 0 {
		return
	}
	perRecord, err := typeutil.EstimateMainIndexSizePerRecord(schema)
	if err != nil || perRecord <= 0 {
		return
	}
	s.Modified.SealSize = s.Modified.Rows * uint64(perRecord)
}

// BinaryCanBeAssign returns the capacity of binary size can be inserted.
func (s *SegmentStats) BinaryCanBeAssign() uint64 {
	return s.MaxBinarySize - s.Modified.BinarySize
}

// wholeRowCeilingCanBeAssign returns the capacity of the whole-row ceiling.
// Caller must ensure MaxFullSegmentSize > 0. The result is saturated to 0 —
// never underflowed — so a segment whose whole-row bytes already exceed the
// ceiling (e.g. the ceiling was lowered and the segment recovered with more
// bytes than the new ceiling) is treated as full and seals immediately, instead
// of the ceiling check being silently bypassed.
func (s *SegmentStats) wholeRowCeilingCanBeAssign() uint64 {
	if s.Modified.BinarySize >= s.MaxFullSegmentSize {
		return 0
	}
	return s.MaxFullSegmentSize - s.Modified.BinarySize
}

// RowsCanBeAssign returns the capacity of rows can be inserted.
func (s *SegmentStats) RowsCanBeAssign() uint64 {
	return s.MaxRows - s.Modified.Rows
}

// ShouldBeSealed returns if the segment should be sealed.
func (s *SegmentStats) ShouldBeSealed() bool {
	return s.ReachLimit
}

// IsEmpty returns if the segment is empty.
func (s *SegmentStats) IsEmpty() bool {
	return s.Modified.Rows == 0
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

// ModifiedMetrics is the metrics of insert/delete operation.
type ModifiedMetrics struct {
	Rows       uint64
	BinarySize uint64
	// SealSize is the per-message bytes consumed against the seal budget in the
	// active size metric's unit (whole-row bytes, or main-index-column bytes).
	// It is NOT persisted in recovery meta; callers fall back to BinarySize
	// when it is empty.
	SealSize uint64
}

// IsZero return true if ModifiedMetrics is zero.
func (m *ModifiedMetrics) IsZero() bool {
	return m.Rows == 0 && m.BinarySize == 0 && m.SealSize == 0
}

// Collect collects other metrics.
func (m *ModifiedMetrics) Collect(other ModifiedMetrics) {
	m.Rows += other.Rows
	m.BinarySize += other.BinarySize
	m.SealSize += other.SealSize
}

// Subtract subtract by other metrics.
func (m *ModifiedMetrics) Subtract(other ModifiedMetrics) {
	if m.Rows < other.Rows {
		panic(fmt.Sprintf("rows cannot be less than zero, current: %d, target: %d", m.Rows, other.Rows))
	}
	if m.BinarySize < other.BinarySize {
		panic(fmt.Sprintf("binary size cannot be less than zero, current: %d, target: %d", m.Rows, other.Rows))
	}
	if m.SealSize < other.SealSize {
		panic(fmt.Sprintf("seal size cannot be less than zero, current: %d, target: %d", m.Rows, other.Rows))
	}
	m.Rows -= other.Rows
	m.BinarySize -= other.BinarySize
	m.SealSize -= other.SealSize
}

// SyncOperationMetrics is the metrics of sync operation.
type SyncOperationMetrics struct {
	BinLogCounterIncr     uint64 // the counter increment of bin log
	BinLogFileCounterIncr uint64 // the counter increment of bin log file
}
