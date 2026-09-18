package utils

import (
	"fmt"
	"math"
	"time"

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
	MaxRows               uint64    // MaxRows is the soft assignment target of the segment and is fixed when the segment becomes growing.
	MaxBinarySize         uint64    // MaxBinarySize is the soft assignment target of the segment and is fixed when the segment becomes growing.
	MaxFullSegmentSize    uint64    // optional hard ceiling on the segment's actual whole-row bytes (0 = disabled). Not persisted: on recovery it is recomputed from the current config, so lowering the ceiling tightens existing growing segments (safe) and raising it relaxes them.
	CreateTime            time.Time // created timestamp of this segment, it's a fixed value when segment is created, not a tso.
	LastModifiedTime      time.Time // LastWriteTime is the last write time of this segment, it's not a tso, just a local time.
	CreateSegmentTimeTick uint64
	BinLogCounter         uint64 // BinLogCounter is the counter of binlog (equal to the binlog file count of primary key), it's an async stat not real time.
	BinLogFileCounter     uint64 // BinLogFileCounter is the counter of binlog files, it's an async stat not real time.
	ReachLimit            bool   // ReachLimit means this segment has accepted its one allocation that crossed the soft assignment target.
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
			SealSize:   statProto.GetModifiedSealSize(),
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
// bytes (0 = disabled). The ceiling is only consulted under the mainIndex
// metric; under wholeRow it is a no-op so current behavior is preserved (I4 of
// the better-segmentation design).
func maxFullSegmentSizeBytes() uint64 {
	if !isMainIndexSizeMetric() {
		return 0
	}
	value := paramtable.Get().DataCoordCfg.MaxFullSegmentSize.GetAsInt64()
	if value <= 0 {
		return 0
	}
	return uint64(value) * 1024 * 1024
}

// isMainIndexSizeMetric reports whether the active size metric is mainIndex.
func isMainIndexSizeMetric() bool {
	return typeutil.IsMainIndexSizeMetric(paramtable.Get().DataCoordCfg.SizeMetric.GetValue())
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
		ModifiedSealSize:      stat.Modified.SealSize,
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
	// Every segment may accept exactly one allocation that crosses its soft
	// assignment target. Once that crossing allocation has been accepted, all
	// later allocations must move to another segment.
	if s.ReachLimit || s.isOverAssignmentTarget() {
		s.ReachLimit = true
		return false
	}

	// The whole-row ceiling is a hard bound (I4), enforced only under the
	// mainIndex metric: an allocation that alone would push the segment past it
	// is never accepted, not even as the single crossing allocation.
	if isMainIndexSizeMetric() && s.MaxFullSegmentSize > 0 && m.BinarySize > s.wholeRowCeilingCanBeAssign() {
		s.ReachLimit = true
		return false
	}

	if !s.canAssign(m) {
		// The seal budget (in the active size metric) and the row cap are soft
		// sealing thresholds, not hard admission limits. Accept the indivisible
		// allocation that crosses them and seal afterwards.
		s.ReachLimit = true
	}

	s.Modified.Collect(m)
	s.LastModifiedTime = time.Now()
	return true
}

// canAssign checks whether the message fits the segment's soft targets: the
// seal budget (in the active size metric) and the row cap. The whole-row
// ceiling is not part of the soft check — AllocRows enforces it as a hard
// bound before this.
func (s *SegmentStats) canAssign(m ModifiedMetrics) bool {
	if m.Rows > s.rowsCanBeAssign() {
		return false
	}
	if incomingSealBudget(m) > s.SealBudgetCanBeAssign() {
		return false
	}
	return true
}

// isOverAssignmentTarget reports whether the segment's accumulated data is
// already past its soft assignment target (strictly over, so a segment exactly
// at the target still accepts its single crossing allocation). The size
// comparison uses the seal budget accumulator in the active metric's unit, so
// under mainIndex a main-column budget is never compared against whole-row
// bytes, and under wholeRow a persisted main-index SealSize (from a previous
// mainIndex period) is never reinterpreted as whole-row bytes. Used both at
// admission time and to recover the sealing decision after SegmentStats is
// rebuilt from persisted stats (ReachLimit is not persisted).
func (s *SegmentStats) isOverAssignmentTarget() bool {
	if s.sealBudgetUsed() > s.MaxBinarySize {
		return true
	}
	return s.MaxRows != 0 && s.Modified.Rows > s.MaxRows
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
// budget. Under mainIndex the seal-specific accumulator (SealSize) is
// authoritative when present; otherwise the whole-row payload size is used
// (wholeRow metric, or a message whose vector column could not be measured —
// premature, safe-direction sealing, see C1 of the third-party review). Under
// wholeRow SealSize is ignored so a persisted main-index value is never
// consumed as whole-row bytes.
func incomingSealBudget(m ModifiedMetrics) uint64 {
	if isMainIndexSizeMetric() && m.SealSize > 0 {
		return m.SealSize
	}
	return m.BinarySize
}

// sealBudgetUsed returns the bytes consumed against the seal budget in the
// active metric's unit: main-index-column bytes when the mainIndex metric is
// active and SealSize is present, whole-row bytes otherwise. It never mixes
// units across the metric.
func (s *SegmentStats) sealBudgetUsed() uint64 {
	if isMainIndexSizeMetric() && s.Modified.SealSize > 0 {
		return s.Modified.SealSize
	}
	return s.Modified.BinarySize
}

// SealBudgetCanBeAssign returns the capacity of the seal budget in the active
// size metric's unit. Falls back to accumulated whole-row bytes when the
// seal-specific accumulator is empty (e.g. a pre-upgrade segment recovered
// without SealSize). The result is saturated to 0 — never underflowed — so a
// recovered segment whose bytes already reached the budget is treated as full
// (seals) instead of becoming unbounded.
func (s *SegmentStats) SealBudgetCanBeAssign() uint64 {
	used := s.sealBudgetUsed()
	if used >= s.MaxBinarySize {
		return 0
	}
	return s.MaxBinarySize - used
}

// BinaryCanBeAssign returns the capacity of binary size can be inserted.
func (s *SegmentStats) BinaryCanBeAssign() uint64 {
	return SaturatingSubUint64(s.MaxBinarySize, s.Modified.BinarySize)
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
	return SaturatingSubUint64(s.MaxRows, s.Modified.Rows)
}

// ShouldBeSealed returns if the segment should be sealed.
func (s *SegmentStats) ShouldBeSealed() bool {
	// ReachLimit is runtime-only, so it is lost when SegmentStats is rebuilt
	// from the persisted assignment stat. Recover the same sealing decision
	// from the persisted modified metrics and assignment targets.
	return s.ReachLimit || s.isOverAssignmentTarget()
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
