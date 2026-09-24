// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/segmentutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// dcSegmentState holds the DC-only fields that are not part of the shared
// datapb.SegmentInfo proto and therefore never go through MetaStore.
type dcSegmentState struct {
	allocations     []*Allocation
	lastFlushTime   time.Time
	isCompacting    bool
	lastWrittenTime time.Time
}

// SegmentsInfo wraps a map, which maintains ID to SegmentInfo relation
type SegmentsInfo struct {
	// store is the primary proto storage.
	store   metacache.MetaStore
	dcState map[UniqueID]*dcSegmentState

	// map the compact relation, value is the segment which `CompactFrom` contains key.
	// now segment could be compacted to multiple segments
	compactionTo map[UniqueID][]UniqueID
}

// SegmentInfo wraps datapb.SegmentInfo and patches some extra info on it
type SegmentInfo struct {
	*datapb.SegmentInfo
	allocations     []*Allocation
	lastFlushTime   time.Time
	isCompacting    bool
	lastWrittenTime time.Time
}

// EnsureStats returns a non-nil Statistics view for read-only aggregate
// queries. It does NOT mutate s — concurrent readers under m.segMu.RLock()
// would race otherwise. The persisted s.Stats is populated eagerly by
// NewSegmentInfo on construction and by the array-mutating operators
// (AddBinlogsOperator, UpdateBinlogsFromSaveBinlogPathsOperator,
// UpdateSegmentStats), both of which run under m.segMu.Lock(). When a
// caller hands us a SegmentInfo built via the struct literal
// `&SegmentInfo{SegmentInfo: ...}` with a nil Stats (the only remaining
// path is now legacy tests), we fall back to a transient recompute so
// readers see the right number; we just don't write it back.
func (s *SegmentInfo) EnsureStats() *datapb.Statistics {
	if s.SegmentInfo == nil {
		return nil
	}
	if stats := s.GetStats(); stats != nil {
		return stats
	}
	return storage.BuildStatsFromFieldBinlogs(s.GetBinlogs(), s.GetStatslogs(), s.GetBm25Statslogs(), s.GetDeltalogs())
}

func (s *SegmentInfo) GetResidualSegmentSize() int64 {
	if s.GetNumOfRows() == 0 {
		return 0
	}
	deltaRatio := float64(s.EnsureStats().GetDeleteNumRows()) / float64(s.GetNumOfRows())
	if deltaRatio >= 1.0 {
		// segments with too many deleted rows should be considered as prioritized segments and be compacted definitely
		return s.getSegmentSize()
	}
	residualRatio := 1.0 - deltaRatio
	return int64(residualRatio * float64(s.getSegmentSize()))
}

func (s *SegmentInfo) GetEarliestTs() uint64 {
	// For import segments, row timestamps predate the actual commit time.
	// Use commit_timestamp as the effective data age so compaction priority
	// and TTL decisions are not distorted by stale row timestamps.
	if commitTs := s.GetCommitTimestamp(); commitTs != 0 {
		return commitTs
	}
	// Stats.TimestampFrom is the exact min(TimestampFrom) across all insert
	// binlogs (populated by StatisticsCollector on the writer side, or by
	// BuildStatsFromFieldBinlogs on V2 fallback / migration).
	return s.EnsureStats().GetTimestampFrom()
}

// NewSegmentInfo create `SegmentInfo` wrapper from `datapb.SegmentInfo`
// assign current rows to last checkpoint and pre-allocate `allocations` slice
// Note that the allocation information is not preserved,
// the worst case scenario is to have a segment with twice size we expects
//
// Stats is populated from the FieldBinlog arrays when nil so legacy
// segments (persisted before Statistics existed) and live aggregate
// reads agree without callers needing a fallback. EnsureStats covers the
// struct-literal construction path that bypasses this constructor.
func NewSegmentInfo(info *datapb.SegmentInfo) *SegmentInfo {
	if info.Stats == nil {
		info.Stats = storage.BuildStatsFromFieldBinlogs(info.GetBinlogs(), info.GetStatslogs(), info.GetBm25Statslogs(), info.GetDeltalogs())
	}
	s := &SegmentInfo{
		SegmentInfo: info,
	}
	// setup growing fields
	if s.GetState() == commonpb.SegmentState_Growing {
		s.allocations = make([]*Allocation, 0, 16)
		s.lastFlushTime = time.Now().Add(-1 * paramtable.Get().DataCoordCfg.SegmentFlushInterval.GetAsDuration(time.Second))
		// A growing segment from recovery can be also considered idle.
		s.lastWrittenTime = getZeroTime()
	}
	return s
}

// NewSegmentsInfo creates a `SegmentsInfo` instance backed by the
// provided MetaStore as the primary proto storage. DC-only fields are kept
// in a local dcState map. Note that no mutex is wrapped so external
// concurrent control is needed.
func NewSegmentsInfo(store metacache.MetaStore) *SegmentsInfo {
	return &SegmentsInfo{
		store:        store,
		dcState:      make(map[UniqueID]*dcSegmentState),
		compactionTo: make(map[UniqueID][]UniqueID),
	}
}

// RebuildFromStore initializes the DataCoord-side state for every segment
// already present in the shared store. Recovery loads segments straight into
// the store (MetaStore.LoadFromCatalog), bypassing SetSegment, so without this
// the compaction relations and the per-segment transient state would stay
// empty until each segment happens to be written again.
func (s *SegmentsInfo) RebuildFromStore() {
	for id, segProto := range s.store.GetAllSegments() {
		s.addCompactTo(&SegmentInfo{SegmentInfo: segProto})
		s.dcState[id] = newDCSegmentState(segProto)
	}
}

// newDCSegmentState seeds the transient state of a segment read back from the
// catalog, mirroring what NewSegmentInfo sets for a growing segment.
func newDCSegmentState(segProto *datapb.SegmentInfo) *dcSegmentState {
	dc := &dcSegmentState{}
	if segProto.GetState() == commonpb.SegmentState_Growing {
		dc.allocations = make([]*Allocation, 0, 16)
		dc.lastFlushTime = time.Now().Add(-1 * paramtable.Get().DataCoordCfg.SegmentFlushInterval.GetAsDuration(time.Second))
		// A growing segment from recovery can be also considered idle.
		dc.lastWrittenTime = getZeroTime()
	}
	return dc
}

// dcStateFor returns the transient state of a segment, creating it when the
// segment entered memory through the store rather than through SetSegment.
func (s *SegmentsInfo) dcStateFor(segmentID UniqueID) *dcSegmentState {
	if dc, ok := s.dcState[segmentID]; ok {
		return dc
	}
	segProto, ok := s.store.GetSegment(segmentID)
	if !ok {
		return nil
	}
	dc := newDCSegmentState(segProto)
	s.dcState[segmentID] = dc
	return dc
}

// assemble combines a proto SegmentInfo (from MetaStore) with the DC-only
// state (from dcState) into a *SegmentInfo. dc may be nil.
// Callers MUST NOT mutate the returned SegmentInfo.SegmentInfo proto; use the
// SegmentsInfo setter methods which clone before writing.
func (s *SegmentsInfo) assemble(segProto *datapb.SegmentInfo, dc *dcSegmentState) *SegmentInfo {
	si := &SegmentInfo{SegmentInfo: segProto}
	if dc != nil {
		si.allocations = dc.allocations
		si.lastFlushTime = dc.lastFlushTime
		si.isCompacting = dc.isCompacting
		si.lastWrittenTime = dc.lastWrittenTime
	}
	return si
}

// GetSegment returns SegmentInfo
// the logPath in meta is empty
// Callers MUST NOT mutate the returned SegmentInfo.SegmentInfo proto; use the
// SegmentsInfo setter methods which clone before writing.
func (s *SegmentsInfo) GetSegment(segmentID UniqueID) *SegmentInfo {
	segProto, ok := s.store.GetSegment(segmentID)
	if !ok {
		return nil
	}
	return s.assemble(segProto, s.dcState[segmentID])
}

// GetSegments iterates internal map and returns all SegmentInfo in a slice
// no deep copy applied
// the logPath in meta is empty
func (s *SegmentsInfo) GetSegments() []*SegmentInfo {
	all := s.store.GetAllSegments()
	result := make([]*SegmentInfo, 0, len(all))
	for id, segProto := range all {
		result = append(result, s.assemble(segProto, s.dcState[id]))
	}
	return result
}

func (s *SegmentsInfo) getCandidates(criterion *segmentCriterion) map[UniqueID]*SegmentInfo {
	var protos map[int64]*datapb.SegmentInfo
	switch {
	case criterion.collectionID > 0 && criterion.channel != "":
		collSegs := s.store.GetSegments(criterion.collectionID)
		protos = make(map[int64]*datapb.SegmentInfo)
		for id, seg := range collSegs {
			if seg.GetInsertChannel() == criterion.channel {
				protos[id] = seg
			}
		}
	case criterion.collectionID > 0:
		protos = s.store.GetSegments(criterion.collectionID)
	case criterion.channel != "":
		protos = s.store.GetSegmentsByChannel(criterion.channel)
	default:
		protos = s.store.GetAllSegments()
	}
	result := make(map[UniqueID]*SegmentInfo, len(protos))
	for id, segProto := range protos {
		result[id] = s.assemble(segProto, s.dcState[id])
	}
	return result
}

func (s *SegmentsInfo) GetSegmentsBySelector(filters ...SegmentFilter) []*SegmentInfo {
	criterion := &segmentCriterion{}
	for _, filter := range filters {
		filter.AddFilter(criterion)
	}

	// apply criterion
	candidates := s.getCandidates(criterion)
	result := make([]*SegmentInfo, 0, len(candidates))
	for _, segment := range candidates {
		if criterion.Match(segment) {
			result = append(result, segment)
		}
	}
	return result
}

func (s *SegmentsInfo) GetRealSegmentsForChannel(channel string) []*SegmentInfo {
	protos := s.store.GetSegmentsByChannel(channel)
	var result []*SegmentInfo
	for id, p := range protos {
		if !p.GetIsFake() {
			result = append(result, s.assemble(p, s.dcState[id]))
		}
	}
	return result
}

// GetCompactionTo returns the segment that the provided segment is compacted to.
// Return (nil, false) if given segmentID can not found in the meta and compact to is nil.
// Return (nil, true) if given segmentID can be found with no compaction to.
// Return (notnil, true) if given segmentID can be found and has compaction to.
func (s *SegmentsInfo) GetCompactionTo(fromSegmentID int64) ([]*SegmentInfo, bool) {
	_, exist := s.store.GetSegment(fromSegmentID)
	if compactTos, ok := s.compactionTo[fromSegmentID]; ok {
		result := []*SegmentInfo{}
		for _, compactTo := range compactTos {
			segProto, ok := s.store.GetSegment(compactTo)
			if !ok {
				mlog.Warn(context.TODO(), "compactionTo relation is broken", mlog.Int64("from", fromSegmentID), mlog.Int64("to", compactTo))
				return nil, exist
			}
			result = append(result, s.assemble(segProto, s.dcState[compactTo]))
		}
		return result, exist
	}
	return nil, exist
}

func (s *SegmentsInfo) DropSegment(segmentID UniqueID) {
	if seg, ok := s.store.GetSegment(segmentID); ok {
		s.deleteCompactFrom(seg.GetCompactionFrom())
	}
	s.store.RemoveSegment(segmentID)
	delete(s.dcState, segmentID)
}

// normalizeSegmentRowCount reconciles a V1/V2 segment's NumOfRows with its
// insert binlogs before the segment enters memory. The catalog applies the same
// correction, but only to the copy it persists (kv_catalog.go), which would
// leave the in-memory segment — the one QueryCoord reads through the shared
// store — carrying the stale count until the next reload.
//
// V3 segments are exempt: their binlog arrays are a pass-through cache that can
// be empty after recovery or delta-only during a growing-source flush, so an
// array-derived count would be wrong.
func normalizeSegmentRowCount(segProto *datapb.SegmentInfo) {
	if segProto.GetStorageVersion() == storage.StorageV3 || segProto.GetManifestPath() != "" {
		return
	}
	segmentutil.ReCalcRowCount(segProto, segProto)
}

func (s *SegmentsInfo) SetSegment(segmentID UniqueID, segment *SegmentInfo) {
	if old, ok := s.store.GetSegment(segmentID); ok {
		s.deleteCompactFrom(old.GetCompactionFrom())
	}
	normalizeSegmentRowCount(segment.SegmentInfo)
	s.store.PutSegment(segment.SegmentInfo)
	s.dcState[segmentID] = &dcSegmentState{
		allocations:     segment.allocations,
		lastFlushTime:   segment.lastFlushTime,
		isCompacting:    segment.isCompacting,
		lastWrittenTime: segment.lastWrittenTime,
	}
	s.addCompactTo(segment)
}

// modifyProto clones the proto for segmentID, applies fn, and writes it back.
func (s *SegmentsInfo) modifyProto(segmentID UniqueID, fn func(*datapb.SegmentInfo)) {
	seg, ok := s.store.GetSegment(segmentID)
	if !ok {
		return
	}
	cloned := proto.Clone(seg).(*datapb.SegmentInfo)
	fn(cloned)
	s.store.PutSegment(cloned)
}

func (s *SegmentsInfo) SetRowCount(segmentID UniqueID, rowCount int64) {
	s.modifyProto(segmentID, func(seg *datapb.SegmentInfo) { seg.NumOfRows = rowCount })
}

func (s *SegmentsInfo) SetDmlPosition(segmentID UniqueID, pos *msgpb.MsgPosition) {
	s.modifyProto(segmentID, func(seg *datapb.SegmentInfo) { seg.DmlPosition = pos })
}

func (s *SegmentsInfo) SetStartPosition(segmentID UniqueID, pos *msgpb.MsgPosition) {
	s.modifyProto(segmentID, func(seg *datapb.SegmentInfo) { seg.StartPosition = pos })
}

func (s *SegmentsInfo) SetAllocations(segmentID UniqueID, allocations []*Allocation) {
	if dc := s.dcStateFor(segmentID); dc != nil {
		dc.allocations = allocations
	}
}

func (s *SegmentsInfo) AddAllocation(segmentID UniqueID, allocation *Allocation) {
	s.modifyProto(segmentID, func(seg *datapb.SegmentInfo) {
		seg.LastExpireTime = allocation.ExpireTime
	})
	if dc := s.dcStateFor(segmentID); dc != nil {
		dc.allocations = append(dc.allocations, allocation)
	}
}

func (s *SegmentsInfo) SetLastWrittenTime(segmentID UniqueID) {
	if dc := s.dcStateFor(segmentID); dc != nil {
		dc.lastWrittenTime = time.Now()
	}
}

func (s *SegmentsInfo) SetFlushTime(segmentID UniqueID, t time.Time) {
	if dc := s.dcStateFor(segmentID); dc != nil {
		dc.lastFlushTime = t
	}
}

// SetIsCompacting sets compaction status for segment.
// NOTE: This method manually updates secondary indexes after ShadowClone.
// Other Set methods (SetRowCount, SetFlushTime, etc.) have the same
// stale-index problem but are not yet fixed. See #48593 for the tracking issue
// to extract a common updateSegment helper for all Set methods.
func (s *SegmentsInfo) SetIsCompacting(segmentID UniqueID, isCompacting bool) {
	if dc := s.dcStateFor(segmentID); dc != nil {
		dc.isCompacting = isCompacting
	}
}

func (s *SegmentInfo) IsDeltaLogExists(logID int64) bool {
	for _, deltaLogs := range s.GetDeltalogs() {
		for _, l := range deltaLogs.GetBinlogs() {
			if l.GetLogID() == logID {
				return true
			}
		}
	}
	return false
}

func (s *SegmentInfo) IsStatsLogExists(logID int64) bool {
	for _, statsLogs := range s.GetStatslogs() {
		for _, l := range statsLogs.GetBinlogs() {
			if l.GetLogID() == logID {
				return true
			}
		}
	}
	return false
}

func (s *SegmentsInfo) SetLevel(segmentID UniqueID, level datapb.SegmentLevel) {
	s.modifyProto(segmentID, func(seg *datapb.SegmentInfo) { seg.Level = level })
}

// Clone deep clone the segment info and return a new instance. Stats lives
// on the proto and is copied by proto.Clone, so the cloned segment's
// aggregate reads stay consistent with its (cloned) binlog arrays. Opts
// that replace binlogs should also refresh Stats eagerly (recompute via
// storage.BuildStatsFromFieldBinlogs); EnsureStats no longer writes back lazily —
// concurrent RLock readers would race.
func (s *SegmentInfo) Clone(opts ...SegmentInfoOption) *SegmentInfo {
	info := proto.Clone(s.SegmentInfo).(*datapb.SegmentInfo)
	cloned := &SegmentInfo{
		SegmentInfo:     info,
		allocations:     s.allocations,
		lastFlushTime:   s.lastFlushTime,
		isCompacting:    s.isCompacting,
		lastWrittenTime: s.lastWrittenTime,
	}
	for _, opt := range opts {
		opt(cloned)
	}
	return cloned
}

// ShadowClone shadow clone the segment and return a new instance
func (s *SegmentInfo) ShadowClone(opts ...SegmentInfoOption) *SegmentInfo {
	cloned := &SegmentInfo{
		SegmentInfo:     s.SegmentInfo,
		allocations:     s.allocations,
		lastFlushTime:   s.lastFlushTime,
		isCompacting:    s.isCompacting,
		lastWrittenTime: s.lastWrittenTime,
	}
	for _, opt := range opts {
		opt(cloned)
	}
	return cloned
}

func (s *SegmentsInfo) addCompactTo(segment *SegmentInfo) {
	for _, from := range segment.GetCompactionFrom() {
		s.compactionTo[from] = append(s.compactionTo[from], segment.GetID())
	}
}

func (s *SegmentsInfo) deleteCompactFrom(compactionFrom []int64) {
	for _, from := range compactionFrom {
		delete(s.compactionTo, from)
	}
}

// SegmentInfoOption is the option to set fields in segment info
type SegmentInfoOption func(segment *SegmentInfo)

// SetRowCount is the option to set row count for segment info
func SetRowCount(rowCount int64) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.NumOfRows = rowCount
	}
}

// SetExpireTime is the option to set expire time for segment info
func SetExpireTime(expireTs Timestamp) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.LastExpireTime = expireTs
	}
}

// SetState is the option to set state for segment info
func SetState(state commonpb.SegmentState) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.State = state
	}
}

// SetDmlPosition is the option to set dml position for segment info
func SetDmlPosition(pos *msgpb.MsgPosition) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.DmlPosition = pos
	}
}

// SetStartPosition is the option to set start position for segment info
func SetStartPosition(pos *msgpb.MsgPosition) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.StartPosition = pos
	}
}

// SetAllocations is the option to set allocations for segment info
func SetAllocations(allocations []*Allocation) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.allocations = allocations
	}
}

// AddAllocation is the option to add allocation info for segment info
func AddAllocation(allocation *Allocation) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.allocations = append(segment.allocations, allocation)
		segment.LastExpireTime = allocation.ExpireTime
	}
}

// SetLastWrittenTime is the option to set last writtent time for segment info
func SetLastWrittenTime() SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.lastWrittenTime = time.Now()
	}
}

// SetFlushTime is the option to set flush time for segment info
func SetFlushTime(t time.Time) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.lastFlushTime = t
	}
}

// SetIsCompacting is the option to set compaction state for segment info
func SetIsCompacting(isCompacting bool) SegmentInfoOption {
	return func(segment *SegmentInfo) {
		segment.isCompacting = isCompacting
	}
}

func (s *SegmentInfo) getSegmentSize() int64 {
	stats := s.EnsureStats()
	return stats.GetInsertBinlogSize() + stats.GetStatsBinlogSize() + stats.GetDeltaBinlogSize()
}

func (s *SegmentInfo) getFieldBinlogSize(fieldID int64) int64 {
	var size int64
	for _, binlogs := range s.GetBinlogs() {
		if binlogs.GetFieldID() == fieldID {
			for _, l := range binlogs.GetBinlogs() {
				size += l.GetMemorySize()
			}
		} else {
			for _, childFieldID := range binlogs.GetChildFields() {
				if childFieldID == fieldID {
					for _, l := range binlogs.GetBinlogs() {
						size += l.GetMemorySize()
					}
				}
			}
		}
	}
	if size <= 0 {
		return s.getSegmentSize()
	}
	return size
}

func (s *SegmentInfo) getDeltaCount() int64 {
	return s.EnsureStats().GetDeleteNumRows()
}

// SegmentInfoSelector is the function type to select SegmentInfo from meta
type SegmentInfoSelector func(*SegmentInfo) bool

// ValidateManifestSegment checks that segments with manifest_path have empty
// legacy stats fields. Returns a descriptive message if validation fails,
// or empty string if the segment is valid.
func ValidateManifestSegment(info *SegmentInfo) string {
	if info.GetManifestPath() == "" {
		return ""
	}

	var nonEmpty []string
	if len(info.GetStatslogs()) > 0 {
		nonEmpty = append(nonEmpty, fmt.Sprintf("statslogs(%d)", len(info.GetStatslogs())))
	}
	if len(info.GetBm25Statslogs()) > 0 {
		nonEmpty = append(nonEmpty, fmt.Sprintf("bm25statslogs(%d)", len(info.GetBm25Statslogs())))
	}
	if len(info.GetTextStatsLogs()) > 0 {
		nonEmpty = append(nonEmpty, fmt.Sprintf("textStatsLogs(%d)", len(info.GetTextStatsLogs())))
	}
	if len(info.GetJsonKeyStats()) > 0 {
		nonEmpty = append(nonEmpty, fmt.Sprintf("jsonKeyStats(%d)", len(info.GetJsonKeyStats())))
	}

	if len(nonEmpty) > 0 {
		return fmt.Sprintf("segment %d has manifest_path but non-empty legacy stats fields: %v",
			info.GetID(), nonEmpty)
	}
	return ""
}

// segmentEffectiveTs returns the start-position timestamp that governs temporal
// decisions for a segment. For import segments with a non-zero commit_timestamp,
// commit_timestamp overrides start_position.Timestamp because the data was not
// "officially present" until the import was committed.
func segmentEffectiveTs(seg *datapb.SegmentInfo) uint64 {
	if ts := seg.GetCommitTimestamp(); ts != 0 {
		return ts
	}
	return seg.GetStartPosition().GetTimestamp()
}

// segmentEffectiveDmlTs returns the DML-position timestamp for temporal decisions.
// Same override logic as segmentEffectiveTs but for dml_position consumers
// (GC eligibility, TruncateChannelByTime).
func segmentEffectiveDmlTs(seg *datapb.SegmentInfo) uint64 {
	if ts := seg.GetCommitTimestamp(); ts != 0 {
		return ts
	}
	return seg.GetDmlPosition().GetTimestamp()
}
