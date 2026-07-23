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

package storage

import (
	"math"
	"sort"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type quantileEntry struct {
	tsTo uint64
	rows int64
}

// StatisticsCollector accumulates the writes of one growing segment across
// its sync tasks. It lives on the metacache SegmentInfo and is read at flush
// via Publish, which emits the complete cumulative Statistics (every field)
// for the segment. That whole object is shipped on flush and stored wholesale
// by DataCoord's UpdateSegmentStats — no per-field recompute on the receiver.
//
// The collector itself is not persisted, but its output — the cumulative
// Statistics — is persisted on SegmentInfo every flush. On datanode restart
// a recovered growing segment's collector is reseeded from that persisted
// Stats (see NewStatisticsCollectorFromStats), so it resumes accumulating
// exactly where it left off.
//
// BuildStatsFromFieldBinlogs (array derivation) survives only as legacy
// migration and as the receiver's nil-stats fallback (storage V1 /
// pre-Statistics datanodes during rolling upgrade) plus the one-shot
// producers (compaction, import); it is no longer on the normal flush path.
//
// Not safe for concurrent use — the metacache SegmentStats wrapper guards it.
type StatisticsCollector struct {
	numRows            int64
	insertBinlogSize   int64
	insertBinlogCount  int64
	statsBinlogSize    int64
	deltaBinlogSize    int64
	deltaBinlogCount   int64
	deleteNumRows      int64
	deltaTimestampFrom uint64
	deltaTimestampTo   uint64
	timestampFrom      uint64
	timestampTo        uint64
	nullCounts         map[int64]int64
	quantileEntries    []quantileEntry
}

// NewStatisticsCollector returns an empty collector.
func NewStatisticsCollector() *StatisticsCollector {
	return &StatisticsCollector{}
}

// NewStatisticsCollectorFromStats reseeds a collector from a previously
// published Statistics — used to restore a growing segment's cumulative state
// on datanode restart from the Stats persisted on SegmentInfo (the binlog
// arrays themselves are not in etcd for V3, but Stats always is). numRows is
// the segment's authoritative row count (Statistics omits the insert row
// count). The persisted TimestampQuantiles are reconstructed as synthetic
// per-bucket entries so post-restart quantiles round-trip and keep
// accumulating as new syncs arrive. Returns an empty collector when stats is
// nil.
func NewStatisticsCollectorFromStats(stats *datapb.Statistics, numRows int64) *StatisticsCollector {
	c := &StatisticsCollector{}
	if stats == nil {
		return c
	}
	c.numRows = numRows
	c.insertBinlogSize = stats.GetInsertBinlogSize()
	c.insertBinlogCount = stats.GetInsertBinlogCount()
	c.statsBinlogSize = stats.GetStatsBinlogSize()
	c.deltaBinlogSize = stats.GetDeltaBinlogSize()
	c.deltaBinlogCount = stats.GetDeltaBinlogCount()
	c.deleteNumRows = stats.GetDeleteNumRows()
	c.deltaTimestampFrom = stats.GetDeltaTimestampFrom()
	c.deltaTimestampTo = stats.GetDeltaTimestampTo()
	c.timestampFrom = stats.GetTimestampFrom()
	c.timestampTo = stats.GetTimestampTo()
	if nc := stats.GetNullCounts(); len(nc) > 0 {
		c.nullCounts = make(map[int64]int64, len(nc))
		for f, n := range nc {
			c.nullCounts[f] = n
		}
	}
	// Rebuild quantile buckets from the persisted marks: bucket i carries the
	// rows between the (i-1)th and ith cumulative thresholds, computed the same
	// way quantiles() derives them (int64(mark_i * numRows)). Sizing by the
	// threshold difference — rather than a flat numRows/N that loses the
	// remainder — makes the walk in quantiles() land on each persisted mark
	// exactly (idempotent restore) even when numRows is not divisible by N;
	// new Digest entries append on top.
	if q := stats.GetTimestampQuantiles(); len(q) > 0 && numRows > 0 {
		n := len(q)
		var prev int64
		for i, ts := range q {
			thr := int64(float64(i+1) / float64(n) * float64(numRows))
			rows := thr - prev
			prev = thr
			if rows > 0 {
				c.quantileEntries = append(c.quantileEntries, quantileEntry{tsTo: uint64(ts), rows: rows})
			}
		}
	}
	return c
}

// Digest folds one sync task's writes into the cumulative state. inserts are
// the sync's insert FieldBinlogs (one per column group), delta its delta
// FieldBinlog (nil if none), statsBlobSize the bloom-filter/BM25 blob bytes
// this sync produced, rows the sync's insert row count, and tsFrom/tsTo the
// batch's insert timestamp range. Every member field of a non-empty insert
// FieldBinlog gets a NullCounts entry (zero included) — the presence contract
// the index task relies on.
func (c *StatisticsCollector) Digest(
	inserts map[int64]*datapb.FieldBinlog,
	delta *datapb.FieldBinlog,
	statsBlobSize, rows int64,
	tsFrom, tsTo uint64,
) {
	c.numRows += rows
	for _, fb := range inserts {
		if len(fb.GetBinlogs()) == 0 {
			continue
		}
		members := fb.GetChildFields()
		if len(members) == 0 {
			members = []int64{fb.GetFieldID()}
		}
		if c.nullCounts == nil {
			c.nullCounts = make(map[int64]int64)
		}
		for _, f := range members {
			if _, ok := c.nullCounts[f]; !ok {
				c.nullCounts[f] = 0
			}
		}
		for _, l := range fb.GetBinlogs() {
			c.insertBinlogSize += l.GetMemorySize()
			c.insertBinlogCount++
			for f, n := range l.GetFieldNullCounts() {
				c.nullCounts[f] += n
			}
		}
	}
	c.statsBinlogSize += statsBlobSize
	if delta != nil {
		for _, l := range delta.GetBinlogs() {
			c.deltaBinlogSize += l.GetMemorySize()
			c.deltaBinlogCount++
			c.deleteNumRows += l.GetEntriesNum()
			if f := l.GetTimestampFrom(); f > 0 && (c.deltaTimestampFrom == 0 || f < c.deltaTimestampFrom) {
				c.deltaTimestampFrom = f
			}
			if t := l.GetTimestampTo(); t > c.deltaTimestampTo {
				c.deltaTimestampTo = t
			}
		}
	}
	if tsFrom > 0 && (c.timestampFrom == 0 || tsFrom < c.timestampFrom) {
		c.timestampFrom = tsFrom
	}
	if tsTo > c.timestampTo {
		c.timestampTo = tsTo
	}
	if rows > 0 && tsTo > 0 {
		c.quantileEntries = append(c.quantileEntries, quantileEntry{tsTo: tsTo, rows: rows})
	}
}

// Publish returns the cumulative Statistics digested so far, or nil if nothing
// has been digested. No scaling — the value reflects exactly what the collector
// has seen.
func (c *StatisticsCollector) Publish() *datapb.Statistics {
	if c.numRows == 0 && c.deltaBinlogCount == 0 && c.statsBinlogSize == 0 && c.insertBinlogCount == 0 {
		return nil
	}
	var nullCounts map[int64]int64
	if len(c.nullCounts) > 0 {
		nullCounts = make(map[int64]int64, len(c.nullCounts))
		for f, n := range c.nullCounts {
			nullCounts[f] = n
		}
	}
	return &datapb.Statistics{
		InsertBinlogSize:   c.insertBinlogSize,
		InsertBinlogCount:  c.insertBinlogCount,
		StatsBinlogSize:    c.statsBinlogSize,
		DeltaBinlogSize:    c.deltaBinlogSize,
		DeltaBinlogCount:   c.deltaBinlogCount,
		DeleteNumRows:      c.deleteNumRows,
		DeltaTimestampFrom: c.deltaTimestampFrom,
		DeltaTimestampTo:   c.deltaTimestampTo,
		TimestampFrom:      c.timestampFrom,
		TimestampTo:        c.timestampTo,
		NullCounts:         nullCounts,
		TimestampQuantiles: c.quantiles(),
	}
}

// quantiles picks the 20/40/60/80/100% TimestampTo marks over the digested
// rows (entries are in sync order; timestamps are roughly monotonic within a
// growing segment).
func (c *StatisticsCollector) quantiles() []int64 {
	if len(c.quantileEntries) == 0 || c.numRows == 0 {
		return nil
	}
	marks := []float64{0.2, 0.4, 0.6, 0.8, 1.0}
	out := make([]int64, len(marks))
	mi := 0
	var cum int64
	for _, e := range c.quantileEntries {
		cum += e.rows
		for mi < len(marks) && cum >= int64(marks[mi]*float64(c.numRows)) {
			out[mi] = int64(e.tsTo)
			mi++
		}
	}
	for ; mi < len(marks); mi++ {
		out[mi] = int64(c.timestampTo)
	}
	return out
}

// Clone returns a deep copy of the collector. Used by the metacache
// SegmentStats wrapper to share the accumulator by pointer across Clone().
func (c *StatisticsCollector) Clone() *StatisticsCollector {
	cp := *c
	if c.nullCounts != nil {
		cp.nullCounts = make(map[int64]int64, len(c.nullCounts))
		for f, n := range c.nullCounts {
			cp.nullCounts[f] = n
		}
	}
	if c.quantileEntries != nil {
		cp.quantileEntries = append([]quantileEntry(nil), c.quantileEntries...)
	}
	return &cp
}

// BuildStatsFromFieldBinlogs reconstructs a datapb.Statistics from the
// cumulative FieldBinlog arrays. Used by the DataCoord side as the V2 /
// legacy fallback when the writer didn't ship Statistics directly.
//
// TimestampQuantiles is approximated from binlog-level TimestampTo
// weighted by EntriesNum: walk the (first field's) binlogs sorted by
// TimestampTo and pick the 20/40/60/80/100 cumulative-rowcount marks.
// This treats every row in a binlog as sharing the file's TimestampTo
// (upward bias) — a best-effort approximation for V2; the live collector
// produces row-level quantiles.
func BuildStatsFromFieldBinlogs(binlogs, statslogs, bm25logs, deltalogs []*datapb.FieldBinlog) *datapb.Statistics {
	s := &datapb.Statistics{}

	// TimestampFrom (min) and TimestampTo (max) come from iterating every
	// FieldBinlog's binlogs: the same row writes the same timestamp across
	// every field, so min/max are stable regardless of how many fields we
	// scan. Iterating all fields ensures we don't lose data on test fixtures
	// that split per-flush files across separate FieldBinlogs.
	var tsFrom uint64 = math.MaxUint64
	var tsTo uint64
	var nullCounts map[int64]int64
	for _, fb := range binlogs {
		if len(fb.GetBinlogs()) == 0 {
			continue
		}
		// Completion rule: every member field of a non-empty FieldBinlog is
		// physically present in the segment, so it must have a NullCounts
		// entry even when the binlogs carry no FieldNullCounts metadata
		// (storage V1 and pre-#46903 binlogs). Packed formats list members
		// in ChildFields; V1 uses FieldID directly. Pre-ChildFields packed
		// binlogs also take the FieldID fallback: their vector/text groups
		// used GroupID == fieldID, so the seeded entry is still the right
		// key for the nullable-vector consumers of this invariant.
		memberFields := fb.GetChildFields()
		if len(memberFields) == 0 {
			memberFields = []int64{fb.GetFieldID()}
		}
		if nullCounts == nil {
			nullCounts = make(map[int64]int64)
		}
		for _, fID := range memberFields {
			if _, ok := nullCounts[fID]; !ok {
				nullCounts[fID] = 0
			}
		}
		for _, l := range fb.GetBinlogs() {
			s.InsertBinlogSize += l.GetMemorySize()
			s.InsertBinlogCount++
			if from := l.GetTimestampFrom(); from > 0 && from < tsFrom {
				tsFrom = from
			}
			if to := l.GetTimestampTo(); to > tsTo {
				tsTo = to
			}
			for fID, n := range l.GetFieldNullCounts() {
				nullCounts[fID] += n
			}
		}
	}
	if tsFrom != math.MaxUint64 {
		s.TimestampFrom = tsFrom
	}
	s.TimestampTo = tsTo
	s.NullCounts = nullCounts
	for _, fb := range statslogs {
		for _, l := range fb.GetBinlogs() {
			s.StatsBinlogSize += l.GetMemorySize()
		}
	}
	for _, fb := range bm25logs {
		for _, l := range fb.GetBinlogs() {
			s.StatsBinlogSize += l.GetMemorySize()
		}
	}
	var deltaFrom uint64 = math.MaxUint64
	var deltaTo uint64
	for _, fb := range deltalogs {
		for _, l := range fb.GetBinlogs() {
			s.DeltaBinlogSize += l.GetMemorySize()
			s.DeleteNumRows += l.GetEntriesNum()
			s.DeltaBinlogCount++
			if from := l.GetTimestampFrom(); from > 0 && from < deltaFrom {
				deltaFrom = from
			}
			if to := l.GetTimestampTo(); to > deltaTo {
				deltaTo = to
			}
		}
	}
	if deltaFrom != math.MaxUint64 {
		s.DeltaTimestampFrom = deltaFrom
	}
	s.DeltaTimestampTo = deltaTo

	if len(binlogs) > 0 {
		type tsEntry struct {
			ts      int64
			entries int64
		}
		var tsEntries []tsEntry
		var totalEntries int64
		// First-field binlogs alone — every field shares the same per-file
		// row counts and timestamps, matching segmentutil.CalcRowCountFromBinLog.
		for _, l := range binlogs[0].GetBinlogs() {
			if l.GetEntriesNum() <= 0 {
				continue
			}
			tsEntries = append(tsEntries, tsEntry{ts: int64(l.GetTimestampTo()), entries: l.GetEntriesNum()})
			totalEntries += l.GetEntriesNum()
		}
		if totalEntries > 0 {
			sort.Slice(tsEntries, func(i, j int) bool { return tsEntries[i].ts < tsEntries[j].ts })
			percentiles := []float64{0.2, 0.4, 0.6, 0.8, 1.0}
			result := make([]int64, len(percentiles))
			for i, p := range percentiles {
				target := int64(math.Ceil(p * float64(totalEntries)))
				if target < 1 {
					target = 1
				}
				var acc int64
				pick := tsEntries[len(tsEntries)-1].ts
				for _, e := range tsEntries {
					acc += e.entries
					if acc >= target {
						pick = e.ts
						break
					}
				}
				result[i] = pick
			}
			s.TimestampQuantiles = result
		}
	}

	return s
}
