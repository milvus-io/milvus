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
	"sort"
	"strconv"
	"strings"

	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/metastore/kv/txn"
	"github.com/milvus-io/milvus/internal/util/segmentutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// SegmentCodec is the catalog's persisted shape of a segment: the record at
// the segment key carries no binlogs, every binlog family lives in per-field
// KVs under its own prefix and is stitched back on read, and text index logs
// keep only file names. A writer that commits through its own transaction
// (DataCoord's versioned segment store) uses it to stay compatible with the
// catalog in both directions.
type SegmentCodec struct {
	chunkManagerRootPath string
	// hasPrefix reports whether a meta-root-relative prefix holds any key; the
	// GC compatibility write of a dropped segment consults it. Nil declares
	// that every segment already keeps its binlogs in binlog KVs, so that write
	// never applies.
	hasPrefix func(ctx context.Context, prefix string) (bool, error)
}

func NewSegmentCodec(chunkManagerRootPath string, hasPrefix func(ctx context.Context, prefix string) (bool, error)) SegmentCodec {
	return SegmentCodec{chunkManagerRootPath: chunkManagerRootPath, hasPrefix: hasPrefix}
}

// SegmentWrite is the encoding of one segment write: Record is stored at
// RecordKey, and Ops are the binlog KVs (in key order) and removals that go
// with it. Keys are relative to the meta root.
type SegmentWrite struct {
	RecordKey string
	Record    *datapb.SegmentInfo
	Ops       []txn.Op
}

// EncodeAction encodes a segment action the way Catalog.Update persists it:
//   - ActionAdd    -> segment record plus its binlog KVs (a new segment).
//   - ActionUpdate -> segment record rewrite; AlterEncoding selects the
//     AlterSegments encoding (record, binlog increments, and the GC
//     compatibility binlog KVs of a dropped segment) over the record-only
//     SaveDroppedSegmentsInBatch encoding.
func (c SegmentCodec) EncodeAction(ctx context.Context, t metastore.ActionType, e metastore.SegmentEntry) (*SegmentWrite, error) {
	if e.Segment == nil {
		return nil, merr.WrapErrServiceInternalMsg("datacoord catalog: nil segment in UpdateAction")
	}
	increments := e.Binlogs
	switch t {
	case metastore.ActionAdd:
		// C26: honor the caller's increments (including DroppedBinlogFieldIDs
		// removals) instead of silently overwriting them with a default; a
		// dropped binlog field whose removal is omitted would be resurrected by
		// the prefix scan in listBinlogs on restart. Fall back to the full
		// segment increment only when none is supplied.
		if len(increments) == 0 {
			increments = []metastore.BinlogsIncrement{{Segment: e.Segment}}
		}
	case metastore.ActionUpdate:
		if !e.AlterEncoding {
			if len(increments) > 0 {
				// C26: the record-only (non-AlterEncoding) ActionUpdate persists no
				// binlog KVs and cannot honor DroppedBinlogFieldIDs removals —
				// silently dropping them would resurrect zombie binlog entries.
				// Reject per the Update contract ("reject what you cannot
				// implement") instead of losing the removals without an error.
				return nil, merr.WrapErrServiceInternalMsg(
					"datacoord catalog: binlog increments are not supported on a record-only segment update; use AlterEncoding")
			}
			return &SegmentWrite{RecordKey: SegmentRecordKey(e.Segment), Record: persistedRecord(e.Segment)}, nil
		}
		// Legacy AlterSegments encoding. A nil binlog increment persists no
		// new binlog KVs (the segment is being retired, not extended); for a
		// Dropped segment it still emits the handleDroppedSegment GC-compat
		// write when the segment predates binlog-prefix persistence, keeping
		// compaction's compactFrom retirement byte-identical to
		// catalog.AlterSegments.
	default:
		return nil, merr.WrapErrServiceInternalMsg("datacoord catalog cannot apply action type %v to a segment", t)
	}
	kvs, err := c.handleDroppedSegment(ctx, e.Segment)
	if err != nil {
		return nil, err
	}
	removals, err := incrementKvs(increments, kvs)
	if err != nil {
		return nil, err
	}
	return &SegmentWrite{RecordKey: SegmentRecordKey(e.Segment), Record: persistedRecord(e.Segment), Ops: binlogOps(kvs, removals)}, nil
}

// EncodeBinlogIncrements encodes binlog increments without a segment record,
// as catalog.AlterSegments persists the increments of segments it does not
// rewrite.
func EncodeBinlogIncrements(binlogs []metastore.BinlogsIncrement) ([]txn.Op, error) {
	kvs := make(map[string]string)
	removals, err := incrementKvs(binlogs, kvs)
	if err != nil {
		return nil, err
	}
	return binlogOps(kvs, removals), nil
}

// binlogOps lists the binlog KVs in key order, then the removals.
func binlogOps(kvs map[string]string, removals []string) []txn.Op {
	ops := make([]txn.Op, 0, len(kvs)+len(removals))
	keys := maps.Keys(kvs)
	sort.Strings(keys)
	for _, key := range keys {
		ops = append(ops, txn.Op{Key: key, Value: kvs[key]})
	}
	for _, key := range removals {
		ops = append(ops, txn.Op{Key: key, Delete: true})
	}
	return ops
}

// SegmentRecordKey is the key of a segment's record.
func SegmentRecordKey(segment *datapb.SegmentInfo) string {
	return buildSegmentPath(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
}

// SegmentRecordPrefixOf is the key prefix of the segment records of a
// collection.
func SegmentRecordPrefixOf(collectionID typeutil.UniqueID) string {
	return buildCollectionPrefix(collectionID)
}

// persistedRecord is the record the catalog stores for segment: a clone
// without binlogs, its V2 row count reconciled from the binlogs, text index
// logs as file names and JSON key stats as relative paths.
func persistedRecord(segment *datapb.SegmentInfo) *datapb.SegmentInfo {
	record, _, _, _, _ := CloneSegmentWithExcludeBinlogs(segment)
	// Row-count reconciliation from binlog arrays is V2-only. V3 segments'
	// arrays may be empty (post-cleanup) and their authoritative row count
	// lives on Stats / NumOfRows already.
	if !isV3Segment(segment) {
		segmentutil.ReCalcRowCount(segment, record)
	}
	metautil.ExtractTextLogFilenames(record.GetTextStatsLogs())
	metautil.ExtractJSONKeyStatsRelativePaths(record.GetJsonKeyStats())
	return record
}

// alterSegmentsKvs computes the persisted key/value writes and orphan
// per-FieldBinlog KV removals of an AlterSegments call.
func (c SegmentCodec) alterSegmentsKvs(ctx context.Context, segments []*datapb.SegmentInfo, binlogs []metastore.BinlogsIncrement) (map[string]string, []string, error) {
	kvs := make(map[string]string)
	for _, segment := range segments {
		compat, err := c.handleDroppedSegment(ctx, segment)
		if err != nil {
			return nil, nil, err
		}
		maps.Copy(kvs, compat)
		k, v, err := buildSegmentKv(persistedRecord(segment))
		if err != nil {
			return nil, nil, err
		}
		kvs[k] = v
	}
	removals, err := incrementKvs(binlogs, kvs)
	if err != nil {
		return nil, nil, err
	}
	return kvs, removals, nil
}

// incrementKvs adds the binlog KVs of binlogs to kvs and returns the keys of
// the binlog KVs they remove.
func incrementKvs(binlogs []metastore.BinlogsIncrement, kvs map[string]string) ([]string, error) {
	var removals []string
	for _, b := range binlogs {
		segment := b.Segment

		// V3 segments persist paths via the LOON manifest — skip writing the
		// per-FieldBinlog KVs entirely. DataCoord scheduling reads
		// SegmentInfo.Stats instead of iterating these arrays.
		if isV3Segment(segment) {
			continue
		}

		binlogKvs, err := buildBinlogKvsWithLogID(
			segment.GetCollectionID(),
			segment.GetPartitionID(),
			segment.GetID(),
			b.GetUpdateBinlogs(),
			b.GetUpdateDeltalogs(),
			b.GetUpdateStatslogs(),
			b.GetUpdateBm25Statslogs())
		if err != nil {
			return nil, err
		}
		maps.Copy(kvs, binlogKvs)

		for _, fid := range b.DroppedBinlogFieldIDs {
			removals = append(removals,
				buildFieldBinlogPath(
					segment.GetCollectionID(),
					segment.GetPartitionID(),
					segment.GetID(),
					fid))
		}
	}
	return removals, nil
}

// handleDroppedSegment is the GC compatibility write of a dropped segment
// that predates binlog-prefix persistence: its binlogs go to binlog KVs, where
// GC looks for them.
func (c SegmentCodec) handleDroppedSegment(ctx context.Context, segment *datapb.SegmentInfo) (map[string]string, error) {
	// V3 segments' GC walks the LOON manifest, not the per-FieldBinlog KVs,
	// so the legacy compatibility write-on-drop has no consumer for them.
	if segment.GetState() != commonpb.SegmentState_Dropped || isV3Segment(segment) || c.hasPrefix == nil {
		return map[string]string{}, nil
	}
	collectionID, partitionID, segmentID := segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID()
	for _, prefix := range []string{
		buildFieldBinlogPathPrefix(collectionID, partitionID, segmentID),
		buildFieldDeltalogPathPrefix(collectionID, partitionID, segmentID),
		buildFieldStatslogPathPrefix(collectionID, partitionID, segmentID),
	} {
		has, err := c.hasPrefix(ctx, prefix)
		if err != nil {
			return nil, err
		}
		if has {
			return map[string]string{}, nil
		}
	}
	return buildBinlogKvsWithLogID(collectionID, partitionID, segmentID, cloneLogs(segment.GetBinlogs()), cloneLogs(segment.GetDeltalogs()), cloneLogs(segment.GetStatslogs()), cloneLogs(segment.GetBm25Statslogs()))
}

// DecodeRecord restores the in-memory form of a persisted segment record:
// text index logs get their full paths back (old records already carry them)
// and JSON key stats keep relative paths.
func (c SegmentCodec) DecodeRecord(segment *datapb.SegmentInfo) {
	metautil.BuildTextLogPaths(
		c.chunkManagerRootPath,
		segment.GetCollectionID(),
		segment.GetPartitionID(),
		segment.GetID(),
		segment.GetTextStatsLogs(),
	)
	metautil.ExtractJSONKeyStatsRelativePaths(segment.GetJsonKeyStats())
}

// segmentBinlogFamilies are the binlog families the catalog keeps out of the
// segment record, each under its own KV prefix.
var segmentBinlogFamilies = []struct {
	prefix string
	field  func(*datapb.SegmentInfo) *[]*datapb.FieldBinlog
}{
	{SegmentBinlogPathPrefix, func(s *datapb.SegmentInfo) *[]*datapb.FieldBinlog { return &s.Binlogs }},
	{SegmentDeltalogPathPrefix, func(s *datapb.SegmentInfo) *[]*datapb.FieldBinlog { return &s.Deltalogs }},
	{SegmentStatslogPathPrefix, func(s *datapb.SegmentInfo) *[]*datapb.FieldBinlog { return &s.Statslogs }},
	{SegmentBM25logPathPrefix, func(s *datapb.SegmentInfo) *[]*datapb.FieldBinlog { return &s.Bm25Statslogs }},
}

// SegmentBinlogPrefixes lists the KV prefix of every binlog family, in the
// order FillSegmentBinlogs takes their binlogs.
func SegmentBinlogPrefixes() []string {
	prefixes := make([]string, len(segmentBinlogFamilies))
	for i, family := range segmentBinlogFamilies {
		prefixes[i] = family.prefix + "/"
	}
	return prefixes
}

// SegmentBinlogPrefixesOf lists the binlog KV prefixes of one segment, in the
// order of SegmentBinlogPrefixes.
func SegmentBinlogPrefixesOf(collectionID, partitionID, segmentID typeutil.UniqueID) []string {
	prefixes := make([]string, len(segmentBinlogFamilies))
	for i, family := range segmentBinlogFamilies {
		prefixes[i] = fmt.Sprintf("%s/%d/%d/%d/", family.prefix, collectionID, partitionID, segmentID)
	}
	return prefixes
}

// DecodeFieldBinlog decodes a binlog KV into the segment it belongs to and its
// field binlog.
func DecodeFieldBinlog(key string, value []byte) (typeutil.UniqueID, *datapb.FieldBinlog, error) {
	fieldBinlog := &datapb.FieldBinlog{}
	if err := proto.Unmarshal(value, fieldBinlog); err != nil {
		return 0, nil, merr.WrapErrDataIntegrity(err, "failed to unmarshal datapb.FieldBinlog of key %s", key)
	}
	segmentID, err := parseBinlogKey(key)
	if err != nil {
		return 0, nil, err
	}
	// set log size to memory size if memory size is zero for old segment before v2.4.3
	for i, b := range fieldBinlog.GetBinlogs() {
		if b.GetMemorySize() == 0 {
			fieldBinlog.Binlogs[i].MemorySize = b.GetLogSize()
		}
	}
	// no need to set log path and only store log id
	return segmentID, fieldBinlog, nil
}

// parseBinlogKey returns the segment ID of a binlog KV key:
// by-dev/meta/datacoord-meta/binlog/{collectionID}/{partitionID}/{segmentID}/{fieldID}
func parseBinlogKey(key string) (int64, error) {
	// A malformed key read back from the metastore during recovery is corrupt
	// stored data, not a caller's bad parameter (like an undecodable value) —
	// classify it as DataIntegrity.
	end := strings.LastIndexByte(key, '/')
	start := -1
	if end > 0 {
		start = strings.LastIndexByte(key[:end], '/')
	}
	if start < 0 {
		return 0, merr.WrapErrDataIntegrityMsg("parse binlog key failed, key:%s", key)
	}
	segmentID, err := strconv.ParseInt(key[start+1:end], 10, 64)
	if err != nil {
		return 0, merr.WrapErrDataIntegrity(err, "parse binlog key failed, key:%s", key)
	}
	return segmentID, nil
}

// FillSegmentBinlogs stitches binlog KVs into the records whose family is
// empty (records written before binlogs moved out of them keep their own) and
// compresses every family to log IDs. binlogs holds one map per family, in the
// order of SegmentBinlogPrefixes, keyed by segment ID.
func FillSegmentBinlogs(segments []*datapb.SegmentInfo, binlogs []map[typeutil.UniqueID][]*datapb.FieldBinlog) error {
	for _, segment := range segments {
		for i, family := range segmentBinlogFamilies {
			field := family.field(segment)
			if len(*field) == 0 {
				*field = binlogs[i][segment.GetID()]
			}
			if err := binlog.CompressFieldBinlogs(*field); err != nil {
				return err
			}
		}
	}
	return nil
}

// HasInlineBinlogs reports whether a persisted record still carries binlogs,
// which only records written outside the catalog encoding do.
func HasInlineBinlogs(record *datapb.SegmentInfo) bool {
	for _, family := range segmentBinlogFamilies {
		if len(*family.field(record)) > 0 {
			return true
		}
	}
	return false
}
