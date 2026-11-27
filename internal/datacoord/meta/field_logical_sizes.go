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

package meta

import (
	"bytes"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Only a committed revision identifies immutable insert data. Legacy opaque
// paths, the earliest revision (0), and a latest-version selector do not.
func committedFieldSizeManifest(path string) bool {
	base, version, err := packed.UnmarshalManifestPath(path)
	return err == nil && strings.TrimSpace(base) != "" && version > packed.ManifestEarliest
}

// CheckFieldLogicalSizes validates an absolute v1 report against its committed
// output manifest and independent row count. Unknown versions are opaque.
func CheckFieldLogicalSizes(sizes *datapb.FieldLogicalSizes, rows int64, manifest string) error {
	if sizes.GetVersion() != storage.FieldLogicalSizeVersion {
		return nil
	}
	if !committedFieldSizeManifest(manifest) {
		return merr.WrapErrDataIntegrityMsg("field logical sizes require a committed V3 manifest with a base path and positive revision")
	}
	if !storage.FieldLogicalSizesCover(sizes, rows) {
		return merr.WrapErrDataIntegrityMsg("field logical sizes cover %d rows, segment has %d", sizes.GetRowCount(), rows)
	}
	for id, size := range sizes.GetFields() {
		if size == nil {
			return merr.WrapErrDataIntegrityMsg("nil logical size for field %d", id)
		}
	}
	return nil
}

// SegmentFieldLogicalSizes is a read-only view of the known totals for this
// exact manifest-backed segment. Never infer coverage from its binlog cache.
func SegmentFieldLogicalSizes(segment *datapb.SegmentInfo) *datapb.FieldLogicalSizes {
	sizes := segment.GetStats().GetFieldLogicalSizes()
	if sizes.GetVersion() == storage.FieldLogicalSizeVersion && segment.GetStorageVersion() == storage.StorageV3 && segment.GetLevel() != datapb.SegmentLevel_L0 &&
		segment.GetFieldLogicalSizesManifest() != "" && packed.SameManifestPath(segment.GetFieldLogicalSizesManifest(), segment.GetManifestPath()) &&
		CheckFieldLogicalSizes(sizes, segment.GetNumOfRows(), segment.GetManifestPath()) == nil {
		return sizes
	}
	return nil
}

// dataViewFieldLogicalBytes copies verified column sizes into a DataView snapshot.
func dataViewFieldLogicalBytes(sizes *datapb.FieldLogicalSizes, rows int64, manifest string) *viewpb.FieldLogicalBytes {
	if sizes.GetVersion() != storage.FieldLogicalSizeVersion || CheckFieldLogicalSizes(sizes, rows, manifest) != nil {
		return nil
	}
	result := &viewpb.FieldLogicalBytes{
		PayloadBytes: make(map[int64]uint64, len(sizes.GetFields())),
		ValidityBits: make(map[int64]uint64),
	}
	for id, size := range sizes.GetFields() {
		result.PayloadBytes[id] = size.GetPayloadBytes()
		if bits := size.GetValidityBits(); bits != 0 {
			result.ValidityBits[id] = bits
		}
	}
	return result
}

// The final WAL seal can publish an existing insert manifest without repeating
// its column sizes. Borrow only the totals for that same manifest and row set.
func FlushDataViewFieldLogicalBytes(segment *SegmentInfo, req *datapb.SaveBinlogPathsRequest, rows int64, manifest string) *viewpb.FieldLogicalBytes {
	if segment.GetStorageVersion() != storage.StorageV3 || segment.GetLevel() == datapb.SegmentLevel_L0 {
		return nil
	}
	reported := dataViewFieldLogicalBytes(req.GetStats().GetFieldLogicalSizes(), rows, req.GetManifestPath())
	if segment.GetNumOfRows() != rows || !packed.SameManifestPath(segment.GetManifestPath(), manifest) {
		return reported
	}
	known := dataViewFieldLogicalBytes(SegmentFieldLogicalSizes(segment.SegmentInfo), rows, manifest)
	if known == nil {
		return reported
	}
	for id, bytes := range reported.GetPayloadBytes() {
		known.PayloadBytes[id] = bytes
		delete(known.ValidityBits, id)
	}
	for id, bits := range reported.GetValidityBits() {
		known.ValidityBits[id] = bits
	}
	return known
}

// installReportedStats adopts the absolute statistics of an insert output. Its
// manifest and authoritative row count must already be on the pending segment.
func (m *Meta) installReportedStats(segment *datapb.SegmentInfo, stats *datapb.Statistics) error {
	if sizes := stats.GetFieldLogicalSizes(); sizes.GetVersion() == storage.FieldLogicalSizeVersion {
		if segment.GetStorageVersion() != storage.StorageV3 || segment.GetLevel() == datapb.SegmentLevel_L0 || m.GetCollection(segment.GetCollectionID()).IsExternal() {
			return merr.WrapErrDataIntegrityMsg("field logical sizes require an ordinary V3 insert segment, segmentID=%d", segment.GetID())
		}
		if err := CheckFieldLogicalSizes(sizes, segment.GetNumOfRows(), segment.GetManifestPath()); err != nil {
			return err
		}
	}
	segment.Stats = copyStatistics(stats)
	bindFieldLogicalSizes(segment)
	return nil
}

// bindFieldLogicalSizes applies only after establishing which insert values
// the current statistics describe. The stamp belongs to the segment catalog,
// so worker results and copied field totals stay independent of storage paths.
func bindFieldLogicalSizes(segment *datapb.SegmentInfo) {
	segment.FieldLogicalSizesManifest = ""
	if segment.GetStats().GetFieldLogicalSizes().GetVersion() == storage.FieldLogicalSizeVersion {
		if !committedFieldSizeManifest(segment.GetManifestPath()) {
			segment.Stats.FieldLogicalSizes = nil
			return
		}
		segment.FieldLogicalSizesManifest = packed.ManifestPathKey(segment.GetManifestPath())
	}
}

func ClearFieldLogicalSizes(segment *datapb.SegmentInfo) {
	if segment.GetStats() != nil {
		segment.Stats.FieldLogicalSizes = nil
	}
	segment.FieldLogicalSizesManifest = ""
}

// moveFieldLogicalSizes carries only sizes already anchored on the old
// revision. Column-replacing callers invalidate or update touched IDs first.
func moveFieldLogicalSizes(segment *datapb.SegmentInfo, manifest string) {
	if segment.GetStats().GetFieldLogicalSizes() != nil && SegmentFieldLogicalSizes(segment) == nil {
		ClearFieldLogicalSizes(segment)
	}
	segment.ManifestPath = manifest
	bindFieldLogicalSizes(segment)
}

func checkCompactionFieldSizeReplay(segment *datapb.SegmentInfo, result *datapb.CompactionSegment) error {
	return CheckFieldLogicalSizesReplay(segment, result.GetManifest(), result.GetNumOfRows(), result.GetStats().GetFieldLogicalSizes())
}

// CheckFieldLogicalSizesReplay compares reports for the same immutable output.
// A later in-place rewrite may have changed its row set or column values.
func CheckFieldLogicalSizesReplay(segment *datapb.SegmentInfo, manifest string, rows int64, sizes *datapb.FieldLogicalSizes) error {
	if sizes.GetVersion() != storage.FieldLogicalSizeVersion || segment.GetManifestPath() == "" || !packed.SameManifestPath(segment.GetManifestPath(), manifest) {
		return nil
	}
	if rows != segment.GetNumOfRows() {
		return merr.WrapErrDataIntegrityMsg("replay for segment %d changed its output row count from %d to %d", segment.GetID(), segment.GetNumOfRows(), rows)
	}
	if err := CheckFieldLogicalSizes(sizes, rows, manifest); err != nil {
		return err
	}
	return checkFieldSizeConflict(segment.GetID(), SegmentFieldLogicalSizes(segment), sizes)
}

// CheckMaterializationFieldSizeReplay checks only columns the original update
// could install. A partial or legacy report was already treated as unmeasured.
func CheckMaterializationFieldSizeReplay(segment *datapb.SegmentInfo, result *datapb.CompactionSegment) error {
	known := SegmentFieldLogicalSizes(segment)
	report := result.GetStats().GetFieldLogicalSizes()
	if known == nil || result.GetNumOfRows() != segment.GetNumOfRows() || !storage.FieldLogicalSizesCover(report, segment.GetNumOfRows()) {
		return nil
	}
	for _, id := range result.GetFieldLogicalSizeUpdateIds() {
		if previous, size := known.GetFields()[id], report.GetFields()[id]; previous != nil && size != nil && !proto.Equal(previous, size) {
			return merr.WrapErrDataIntegrityMsg("conflicting materialization replay for segment %d field %d", segment.GetID(), id)
		}
	}
	return nil
}

// InvalidateReplacedManifestFieldSizes forgets every column on adoption of an
// opaque prepared revision. Replaying the installed revision changes no values.
func InvalidateReplacedManifestFieldSizes(segmentID int64, manifest string) UpdateOperator {
	return func(pack *UpdateSegmentPack) bool {
		segment := pack.Get(segmentID)
		if segment == nil || segment.GetStats().GetFieldLogicalSizes() == nil || packed.SameManifestPath(segment.GetManifestPath(), manifest) {
			return false
		}
		ClearFieldLogicalSizes(segment.SegmentInfo)
		return true
	}
}

// InvalidateExternalFieldSizes runs in the same transaction as a structured
// Spark backfill. An opaque group leaves no insert-column total safe.
func InvalidateExternalFieldSizes(segmentID int64, groups []packed.ColumnGroupEntry) UpdateOperator {
	var ids []int64
	for _, group := range groups {
		fields := droppedColumnFieldIDs(group.Columns)
		if len(fields) == 0 || len(fields) != len(group.Columns) {
			ids = nil
			break
		}
		ids = append(ids, fields...)
	}
	return func(pack *UpdateSegmentPack) bool {
		segment := pack.Get(segmentID)
		if segment == nil || segment.GetStats().GetFieldLogicalSizes() == nil {
			return false
		}
		sizes := segment.Stats.FieldLogicalSizes
		if len(ids) == 0 || SegmentFieldLogicalSizes(segment.SegmentInfo) == nil {
			ClearFieldLogicalSizes(segment.SegmentInfo)
		} else {
			for _, id := range ids {
				delete(sizes.Fields, id)
			}
		}
		return true
	}
}

func checkFieldSizeConflict(segmentID int64, committed, report *datapb.FieldLogicalSizes) error {
	for id, size := range report.GetFields() {
		if previous := committed.GetFields()[id]; previous != nil && !proto.Equal(previous, size) {
			return merr.WrapErrDataIntegrityMsg("conflicting logical size for segment %d field %d at the same insert row set", segmentID, id)
		}
	}
	return nil
}

// saveBinlogCoverage compares the *reported* checkpoint and manifest with the
// committed pair, not just the resulting SegmentInfo: the incremental operator
// ignores an old checkpoint and would otherwise make a stale report look current.
func saveBinlogCoverage(current *datapb.SegmentInfo, req *datapb.SaveBinlogPathsRequest) (cp *datapb.CheckPoint, sameManifest, stale bool, err error) {
	for _, check := range req.GetCheckPoints() {
		if check.GetSegmentID() != current.GetID() || check.GetPosition() == nil {
			continue
		}
		if cp != nil && !proto.Equal(cp, check) {
			return nil, false, false, merr.WrapErrDataIntegrityMsg("conflicting checkpoints for segment %d", current.GetID())
		}
		cp = check
	}
	if cp != nil && cp.GetPosition().GetTimestamp() < current.GetDmlPosition().GetTimestamp() {
		return cp, false, true, nil
	}
	if manifest := req.GetManifestPath(); manifest != "" && current.GetManifestPath() != "" {
		cmp, err := packed.CompareManifestPath(manifest, current.GetManifestPath())
		if err != nil {
			// Older writers can carry opaque locations. They cannot establish
			// continuity for field sizes, but their file statistics still apply.
			if req.GetStats().GetFieldLogicalSizes().GetVersion() == storage.FieldLogicalSizeVersion {
				return nil, false, false, err
			}
		} else {
			if cmp < 0 {
				return cp, false, true, nil
			}
			sameManifest = cmp == 0
		}
	}
	if cp != nil {
		position := cp.GetPosition()
		previous := current.GetDmlPosition()
		sameTick := previous != nil && position.GetTimestamp() == previous.GetTimestamp()
		if sameTick && len(position.GetMsgID()) != 0 && len(previous.GetMsgID()) != 0 && !bytes.Equal(position.GetMsgID(), previous.GetMsgID()) {
			return nil, false, false, merr.WrapErrDataIntegrityMsg("conflicting checkpoint positions for segment %d at timestamp %d", current.GetID(), position.GetTimestamp())
		}
		if cp.GetNumOfRows() < 0 || (cp.GetNumOfRows() > 0 && cp.GetNumOfRows() != current.GetNumOfRows() && (cp.GetNumOfRows() < current.GetNumOfRows() || sameManifest || sameTick)) {
			return nil, false, false, merr.WrapErrDataIntegrityMsg("conflicting cumulative row counts for segment %d: committed %d, reported %d", current.GetID(), current.GetNumOfRows(), cp.GetNumOfRows())
		}
	}
	return cp, sameManifest, false, nil
}

// CheckSealedFieldSizeReplay also checks the fast path which returns an already
// published DataView version without running any catalog operators. A newer
// index/backfill manifest must not be compared with a historical flush result.
func CheckSealedFieldSizeReplay(segment *datapb.SegmentInfo, req *datapb.SaveBinlogPathsRequest) error {
	sizes := req.GetStats().GetFieldLogicalSizes()
	if sizes.GetVersion() != storage.FieldLogicalSizeVersion || segment.GetManifestPath() == "" || !packed.SameManifestPath(segment.GetManifestPath(), req.GetManifestPath()) {
		return nil
	}
	cp, _, stale, err := saveBinlogCoverage(segment, req)
	if err != nil || stale {
		return err
	}
	if len(req.GetCheckPoints()) != 0 && (cp == nil || cp.GetNumOfRows() != segment.GetNumOfRows()) {
		return merr.WrapErrDataIntegrityMsg("sealed field logical sizes for segment %d have no matching checkpoint", segment.GetID())
	}
	if err := CheckFieldLogicalSizes(sizes, segment.GetNumOfRows(), segment.GetManifestPath()); err != nil {
		return err
	}
	return checkFieldSizeConflict(segment.GetID(), SegmentFieldLogicalSizes(segment), sizes)
}

// UpdateSaveBinlogStats runs after the row-count, binlog and manifest operators,
// under the same writer lock. Other Statistics fields keep their existing
// wholesale/array-fallback semantics. Only insert-column totals need to survive
// an unmetered checkpoint that did not add any rows.
func UpdateSaveBinlogStats(req *datapb.SaveBinlogPathsRequest) UpdateOperator {
	return func(pack *UpdateSegmentPack) bool {
		id := req.GetSegmentID()
		segment := pack.Get(id)
		if segment == nil {
			return false
		}
		if segment.GetStorageVersion() != storage.StorageV3 || segment.GetLevel() == datapb.SegmentLevel_L0 {
			return UpdateSegmentStats(id, req.GetStats())(pack)
		}
		current := pack.Peek(id)
		if current.GetState() == commonpb.SegmentState_Flushed && !req.GetDropped() {
			if err := CheckSealedFieldSizeReplay(current.SegmentInfo, req); err != nil {
				return pack.Fail(err)
			}
			return pack.Fail(errIgnoredSegmentMetaOperation)
		}
		cp, sameManifest, stale, err := saveBinlogCoverage(current.SegmentInfo, req)
		if err != nil {
			return pack.Fail(err)
		}
		if stale {
			return pack.Fail(errIgnoredSegmentMetaOperation)
		}
		reported := req.GetStats().GetFieldLogicalSizes()
		rows := segment.GetNumOfRows() // what UpdateCheckPointOperator actually installed
		if err := CheckFieldLogicalSizes(reported, rows, req.GetManifestPath()); err != nil {
			return pack.Fail(err)
		}
		// Sealing via the WAL lifecycle reports no checkpoint: all chunks have
		// already published and the final request refers to the same manifest.
		provedRows := cp != nil && cp.GetPosition().GetTimestamp() != 0 && cp.GetNumOfRows() == rows
		knownManifest := sameManifest && len(req.GetCheckPoints()) == 0
		if reported.GetVersion() == storage.FieldLogicalSizeVersion && (req.GetManifestPath() == "" || (!provedRows && !knownManifest)) {
			return pack.Fail(merr.WrapErrDataIntegrityMsg("field logical sizes for segment %d have no matching checkpoint or manifest", id))
		}
		if !UpdateSegmentStats(id, req.GetStats())(pack) {
			return false
		}
		// A growing segment only appends inserts. Equal authoritative counts
		// identify an unchanged insert prefix; an unanchored manifest advance
		// does not, even if the old cached row count happened to stay the same.
		hasManifestOrBinlogs := req.GetManifestPath() != "" || len(req.GetField2BinlogPaths()) != 0
		if rows != current.GetNumOfRows() || (!provedRows && !sameManifest && hasManifestOrBinlogs) {
			return true
		}
		known := SegmentFieldLogicalSizes(current.SegmentInfo)
		if known == nil {
			return true
		}
		if reported.GetVersion() != storage.FieldLogicalSizeVersion {
			if _, err := packed.CompareManifestPath(current.GetManifestPath(), segment.GetManifestPath()); err != nil {
				return true
			}
			segment.Stats.FieldLogicalSizes = proto.Clone(known).(*datapb.FieldLogicalSizes)
			bindFieldLogicalSizes(segment.SegmentInfo)
			return true
		}
		if err := checkFieldSizeConflict(id, known, reported); err != nil {
			return pack.Fail(err)
		}
		for fieldID, size := range known.GetFields() {
			if _, ok := reported.GetFields()[fieldID]; !ok {
				if segment.Stats.FieldLogicalSizes.Fields == nil {
					segment.Stats.FieldLogicalSizes.Fields = make(map[int64]*datapb.FieldLogicalSize)
				}
				segment.Stats.FieldLogicalSizes.Fields[fieldID] = proto.Clone(size).(*datapb.FieldLogicalSize)
			}
		}
		return true
	}
}
