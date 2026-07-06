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

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/segmentutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// SegmentOperator mutates a segment in place and reports:
//   - the binlog fields it changed (if any) so the caller can rewrite the
//     matching side-prefix KVs;
//   - whether the write should proceed at all.
//
// Return (BinlogIncrement{}, true) for state-only mutations.
// Return (_, false) to skip the write for this segment.
type SegmentOperator func(segment *SegmentInfo) (BinlogIncrement, bool)

func SetMaxRowCount(maxRow int64) SegmentOperator {
	return func(segment *SegmentInfo) (BinlogIncrement, bool) {
		if segment.MaxRowNum == maxRow {
			return BinlogIncrement{}, false
		}
		segment.MaxRowNum = maxRow
		return BinlogIncrement{}, true
	}
}

func SetTextIndexLogs(textIndexLogs map[int64]*datapb.TextIndexStats) SegmentOperator {
	return func(segment *SegmentInfo) (BinlogIncrement, bool) {
		if segment.TextStatsLogs == nil {
			segment.TextStatsLogs = make(map[int64]*datapb.TextIndexStats)
		}
		for field, logs := range textIndexLogs {
			segment.TextStatsLogs[field] = logs
		}
		return BinlogIncrement{}, true
	}
}

func SetStatslogs(statslogs []*datapb.FieldBinlog) SegmentOperator {
	return func(segment *SegmentInfo) (BinlogIncrement, bool) {
		segment.Statslogs = statslogs
		return BinlogIncrement{Statslogs: statslogs}, true
	}
}

func SetBm25Statslogs(bm25Statslogs []*datapb.FieldBinlog) SegmentOperator {
	return func(segment *SegmentInfo) (BinlogIncrement, bool) {
		segment.Bm25Statslogs = bm25Statslogs
		return BinlogIncrement{Bm25Statslogs: bm25Statslogs}, true
	}
}

func SetJSONKeyIndexLogs(jsonKeyIndexLogs map[int64]*datapb.JsonKeyStats) SegmentOperator {
	return func(segment *SegmentInfo) (BinlogIncrement, bool) {
		if segment.JsonKeyStats == nil {
			segment.JsonKeyStats = make(map[int64]*datapb.JsonKeyStats)
		}
		for field, logs := range jsonKeyIndexLogs {
			segment.JsonKeyStats[field] = logs
		}
		return BinlogIncrement{}, true
	}
}

func SetSchemaVersion(schemaVersion int32) SegmentOperator {
	return func(segment *SegmentInfo) (BinlogIncrement, bool) {
		if segment.GetSchemaVersion() == schemaVersion {
			return BinlogIncrement{}, false
		}
		segment.SchemaVersion = schemaVersion
		return BinlogIncrement{}, true
	}
}

func UpdateCheckPointOperator(segmentID int64, checkpoints []*datapb.CheckPoint, skipDmlPositionCheck ...bool) map[int64][]SegmentOperator {
	return map[int64][]SegmentOperator{
		segmentID: {func(segment *SegmentInfo) (BinlogIncrement, bool) {
			var cpNumRows int64
			for _, cp := range checkpoints {
				if cp.GetSegmentID() != segmentID {
					mlog.Warn(context.TODO(), "checkpoint in segment is not same as flush segment to update, ignore",
						mlog.Int64("current", segmentID),
						mlog.Int64("checkpoint segment", cp.GetSegmentID()))
					continue
				}
				if cp.GetPosition() == nil {
					mlog.Warn(context.TODO(), "checkpoint has nil position, skip", mlog.Int64("segmentID", segmentID))
					continue
				}
				if segment.GetDmlPosition() != nil &&
					segment.GetDmlPosition().GetTimestamp() >= cp.GetPosition().GetTimestamp() &&
					(len(skipDmlPositionCheck) == 0 || !skipDmlPositionCheck[0]) {
					mlog.Warn(context.TODO(), "checkpoint in segment is larger than reported",
						mlog.Any("current", segment.GetDmlPosition()),
						mlog.Any("reported", cp.GetPosition()))
					continue
				}
				cpNumRows = cp.GetNumOfRows()
				segment.DmlPosition = cp.GetPosition()
			}

			count := segmentutil.CalcRowCountFromBinLog(segment.SegmentInfo)
			if count > 0 {
				if cpNumRows != count {
					mlog.Info(context.TODO(), "check point reported row count inconsistent with binlog row count",
						mlog.Int64("segmentID", segmentID),
						mlog.Int64("binlog reported (wrong)", cpNumRows),
						mlog.Int64("segment binlog row count (correct)", count))
				}
				segment.NumOfRows = count
			} else if cpNumRows > 0 && segment.GetStorageVersion() == storage.StorageV3 {
				segment.NumOfRows = cpNumRows
			}
			return BinlogIncrement{}, true
		}},
	}
}

// UpdateSegmentColumnGroupsOperator upserts storage-v2 column groups on a
// segment's FieldBinlogs and removes the listed child fields from any other
// pre-existing group whose child_fields contained them, so that every field
// lives in exactly one column group. Idempotent: if a group with the same
// top-level fieldID already exists, it is replaced in place.
func UpdateSegmentColumnGroupsOperator(segmentID int64, groups map[int64]*datapb.FieldBinlog) SegmentOperator {
	return func(segment *SegmentInfo) (BinlogIncrement, bool) {
		incomingChildFields := typeutil.NewSet[int64]()
		for _, g := range groups {
			incomingChildFields.Insert(g.GetChildFields()...)
		}

		var droppedFieldIDs []int64
		kept := segment.Binlogs[:0]
		for _, existing := range segment.Binlogs {
			if _, replaced := groups[existing.GetFieldID()]; replaced {
				continue
			}
			if len(existing.GetChildFields()) > 0 {
				existing.ChildFields = lo.Filter(existing.GetChildFields(), func(fid int64, _ int) bool {
					return !incomingChildFields.Contain(fid)
				})
				if len(existing.ChildFields) == 0 {
					droppedFieldIDs = append(droppedFieldIDs, existing.GetFieldID())
					continue
				}
			}
			kept = append(kept, existing)
		}
		segment.Binlogs = kept

		for _, g := range groups {
			segment.Binlogs = append(segment.Binlogs, g)
		}

		// Bump DataVersion so querynodes with the segment already loaded will Reopen;
		// ManifestPath is intentionally not moved here (see segment_checker.isSegmentUpdate).
		segment.DataVersion++

		return BinlogIncrement{
			Binlogs:               segment.Binlogs,
			DroppedBinlogFieldIDs: droppedFieldIDs,
		}, true
	}
}

func UpdateManifestVersion(segmentID int64, manifestVersion int64) SegmentOperator {
	return func(segment *SegmentInfo) (BinlogIncrement, bool) {
		if segment.GetManifestPath() == "" {
			mlog.Warn(context.TODO(), "meta update: update manifest version failed - no manifest path",
				mlog.Int64("segmentID", segmentID))
			return BinlogIncrement{}, false
		}
		basePath, currentVer, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
		if err != nil {
			return BinlogIncrement{}, false
		}
		// Guard against version rollback. classifyBackfillSegments pre-checks
		// monotonicity at broadcast time, but a concurrent compaction may advance
		// ManifestPath between pre-check and this apply.
		if currentVer >= manifestVersion {
			if currentVer > manifestVersion {
				mlog.Warn(context.TODO(), "meta update: update manifest version rejected - would regress",
					mlog.Int64("segmentID", segmentID),
					mlog.Int64("currentVer", currentVer),
					mlog.Int64("incomingVer", manifestVersion))
			}
			return BinlogIncrement{}, false
		}
		segment.ManifestPath = packed.MarshalManifestPath(basePath, manifestVersion)
		return BinlogIncrement{}, true
	}
}

func updateManifestPathIfNewer(segment *SegmentInfo, manifestPath string) error {
	if manifestPath == "" || segment.GetManifestPath() == manifestPath {
		return nil
	}

	currentBase, currentVersion, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
	if err != nil {
		return err
	}
	incomingBase, incomingVersion, err := packed.UnmarshalManifestPath(manifestPath)
	if err != nil {
		return err
	}
	if currentBase != incomingBase {
		return merr.WrapErrServiceInternal(fmt.Sprintf("manifest base path mismatch for segment %d: current %s, incoming %s", segment.GetID(), currentBase, incomingBase))
	}
	if incomingVersion > currentVersion {
		segment.ManifestPath = manifestPath
	}
	return nil
}

func clearBinlogPaths(fieldBinlogs []*datapb.FieldBinlog) {
	for _, fieldBinlog := range fieldBinlogs {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			binlog.LogPath = ""
		}
	}
}

func cloneAndClearBinlogPaths(fieldBinlogs []*datapb.FieldBinlog) []*datapb.FieldBinlog {
	cloned := make([]*datapb.FieldBinlog, 0, len(fieldBinlogs))
	for _, fieldBinlog := range fieldBinlogs {
		if fieldBinlog == nil {
			cloned = append(cloned, nil)
			continue
		}
		cloned = append(cloned, typeutil.Clone(fieldBinlog))
	}
	clearBinlogPaths(cloned)
	return cloned
}

func mergeSegmentMutations(dst map[int64][]SegmentOperator, src map[int64][]SegmentOperator) {
	for segmentID, operators := range src {
		dst[segmentID] = append(dst[segmentID], operators...)
	}
}

type l0ManifestUpdate struct {
	segmentID            int64
	deltalogs            []*datapb.FieldBinlog
	storageConfig        *indexpb.StorageConfig
	committedV3Manifests map[int64]string
	segment              *SegmentInfo
	manifestPath         string
	entries              []packed.DeltaLogEntry
}

func (u *l0ManifestUpdate) prepare(segment *SegmentInfo) (bool, error) {
	u.segment = segment
	u.deltalogs = cloneFieldBinlogs(u.deltalogs)
	if len(u.deltalogs) == 0 {
		return false, nil
	}

	if u.segment.GetManifestPath() == "" {
		if err := binlog.CompressFieldBinlogs(u.deltalogs); err != nil {
			return false, err
		}
		return true, nil
	}

	if u.committedV3Manifests != nil {
		u.manifestPath = u.committedV3Manifests[u.segmentID]
	}
	if u.manifestPath != "" {
		return true, nil
	}

	entries, err := buildL0V3DeltaLogEntries(u.segmentID, u.deltalogs)
	if err != nil {
		return false, err
	}
	if len(entries) == 0 {
		return false, nil
	}
	u.entries = entries
	return true, nil
}

func (u *l0ManifestUpdate) commitManifest() error {
	if u.segment.GetManifestPath() == "" || u.manifestPath != "" || len(u.entries) == 0 {
		return nil
	}
	manifestPath, err := packed.AddDeltaLogsToManifestOverwrite(u.segment.GetManifestPath(), u.storageConfig, u.entries)
	if err != nil {
		return err
	}
	u.manifestPath = manifestPath
	return nil
}

func commitL0ManifestUpdates(updates []*l0ManifestUpdate) error {
	updates = lo.Filter(updates, func(update *l0ManifestUpdate, _ int) bool {
		return update.segment.GetManifestPath() != ""
	})
	if len(updates) == 0 {
		return nil
	}

	groups := make(map[int64][]*l0ManifestUpdate)
	for _, update := range updates {
		groups[update.segmentID] = append(groups[update.segmentID], update)
	}

	poolSize := paramtable.Get().DataCoordCfg.L0ManifestUpdatePoolSize.GetAsInt()
	if poolSize < 1 {
		poolSize = 1
	}
	if poolSize > len(groups) {
		poolSize = len(groups)
	}

	pool := conc.NewPool[struct{}](poolSize)
	defer pool.Release()

	futures := make([]*conc.Future[struct{}], 0, len(groups))
	for _, group := range groups {
		group := group
		futures = append(futures, pool.Submit(func() (struct{}, error) {
			return struct{}{}, commitL0ManifestUpdateGroup(group)
		}))
	}
	if err := conc.BlockOnAll(futures...); err != nil {
		return err
	}
	for _, update := range updates {
		if update.committedV3Manifests != nil && update.manifestPath != "" {
			update.committedV3Manifests[update.segmentID] = update.manifestPath
		}
	}
	return nil
}

func commitL0ManifestUpdateGroup(updates []*l0ManifestUpdate) error {
	for _, update := range updates {
		if update.manifestPath != "" {
			if err := updateManifestPathIfNewer(update.segment, update.manifestPath); err != nil {
				return err
			}
			continue
		}
		if len(update.entries) == 0 {
			continue
		}
		if err := update.commitManifest(); err != nil {
			return err
		}
		if err := updateManifestPathIfNewer(update.segment, update.manifestPath); err != nil {
			return err
		}
	}
	return nil
}

func (u *l0ManifestUpdate) apply() (BinlogIncrement, bool, error) {
	deltalogs := u.deltalogs
	if u.segment.GetManifestPath() != "" {
		if err := updateManifestPathIfNewer(u.segment, u.manifestPath); err != nil {
			return BinlogIncrement{}, false, err
		}
		deltalogs = cloneAndClearBinlogPaths(deltalogs)
	}

	if len(deltalogs) == 0 {
		return BinlogIncrement{}, false, nil
	}
	u.segment.Deltalogs = mergeFieldBinlogs(u.segment.GetDeltalogs(), deltalogs)
	u.segment.deltaRowcount.Store(-1)
	return BinlogIncrement{Deltalogs: u.segment.Deltalogs}, true, nil
}

func cloneFieldBinlogs(fieldBinlogs []*datapb.FieldBinlog) []*datapb.FieldBinlog {
	cloned := make([]*datapb.FieldBinlog, 0, len(fieldBinlogs))
	for _, fieldBinlog := range fieldBinlogs {
		if fieldBinlog == nil {
			cloned = append(cloned, nil)
			continue
		}
		cloned = append(cloned, typeutil.Clone(fieldBinlog))
	}
	return cloned
}

func AddL0DeltalogsAndUpdateManifestOperator(
	segmentID int64,
	deltalogs []*datapb.FieldBinlog,
	storageConfig *indexpb.StorageConfig,
	committedV3Manifests map[int64]string,
) map[int64][]SegmentOperator {
	return map[int64][]SegmentOperator{
		segmentID: {func(segment *SegmentInfo) (BinlogIncrement, bool) {
			update := &l0ManifestUpdate{
				segmentID:            segmentID,
				deltalogs:            deltalogs,
				storageConfig:        storageConfig,
				committedV3Manifests: committedV3Manifests,
			}
			ok, err := update.prepare(segment)
			if err != nil {
				segment.pendingMutationErr = err
				return BinlogIncrement{}, false
			}
			if !ok {
				return BinlogIncrement{}, false
			}
			segment.pendingL0ManifestUpdates = append(segment.pendingL0ManifestUpdates, update)
			return BinlogIncrement{}, true
		}},
	}
}

// ResetImportingSegmentRows clears NumOfRows and MaxRowNum on importing
// segments. It returns the mutation map consumed by meta.UpdateSegmentsInfo.
func ResetImportingSegmentRows(segmentIDs ...int64) map[int64][]SegmentOperator {
	mutations := make(map[int64][]SegmentOperator, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		segID := segmentID
		mutations[segID] = []SegmentOperator{func(segment *SegmentInfo) (BinlogIncrement, bool) {
			if segment.GetState() != commonpb.SegmentState_Importing {
				mlog.Warn(context.TODO(), "meta update: reset importing segment rows skipped - segment not in Importing state",
					mlog.Int64("segmentID", segID),
					mlog.String("state", segment.GetState().String()))
				return BinlogIncrement{}, false
			}
			segment.NumOfRows = 0
			segment.MaxRowNum = 0
			return BinlogIncrement{}, true
		}}
	}
	return mutations
}

// UpdateCommitTimestamp sets the commit_timestamp on an import/CDC segment.
// It returns the mutation map consumed by meta.UpdateSegmentsInfo.
func UpdateCommitTimestamp(segmentID int64, ts uint64) map[int64][]SegmentOperator {
	return map[int64][]SegmentOperator{
		segmentID: {func(segment *SegmentInfo) (BinlogIncrement, bool) {
			if ts != 0 {
				var maxTsTo uint64
				for _, fieldBinlogs := range segment.GetBinlogs() {
					for _, l := range fieldBinlogs.GetBinlogs() {
						if l.GetTimestampTo() > maxTsTo {
							maxTsTo = l.GetTimestampTo()
						}
					}
				}
				if ts < maxTsTo {
					mlog.Error(context.TODO(), "meta update: update commit timestamp rejected - commit_ts < max(binlog.TimestampTo)",
						mlog.Int64("segmentID", segmentID),
						mlog.Uint64("commitTs", ts),
						mlog.Uint64("maxBinlogTimestampTo", maxTsTo))
					return BinlogIncrement{}, false
				}
			}
			segment.CommitTimestamp = ts
			return BinlogIncrement{}, true
		}},
	}
}

type segmentCriterion struct {
	collectionID int64
	channel      string
	partitionID  int64
	others       []SegmentFilter
}

func (sc *segmentCriterion) Match(segment *SegmentInfo) bool {
	for _, filter := range sc.others {
		if !filter.Match(segment) {
			return false
		}
	}
	return true
}

type SegmentFilter interface {
	Match(segment *SegmentInfo) bool
	AddFilter(*segmentCriterion)
}

type CollectionFilter int64

func (f CollectionFilter) Match(segment *SegmentInfo) bool {
	return segment.GetCollectionID() == int64(f)
}

func (f CollectionFilter) AddFilter(criterion *segmentCriterion) {
	criterion.collectionID = int64(f)
}

func WithCollection(collectionID int64) SegmentFilter {
	return CollectionFilter(collectionID)
}

type ChannelFilter string

func (f ChannelFilter) Match(segment *SegmentInfo) bool {
	return segment.GetInsertChannel() == string(f)
}

func (f ChannelFilter) AddFilter(criterion *segmentCriterion) {
	criterion.channel = string(f)
}

// WithChannel WithCollection has a higher priority if both WithCollection and WithChannel are in condition together.
func WithChannel(channel string) SegmentFilter {
	return ChannelFilter(channel)
}

type SegmentFilterFunc func(*SegmentInfo) bool

func (f SegmentFilterFunc) Match(segment *SegmentInfo) bool {
	return f(segment)
}

func (f SegmentFilterFunc) AddFilter(criterion *segmentCriterion) {
	criterion.others = append(criterion.others, f)
}
