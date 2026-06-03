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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
			log.Ctx(context.TODO()).Warn("meta update: update manifest version failed - no manifest path",
				zap.Int64("segmentID", segmentID))
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
				log.Ctx(context.TODO()).Warn("meta update: update manifest version rejected - would regress",
					zap.Int64("segmentID", segmentID),
					zap.Int64("currentVer", currentVer),
					zap.Int64("incomingVer", manifestVersion))
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

func mergeSegmentMutations(dst map[int64][]SegmentOperator, src map[int64][]SegmentOperator) {
	for segmentID, operators := range src {
		dst[segmentID] = append(dst[segmentID], operators...)
	}
}

func AddL0DeltalogsAndUpdateManifestOperator(
	segmentID int64,
	deltalogs []*datapb.FieldBinlog,
	storageConfig *indexpb.StorageConfig,
	committedV3Manifests map[int64]string,
) map[int64][]SegmentOperator {
	return map[int64][]SegmentOperator{
		segmentID: {func(segment *SegmentInfo) (BinlogIncrement, bool) {
			if len(deltalogs) == 0 {
				return BinlogIncrement{}, false
			}

			if segment.GetManifestPath() == "" {
				if err := binlog.CompressFieldBinlogs(deltalogs); err != nil {
					log.Ctx(context.TODO()).Warn("meta update: compress L0 deltalog failed", zap.Int64("segmentID", segmentID), zap.Error(err))
					return BinlogIncrement{}, false
				}
			} else {
				manifestPath := ""
				if committedV3Manifests != nil {
					manifestPath = committedV3Manifests[segmentID]
				}
				if manifestPath == "" {
					entries, err := buildL0V3DeltaLogEntries(segmentID, deltalogs)
					if err != nil {
						log.Ctx(context.TODO()).Warn("meta update: build L0 V3 delta entries failed", zap.Int64("segmentID", segmentID), zap.Error(err))
						return BinlogIncrement{}, false
					}
					if len(entries) == 0 {
						return BinlogIncrement{}, false
					}
					manifestPath, err = packed.AddDeltaLogsToManifestOverwrite(segment.GetManifestPath(), storageConfig, entries)
					if err != nil {
						log.Ctx(context.TODO()).Warn("meta update: commit L0 V3 delta manifest failed", zap.Int64("segmentID", segmentID), zap.Error(err))
						return BinlogIncrement{}, false
					}
					if committedV3Manifests != nil {
						committedV3Manifests[segmentID] = manifestPath
					}
				}
				if err := updateManifestPathIfNewer(segment, manifestPath); err != nil {
					log.Ctx(context.TODO()).Warn("meta update: update L0 V3 manifest failed", zap.Int64("segmentID", segmentID), zap.Error(err))
					return BinlogIncrement{}, false
				}
				clearBinlogPaths(deltalogs)
			}

			segment.Deltalogs = mergeFieldBinlogs(segment.GetDeltalogs(), deltalogs)
			segment.deltaRowcount.Store(-1)
			return BinlogIncrement{Deltalogs: segment.Deltalogs}, true
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
				log.Ctx(context.TODO()).Warn("meta update: reset importing segment rows skipped - segment not in Importing state",
					zap.Int64("segmentID", segID),
					zap.String("state", segment.GetState().String()))
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
					log.Ctx(context.TODO()).Error("meta update: update commit timestamp rejected - commit_ts < max(binlog.TimestampTo)",
						zap.Int64("segmentID", segmentID),
						zap.Uint64("commitTs", ts),
						zap.Uint64("maxBinlogTimestampTo", maxTsTo))
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
