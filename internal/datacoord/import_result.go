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
	"path"
	"slices"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

func loadImportV3Object(ctx context.Context, cm storage.ChunkManager, objectPath string) ([]byte, error) {
	fullPath := path.Join(cm.RootPath(), objectPath)
	data, err := cm.Read(ctx, fullPath)
	if err != nil {
		return nil, merr.Wrap(err, "read import v3 object")
	}
	return data, nil
}

func loadReshardResultManifest(ctx context.Context, cm storage.ChunkManager, jobID, taskID, runID int64) (*datapb.ReshardManifest, error) {
	data, err := loadImportV3Object(ctx, cm, metautil.BuildImportReshardResultPath(jobID, taskID, runID))
	if err != nil {
		return nil, err
	}
	manifest := &datapb.ReshardManifest{}
	if err := proto.Unmarshal(data, manifest); err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "unmarshal reshard result manifest")
	}
	return manifest, nil
}

func validateReshardManifest(manifest *datapb.ReshardManifest) error {
	if manifest == nil {
		return merr.WrapErrDataIntegrityMsg("reshard manifest is nil")
	}
	seen := make(map[string]struct{}, len(manifest.GetFragments()))
	for _, fragment := range manifest.GetFragments() {
		// LogicalBytes is the fragment's normalized decoded bytes; it only sizes
		// segment plans, so the check stays a non-negativity guard.
		if fragment == nil || fragment.GetPath() == "" || fragment.GetRows() < 0 || fragment.GetLogicalBytes() < 0 || fragment.GetChannelIndex() < 0 || fragment.GetPartitionIndex() < 0 {
			return merr.WrapErrDataIntegrityMsg("invalid reshard fragment descriptor")
		}
		if _, ok := seen[fragment.GetPath()]; ok {
			return merr.WrapErrDataIntegrityMsg("duplicate reshard fragment path: %s", fragment.GetPath())
		}
		seen[fragment.GetPath()] = struct{}{}
	}
	return nil
}

// validateImportResults validates the worker result contract
// before any SegmentInfo is made Flushed or the task marker is persisted.
// Ordering is defined by the task's fixed output segment slice. Timestamp
// checks use the durable Statistics object so the same fence survives a
// DataCoord restart.
func validateImportResults(results []*datapb.SegmentResult, segmentCount int) error {
	if len(results) != segmentCount {
		return merr.WrapErrDataIntegrityMsg("import result segment count mismatch: got=%d want=%d", len(results), segmentCount)
	}
	for index, result := range results {
		if result == nil || result.GetRows() < 0 {
			return merr.WrapErrDataIntegrityMsg("invalid import segment result at index %d", index)
		}
		if result.GetRows() == 0 {
			if result.GetStatistics() != nil ||
				len(result.GetInsertLogs()) > 0 || result.GetPkLog() != nil ||
				len(result.GetBm25Logs()) > 0 || result.GetManifestPath() != "" ||
				len(result.GetExpirationQuantiles()) > 0 {
				return merr.WrapErrDataIntegrityMsg("zero-row import segment result at index %d has output", index)
			}
		} else {
			if result.GetStatistics() == nil || result.GetStatistics().GetTimestampFrom() > result.GetStatistics().GetTimestampTo() {
				return merr.WrapErrDataIntegrityMsg("non-empty import segment result at index %d has invalid statistics", index)
			}
		}
	}
	return nil
}

// applyImportResults updates all preallocated segments first. The caller
// persists the task Completed marker only after this function succeeds, which
// keeps the existing V2 marker-last recovery shape: a crash between segment
// batches and the task marker simply replays the same worker result.
func applyImportResults(ctx context.Context, meta *meta, collectionID int64, schemaVersion int32, segmentIDs []int64, sorted, namespaceSorted bool, results []*datapb.SegmentResult) error {
	for _, result := range results {
		if result == nil || result.GetManifestPath() != "" {
			continue
		}
		// The StorageV2 writer reports FieldBinlogs by LogPath with no LogID,
		// while the catalog only persists LogID-only entries. Compress before
		// validation so the replay check compares the same shape that is
		// persisted (mirroring the V2 import path).
		statslogs := []*datapb.FieldBinlog(nil)
		if result.GetPkLog() != nil {
			statslogs = []*datapb.FieldBinlog{result.GetPkLog()}
		}
		if err := binlog.CompressBinLogs(result.GetInsertLogs(), statslogs, result.GetBm25Logs()); err != nil {
			return err
		}
	}
	operators := make([]UpdateOperator, 0, len(results)*9)
	for index, result := range results {
		segmentID := segmentIDs[index]
		operators = append(operators, validateImportResultSegmentOperator(collectionID, segmentID, result, sorted, namespaceSorted))
		if result.GetRows() == 0 {
			// A zero-row result leaves the preallocated placeholder with nothing
			// to load, index, or commit: drop it at acceptance. The drop is
			// idempotent for replays (already-Dropped segments are skipped), so
			// the marker-last crash recovery keeps working unchanged.
			operators = append(operators, dropImportV3Segments([]int64{segmentID}))
			continue
		}
		segmentOps := make([]UpdateOperator, 0, 8)
		if result.GetManifestPath() != "" {
			// Storage V3 segment: the manifest is the source of truth for files.
			// SegmentInfo only persists the accepted manifest path, statistics and
			// positions; FieldBinlog arrays are intentionally not persisted.
			segmentOps = append(segmentOps,
				UpdateManifest(segmentID, result.GetManifestPath()),
				UpdateSegmentStats(segmentID, result.GetStatistics()),
				UpdateImportedRows(segmentID, result.GetRows()),
				UpdateImportSegmentPosition(segmentID, result.GetStatistics().GetTimestampFrom(), result.GetStatistics().GetTimestampTo()),
				updateImportResultProjectionOperator(segmentID, result, sorted, namespaceSorted),
				UpdateStatusOperator(segmentID, commonpb.SegmentState_Flushed),
				updateImportV3SchemaVersion(segmentID, schemaVersion),
			)
		} else {
			statslogs := []*datapb.FieldBinlog(nil)
			if result.GetPkLog() != nil {
				statslogs = []*datapb.FieldBinlog{result.GetPkLog()}
			}
			segmentOps = append(segmentOps,
				UpdateBinlogsOperator(segmentID, result.GetInsertLogs(), statslogs, nil, result.GetBm25Logs()),
				UpdateManifest(segmentID, result.GetManifestPath()),
				UpdateSegmentStats(segmentID, result.GetStatistics()),
				UpdateImportedRows(segmentID, result.GetRows()),
				UpdateImportSegmentPosition(segmentID, result.GetStatistics().GetTimestampFrom(), result.GetStatistics().GetTimestampTo()),
				updateImportResultProjectionOperator(segmentID, result, sorted, namespaceSorted),
				UpdateStatusOperator(segmentID, commonpb.SegmentState_Flushed),
				updateImportV3SchemaVersion(segmentID, schemaVersion),
			)
		}
		operators = append(operators, segmentOps...)
	}
	return meta.UpdateSegmentsInfo(ctx, operators...)
}

func updateImportV3SchemaVersion(segmentID int64, schemaVersion int32) UpdateOperator {
	return func(pack *updateSegmentPack) bool {
		segment := pack.Get(segmentID)
		if segment == nil {
			return pack.fail(merr.WrapErrDataIntegrityMsg("import v3 segment %d is missing", segmentID))
		}
		segment.SchemaVersion = schemaVersion
		return true
	}
}

func importV3SegmentStatsNonZero(stats *datapb.Statistics) bool {
	if stats == nil {
		return false
	}
	return stats.GetInsertBinlogSize() != 0 ||
		stats.GetStatsBinlogSize() != 0 ||
		stats.GetDeltaBinlogSize() != 0 ||
		stats.GetDeleteNumRows() != 0 ||
		stats.GetInsertBinlogCount() != 0 ||
		stats.GetDeltaBinlogCount() != 0 ||
		len(stats.GetTimestampQuantiles()) > 0 ||
		stats.GetTimestampFrom() != 0 ||
		stats.GetTimestampTo() != 0 ||
		stats.GetDeltaTimestampFrom() != 0 ||
		stats.GetDeltaTimestampTo() != 0 ||
		len(stats.GetNullCounts()) > 0
}

func validateImportResultSegmentOperator(collectionID, segmentID int64, result *datapb.SegmentResult, sorted, namespaceSorted bool) UpdateOperator {
	return func(pack *updateSegmentPack) bool {
		segment := pack.Get(segmentID)
		if segment == nil {
			return pack.fail(merr.WrapErrDataIntegrityMsg("import v3 segment %d is missing", segmentID))
		}
		if segment.GetCollectionID() != collectionID {
			return pack.fail(merr.WrapErrDataIntegrityMsg("import v3 collection mismatch for segment %d", segmentID))
		}
		if !segment.GetIsImporting() {
			return pack.fail(merr.WrapErrDataIntegrityMsg("import v3 segment %d is not importing", segmentID))
		}
		if result.GetRows() == 0 {
			switch segment.GetState() {
			case commonpb.SegmentState_Importing:
				if segment.GetNumOfRows() != 0 {
					return pack.fail(merr.WrapErrDataIntegrityMsg("zero-row import v3 segment %d has conflicting progress", segmentID))
				}
			case commonpb.SegmentState_Dropped:
				// Replayed acceptance: the placeholder was already dropped by the
				// first acceptance; the drop below is a no-op.
			default:
				return pack.fail(merr.WrapErrDataIntegrityMsg("zero-row import v3 segment %d has conflicting progress", segmentID))
			}
			return true
		}
		switch segment.GetState() {
		case commonpb.SegmentState_Importing:
			if segment.GetNumOfRows() != 0 || segment.GetManifestPath() != "" || importV3SegmentStatsNonZero(segment.GetStats()) ||
				len(segment.GetBinlogs()) > 0 || len(segment.GetStatslogs()) > 0 || len(segment.GetBm25Statslogs()) > 0 {
				return pack.fail(merr.WrapErrDataIntegrityMsg("import v3 segment %d contains unaccepted output", segmentID))
			}
		case commonpb.SegmentState_Flushed:
			if !acceptedImportSegmentMatches(segment, result, sorted, namespaceSorted) {
				return pack.fail(merr.WrapErrDataIntegrityMsg("replayed import v3 result conflicts with segment %d", segmentID))
			}
		default:
			return pack.fail(merr.WrapErrDataIntegrityMsg("import v3 segment %d has invalid state %s", segmentID, segment.GetState()))
		}
		// Post-validation: the segment record froze its storage version at
		// planning while the worker derived the writer layout from live config
		// at dispatch (retries self-heal -- prepareRetry allocates the
		// replacement segment with the current version). A hot-flipped
		// common.storage.useLoonFFI between planning and the FIRST dispatch of
		// a task would otherwise persist a segment whose recorded version
		// disagrees with the written layout, and QueryNode loads by the
		// recorded version -- the data would be silently invisible. Fail the
		// task instead so the operator resubmits the job.
		if manifestWritten := result.GetManifestPath() != ""; manifestWritten != (segment.GetStorageVersion() == storage.StorageV3) {
			written := "V2 binlogs"
			if manifestWritten {
				written = "V3 manifest"
			}
			return pack.fail(merr.WrapErrDataIntegrityMsg(
				"import v3 segment %d records storage version %d but the writer produced %s; common.storage.useLoonFFI changed during the import, resubmit the job",
				segmentID, segment.GetStorageVersion(), written))
		}
		return true
	}
}

func updateImportResultProjectionOperator(segmentID int64, result *datapb.SegmentResult, sorted, namespaceSorted bool) UpdateOperator {
	return func(pack *updateSegmentPack) bool {
		segment := pack.Get(segmentID)
		if segment == nil {
			return pack.fail(merr.WrapErrDataIntegrityMsg("import v3 segment %d is missing", segmentID))
		}
		segment.IsSorted = sorted
		segment.IsSortedByNamespace = namespaceSorted
		segment.ExpirQuantiles = append([]int64(nil), result.GetExpirationQuantiles()...)
		return true
	}
}

func acceptedImportSegmentMatches(segment *SegmentInfo, result *datapb.SegmentResult, sorted, namespaceSorted bool) bool {
	if segment.GetNumOfRows() != result.GetRows() || segment.GetManifestPath() != result.GetManifestPath() ||
		segment.GetStartPosition().GetTimestamp() != result.GetStatistics().GetTimestampFrom() || segment.GetDmlPosition().GetTimestamp() != result.GetStatistics().GetTimestampTo() ||
		segment.GetIsSorted() != sorted || segment.GetIsSortedByNamespace() != namespaceSorted ||
		!slices.Equal(segment.GetExpirQuantiles(), result.GetExpirationQuantiles()) ||
		!proto.Equal(segment.GetStats(), result.GetStatistics()) {
		return false
	}
	// Manifest-backed segments intentionally do not persist FieldBinlog arrays.
	// For V2, the arrays remain part of the durable accepted projection.
	if result.GetManifestPath() == "" {
		statslogs := []*datapb.FieldBinlog(nil)
		if result.GetPkLog() != nil {
			statslogs = []*datapb.FieldBinlog{result.GetPkLog()}
		}
		return proto.Equal(&datapb.SegmentInfo{Binlogs: segment.GetBinlogs(), Statslogs: segment.GetStatslogs(), Bm25Statslogs: segment.GetBm25Statslogs()},
			&datapb.SegmentInfo{Binlogs: result.GetInsertLogs(), Statslogs: statslogs, Bm25Statslogs: result.GetBm25Logs()})
	}
	return true
}

// dropImportV3Segments marks IsImporting segments Dropped. Callers scope it:
// acceptance drops zero-row placeholders, retry drops the superseded
// preallocated segment, failed-job GC drops what the job accepted but never
// committed. Once the commit fence clears IsImporting, no caller drops the
// segment through this operator.
func dropImportV3Segments(segmentIDs []int64) UpdateOperator {
	return func(pack *updateSegmentPack) bool {
		updated := false
		for _, segmentID := range segmentIDs {
			// Peek the committed segment before pack.Get: pack.Get clones the
			// segment into the update pack, which makes UpdateSegmentsInfo
			// persist and log the segment even when this operator drops
			// nothing — and failed-job GC calls here every tick during
			// retention. The peek reads without cloning (same pattern as
			// CreateL0Operator).
			committed := pack.meta.segments.GetSegment(segmentID)
			if committed == nil || !committed.GetIsImporting() ||
				committed.GetState() == commonpb.SegmentState_Dropped {
				continue
			}
			segment := pack.Get(segmentID)
			updateSegStateAndPrepareMetrics(segment, commonpb.SegmentState_Dropped, pack.metricMutation)
			segment.DroppedAt = uint64(time.Now().UnixNano())
			updated = true
		}
		return updated
	}
}
