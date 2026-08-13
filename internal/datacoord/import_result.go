package datacoord

import (
	"bytes"
	"context"
	"hash/crc64"
	"path"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func loadImportV3Object(ctx context.Context, cm storage.ChunkManager, ref, expectedPrefix string, expectedDigest []byte) ([]byte, error) {
	cleanRef, cleanPrefix := path.Clean(ref), path.Clean(expectedPrefix)
	if ref == "" || expectedPrefix == "" || (cleanRef != cleanPrefix && !strings.HasPrefix(cleanRef, cleanPrefix+"/")) {
		return nil, merr.WrapErrDataIntegrityMsg("import v3 object reference is outside task output prefix: ref=%s prefix=%s", ref, expectedPrefix)
	}
	data, err := cm.Read(ctx, cleanRef)
	if err != nil {
		return nil, merr.Wrap(err, "read import v3 object")
	}
	digestValue := crc64.Checksum(data, crc64.MakeTable(crc64.ECMA))
	actualDigest := fmt.Sprintf("crc64-ecma:%016x", digestValue)
	if string(expectedDigest) != actualDigest {
		return nil, merr.WrapErrDataIntegrityMsg("import v3 object digest mismatch: ref=%s", ref)
	}
	digestToken := strings.TrimPrefix(actualDigest, "crc64-ecma:")
	if !strings.Contains(path.Base(cleanRef), digestToken) {
		return nil, merr.WrapErrDataIntegrityMsg("import v3 object reference does not match digest: ref=%s", ref)
	}
	return data, nil
}

func loadImportResultManifest(ctx context.Context, cm storage.ChunkManager, ref string, expectedDigest []byte) (*datapb.ImportResultManifest, error) {
	data, err := cm.Read(ctx, ref)
	if err != nil {
		return nil, merr.Wrap(err, "read import result manifest")
	}
	manifest := &datapb.ImportResultManifest{}
	if err := proto.Unmarshal(data, manifest); err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "unmarshal import result manifest")
	}
	if len(expectedDigest) == 0 {
		return nil, merr.WrapErrDataIntegrityMsg("import result manifest digest is empty: ref=%s", ref)
	}
	return manifest, nil
}

func loadImportResultManifestV3(ctx context.Context, cm storage.ChunkManager, ref, expectedPrefix string, expectedDigest []byte) (*datapb.ImportResultManifest, error) {
	data, err := loadImportV3Object(ctx, cm, ref, expectedPrefix, expectedDigest)
	if err != nil {
		return nil, err
	}
	manifest := &datapb.ImportResultManifest{}
	if err := proto.Unmarshal(data, manifest); err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "unmarshal import result manifest")
	}
	return manifest, nil
}

func loadReshardResultManifest(ctx context.Context, cm storage.ChunkManager, ref, expectedPrefix string, expectedDigest []byte) (*datapb.ReshardManifest, error) {
	data, err := loadImportV3Object(ctx, cm, ref, expectedPrefix, expectedDigest)
	if err != nil {
		return nil, err
	}
	manifest := &datapb.ReshardManifest{}
	if err := proto.Unmarshal(data, manifest); err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "unmarshal reshard result manifest")
	}
	return manifest, nil
}

func validateReshardManifest(manifest *datapb.ReshardManifest, jobID, taskID, runID int64, planDigest []byte) error {
	if manifest == nil {
		return merr.WrapErrDataIntegrityMsg("reshard manifest is nil")
	}
	if manifest.GetJobId() != jobID || manifest.GetTaskId() != taskID || manifest.GetRunId() != runID {
		return merr.WrapErrDataIntegrityMsg("reshard manifest identity mismatch: job=%d task=%d run=%d", manifest.GetJobId(), manifest.GetTaskId(), manifest.GetRunId())
	}
	if len(planDigest) == 0 || !bytes.Equal(planDigest, manifest.GetTaskPlanDigest()) {
		return merr.WrapErrDataIntegrityMsg("reshard manifest task plan digest mismatch")
	}
	var rows, logicalBytes, physicalBytes int64
	seen := make(map[string]struct{}, len(manifest.GetFragments()))
	for _, fragment := range manifest.GetFragments() {
		if fragment == nil || fragment.GetPath() == "" || fragment.GetRows() < 0 || fragment.GetLogicalBytes() < 0 || fragment.GetPhysicalBytes() < 0 {
			return merr.WrapErrDataIntegrityMsg("invalid reshard fragment descriptor")
		}
		if _, ok := seen[fragment.GetPath()]; ok {
			return merr.WrapErrDataIntegrityMsg("duplicate reshard fragment path: %s", fragment.GetPath())
		}
		seen[fragment.GetPath()] = struct{}{}
		rows += fragment.GetRows()
		logicalBytes += fragment.GetLogicalBytes()
		physicalBytes += fragment.GetPhysicalBytes()
	}
	if rows != manifest.GetTotalRows() || logicalBytes != manifest.GetTotalLogicalBytes() || physicalBytes != manifest.GetTotalPhysicalBytes() {
		return merr.WrapErrDataIntegrityMsg("reshard manifest totals mismatch")
	}
	return nil
}

// validateImportResultManifest validates the immutable control-plane contract
// before any SegmentInfo is made Flushed or the task marker is persisted.
// Segment IDs and ordering are checked against the task's fixed output list;
// all timestamp and statistics checks use the durable Statistics object so the
// same fence survives DataCoord restart.
func validateImportResultManifest(manifest *datapb.ImportResultManifest, jobID, taskID, runID, generation int64, planDigest []byte, outputSegmentIDs []int64) error {
	if manifest == nil {
		return merr.WrapErrDataIntegrityMsg("import result manifest is nil")
	}
	if manifest.GetJobId() != jobID || manifest.GetTaskId() != taskID || manifest.GetRunId() != runID || manifest.GetPlanningGeneration() != generation {
		return merr.WrapErrDataIntegrityMsg("import result manifest identity mismatch")
	}
	if len(planDigest) == 0 || !bytes.Equal(planDigest, manifest.GetTaskPlanDigest()) {
		return merr.WrapErrDataIntegrityMsg("import result task plan digest mismatch")
	}
	if manifest.GetTotalRows() < 0 || manifest.GetTotalPhysicalBytes() < 0 {
		return merr.WrapErrDataIntegrityMsg("import result totals must be non-negative")
	}
	if len(manifest.GetSegments()) != len(outputSegmentIDs) {
		return merr.WrapErrDataIntegrityMsg("import result segment count mismatch: got=%d want=%d", len(manifest.GetSegments()), len(outputSegmentIDs))
	}
	seen := make(map[int64]struct{}, len(outputSegmentIDs))
	var rows, physicalBytes int64
	for index, result := range manifest.GetSegments() {
		if result == nil || result.GetPhysicalSegmentId() != outputSegmentIDs[index] || result.GetRows() < 0 || result.GetPhysicalBytes() < 0 {
			return merr.WrapErrDataIntegrityMsg("invalid import segment result at ordinal %d", index)
		}
		if result.GetLogicalSegmentOrdinal() != int64(index) {
			return merr.WrapErrDataIntegrityMsg("import result logical segment ordinal mismatch: got=%d want=%d", result.GetLogicalSegmentOrdinal(), index)
		}
		if result.GetStorageVersion() != 0 && result.GetStorageVersion() != storage.StorageV2 && result.GetStorageVersion() != storage.StorageV3 {
			return merr.WrapErrDataIntegrityMsg("segment %d has unsupported storage version %d", result.GetPhysicalSegmentId(), result.GetStorageVersion())
		}
		if _, ok := seen[result.GetPhysicalSegmentId()]; ok {
			return merr.WrapErrDataIntegrityMsg("duplicate import segment result: %d", result.GetPhysicalSegmentId())
		}
		seen[result.GetPhysicalSegmentId()] = struct{}{}
		if result.GetRows() == 0 {
			// A zero-row plan is a placeholder only.  It must not carry any
			// physical output or metadata that could make it look consumable to
			// GC, index, stats, or commit code during marker-last recovery.
			if result.GetMaterialized() || result.GetStatistics() != nil ||
				len(result.GetInsertLogs()) > 0 || result.GetPkStatsLog() != nil ||
				len(result.GetBm25Logs()) > 0 || len(result.GetTextStatsLogs()) > 0 ||
				result.GetManifestPath() != "" || result.GetPhysicalBytes() != 0 ||
				result.GetMinTimestamp() != 0 || result.GetMaxTimestamp() != 0 ||
				len(result.GetExpirationQuantiles()) > 0 {
				return merr.WrapErrDataIntegrityMsg("zero-row import segment %d must not be materialized", result.GetPhysicalSegmentId())
			}
		} else {
			if !result.GetMaterialized() || result.GetStatistics() == nil {
				return merr.WrapErrDataIntegrityMsg("non-empty import segment %d is not materialized", result.GetPhysicalSegmentId())
			}
			if result.GetMinTimestamp() > result.GetMaxTimestamp() {
				return merr.WrapErrDataIntegrityMsg("segment %d timestamp range is reversed", result.GetPhysicalSegmentId())
			}
			if result.GetMinTimestamp() != result.GetStatistics().GetTimestampFrom() {
				return merr.WrapErrDataIntegrityMsg("segment %d min timestamp does not match Statistics.TimestampFrom", result.GetPhysicalSegmentId())
			}
			if result.GetMaxTimestamp() != result.GetStatistics().GetTimestampTo() {
				return merr.WrapErrDataIntegrityMsg("segment %d max timestamp does not match Statistics.TimestampTo", result.GetPhysicalSegmentId())
			}
		}
		rows += result.GetRows()
		physicalBytes += result.GetPhysicalBytes()
	}
	if rows != manifest.GetTotalRows() || physicalBytes != manifest.GetTotalPhysicalBytes() {
		return merr.WrapErrDataIntegrityMsg("import result totals mismatch")
	}
	return nil
}

// applyImportResultManifest updates all skeleton segments first. The caller
// persists the task Completed marker only after this function succeeds, which
// keeps the existing V2 marker-last recovery shape: a crash between segment
// batches and the task marker simply replays the same immutable manifest.
func applyImportResultManifest(ctx context.Context, meta *meta, collectionID int64, manifest *datapb.ImportResultManifest) error {
	operators := make([]UpdateOperator, 0, len(manifest.GetSegments())*9)
	for _, result := range manifest.GetSegments() {
		segmentID := result.GetPhysicalSegmentId()
		operators = append(operators, validateImportResultSegmentOperator(collectionID, result))
		if result.GetRows() == 0 {
			// Keep the zero-row skeleton invisible and Importing until the worker
			// task is dropped/unbound.  This avoids publishing a lifecycle change
			// before the task Completed marker is durable.
			continue
		}
		statslogs := []*datapb.FieldBinlog(nil)
		if result.GetPkStatsLog() != nil {
			statslogs = []*datapb.FieldBinlog{result.GetPkStatsLog()}
		}
		operators = append(operators,
			UpdateBinlogsOperator(segmentID, result.GetInsertLogs(), statslogs, nil, result.GetBm25Logs()),
			UpdateManifest(segmentID, result.GetManifestPath()),
			UpdateSegmentStats(segmentID, result.GetStatistics()),
			UpdateImportedRows(segmentID, result.GetRows()),
			UpdateImportSegmentPosition(segmentID, result.GetMinTimestamp(), result.GetMaxTimestamp()),
			updateImportResultProjectionOperator(result),
			UpdateStatusOperator(segmentID, commonpb.SegmentState_Flushed),
		)
	}
	return meta.UpdateSegmentsInfo(ctx, operators...)
}

func validateImportResultSegmentOperator(collectionID int64, result *datapb.SegmentResult) UpdateOperator {
	return func(pack *updateSegmentPack) bool {
		segment := pack.Get(result.GetPhysicalSegmentId())
		if segment == nil {
			return pack.fail(merr.WrapErrDataIntegrityMsg("import v3 skeleton segment %d is missing", result.GetPhysicalSegmentId()))
		}
		if segment.GetCollectionID() != collectionID || segment.GetPartitionID() != result.GetPartitionId() ||
			segment.GetInsertChannel() != result.GetVchannel() || segment.GetStorageVersion() != result.GetStorageVersion() ||
			segment.GetSchemaVersion() != result.GetSchemaVersion() {
			return pack.fail(merr.WrapErrDataIntegrityMsg("import v3 skeleton identity mismatch for segment %d", result.GetPhysicalSegmentId()))
		}
		if !segment.GetIsImporting() || !segment.GetIsInvisible() {
			return pack.fail(merr.WrapErrDataIntegrityMsg("import v3 skeleton %d is already visible or not importing", result.GetPhysicalSegmentId()))
		}
		if result.GetRows() == 0 {
			if segment.GetState() != commonpb.SegmentState_Importing || segment.GetNumOfRows() != 0 {
				return pack.fail(merr.WrapErrDataIntegrityMsg("zero-row import v3 skeleton %d has conflicting progress", result.GetPhysicalSegmentId()))
			}
			return true
		}
		switch segment.GetState() {
		case commonpb.SegmentState_Importing:
			if segment.GetNumOfRows() != 0 || segment.GetManifestPath() != "" || segment.GetStats() != nil ||
				len(segment.GetBinlogs()) > 0 || len(segment.GetStatslogs()) > 0 || len(segment.GetBm25Statslogs()) > 0 {
				return pack.fail(merr.WrapErrDataIntegrityMsg("import v3 skeleton %d contains unaccepted output", result.GetPhysicalSegmentId()))
			}
		case commonpb.SegmentState_Flushed:
			if !acceptedImportSegmentMatches(segment, result) {
				return pack.fail(merr.WrapErrDataIntegrityMsg("replayed import v3 result conflicts with segment %d", result.GetPhysicalSegmentId()))
			}
		default:
			return pack.fail(merr.WrapErrDataIntegrityMsg("import v3 skeleton %d has invalid state %s", result.GetPhysicalSegmentId(), segment.GetState()))
		}
		return true
	}
}

func updateImportResultProjectionOperator(result *datapb.SegmentResult) UpdateOperator {
	return func(pack *updateSegmentPack) bool {
		segment := pack.Get(result.GetPhysicalSegmentId())
		if segment == nil {
			return pack.fail(merr.WrapErrDataIntegrityMsg("import v3 skeleton segment %d is missing", result.GetPhysicalSegmentId()))
		}
		segment.IsSorted = result.GetIsSorted()
		segment.IsSortedByNamespace = result.GetIsSortedByNamespace()
		segment.ExpirQuantiles = append([]int64(nil), result.GetExpirationQuantiles()...)
		segment.TextStatsLogs = proto.Clone(result).(*datapb.SegmentResult).GetTextStatsLogs()
		return true
	}
}

func acceptedImportSegmentMatches(segment *SegmentInfo, result *datapb.SegmentResult) bool {
	if segment.GetNumOfRows() != result.GetRows() || segment.GetManifestPath() != result.GetManifestPath() ||
		segment.GetStartPosition().GetTimestamp() != result.GetMinTimestamp() || segment.GetDmlPosition().GetTimestamp() != result.GetMaxTimestamp() ||
		segment.GetIsSorted() != result.GetIsSorted() || segment.GetIsSortedByNamespace() != result.GetIsSortedByNamespace() ||
		!slices.Equal(segment.GetExpirQuantiles(), result.GetExpirationQuantiles()) ||
		!proto.Equal(segment.GetStats(), result.GetStatistics()) {
		return false
	}
	wantText := proto.Clone(result).(*datapb.SegmentResult).GetTextStatsLogs()
	if !proto.Equal(&datapb.SegmentInfo{TextStatsLogs: segment.GetTextStatsLogs()}, &datapb.SegmentInfo{TextStatsLogs: wantText}) {
		return false
	}
	// Manifest-backed segments intentionally do not persist FieldBinlog arrays.
	// For V2, the arrays remain part of the durable accepted projection.
	if result.GetManifestPath() == "" {
		statslogs := []*datapb.FieldBinlog(nil)
		if result.GetPkStatsLog() != nil {
			statslogs = []*datapb.FieldBinlog{result.GetPkStatsLog()}
		}
		return proto.Equal(&datapb.SegmentInfo{Binlogs: segment.GetBinlogs(), Statslogs: segment.GetStatslogs(), Bm25Statslogs: segment.GetBm25Statslogs()},
			&datapb.SegmentInfo{Binlogs: result.GetInsertLogs(), Statslogs: statslogs, Bm25Statslogs: result.GetBm25Logs()})
	}
	return true
}

func dropImportV3Skeletons(segmentIDs []int64, zeroOnly bool) UpdateOperator {
	return func(pack *updateSegmentPack) bool {
		updated := false
		for _, segmentID := range segmentIDs {
			segment := pack.Get(segmentID)
			if segment == nil || !segment.GetIsInvisible() || !segment.GetIsImporting() ||
				(zeroOnly && (segment.GetState() != commonpb.SegmentState_Importing || segment.GetNumOfRows() != 0)) {
				continue
			}
			updateSegStateAndPrepareMetrics(segment, commonpb.SegmentState_Dropped, pack.metricMutation)
			segment.DroppedAt = uint64(time.Now().UnixNano())
			updated = true
		}
		return updated
	}
}
