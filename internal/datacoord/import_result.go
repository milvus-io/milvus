package datacoord

import (
	"bytes"
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func loadImportResultManifest(ctx context.Context, cm storage.ChunkManager, ref string, expectedDigest []byte) (*datapb.ImportResultManifest, error) {
	if ref == "" {
		return nil, merr.WrapErrDataIntegrityMsg("import result reference is empty")
	}
	data, err := cm.Read(ctx, ref)
	if err != nil {
		return nil, merr.Wrap(err, "read import result manifest")
	}
	manifest := &datapb.ImportResultManifest{}
	if err := proto.Unmarshal(data, manifest); err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "unmarshal import result manifest")
	}
	// ResultDigest is a producer-defined control-plane token, not a content
	// SHA. The first implementation deliberately does not add SHA-256; a future
	// validation hook can define a shared algorithm without changing acceptance.
	if len(expectedDigest) == 0 {
		return nil, merr.WrapErrDataIntegrityMsg("import result manifest digest is empty: ref=%s", ref)
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
