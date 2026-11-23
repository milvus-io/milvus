package qnview

import (
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/segmentutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// resolveQueryViewManifestIndexes selects the View's exact index definitions
// and joins them to the artifacts named by the accepted segment snapshot.
func resolveQueryViewManifestIndexes(info *querypb.SegmentLoadInfo, metadata *querypb.QueryViewManifestIndexes, selected map[int64]*indexpb.IndexInfo, manifest *packed.LoadManifest, rootPath string) ([]*querypb.FieldIndexInfo, error) {
	artifacts := make(map[int64]*indexpb.IndexFilePathInfo)
	for _, task := range metadata.GetFinishedTasks() {
		id := task.GetIndexID()
		definition := selected[id]
		if definition == nil {
			continue
		}
		if task.GetSegmentID() != info.GetSegmentID() || task.GetBuildID() <= 0 ||
			task.GetFieldID() != definition.GetFieldID() || artifacts[id] != nil {
			return nil, merr.WrapErrDataIntegrityMsg("invalid finished task for segment %d index %d", info.GetSegmentID(), id)
		}
		artifacts[id] = task
	}

	requested := make(map[int64]*datapb.SegmentIndexSummary)
	seen := make(map[int64]struct{})
	for _, summary := range metadata.GetSummaries() {
		id := summary.GetIndexId()
		definition := selected[id]
		if definition == nil {
			continue
		}
		_, duplicate := seen[id]
		if duplicate || summary.GetBuildId() <= 0 || summary.GetFieldId() != definition.GetFieldID() {
			return nil, merr.WrapErrDataIntegrityMsg("invalid summary for segment %d index %d", info.GetSegmentID(), id)
		}
		seen[id] = struct{}{}
		if artifacts[id].GetBuildID() == summary.GetBuildId() {
			continue
		}
		if summary.GetHasArtifact() {
			delete(artifacts, id)
			requested[id] = summary
			continue
		}
		rows := info.GetNumOfRows()
		if summary.NumRows != nil {
			rows = summary.GetNumRows()
		}
		artifacts[id] = &indexpb.IndexFilePathInfo{
			IndexID: id, BuildID: summary.GetBuildId(), IndexVersion: summary.GetIndexVersion(),
			IndexType: summary.GetIndexType(), MemSize: summary.GetMemSize(), NumRows: rows,
			CurrentIndexVersion:       summary.GetVectorEngineVersion(),
			CurrentScalarIndexVersion: summary.GetScalarEngineVersion(),
			IndexStorePathVersion:     indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
		}
	}

	if len(requested) != 0 {
		entries, err := manifest.Indexes()
		if err != nil {
			return nil, merr.Wrapf(err, "read index artifacts for segment %d", info.GetSegmentID())
		}
		for _, entry := range entries {
			id := entry.IndexID
			summary := requested[id]
			if summary == nil {
				continue
			}
			if artifacts[id] != nil || entry.BuildID != summary.GetBuildId() ||
				entry.FieldID != summary.GetFieldId() || entry.IndexVersion != summary.GetIndexVersion() {
				return nil, merr.WrapErrDataIntegrityMsg("manifest index identity disagrees with summary for segment %d index %d", info.GetSegmentID(), id)
			}
			artifact, valid := packed.ManifestIndexFilePathInfoForSegment(rootPath, info.GetManifestPath(),
				info.GetCollectionID(), info.GetPartitionID(), info.GetSegmentID(), entry)
			if !valid {
				return nil, merr.WrapErrDataIntegrityMsg("invalid manifest index for segment %d index %d", info.GetSegmentID(), id)
			}
			artifacts[id] = artifact
		}
		for id, summary := range requested {
			if artifacts[id] == nil {
				return nil, merr.WrapErrDataIntegrityMsg("manifest missing summary build %d for segment %d", summary.GetBuildId(), info.GetSegmentID())
			}
		}
	}

	indexes := make([]*querypb.FieldIndexInfo, 0, len(artifacts))
	for id, artifact := range artifacts {
		indexes = append(indexes, segmentutil.PackQueryViewFieldIndexInfo(selected[id], artifact))
	}
	return indexes, nil
}
