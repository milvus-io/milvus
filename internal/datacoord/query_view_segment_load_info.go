package datacoord

import (
	"context"
	"fmt"
	"maps"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/segmentutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (s *Server) GetQueryViewSegmentLoadInfos(ctx context.Context, collectionID int64, segmentIDs []int64, resolveManifestIndexesLocally bool) ([]*querypb.QueryViewSegmentLoadInfoResult, []*indexpb.IndexInfo, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return nil, nil, err
	}
	if collectionID == 0 {
		return nil, nil, merr.WrapErrParameterInvalidMsg("collection id is zero")
	}
	if len(segmentIDs) == 0 {
		return nil, nil, nil
	}

	localIndexes := false
	if resolveManifestIndexesLocally && s.indexMeta.UsesSegmentSummaries() {
		collection := s.meta.GetCollection(collectionID)
		localIndexes = collection != nil && collection.Schema != nil && !typeutil.IsExternalCollection(collection.Schema)
	}

	indexes, segmentIndexes := s.indexMeta.GetQueryViewIndexSnapshot(collectionID, segmentIDs)
	indexInfos := packQueryViewCollectionIndexInfos(indexes)
	results := make([]*querypb.QueryViewSegmentLoadInfoResult, 0, len(segmentIDs))
	seen := make(map[int64]struct{}, len(segmentIDs))
	liveIndexes := lo.SliceToMap(indexInfos, func(index *indexpb.IndexInfo) (int64, *indexpb.IndexInfo) {
		return index.GetIndexID(), index
	})
	for _, segmentID := range segmentIDs {
		if _, ok := seen[segmentID]; ok {
			continue
		}
		seen[segmentID] = struct{}{}
		info, indexes, err := s.getQueryViewSegmentLoadInfo(ctx, collectionID, segmentID, liveIndexes, segmentIndexes[segmentID], localIndexes)
		results = append(results, &querypb.QueryViewSegmentLoadInfoResult{
			SegmentID: segmentID, Status: merr.Status(err), LoadInfo: info, ManifestIndexes: indexes,
		})
	}
	return results, indexInfos, nil
}

func (s *Server) getQueryViewSegmentLoadInfo(ctx context.Context, collectionID, segmentID int64, indexInfos map[int64]*indexpb.IndexInfo, segmentIndexes map[int64]*model.SegmentIndex, localIndexes bool) (*querypb.SegmentLoadInfo, *querypb.QueryViewManifestIndexes, error) {
	segment := s.meta.GetSegment(ctx, segmentID)
	if segment == nil {
		return nil, nil, merr.WrapErrSegmentNotFound(segmentID, "missing segment info for query view")
	}
	if segment.GetCollectionID() != collectionID {
		return nil, nil, merr.WrapErrSegmentNotFound(segmentID, fmt.Sprintf("segment does not belong to collection %d", collectionID))
	}
	if segment.GetTransformStartAfterTimetick() == 0 {
		return nil, nil, merr.WrapErrServiceNotReadyMsg("segment %d has no certified transform replay cursor", segmentID)
	}
	var manifestIndexes *querypb.QueryViewManifestIndexes
	if s.indexMeta.UsesSegmentSummaries() {
		var err error
		if localIndexes && segment.GetStorageVersion() == storage.StorageV3 &&
			segment.GetLevel() != datapb.SegmentLevel_L0 && segment.GetManifestPath() != "" {
			manifestIndexes, err = s.packQueryViewManifestIndexes(segment.SegmentInfo, indexInfos, segmentIndexes)
			segmentIndexes = nil
		} else {
			segmentIndexes, err = s.meta.ResolveSegmentIndexes(ctx, segment.SegmentInfo, segmentIndexes, func(indexID int64) bool {
				return indexInfos[indexID] != nil
			})
		}
		if err != nil {
			return nil, nil, err
		}
	}
	cloned := segment.Clone()
	segmentutil.ReCalcRowCount(segment.SegmentInfo, cloned.SegmentInfo)
	// Expand compressed log IDs on the snapshot, preserving the compact
	// representation in the metadata cache.
	if err := binlog.DecompressBinLogs(cloned.SegmentInfo); err != nil {
		return nil, nil, merr.Wrapf(err, "expand query view binlogs for segment %d", segmentID)
	}
	info, err := s.packQueryViewSegmentLoadInfo(cloned.SegmentInfo, indexInfos, segmentIndexes)
	return info, manifestIndexes, err
}

// packQueryViewManifestIndexes uses the same pinned inventory and task
// precedence as the full response. Only retained task rows need file paths.
func (s *Server) packQueryViewManifestIndexes(segment *datapb.SegmentInfo, definitions map[int64]*indexpb.IndexInfo, tasks map[int64]*model.SegmentIndex) (*querypb.QueryViewManifestIndexes, error) {
	live := func(id int64) bool { return definitions[id] != nil }
	records, _, err := meta.MergeSegmentIndexSummaries(segment, maps.Clone(tasks), live)
	if err != nil {
		return nil, err
	}
	result := &querypb.QueryViewManifestIndexes{}
	for _, summary := range segment.GetIndexSummaries() {
		id := summary.GetIndexId()
		if live(id) && records[id] != tasks[id] {
			result.Summaries = append(result.Summaries, summary)
		}
	}
	for id, task := range tasks {
		definition := definitions[id]
		if definition == nil || task.IndexState != commonpb.IndexState_Finished || records[id] != task {
			continue
		}
		artifact := indexFilePathInfo(segment.GetID(), task, definition, nil)
		artifact.IndexFilePaths, err = meta.SegmentIndexFilePaths(s.meta.ChunkManager.RootPath(), task, segment)
		if err != nil {
			return nil, merr.Wrap(err, "resolve query view index files")
		}
		result.FinishedTasks = append(result.FinishedTasks, artifact)
	}
	return result, nil
}

func (s *Server) packQueryViewSegmentLoadInfo(segment *datapb.SegmentInfo, indexInfos map[int64]*indexpb.IndexInfo, segmentIndexes map[int64]*model.SegmentIndex) (*querypb.SegmentLoadInfo, error) {
	fieldIndexes, err := s.packQueryViewFieldIndexInfos(segment, segmentIndexes, indexInfos)
	if err != nil {
		return nil, err
	}
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:                   segment.GetID(),
		PartitionID:                 segment.GetPartitionID(),
		CollectionID:                segment.GetCollectionID(),
		BinlogPaths:                 segment.GetBinlogs(),
		NumOfRows:                   segment.GetNumOfRows(),
		Deltalogs:                   segment.GetDeltalogs(),
		CompactionFrom:              segment.GetCompactionFrom(),
		IndexInfos:                  fieldIndexes,
		InsertChannel:               segment.GetInsertChannel(),
		StartPosition:               segment.GetStartPosition(),
		DeltaPosition:               segment.GetDmlPosition(),
		Level:                       segment.GetLevel(),
		StorageVersion:              segment.GetStorageVersion(),
		IsSorted:                    segment.GetIsSorted(),
		Priority:                    commonpb.LoadPriority_HIGH,
		ManifestPath:                segment.GetManifestPath(),
		Stats:                       segment.GetStats(),
		DataVersion:                 segment.GetDataVersion(),
		CommitTimestamp:             segment.GetCommitTimestamp(),
		TransformStartAfterTimetick: segment.GetTransformStartAfterTimetick(),
	}
	if segment.GetManifestPath() == "" {
		loadInfo.Statslogs = segment.GetStatslogs()
		loadInfo.TextStatsLogs = segment.GetTextStatsLogs()
		loadInfo.Bm25Logs = segment.GetBm25Statslogs()
		loadInfo.JsonKeyStatsLogs = segment.GetJsonKeyStats()
	} else {
		loadInfo.JsonKeyStatsLogs = segment.GetJsonKeyStats()
	}
	return loadInfo, nil
}

func (s *Server) packQueryViewFieldIndexInfos(segment *datapb.SegmentInfo, segmentIndexes map[int64]*model.SegmentIndex, collectionIndexByID map[int64]*indexpb.IndexInfo) ([]*querypb.FieldIndexInfo, error) {
	if len(segmentIndexes) == 0 {
		return nil, nil
	}
	infos := make([]*querypb.FieldIndexInfo, 0, len(segmentIndexes))
	for _, segmentIndex := range segmentIndexes {
		if segmentIndex.IndexState != commonpb.IndexState_Finished {
			continue
		}
		collectionIndex, ok := collectionIndexByID[segmentIndex.IndexID]
		if !ok {
			continue
		}
		artifact := indexFilePathInfo(segment.GetID(), segmentIndex, collectionIndex, nil)
		filePaths, err := meta.SegmentIndexFilePaths(s.meta.ChunkManager.RootPath(), segmentIndex, segment)
		if err != nil {
			return nil, merr.Wrap(err, "resolve query view index files")
		}
		artifact.IndexFilePaths = filePaths
		infos = append(infos, segmentutil.PackQueryViewFieldIndexInfo(collectionIndex, artifact))
	}
	return infos, nil
}
