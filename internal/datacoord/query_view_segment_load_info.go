package datacoord

import (
	"context"
	"fmt"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/segmentutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/indexparams"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

func (s *Server) GetQueryViewSegmentLoadInfos(ctx context.Context, collectionID int64, segmentIDs []int64) ([]*querypb.SegmentLoadInfo, []*indexpb.IndexInfo, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return nil, nil, err
	}
	if collectionID == 0 {
		return nil, nil, merr.WrapErrParameterInvalidMsg("collection id is zero")
	}
	if len(segmentIDs) == 0 {
		return nil, nil, nil
	}

	indexes, segmentIndexes := s.meta.indexMeta.getQueryViewIndexSnapshot(collectionID, segmentIDs)
	indexInfos := packQueryViewCollectionIndexInfos(indexes)
	infos := make([]*querypb.SegmentLoadInfo, 0, len(segmentIDs))
	for _, segmentID := range segmentIDs {
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
		cloned := segment.Clone()
		segmentutil.ReCalcRowCount(segment.SegmentInfo, cloned.SegmentInfo)
		// Unlike GetSegmentInfo's legacy QueryCoord broker, QueryView consumers
		// receive load-ready metadata directly. Expand compressed log IDs on the
		// snapshot, preserving the compact representation in the metadata cache.
		if err := binlog.DecompressBinLogs(cloned.SegmentInfo); err != nil {
			return nil, nil, merr.Wrapf(err, "expand query view binlogs for segment %d", segmentID)
		}
		infos = append(infos, s.packQueryViewSegmentLoadInfo(cloned.SegmentInfo, indexInfos, segmentIndexes[segmentID]))
	}
	return infos, indexInfos, nil
}

func (s *Server) packQueryViewSegmentLoadInfo(segment *datapb.SegmentInfo, indexInfos []*indexpb.IndexInfo, segmentIndexes map[int64]*model.SegmentIndex) *querypb.SegmentLoadInfo {
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:                   segment.GetID(),
		PartitionID:                 segment.GetPartitionID(),
		CollectionID:                segment.GetCollectionID(),
		BinlogPaths:                 segment.GetBinlogs(),
		NumOfRows:                   segment.GetNumOfRows(),
		Deltalogs:                   segment.GetDeltalogs(),
		CompactionFrom:              segment.GetCompactionFrom(),
		IndexInfos:                  s.packQueryViewFieldIndexInfos(segmentIndexes, indexInfos),
		InsertChannel:               segment.GetInsertChannel(),
		StartPosition:               segment.GetStartPosition(),
		DeltaPosition:               segment.GetDmlPosition(),
		Level:                       segment.GetLevel(),
		StorageVersion:              segment.GetStorageVersion(),
		IsSorted:                    segment.GetIsSorted(),
		Priority:                    commonpb.LoadPriority_HIGH,
		ManifestPath:                segment.GetManifestPath(),
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
	return loadInfo
}

func (s *Server) packQueryViewFieldIndexInfos(segmentIndexes map[int64]*model.SegmentIndex, collectionIndexes []*indexpb.IndexInfo) []*querypb.FieldIndexInfo {
	if len(segmentIndexes) == 0 {
		return nil
	}
	collectionIndexByID := lo.SliceToMap(collectionIndexes, func(index *indexpb.IndexInfo) (int64, *indexpb.IndexInfo) {
		return index.GetIndexID(), index
	})
	infos := make([]*querypb.FieldIndexInfo, 0, len(segmentIndexes))
	for _, segmentIndex := range segmentIndexes {
		if segmentIndex.IndexState != commonpb.IndexState_Finished {
			continue
		}
		collectionIndex, ok := collectionIndexByID[segmentIndex.IndexID]
		if !ok {
			continue
		}
		indexParams := common.CloneKeyValuePairs(collectionIndex.GetIndexParams())
		indexParams = append(indexParams, common.CloneKeyValuePairs(collectionIndex.GetTypeParams())...)
		for _, param := range indexParams {
			if param.Key == common.IndexTypeKey && segmentIndex.IndexType != "" && segmentIndex.IndexType != param.Value {
				param.Value = segmentIndex.IndexType
				break
			}
		}
		indexName := collectionIndex.GetIndexName()
		if segmentIndex.IndexType != "" && segmentIndex.IndexType != indexName {
			indexName = segmentIndex.IndexType
		}
		params := funcutil.KeyValuePair2Map(indexParams)
		for _, kv := range collectionIndex.GetUserIndexParams() {
			if indexparams.IsConfigableIndexParam(kv.GetKey()) {
				params[kv.GetKey()] = kv.GetValue()
			}
		}
		indexParams = funcutil.Map2KeyValuePair(params)
		indexParams = append(indexParams, &commonpb.KeyValuePair{
			Key:   common.LoadPriorityKey,
			Value: commonpb.LoadPriority_HIGH.String(),
		})
		builder := metautil.NewIndexPathBuilder(s.meta.chunkManager.RootPath(),
			segmentIndex.IndexStorePathVersion,
			segmentIndex.CollectionID,
			segmentIndex.PartitionID,
			segmentIndex.SegmentID,
			segmentIndex.BuildID,
			segmentIndex.IndexVersion)
		infos = append(infos, &querypb.FieldIndexInfo{
			FieldID:                   collectionIndex.GetFieldID(),
			EnableIndex:               true,
			IndexName:                 indexName,
			IndexID:                   segmentIndex.IndexID,
			BuildID:                   segmentIndex.BuildID,
			IndexParams:               indexParams,
			IndexFilePaths:            builder.BuildFilePaths(segmentIndex.IndexFileKeys),
			IndexSize:                 int64(segmentIndex.IndexMemSize),
			IndexVersion:              segmentIndex.IndexVersion,
			NumRows:                   segmentIndex.NumRows,
			CurrentIndexVersion:       segmentIndex.CurrentIndexVersion,
			CurrentScalarIndexVersion: segmentIndex.CurrentScalarIndexVersion,
			IndexStorePathVersion:     segmentIndex.IndexStorePathVersion,
		})
	}
	return infos
}
