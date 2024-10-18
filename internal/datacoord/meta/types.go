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
	"context"
	"fmt"
	"path"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/meta/analyze"
	"github.com/milvus-io/milvus/internal/datacoord/meta/channel"
	"github.com/milvus-io/milvus/internal/datacoord/meta/collection"
	"github.com/milvus-io/milvus/internal/datacoord/meta/compaction"
	"github.com/milvus-io/milvus/internal/datacoord/meta/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta/partitionstats"
	"github.com/milvus-io/milvus/internal/datacoord/meta/segment"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

type Meta interface {
	// collection cache
	collection.CollectionMetaCache
	// segment meta
	segment.SegmentManager
	// channel meta
	channel.ChannelManager
	// compaction meta
	compaction.CompactionMeta
	// index meta
	index.IndexMeta
	// analyze meta
	analyze.AnalyzeMeta
	// partition stats meta
	partitionstats.PartitionStatsMeta

	// extra methods like wrapper or method need multiple managers
	ReloadCollectionsFromRootcoord(ctx context.Context, broker broker.Broker) error

	// metrics info related APIs
	GetQuotaInfo() *metricsinfo.DataCoordQuotaMetrics
	GetAllCollectionNumRows() map[int64]int64
	GetCollectionIndexFilesSize() uint64
	CleanPartitionStatsInfo(info *datapb.PartitionStatsInfo) error
}

var _ Meta = (*metaImpl)(nil)

type metaImpl struct {
	// collection cache
	collection.CollectionMetaCache
	// segment meta
	segment.SegmentManager
	// channel meta
	channel.ChannelManager
	// compaction meta
	compaction.CompactionMeta
	// index meta
	index.IndexMeta
	// analyze meta
	analyze.AnalyzeMeta
	// partition stats meta
	partitionstats.PartitionStatsMeta

	catalog      metastore.DataCoordCatalog
	chunkManager storage.ChunkManager
}

func NewMeta(ctx context.Context, catalog metastore.DataCoordCatalog, chunkManager storage.ChunkManager) (*metaImpl, error) {
	segmentManager, err := segment.NewSegmentManager(ctx, catalog)
	if err != nil {
		return nil, err
	}
	channelManager, err := channel.NewChannelManager(ctx, catalog)
	if err != nil {
		return nil, err
	}
	compactionMeta, err := compaction.NewCompactionTaskMeta(ctx, catalog)
	if err != nil {
		return nil, err
	}
	indexMeta, err := index.NewIndexMeta(ctx, catalog)
	if err != nil {
		return nil, err
	}
	analyzeMeta, err := analyze.NewAnalyzeMeta(ctx, catalog)
	if err != nil {
		return nil, err
	}
	partitionStats, err := partitionstats.NewPartitionStatsMeta(ctx, catalog)

	m := &metaImpl{
		catalog:      catalog,
		chunkManager: chunkManager,

		CollectionMetaCache: collection.NewCollectionCache(),
		SegmentManager:      segmentManager,
		ChannelManager:      channelManager,
		CompactionMeta:      compactionMeta,
		IndexMeta:           indexMeta,
		AnalyzeMeta:         analyzeMeta,
		PartitionStatsMeta:  partitionStats,
	}

	return m, nil
}

func (m *metaImpl) ReloadCollectionsFromRootcoord(ctx context.Context, broker broker.Broker) error {
	resp, err := broker.ListDatabases(ctx)
	if err != nil {
		return err
	}
	for _, dbName := range resp.GetDbNames() {
		resp, err := broker.ShowCollections(ctx, dbName)
		if err != nil {
			return err
		}
		for _, collectionID := range resp.GetCollectionIds() {
			resp, err := broker.DescribeCollectionInternal(ctx, collectionID)
			if err != nil {
				return err
			}
			partitionIDs, err := broker.ShowPartitionsInternal(ctx, collectionID)
			if err != nil {
				return err
			}
			collection := &collection.CollectionInfo{
				ID:             collectionID,
				Schema:         resp.GetSchema(),
				Partitions:     partitionIDs,
				StartPositions: resp.GetStartPositions(),
				Properties:     funcutil.KeyValuePair2Map(resp.GetProperties()),
				CreatedAt:      resp.GetCreatedTimestamp(),
				DatabaseName:   resp.GetDbName(),
				DatabaseID:     resp.GetDbId(),
				VChannelNames:  resp.GetVirtualChannelNames(),
			}
			m.AddCollection(collection)
		}
	}
	return nil
}

func (m *metaImpl) GetQuotaInfo() *metricsinfo.DataCoordQuotaMetrics {
	info := &metricsinfo.DataCoordQuotaMetrics{}
	// m.RLock()
	// defer m.RUnlock()
	collectionBinlogSize := make(map[int64]int64)
	partitionBinlogSize := make(map[int64]map[int64]int64)
	collectionRowsNum := make(map[int64]map[commonpb.SegmentState]int64)
	// collection id => l0 delta entry count
	collectionL0RowCounts := make(map[int64]int64)

	segments := m.GetSegments(segment.WithHealthyState(), segment.WithSegmentFilterFunc(func(si *segment.SegmentInfo) bool {
		return !si.GetIsImporting()
	}))
	var total int64
	metrics.DataCoordStoredBinlogSize.Reset()
	for _, segment := range segments {
		segmentSize := segment.GetSegmentSize()
		// if isSegmentHealthy(segment) && !segment.GetIsImporting() {
		total += segmentSize
		collectionBinlogSize[segment.GetCollectionID()] += segmentSize

		partBinlogSize, ok := partitionBinlogSize[segment.GetCollectionID()]
		if !ok {
			partBinlogSize = make(map[int64]int64)
			partitionBinlogSize[segment.GetCollectionID()] = partBinlogSize
		}
		partBinlogSize[segment.GetPartitionID()] += segmentSize

		coll := m.GetCollection(segment.GetCollectionID())
		if coll != nil {
			metrics.DataCoordStoredBinlogSize.WithLabelValues(coll.DatabaseName,
				fmt.Sprint(segment.GetCollectionID()), fmt.Sprint(segment.GetID()), segment.GetState().String()).Set(float64(segmentSize))
		} else {
			log.Warn("collection not found ", zap.Int64("collectionID", segment.GetCollectionID()))
		}

		if _, ok := collectionRowsNum[segment.GetCollectionID()]; !ok {
			collectionRowsNum[segment.GetCollectionID()] = make(map[commonpb.SegmentState]int64)
		}
		collectionRowsNum[segment.GetCollectionID()][segment.GetState()] += segment.GetNumOfRows()

		if segment.GetLevel() == datapb.SegmentLevel_L0 {
			collectionL0RowCounts[segment.GetCollectionID()] += segment.GetDeltaCount()
		}
	}

	metrics.DataCoordNumStoredRows.Reset()
	for collectionID, statesRows := range collectionRowsNum {
		for state, rows := range statesRows {
			coll := m.GetCollection(collectionID)
			if coll != nil {
				metrics.DataCoordNumStoredRows.WithLabelValues(coll.DatabaseName, fmt.Sprint(collectionID), state.String()).Set(float64(rows))
			}
		}
	}

	info.TotalBinlogSize = total
	info.CollectionBinlogSize = collectionBinlogSize
	info.PartitionsBinlogSize = partitionBinlogSize
	info.CollectionL0RowCount = collectionL0RowCounts

	return info
}

// GetAllCollectionNumRows returns
func (m *metaImpl) GetAllCollectionNumRows() map[int64]int64 {
	segments := m.GetSegments(segment.WithHealthyState())
	collSegments := lo.GroupBy(segments, func(seg *segment.SegmentInfo) int64 {
		return seg.CollectionID
	})

	return lo.MapValues(collSegments, func(segments []*segment.SegmentInfo, collID int64) int64 {
		return lo.SumBy(segments, func(seg *segment.SegmentInfo) int64 { return seg.GetNumOfRows() })
	})
}

// GetCollectionIndexFilesSize returns the total index files size of all segment for each collection.
func (m *metaImpl) GetCollectionIndexFilesSize() uint64 {
	var total uint64
	for _, segmentIdx := range m.GetAllSegIndexes() {
		coll := m.GetCollection(segmentIdx.CollectionID)
		if coll != nil {
			metrics.DataCoordStoredIndexFilesSize.WithLabelValues(coll.DatabaseName,
				fmt.Sprint(segmentIdx.CollectionID), fmt.Sprint(segmentIdx.SegmentID)).Set(float64(segmentIdx.IndexSize))
			total += segmentIdx.IndexSize
		}
	}
	return total
}

func (m *metaImpl) CleanPartitionStatsInfo(info *datapb.PartitionStatsInfo) error {
	removePaths := make([]string, 0)
	partitionStatsPath := path.Join(m.chunkManager.RootPath(), common.PartitionStatsPath,
		metautil.JoinIDPath(info.CollectionID, info.PartitionID),
		info.GetVChannel(), strconv.FormatInt(info.GetVersion(), 10))
	removePaths = append(removePaths, partitionStatsPath)
	analyzeT := m.GetAnalyzeTask(info.GetAnalyzeTaskID())
	if analyzeT != nil {
		centroidsFilePath := path.Join(m.chunkManager.RootPath(), common.AnalyzeStatsPath,
			metautil.JoinIDPath(analyzeT.GetTaskID(), analyzeT.GetVersion(), analyzeT.GetCollectionID(),
				analyzeT.GetPartitionID(), analyzeT.GetFieldID()),
			"centroids",
		)
		removePaths = append(removePaths, centroidsFilePath)
		for _, segID := range info.GetSegmentIDs() {
			segmentOffsetMappingFilePath := path.Join(m.chunkManager.RootPath(), common.AnalyzeStatsPath,
				metautil.JoinIDPath(analyzeT.GetTaskID(), analyzeT.GetVersion(), analyzeT.GetCollectionID(),
					analyzeT.GetPartitionID(), analyzeT.GetFieldID(), segID),
				"offset_mapping",
			)
			removePaths = append(removePaths, segmentOffsetMappingFilePath)
		}
	}

	log.Debug("remove clustering compaction stats files",
		zap.Int64("collectionID", info.GetCollectionID()),
		zap.Int64("partitionID", info.GetPartitionID()),
		zap.String("vChannel", info.GetVChannel()),
		zap.Int64("planID", info.GetVersion()),
		zap.Strings("removePaths", removePaths))
	err := m.chunkManager.MultiRemove(context.Background(), removePaths)
	if err != nil {
		log.Warn("remove clustering compaction stats files failed", zap.Error(err))
		return err
	}

	// first clean analyze task
	if err = m.DropAnalyzeTask(info.GetAnalyzeTaskID()); err != nil {
		log.Warn("remove analyze task failed", zap.Int64("analyzeTaskID", info.GetAnalyzeTaskID()), zap.Error(err))
		return err
	}

	// finally, clean up the partition stats info, and make sure the analysis task is cleaned up
	err = m.DropPartitionStatsInfo(info)
	log.Debug("drop partition stats meta",
		zap.Int64("collectionID", info.GetCollectionID()),
		zap.Int64("partitionID", info.GetPartitionID()),
		zap.String("vChannel", info.GetVChannel()),
		zap.Int64("planID", info.GetVersion()))
	if err != nil {
		return err
	}
	return nil
}
