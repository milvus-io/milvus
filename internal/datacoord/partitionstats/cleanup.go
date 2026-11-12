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

package partitionstats

import (
	"context"
	"path"
	"strconv"

	"github.com/milvus-io/milvus/internal/datacoord/analyze"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

// PartitionStatsCleaner removes a partition stats version: its stats files, the
// centroid and offset-mapping files of its analyze task, and its catalog records.
type PartitionStatsCleaner interface {
	CleanPartitionStatsInfo(ctx context.Context, info *datapb.PartitionStatsInfo) error
}

type partitionStatsCleaner struct {
	chunkManager       storage.ChunkManager
	catalog            metastore.DataCoordCatalog
	analyzeMeta        *analyze.AnalyzeMeta
	partitionStatsMeta *PartitionStatsMeta
}

var _ PartitionStatsCleaner = (*partitionStatsCleaner)(nil)

func NewPartitionStatsCleaner(chunkManager storage.ChunkManager, catalog metastore.DataCoordCatalog, analyzeMeta *analyze.AnalyzeMeta, partitionStatsMeta *PartitionStatsMeta) *partitionStatsCleaner {
	return &partitionStatsCleaner{
		chunkManager:       chunkManager,
		catalog:            catalog,
		analyzeMeta:        analyzeMeta,
		partitionStatsMeta: partitionStatsMeta,
	}
}

func (c *partitionStatsCleaner) CleanPartitionStatsInfo(ctx context.Context, info *datapb.PartitionStatsInfo) error {
	removePaths := make([]string, 0)
	partitionStatsPath := path.Join(c.chunkManager.RootPath(), common.PartitionStatsPath,
		metautil.JoinIDPath(info.CollectionID, info.PartitionID),
		info.GetVChannel(), strconv.FormatInt(info.GetVersion(), 10))
	removePaths = append(removePaths, partitionStatsPath)
	analyzeT := c.analyzeMeta.GetTask(info.GetAnalyzeTaskID())
	if analyzeT != nil {
		centroidsFilePath := path.Join(c.chunkManager.RootPath(), common.AnalyzeStatsPath,
			metautil.JoinIDPath(analyzeT.GetTaskID(), analyzeT.GetVersion(), analyzeT.GetCollectionID(),
				analyzeT.GetPartitionID(), analyzeT.GetFieldID()),
			"centroids",
		)
		removePaths = append(removePaths, centroidsFilePath)
		for _, segID := range info.GetSegmentIDs() {
			segmentOffsetMappingFilePath := path.Join(c.chunkManager.RootPath(), common.AnalyzeStatsPath,
				metautil.JoinIDPath(analyzeT.GetTaskID(), analyzeT.GetVersion(), analyzeT.GetCollectionID(),
					analyzeT.GetPartitionID(), analyzeT.GetFieldID(), segID),
				"offset_mapping",
			)
			removePaths = append(removePaths, segmentOffsetMappingFilePath)
		}
	}

	mlog.Debug(ctx, "remove clustering compaction stats files",
		mlog.Int64("collectionID", info.GetCollectionID()),
		mlog.Int64("partitionID", info.GetPartitionID()),
		mlog.String("vChannel", info.GetVChannel()),
		mlog.Int64("planID", info.GetVersion()),
		mlog.Strings("removePaths", removePaths))
	err := c.chunkManager.MultiRemove(context.Background(), removePaths)
	if err != nil {
		mlog.Warn(ctx, "remove clustering compaction stats files failed", mlog.Err(err))
		return err
	}

	// Persist the analyze task removal, the current-partition-stats-version
	// rollback (if the dropped version is the current one), and the
	// partition-stats info removal as a single composite catalog write, with
	// the partition-stats info removal landing last as the commit marker.
	//
	// Both meta locks are held across the WHOLE compute -> catalog.Update ->
	// in-memory apply sequence, mirroring the single critical section the
	// legacy partitionStatsMeta.DropPartitionStatsInfo held around its own
	// catalog write. This is what keeps the rollback correct: without it, a
	// concurrent clustering-compaction completion (SaveCurrentPartitionStats
	// Version for the same coll/part/vchannel) could bump the current version
	// between the rollback compute and its application, and the apply would
	// clobber that newer version back to the stale rollback target. In-memory
	// bookkeeping is only applied after the write succeeds, so a failed write
	// never desyncs memory from disk.
	c.analyzeMeta.Lock()
	defer c.analyzeMeta.Unlock()
	c.partitionStatsMeta.Lock()
	defer c.partitionStatsMeta.Unlock()

	rollbackVersion := c.partitionStatsMeta.getRollbackVersionLocked(info)

	actions := []metastore.UpdateAction{metastore.DropAnalyzeTask(info.GetAnalyzeTaskID())}
	if rollbackVersion != nil {
		actions = append(actions, metastore.SavePartitionStatsVersion(
			info.GetCollectionID(), info.GetPartitionID(), info.GetVChannel(), *rollbackVersion))
	}
	actions = append(actions, metastore.DropPartitionStats(info))

	if err := c.catalog.Update(ctx, actions...); err != nil {
		mlog.Warn(ctx, "clean partition stats info failed",
			mlog.Int64("collectionID", info.GetCollectionID()),
			mlog.Int64("partitionID", info.GetPartitionID()),
			mlog.String("vChannel", info.GetVChannel()),
			mlog.Int64("planID", info.GetVersion()),
			mlog.Int64("analyzeTaskID", info.GetAnalyzeTaskID()),
			mlog.Err(err))
		return err
	}

	c.analyzeMeta.DropTaskFromMemoryLocked(info.GetAnalyzeTaskID())
	c.partitionStatsMeta.applyDropLocked(info, rollbackVersion)

	mlog.Debug(ctx, "drop partition stats meta",
		mlog.Int64("collectionID", info.GetCollectionID()),
		mlog.Int64("partitionID", info.GetPartitionID()),
		mlog.String("vChannel", info.GetVChannel()),
		mlog.Int64("planID", info.GetVersion()))
	return nil
}
