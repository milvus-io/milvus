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
	"strconv"
	"strings"
	"time"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func parseClusterStatsSegmentID(root, file string) (int64, error) {
	prefix := path.Join(root, common.ClusterStats) + "/"
	if !strings.HasPrefix(file, prefix) {
		return 0, merr.WrapErrServiceInternalMsg("invalid cluster_stats path")
	}
	parts := strings.Split(strings.TrimPrefix(file, prefix), "/")
	if len(parts) != 5 {
		return 0, merr.WrapErrServiceInternalMsg("invalid cluster_stats path")
	}
	return strconv.ParseInt(parts[2], 10, 64)
}

func (gc *garbageCollector) clusterOutputInFlight(id int64) bool {
	if gc.meta.GetCompactionTaskMeta() == nil {
		return false
	}
	for _, tasks := range gc.meta.GetCompactionTasks(gc.ctx) {
		for _, task := range tasks {
			if task.Type != datapb.CompactionType_ClusteringCompaction && task.Type != datapb.CompactionType_ClusterSortCompaction {
				continue
			}
			if isCompactionTaskFinished(task) {
				continue
			}
			ids := task.GetPreAllocatedSegmentIDs()
			if id >= ids.GetBegin() && id < ids.GetEnd() {
				return true
			}
		}
	}
	return false
}

// Crash leftovers are reclaimed only after the owning child task finishes and
// the ordinary orphan-file grace period expires. No collection data prefix is
// recursively removed here; each enumerated temporary object is checked again.
func (gc *garbageCollector) recycleClusterSortRuns(ctx context.Context) {
	if gc.meta.GetCompactionTaskMeta() == nil {
		return
	}
	prefix := path.Join(gc.option.cli.RootPath(), "cluster_sort_runs") + "/"
	err := gc.option.cli.WalkWithPrefix(ctx, prefix, true, func(info *storage.ChunkObjectInfo) bool {
		if ctx.Err() != nil {
			return false
		}
		if time.Since(info.ModifyTime) <= gc.option.missingTolerance {
			return true
		}
		if !strings.HasPrefix(info.FilePath, prefix) {
			return true
		}
		parts := strings.Split(strings.TrimPrefix(info.FilePath, prefix), "/")
		if len(parts) < 3 {
			return true
		}
		id, e := strconv.ParseInt(parts[0], 10, 64)
		if e != nil {
			return true
		}
		task := gc.meta.GetCompactionTaskMeta().GetCompactionTask(id)
		if task != nil && (!isCompactionTaskFinished(task) || gc.collectionGCPaused(task.CollectionID)) {
			return true
		}
		if e = gc.option.cli.Remove(ctx, info.FilePath); e != nil {
			mlog.Warn(ctx, "remove orphan cluster sort object failed", mlog.String("path", info.FilePath), mlog.Err(e))
		}
		return true
	})
	if err != nil {
		mlog.Warn(ctx, "scan cluster sort temporary objects failed", mlog.Err(err))
	}
}
