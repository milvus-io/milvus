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

package indexcoord

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/storage"
	"go.uber.org/zap"
)

type garbageCollector struct {
	ctx    context.Context
	cancel context.CancelFunc

	wg             sync.WaitGroup
	gcFileDuration time.Duration
	gcMetaDuration time.Duration

	metaTable    *metaTable
	chunkManager storage.ChunkManager
}

func newGarbageCollector(ctx context.Context, meta *metaTable, chunkManager storage.ChunkManager) *garbageCollector {
	ctx, cancel := context.WithCancel(ctx)
	return &garbageCollector{
		ctx:            ctx,
		cancel:         cancel,
		gcFileDuration: Params.IndexCoordCfg.GCInterval,
		gcMetaDuration: time.Second * 10,
		metaTable:      meta,
		chunkManager:   chunkManager,
	}
}

func (gc *garbageCollector) Start() {
	gc.wg.Add(1)
	go gc.recycleUnusedIndexes()

	gc.wg.Add(1)
	go gc.recycleUnusedSegIndexes()

	gc.wg.Add(1)
	go gc.recycleUnusedIndexFiles()
}

func (gc *garbageCollector) Stop() {
	gc.cancel()
	gc.wg.Wait()
}

func (gc *garbageCollector) recycleUnusedIndexes() {
	defer gc.wg.Done()
	log.Info("IndexCoord garbageCollector recycleUnusedIndexes start")

	ticker := time.NewTicker(gc.gcMetaDuration)
	defer ticker.Stop()
	for {
		select {
		case <-gc.ctx.Done():
			log.Info("IndexCoord garbageCollector recycleUnusedMetaLoop context has done")
			return
		case <-ticker.C:
			deletedIndexes := gc.metaTable.GetDeletedIndexes()
			for _, index := range deletedIndexes {
				buildIDs := gc.metaTable.GetBuildIDsForIndexID(index.IndexID)
				if len(buildIDs) == 0 {
					if err := gc.metaTable.RemoveIndex(index.CollectionID, index.IndexID); err != nil {
						log.Warn("IndexCoord remove index on collection fail", zap.Int64("collID", index.CollectionID),
							zap.Int64("indexID", index.IndexID), zap.Error(err))
						continue
					}
				} else {
					for _, buildID := range buildIDs {
						if err := gc.metaTable.MarkSegIndexAsDeleted(buildID); err != nil {
							log.Warn("IndexCoord mark segment index as deleted fail",
								zap.Int64("buildID", buildID), zap.Error(err))
							continue
						}
					}
				}
			}
		}
	}
}

func (gc *garbageCollector) recycleUnusedSegIndexes() {
	defer gc.wg.Done()
	log.Info("IndexCoord garbageCollector recycleUnusedSegIndexes start")

	ticker := time.NewTicker(gc.gcMetaDuration)
	defer ticker.Stop()

	for {
		select {
		case <-gc.ctx.Done():
			log.Info("IndexCoord garbageCollector recycleUnusedMetaLoop context has done")
			return
		case <-ticker.C:
			segIndexes := gc.metaTable.GetDeletedSegIndexes()
			for _, meta := range segIndexes {
				log.Info("index meta is deleted, recycle it", zap.Int64("buildID", meta.BuildID),
					zap.Int64("nodeID", meta.NodeID))
				if meta.NodeID != 0 {
					// wait for releasing reference lock
					continue
				}
				if err := gc.metaTable.RemoveSegmentIndex(meta.BuildID); err != nil {
					log.Warn("delete index meta from etcd failed, wait to retry", zap.Int64("buildID", meta.BuildID),
						zap.Int64("nodeID", meta.NodeID), zap.Error(err))
					continue
				}
			}
		}
	}
}

// recycleUnusedIndexFiles is used to delete those index files that no longer exist in the meta.
func (gc *garbageCollector) recycleUnusedIndexFiles() {
	defer gc.wg.Done()
	log.Info("IndexCoord garbageCollector start recycleUnusedIndexFiles loop")

	ticker := time.NewTicker(gc.gcFileDuration)
	defer ticker.Stop()

	for {
		select {
		case <-gc.ctx.Done():
			return
		case <-ticker.C:
			prefix := Params.IndexNodeCfg.IndexStorageRootPath + "/"
			// list dir first
			keys, _, err := gc.chunkManager.ListWithPrefix(prefix, false)
			if err != nil {
				log.Error("IndexCoord garbageCollector recycleUnusedIndexFiles list keys from chunk manager failed", zap.Error(err))
				continue
			}
			for _, key := range keys {
				buildID, err := parseBuildIDFromFilePath(key)
				if err != nil {
					log.Error("IndexCoord garbageCollector recycleUnusedIndexFiles parseIndexFileKey", zap.String("key", key), zap.Error(err))
					continue
				}
				log.Info("IndexCoord garbageCollector will recycle index files", zap.Int64("buildID", buildID))
				if !gc.metaTable.HasBuildID(buildID) {
					// buildID no longer exists in meta, remove all index files
					log.Info("IndexCoord garbageCollector recycleUnusedIndexFiles find meta has not exist, remove index files",
						zap.Int64("buildID", buildID))
					err = gc.chunkManager.RemoveWithPrefix(key)
					if err != nil {
						log.Warn("IndexCoord garbageCollector recycleUnusedIndexFiles remove index files failed",
							zap.Int64("buildID", buildID), zap.String("prefix", key), zap.Error(err))
						continue
					}
					continue
				}
				log.Info("index meta can be recycled, recycle index files", zap.Int64("buildID", buildID))
				indexFilePaths := gc.metaTable.GetIndexFilePathByBuildID(buildID)
				if err != nil {
					// Even if the index is marked as deleted, the index file will not be recycled, wait for the next gc,
					// and delete all index files about the buildID at one time.
					log.Warn("IndexCoord garbageCollector get index files fail", zap.Int64("buildID", buildID),
						zap.Error(err))
					continue
				}
				filesMap := make(map[string]bool)
				for _, file := range indexFilePaths {
					filesMap[file] = true
				}
				files, _, err := gc.chunkManager.ListWithPrefix(key, true)
				if err != nil {
					log.Warn("IndexCoord garbageCollector recycleUnusedIndexFiles list files failed",
						zap.Int64("buildID", buildID), zap.String("prefix", key), zap.Error(err))
					continue
				}
				log.Info("recycle index files", zap.Int64("buildID", buildID), zap.Int("meta files num", len(filesMap)),
					zap.Int("chunkManager files num", len(files)))
				deletedFilesNum := 0
				for _, file := range files {
					if _, ok := filesMap[file]; !ok {
						if err = gc.chunkManager.Remove(file); err != nil {
							log.Warn("IndexCoord garbageCollector recycleUnusedIndexFiles remove file failed",
								zap.Int64("buildID", buildID), zap.String("file", file), zap.Error(err))
							continue
						}
						deletedFilesNum++
					}
				}
				log.Info("index files recycle success", zap.Int64("buildID", buildID),
					zap.Int("delete index files num", deletedFilesNum))
			}
		}
	}
}
