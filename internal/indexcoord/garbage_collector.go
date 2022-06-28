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
	go gc.recycleUnusedMeta()

	gc.wg.Add(1)
	go gc.recycleUnusedIndexFiles()
}

func (gc *garbageCollector) Stop() {
	gc.cancel()
	gc.wg.Wait()
}

func (gc *garbageCollector) recycleUnusedMeta() {
	defer gc.wg.Done()
	log.Info("IndexCoord garbageCollector recycleUnusedMetaLoop start")

	ticker := time.NewTicker(gc.gcMetaDuration)
	defer ticker.Stop()

	for {
		select {
		case <-gc.ctx.Done():
			log.Info("IndexCoord garbageCollector recycleUnusedMetaLoop context has done")
			return
		case <-ticker.C:
			metas := gc.metaTable.GetDeletedMetas()
			for _, meta := range metas {
				log.Info("index meta is deleted, recycle it", zap.Int64("buildID", meta.IndexBuildID),
					zap.Int64("nodeID", meta.NodeID))
				if meta.NodeID != 0 {
					// wait for releasing reference lock
					continue
				}
				if err := gc.metaTable.DeleteIndex(meta.IndexBuildID); err != nil {
					log.Warn("delete index meta from etcd failed, wait to retry", zap.Int64("buildID", meta.IndexBuildID),
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
			indexID2Files := gc.metaTable.GetBuildID2IndexFiles()
			prefix := Params.IndexNodeCfg.IndexStorageRootPath + "/"
			// list dir first
			keys, err := gc.chunkManager.ListWithPrefix(prefix, false)
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
				indexFiles, ok := indexID2Files[buildID]
				if !ok {
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
				// Prevent IndexNode from being recycled as soon as the index file is written
				if !gc.metaTable.CanBeRecycledIndexFiles(buildID) {
					continue
				}
				// buildID still exists in meta, remove unnecessary index files
				filesMap := make(map[string]bool)
				for _, file := range indexFiles {
					filesMap[file] = true
				}
				files, err := gc.chunkManager.ListWithPrefix(key, true)
				if err != nil {
					log.Warn("IndexCoord garbageCollector recycleUnusedIndexFiles list files failed",
						zap.Int64("buildID", buildID), zap.String("prefix", key), zap.Error(err))
					continue
				}
				for _, file := range files {
					if _, ok := filesMap[file]; !ok {
						if err = gc.chunkManager.Remove(file); err != nil {
							log.Warn("IndexCoord garbageCollector recycleUnusedIndexFiles remove file failed",
								zap.Int64("buildID", buildID), zap.String("file", file), zap.Error(err))
							continue
						}
					}
				}
			}
		}
	}
}
